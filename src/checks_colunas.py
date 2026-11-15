"""
Checks no nível de Colunas/Dados internos de cada partição (Micro).

Inclui:
1. Completude (`completude`): validação de percentual de nulos com suporte a duplo estágio (`WARNING`/`ANOMALIA`).
2. Unicidade (`unicidade_chave_primaria` e `unicidade_linha_completa`):
   - Reprova qualquer nulo em coluna de Chave Primária como falha estrutural crítica (Item 1.1).
   - Avalia corretamente Chaves Primárias Compostas tanto em memória quanto em `CACHE_GOLD`/Streaming (Item 1.2).
3. Domínio Categórico (`dominio`) e *Distribution Drift* (`drift_distribuicao` via PSI e JSD, Item 2.3).
4. Intervalo Numérico (`intervalo_numerico`).
5. Formato / Regex Vetorizado (`formato_regex`, Item 1.3).
6. Dígito Verificador Módulo 11 (`digito_verificador` para CPF e CNPJ, Item 2.6).
7. Consistência Relacional Entre Colunas (`consistencia_entre_colunas` / *Cross-Column*, Item 2.6).
"""

from __future__ import annotations

import json
import re
from typing import Any

import pandas as pd

from src.models import ResultadoCheck
from src.stats_utils import (
    calcular_divergencia_js,
    calcular_psi,
    detectar_caracteres_repetidos_serie,
    validar_cnpj_serie,
    validar_cpf_serie,
)


def cache_gold_suporta_regras_atuais(
    perfil_cache: dict[str, Any],
    chaves_primarias: list[str],
    cfg_colunas: dict[str, Any],
) -> bool:
    """
    Verifica se o `perfil_dq_json` armazenado no `CACHE_GOLD` possui todas as métricas
    necessárias para avaliar as regras atuais (incluindo eventuais novos overrides manuais no YAML).
    Caso uma chave composta nova ou um Regex manual arbitrário não conste no perfil em cache,
    retorna `False` para invalidar o cache da partição e forçar a leitura do Parquet.
    """
    try:
        pdq = json.loads(str(perfil_cache.get("perfil_dq_json", "{}")))
    except Exception:
        return False

    colunas_part = set(pdq.get("colunas", []))
    pks_pres = [pk for pk in chaves_primarias if pk in colunas_part]

    # 1. Verifica suporte à chave primária (simples ou composta)
    if len(pks_pres) == 1:
        if pks_pres[0] not in pdq.get("unicidade_cols", {}):
            return False
    elif len(pks_pres) > 1:
        chave_comp = ",".join(pks_pres)
        if chave_comp not in pdq.get("unicidade_composta", {}):
            return False

    # 2. Verifica suporte às regras de Regex (especialmente overrides manuais do YAML)
    cfg_reg = cfg_colunas.get("formato_regex", {}) or {}
    if cfg_reg.get("ativo", True):
        regras_reg = cfg_reg.get("regras", {}) or {}
        reg_cols = pdq.get("regex_str_cols", {})
        for col, params_reg in regras_reg.items():
            if col not in colunas_part or not isinstance(params_reg, dict):
                continue
            padrao = str(params_reg.get("padrao", ".*"))
            info_r = reg_cols.get(col)
            if not info_r:
                return False
            padrao_inf = str(
                info_r.get("padrao_inferido", f"^\\d{{{info_r.get('tam_med', 0)}}}$")
            )
            padroes_custom = info_r.get("padroes_custom", {}) or {}
            # Se o padrão não é o padrão de dígitos inferido, nem um ^\d{N}$ comparável com tam_med,
            # nem está pré-calculado em padroes_custom, invalida o cache para ler o Parquet!
            if (
                padrao != padrao_inf
                and padrao not in padroes_custom
                and not re.match(r"^\^\\d\{\d+\}\$$", padrao)
            ):
                return False

    return True


def _executar_checks_colunas_via_cache(
    run_id: str,
    dt_referencia: str,
    ts_execucao: str,
    tabela: str,
    nome_part: str,
    perfil_cache: dict[str, Any],
    chaves_primarias: list[str],
    cfg_colunas: dict[str, Any],
    sufixo_modo: str = " [Cache GOLD]",
) -> list[ResultadoCheck]:
    """
    Avalia todas as regras de coluna diretamente sobre o perfil estatístico pré-agregado
    de uma partição (`CACHE_GOLD` ou modo *Streaming* com liberação imediata de RAM).
    """
    resultados: list[ResultadoCheck] = []
    n_total = int(perfil_cache.get("qtd_registros", 0))
    if n_total <= 0:
        return resultados

    nulos_map = json.loads(str(perfil_cache.get("nulos_json", "{}")))
    mins_map = json.loads(str(perfil_cache.get("mins_numericos_json", "{}")))
    maxs_map = json.loads(str(perfil_cache.get("maxs_numericos_json", "{}")))
    doms_map = json.loads(str(perfil_cache.get("dominios_json", "{}")))
    pdq = json.loads(str(perfil_cache.get("perfil_dq_json", "{}")))
    colunas_part = set(pdq.get("colunas", []))

    cfg_comp = cfg_colunas.get("completude", {})
    cfg_unic = cfg_colunas.get("unicidade", {})
    cfg_dom = cfg_colunas.get("dominio", {})
    cfg_int = cfg_colunas.get("intervalo_numerico", {})
    cfg_reg = cfg_colunas.get("formato_regex", {})
    cfg_dv = cfg_colunas.get("digito_verificador", {})
    cfg_cross = cfg_colunas.get("consistencia_entre_colunas", {})

    # 1. Completude
    if cfg_comp.get("ativo", True):
        limites = cfg_comp.get("limites_nulos_pct", {}) or {}
        origens_comp = cfg_comp.get("origens", {}) or {}
        tol_warn_comp = float(cfg_comp.get("tolerancia_warning_pct", 0.0))
        for col, max_pct in limites.items():
            if col not in colunas_part:
                continue
            lim_max = float(max_pct)
            origem_c = origens_comp.get(col, "AUTO_APRENDIDO")
            tag_origem = "aprendido" if origem_c == "AUTO_APRENDIDO" else "YAML"
            qtd_nulos = int(nulos_map.get(col, 0))
            pct_nulos = qtd_nulos / n_total

            if pct_nulos > lim_max:
                if (
                    col not in chaves_primarias
                    and tol_warn_comp > 0.0
                    and (pct_nulos - lim_max) <= tol_warn_comp
                ):
                    status = "WARNING"
                else:
                    status = "ANOMALIA"
                detalhe = (
                    f"Coluna `{col}` possui {qtd_nulos}/{n_total} nulos ({pct_nulos:.2%}), "
                    f"acima do limite {tag_origem} de {lim_max:.2%}"
                )
            else:
                status = "OK"
                detalhe = (
                    f"Completude OK em `{col}`: {qtd_nulos}/{n_total} nulos "
                    f"({pct_nulos:.2%} <= limite {tag_origem} {lim_max:.2%}){sufixo_modo}"
                )

            resultados.append(
                ResultadoCheck(
                    run_id=run_id,
                    dt_referencia=dt_referencia,
                    ts_execucao=ts_execucao,
                    tabela=tabela,
                    particao=nome_part,
                    nivel_check="COLUNA",
                    tipo_check="completude",
                    coluna=col,
                    valor_observado=round(pct_nulos, 4),
                    valor_esperado=f"<= {lim_max:.4f} ({tag_origem})",
                    status=status,
                    detalhes=detalhe,
                )
            )

    # 2. Unicidade (Chave Primária Simples/Composta + Verificação Crítica de Nulos em PK)
    if cfg_unic.get("ativo", True):
        pks_presentes = [pk for pk in chaves_primarias if pk in colunas_part]
        unic_cols = pdq.get("unicidade_cols", {})
        unic_comp = pdq.get("unicidade_composta", {})

        if cfg_unic.get("verificar_chave_primaria", True) and pks_presentes:
            pk_label = ",".join(pks_presentes)
            if len(pks_presentes) == 1:
                pk_unica = pks_presentes[0]
                info_u = unic_cols.get(pk_unica, {})
                qtd_dup_pk = int(info_u.get("qtd_dup", 0))
                qtd_nulos_pk = int(
                    info_u.get("n_nulos", nulos_map.get(pk_unica, 0))
                )
            else:
                info_uc = unic_comp.get(pk_label)
                if info_uc is not None:
                    qtd_dup_pk = int(info_uc.get("qtd_dup", 0))
                    qtd_nulos_pk = int(info_uc.get("n_nulos", 0))
                else:
                    qtd_nulos_pk = max(
                        int(nulos_map.get(pk, 0)) for pk in pks_presentes
                    )
                    qtd_dup_pk = 0

            violacoes_pk = qtd_dup_pk + qtd_nulos_pk
            if violacoes_pk > 0:
                status_pk = "ANOMALIA"
                partes_msg_pk = []
                if qtd_dup_pk > 0:
                    partes_msg_pk.append(f"{qtd_dup_pk} registros duplicados")
                if qtd_nulos_pk > 0:
                    partes_msg_pk.append(f"{qtd_nulos_pk} registros com valor NULO")
                detalhe_pk = (
                    f"{' e '.join(partes_msg_pk)} na chave primária ({pk_label})"
                )
            else:
                status_pk = "OK"
                detalhe_pk = (
                    f"Chave primária ({pk_label}) 100% única e sem nulos ({n_total} linhas){sufixo_modo}"
                )

            resultados.append(
                ResultadoCheck(
                    run_id=run_id,
                    dt_referencia=dt_referencia,
                    ts_execucao=ts_execucao,
                    tabela=tabela,
                    particao=nome_part,
                    nivel_check="COLUNA",
                    tipo_check="unicidade_chave_primaria",
                    coluna=pk_label,
                    valor_observado=violacoes_pk,
                    valor_esperado="0 duplicatas e 0 nulos",
                    status=status_pk,
                    detalhes=detalhe_pk,
                )
            )

        if cfg_unic.get("verificar_linha_completa", True):
            qtd_dup_linha = int(pdq.get("qtd_dup_linha", 0))
            if qtd_dup_linha > 0:
                status_ln = "ANOMALIA"
                detalhe_ln = (
                    f"{qtd_dup_linha} linhas inteiramente duplicadas na partição"
                )
            else:
                status_ln = "OK"
                detalhe_ln = f"Nenhuma linha inteiramente duplicada{sufixo_modo}"

            resultados.append(
                ResultadoCheck(
                    run_id=run_id,
                    dt_referencia=dt_referencia,
                    ts_execucao=ts_execucao,
                    tabela=tabela,
                    particao=nome_part,
                    nivel_check="COLUNA",
                    tipo_check="unicidade_linha_completa",
                    coluna="*",
                    valor_observado=qtd_dup_linha,
                    valor_esperado="0 duplicatas",
                    status=status_ln,
                    detalhes=detalhe_ln,
                )
            )

    # 3. Domínio Categórico e Distribution Drift (PSI / JSD)
    if cfg_dom.get("ativo", True):
        regras_dom = cfg_dom.get("regras", {}) or {}
        origens_dom = cfg_dom.get("origens", {}) or {}
        dist_esp_map = cfg_dom.get("distribuicao_esperada", {}) or {}
        verificar_drift = bool(cfg_dom.get("verificar_drift_mix", True))
        psi_warn = float(cfg_dom.get("psi_warning", 0.25))
        psi_anom = float(cfg_dom.get("psi_anomalia", 0.50))
        tol_warn_dom = float(cfg_dom.get("tolerancia_warning_pct", 0.0))

        for col, valores_validos in regras_dom.items():
            if col not in colunas_part:
                continue
            origem_d = origens_dom.get(col, "AUTO_APRENDIDO")
            tag_d = "aprendido" if origem_d == "AUTO_APRENDIDO" else "YAML"
            conjunto_valido = {str(v) for v in valores_validos}
            dom_col = doms_map.get(col, {})
            cats_inv = [k for k in dom_col.keys() if str(k) not in conjunto_valido]
            qtd_inv = sum(int(dom_col[k]) for k in cats_inv)

            if qtd_inv > 0:
                if tol_warn_dom > 0.0 and (qtd_inv / n_total) <= tol_warn_dom:
                    status_d = "WARNING"
                else:
                    status_d = "ANOMALIA"
                detalhe_d = (
                    f"{qtd_inv} valores fora do domínio {tag_d} em `{col}`. "
                    f"Valores inválidos encontrados: {cats_inv[:4]}"
                )
            else:
                status_d = "OK"
                detalhe_d = (
                    f"Todos os valores preenchidos de `{col}` pertencem ao domínio {tag_d}{sufixo_modo}"
                )

            resultados.append(
                ResultadoCheck(
                    run_id=run_id,
                    dt_referencia=dt_referencia,
                    ts_execucao=ts_execucao,
                    tabela=tabela,
                    particao=nome_part,
                    nivel_check="COLUNA",
                    tipo_check="dominio",
                    coluna=col,
                    valor_observado=qtd_inv,
                    valor_esperado=f"In {sorted(conjunto_valido)} ({tag_d})",
                    status=status_d,
                    detalhes=detalhe_d,
                )
            )

            # Check de Distribution Drift (PSI + JSD, Item 2.3) sobre categorias válidas
            if verificar_drift and col in dist_esp_map and dom_col:
                dist_esp_col = dist_esp_map[col]
                dom_valido = {
                    str(k): float(v)
                    for k, v in dom_col.items()
                    if str(k) in conjunto_valido
                }
                tot_valido = sum(dom_valido.values())
                if tot_valido >= 20 and dist_esp_col:
                    psi_val = calcular_psi(dist_esp_col, dom_valido)
                    jsd_val = calcular_divergencia_js(dist_esp_col, dom_valido)
                    if psi_val >= psi_anom:
                        status_dr = "ANOMALIA"
                        detalhe_dr = (
                            f"Mudança severa de mix categórico (Distribution Drift) em `{col}`: "
                            f"PSI={psi_val:.3f} >= {psi_anom:.2f} (JSD={jsd_val:.3f})"
                        )
                    elif psi_val >= psi_warn:
                        status_dr = "WARNING"
                        detalhe_dr = (
                            f"Desvio moderado de mix categórico em `{col}`: "
                            f"PSI={psi_val:.3f} >= {psi_warn:.2f} (JSD={jsd_val:.3f})"
                        )
                    else:
                        status_dr = "OK"
                        detalhe_dr = (
                            f"Mix categórico estável em `{col}`: PSI={psi_val:.3f} < {psi_warn:.2f} "
                            f"(JSD={jsd_val:.3f}){sufixo_modo}"
                        )

                    resultados.append(
                        ResultadoCheck(
                            run_id=run_id,
                            dt_referencia=dt_referencia,
                            ts_execucao=ts_execucao,
                            tabela=tabela,
                            particao=nome_part,
                            nivel_check="COLUNA",
                            tipo_check="drift_distribuicao",
                            coluna=col,
                            valor_observado=round(psi_val, 4),
                            valor_esperado=f"PSI < {psi_warn:.2f} (anom >= {psi_anom:.2f})",
                            status=status_dr,
                            detalhes=detalhe_dr,
                        )
                    )

    # 4. Intervalo Numérico
    if cfg_int.get("ativo", True):
        regras_int = cfg_int.get("regras", {}) or {}
        origens_int = cfg_int.get("origens", {}) or {}
        amostras_num_map = pdq.get("amostras_numericas", {})
        for col, limites_num in regras_int.items():
            if col not in colunas_part or not isinstance(limites_num, dict):
                continue
            origem_n = origens_int.get(col, "AUTO_APRENDIDO")
            tag_n = "aprendido" if origem_n == "AUTO_APRENDIDO" else "YAML"
            v_min = limites_num.get("min")
            v_max = limites_num.get("max")
            if col not in mins_map or col not in maxs_map:
                continue
            min_obs = float(mins_map[col])
            max_obs = float(maxs_map[col])

            fora = False
            if v_min is not None and min_obs < float(v_min):
                fora = True
            if v_max is not None and max_obs > float(v_max):
                fora = True

            if fora:
                status_n = "ANOMALIA"
                amostras_c = amostras_num_map.get(col, [min_obs, max_obs])
                exemplos_fora = [
                    x
                    for x in amostras_c
                    if (v_min is not None and float(x) < float(v_min))
                    or (v_max is not None and float(x) > float(v_max))
                ] or [min_obs, max_obs]
                qtd_fora_num = len(exemplos_fora)
                detalhe_n = (
                    f"Extremos de `{col}` ([{min_obs}, {max_obs}]) fora do intervalo "
                    f"{tag_n} [{v_min}, {v_max}]. Exemplos: {exemplos_fora[:4]}"
                )
            else:
                status_n = "OK"
                qtd_fora_num = 0
                detalhe_n = (
                    f"Todos os valores de `{col}` estão dentro do intervalo {tag_n} [{v_min}, {v_max}]{sufixo_modo}"
                )

            resultados.append(
                ResultadoCheck(
                    run_id=run_id,
                    dt_referencia=dt_referencia,
                    ts_execucao=ts_execucao,
                    tabela=tabela,
                    particao=nome_part,
                    nivel_check="COLUNA",
                    tipo_check="intervalo_numerico",
                    coluna=col,
                    valor_observado=qtd_fora_num,
                    valor_esperado=f"[{v_min}, {v_max}] ({tag_n})",
                    status=status_n,
                    detalhes=detalhe_n,
                )
            )

    # 5. Formato / Regex (Item 1.3)
    if cfg_reg.get("ativo", True):
        regras_reg = cfg_reg.get("regras", {}) or {}
        origens_reg = cfg_reg.get("origens", {}) or {}
        reg_cols = pdq.get("regex_str_cols", {})
        for col, params_reg in regras_reg.items():
            if col not in colunas_part or not isinstance(params_reg, dict):
                continue
            origem_r = origens_reg.get(col, "AUTO_APRENDIDO")
            tag_r = "aprendido" if origem_r == "AUTO_APRENDIDO" else "YAML"
            padrao = str(params_reg.get("padrao", ".*"))
            rejeitar_rep = bool(params_reg.get("rejeitar_digitos_repetidos", False))
            info_r = reg_cols.get(col, {})

            padroes_custom = info_r.get("padroes_custom", {}) or {}
            tam_med = int(info_r.get("tam_med", 0))
            tot_preench = int(info_r.get("total_preenchidos", n_total))
            taxa_dig = float(info_r.get("taxa_dig", 1.0))
            taxa_nao_rep = float(info_r.get("taxa_nao_rep", 1.0))

            exemplos_r: list[str] = []
            if padrao in padroes_custom:
                qtd_inv_reg = int(padroes_custom[padrao].get("qtd_inv", 0))
                exemplos_r = list(padroes_custom[padrao].get("exemplos_inv", []))
                if rejeitar_rep and int(info_r.get("qtd_rep", 0)) > 0:
                    qtd_inv_reg += int(info_r.get("qtd_rep", 0))
                    exemplos_r.extend(info_r.get("exemplos_rep", []))
            else:
                # Verifica se o padrão é `^\d{N}$`
                m_dig_n = re.match(r"^\^\\d\{(\d+)\}\$$", padrao)
                if m_dig_n:
                    n_esp = int(m_dig_n.group(1))
                    if tam_med != n_esp:
                        qtd_inv_reg = tot_preench
                    else:
                        qtd_inv_dig = int(
                            info_r.get(
                                "qtd_inv_dig",
                                round((1.0 - taxa_dig) * tot_preench),
                            )
                        )
                        qtd_rep = (
                            int(
                                info_r.get(
                                    "qtd_rep",
                                    round((1.0 - taxa_nao_rep) * tot_preench),
                                )
                            )
                            if rejeitar_rep
                            else 0
                        )
                        qtd_inv_reg = min(tot_preench, qtd_inv_dig + qtd_rep)
                        exemplos_r = list(info_r.get("exemplos_inv_dig", [])) + (
                            list(info_r.get("exemplos_rep", []))
                            if rejeitar_rep
                            else []
                        )
                else:
                    qtd_inv_reg = (
                        0
                        if (
                            taxa_dig == 1.0
                            and (not rejeitar_rep or taxa_nao_rep == 1.0)
                        )
                        else 1
                    )

            if qtd_inv_reg > 0:
                status_r = "ANOMALIA"
                ex_str = f". Exemplos: {exemplos_r[:4]}" if exemplos_r else ""
                detalhe_r = (
                    f"{qtd_inv_reg} valores com formato inválido em `{col}` "
                    f"(padrão {tag_r} `{padrao}`){ex_str}"
                )
            else:
                status_r = "OK"
                detalhe_r = (
                    f"100% dos valores preenchidos em `{col}` seguem o formato {tag_r} `{padrao}`{sufixo_modo}"
                )

            resultados.append(
                ResultadoCheck(
                    run_id=run_id,
                    dt_referencia=dt_referencia,
                    ts_execucao=ts_execucao,
                    tabela=tabela,
                    particao=nome_part,
                    nivel_check="COLUNA",
                    tipo_check="formato_regex",
                    coluna=col,
                    valor_observado=qtd_inv_reg,
                    valor_esperado=f"Regex {padrao} ({tag_r})",
                    status=status_r,
                    detalhes=detalhe_r,
                )
            )

    # 6. Dígito Verificador Módulo 11 (CPF / CNPJ, Item 2.6)
    if cfg_dv.get("ativo", False):
        regras_dv = cfg_dv.get("regras", {}) or {}
        origens_dv = cfg_dv.get("origens", {}) or {}
        reg_cols = pdq.get("regex_str_cols", {})
        for col, tipo_doc in regras_dv.items():
            if col not in colunas_part:
                continue
            doc_upper = str(tipo_doc).upper()
            origem_v = origens_dv.get(col, "AUTO_APRENDIDO")
            tag_v = "aprendido" if origem_v == "AUTO_APRENDIDO" else "YAML"
            info_r = reg_cols.get(col, {})
            if doc_upper == "CPF":
                qtd_inv_dv = int(info_r.get("qtd_inv_cpf_mod11", 0))
                exemplos_dv = info_r.get("exemplos_inv_cpf_mod11", [])
            elif doc_upper == "CNPJ":
                qtd_inv_dv = int(info_r.get("qtd_inv_cnpj_mod11", 0))
                exemplos_dv = info_r.get("exemplos_inv_cnpj_mod11", [])
            else:
                continue

            if qtd_inv_dv > 0:
                status_dv = "ANOMALIA"
                detalhe_dv = (
                    f"{qtd_inv_dv} documento(s) em `{col}` com dígito verificador {doc_upper} "
                    f"(Módulo 11) inválido. Exemplos: {exemplos_dv[:4]}"
                )
            else:
                status_dv = "OK"
                detalhe_dv = (
                    f"100% dos documentos com máscara válida em `{col}` possuem dígito verificador "
                    f"{doc_upper} (Módulo 11) íntegro ({tag_v}){sufixo_modo}"
                )

            resultados.append(
                ResultadoCheck(
                    run_id=run_id,
                    dt_referencia=dt_referencia,
                    ts_execucao=ts_execucao,
                    tabela=tabela,
                    particao=nome_part,
                    nivel_check="COLUNA",
                    tipo_check="digito_verificador",
                    coluna=col,
                    valor_observado=qtd_inv_dv,
                    valor_esperado=f"Módulo 11 {doc_upper} válido ({tag_v})",
                    status=status_dv,
                    detalhes=detalhe_dv,
                )
            )

    # 7. Consistência Entre Colunas / Cross-Column (Item 2.6)
    if cfg_cross.get("ativo", False):
        regras_cross = cfg_cross.get("regras", []) or []
        rels_part = pdq.get("relacoes_colunas", {})
        for regra_c in regras_cross:
            c_maior = str(regra_c.get("col_maior", ""))
            c_menor = str(regra_c.get("col_menor", ""))
            op = str(regra_c.get("operador", ">="))
            if c_maior not in colunas_part or c_menor not in colunas_part:
                continue
            chave_rel = f"{c_maior}{op}{c_menor}"
            info_rel = rels_part.get(chave_rel)
            if info_rel is None:
                continue
            tot_pares = int(info_rel.get("total_pares", 0))
            if tot_pares == 0:
                continue
            qtd_viol = int(info_rel.get("qtd_violacoes", 0))
            exemplos_v = info_rel.get("exemplos_violacoes", [])

            if qtd_viol > 0:
                status_cr = "ANOMALIA"
                detalhe_cr = (
                    f"{qtd_viol}/{tot_pares} registros violam a regra relacional "
                    f"`{c_maior} {op} {c_menor}`. Exemplos: {exemplos_v[:3]}"
                )
            else:
                status_cr = "OK"
                detalhe_cr = (
                    f"100% dos {tot_pares} pares preenchidos respeitam `{c_maior} {op} {c_menor}`{sufixo_modo}"
                )

            resultados.append(
                ResultadoCheck(
                    run_id=run_id,
                    dt_referencia=dt_referencia,
                    ts_execucao=ts_execucao,
                    tabela=tabela,
                    particao=nome_part,
                    nivel_check="COLUNA",
                    tipo_check="consistencia_entre_colunas",
                    coluna=chave_rel,
                    valor_observado=qtd_viol,
                    valor_esperado=f"0 violações ({c_maior} {op} {c_menor})",
                    status=status_cr,
                    detalhes=detalhe_cr,
                )
            )

    return resultados


def executar_checks_colunas(
    run_id: str,
    dt_referencia: str,
    ts_execucao: str,
    tabela: str,
    chaves_primarias: list[str],
    particoes_dfs: dict[str, pd.DataFrame],
    cfg_colunas: dict[str, Any],
    perfis_cache_gold: dict[str, dict[str, Any]] | None = None,
    perfis_particoes: dict[str, dict[str, Any]] | None = None,
) -> list[ResultadoCheck]:
    """
    Executa todos os checks de colunas configurados/aprendidos em cada partição.
    Suporta avaliação em memória (`particoes_dfs`), via Cache GOLD (`perfis_cache_gold`)
    e via Streaming de perfis (`perfis_particoes` quando os DataFrames já foram liberados da RAM).
    """
    resultados: list[ResultadoCheck] = []
    cache_map = perfis_cache_gold or {}
    perfis_todos_map = perfis_particoes or {}
    todas_particoes = sorted(
        set(particoes_dfs.keys())
        | set(cache_map.keys())
        | set(perfis_todos_map.keys())
    )
    if not todas_particoes:
        return resultados

    cfg_comp = cfg_colunas.get("completude", {})
    cfg_unic = cfg_colunas.get("unicidade", {})
    cfg_dom = cfg_colunas.get("dominio", {})
    cfg_int = cfg_colunas.get("intervalo_numerico", {})
    cfg_reg = cfg_colunas.get("formato_regex", {})
    cfg_dv = cfg_colunas.get("digito_verificador", {})
    cfg_cross = cfg_colunas.get("consistencia_entre_colunas", {})

    for nome_part in todas_particoes:
        if nome_part not in particoes_dfs:
            if nome_part in cache_map:
                resultados.extend(
                    _executar_checks_colunas_via_cache(
                        run_id=run_id,
                        dt_referencia=dt_referencia,
                        ts_execucao=ts_execucao,
                        tabela=tabela,
                        nome_part=nome_part,
                        perfil_cache=cache_map[nome_part],
                        chaves_primarias=chaves_primarias,
                        cfg_colunas=cfg_colunas,
                        sufixo_modo=" [Cache GOLD]",
                    )
                )
            elif nome_part in perfis_todos_map:
                resultados.extend(
                    _executar_checks_colunas_via_cache(
                        run_id=run_id,
                        dt_referencia=dt_referencia,
                        ts_execucao=ts_execucao,
                        tabela=tabela,
                        nome_part=nome_part,
                        perfil_cache=perfis_todos_map[nome_part],
                        chaves_primarias=chaves_primarias,
                        cfg_colunas=cfg_colunas,
                        sufixo_modo="",
                    )
                )
            continue

        df_part = particoes_dfs[nome_part]
        n_total = len(df_part)
        if n_total == 0:
            continue

        # ---------------------------------------------------------------------
        # 1. Completude (Nulos)
        # ---------------------------------------------------------------------
        if cfg_comp.get("ativo", True):
            limites = cfg_comp.get("limites_nulos_pct", {}) or {}
            origens_comp = cfg_comp.get("origens", {}) or {}
            tol_warn_comp = float(cfg_comp.get("tolerancia_warning_pct", 0.0))
            for col, max_pct in limites.items():
                if col not in df_part.columns:
                    continue
                lim_max = float(max_pct)
                origem_c = origens_comp.get(col, "AUTO_APRENDIDO")
                tag_origem = "aprendido" if origem_c == "AUTO_APRENDIDO" else "YAML"
                qtd_nulos = int(df_part[col].isna().sum())
                pct_nulos = qtd_nulos / n_total

                if pct_nulos > lim_max:
                    if (
                        col not in chaves_primarias
                        and tol_warn_comp > 0.0
                        and (pct_nulos - lim_max) <= tol_warn_comp
                    ):
                        status = "WARNING"
                    else:
                        status = "ANOMALIA"
                    detalhe = (
                        f"Coluna `{col}` possui {qtd_nulos}/{n_total} nulos ({pct_nulos:.2%}), "
                        f"acima do limite {tag_origem} de {lim_max:.2%}"
                    )
                else:
                    status = "OK"
                    detalhe = (
                        f"Completude OK em `{col}`: {qtd_nulos}/{n_total} nulos "
                        f"({pct_nulos:.2%} <= limite {tag_origem} {lim_max:.2%})"
                    )

                resultados.append(
                    ResultadoCheck(
                        run_id=run_id,
                        dt_referencia=dt_referencia,
                        ts_execucao=ts_execucao,
                        tabela=tabela,
                        particao=nome_part,
                        nivel_check="COLUNA",
                        tipo_check="completude",
                        coluna=col,
                        valor_observado=round(pct_nulos, 4),
                        valor_esperado=f"<= {lim_max:.4f} ({tag_origem})",
                        status=status,
                        detalhes=detalhe,
                    )
                )

        # ---------------------------------------------------------------------
        # 2. Unicidade (Chave Primária e Linha Completa — Reprova Nulos em PK, Item 1.1)
        # ---------------------------------------------------------------------
        if cfg_unic.get("ativo", True):
            pks_presentes = [pk for pk in chaves_primarias if pk in df_part.columns]
            if cfg_unic.get("verificar_chave_primaria", True) and pks_presentes:
                qtd_nulos_pk = int(df_part[pks_presentes].isna().any(axis=1).sum())
                df_sem_nulo_pk = df_part.dropna(subset=pks_presentes)
                qtd_dup_pk = int(df_sem_nulo_pk.duplicated(subset=pks_presentes).sum())
                pk_label = ",".join(pks_presentes)
                violacoes_pk = qtd_dup_pk + qtd_nulos_pk

                if violacoes_pk > 0:
                    status_pk = "ANOMALIA"
                    partes_msg_pk = []
                    if qtd_dup_pk > 0:
                        partes_msg_pk.append(f"{qtd_dup_pk} registros duplicados")
                    if qtd_nulos_pk > 0:
                        partes_msg_pk.append(f"{qtd_nulos_pk} registros com valor NULO")
                    detalhe_pk = (
                        f"{' e '.join(partes_msg_pk)} na chave primária ({pk_label})"
                    )
                else:
                    status_pk = "OK"
                    detalhe_pk = f"Chave primária ({pk_label}) 100% única ({n_total} linhas)"

                resultados.append(
                    ResultadoCheck(
                        run_id=run_id,
                        dt_referencia=dt_referencia,
                        ts_execucao=ts_execucao,
                        tabela=tabela,
                        particao=nome_part,
                        nivel_check="COLUNA",
                        tipo_check="unicidade_chave_primaria",
                        coluna=pk_label,
                        valor_observado=violacoes_pk,
                        valor_esperado="0 duplicatas",
                        status=status_pk,
                        detalhes=detalhe_pk,
                    )
                )

            if cfg_unic.get("verificar_linha_completa", True):
                qtd_dup_linha = int(df_part.duplicated().sum())
                if qtd_dup_linha > 0:
                    status_ln = "ANOMALIA"
                    detalhe_ln = (
                        f"{qtd_dup_linha} linhas inteiramente duplicadas na partição"
                    )
                else:
                    status_ln = "OK"
                    detalhe_ln = "Nenhuma linha inteiramente duplicada"

                resultados.append(
                    ResultadoCheck(
                        run_id=run_id,
                        dt_referencia=dt_referencia,
                        ts_execucao=ts_execucao,
                        tabela=tabela,
                        particao=nome_part,
                        nivel_check="COLUNA",
                        tipo_check="unicidade_linha_completa",
                        coluna="*",
                        valor_observado=qtd_dup_linha,
                        valor_esperado="0 duplicatas",
                        status=status_ln,
                        detalhes=detalhe_ln,
                    )
                )

        # ---------------------------------------------------------------------
        # 3. Domínio e Distribution Drift (PSI / JSD, Item 2.3)
        # ---------------------------------------------------------------------
        if cfg_dom.get("ativo", True):
            regras_dom = cfg_dom.get("regras", {}) or {}
            origens_dom = cfg_dom.get("origens", {}) or {}
            dist_esp_map = cfg_dom.get("distribuicao_esperada", {}) or {}
            verificar_drift = bool(cfg_dom.get("verificar_drift_mix", True))
            psi_warn = float(cfg_dom.get("psi_warning", 0.25))
            psi_anom = float(cfg_dom.get("psi_anomalia", 0.50))
            tol_warn_dom = float(cfg_dom.get("tolerancia_warning_pct", 0.0))

            for col, valores_validos in regras_dom.items():
                if col not in df_part.columns:
                    continue
                origem_d = origens_dom.get(col, "AUTO_APRENDIDO")
                tag_d = "aprendido" if origem_d == "AUTO_APRENDIDO" else "YAML"
                conjunto_valido = set(valores_validos)
                serie_preenchida = df_part[col].dropna()
                invalidos = serie_preenchida[~serie_preenchida.isin(conjunto_valido)]
                qtd_inv = len(invalidos)

                if qtd_inv > 0:
                    exemplos = invalidos.unique()[:4].tolist()
                    if tol_warn_dom > 0.0 and (qtd_inv / n_total) <= tol_warn_dom:
                        status_d = "WARNING"
                    else:
                        status_d = "ANOMALIA"
                    detalhe_d = (
                        f"{qtd_inv} valores fora do domínio {tag_d} em `{col}`. "
                        f"Valores inválidos encontrados: {exemplos}"
                    )
                else:
                    status_d = "OK"
                    detalhe_d = f"Todos os valores preenchidos de `{col}` pertencem ao domínio {tag_d}"

                resultados.append(
                    ResultadoCheck(
                        run_id=run_id,
                        dt_referencia=dt_referencia,
                        ts_execucao=ts_execucao,
                        tabela=tabela,
                        particao=nome_part,
                        nivel_check="COLUNA",
                        tipo_check="dominio",
                        coluna=col,
                        valor_observado=qtd_inv,
                        valor_esperado=f"In {sorted(conjunto_valido)} ({tag_d})",
                        status=status_d,
                        detalhes=detalhe_d,
                    )
                )

                # Distribution Drift (PSI + JSD) sobre os registros válidos da coluna
                if verificar_drift and col in dist_esp_map:
                    dist_esp_col = dist_esp_map[col]
                    serie_valida_dom = serie_preenchida[
                        serie_preenchida.isin(conjunto_valido)
                    ]
                    if len(serie_valida_dom) >= 20 and dist_esp_col:
                        vc_obs = serie_valida_dom.astype(str).value_counts()
                        dist_obs_col = {str(k): float(v) for k, v in vc_obs.items()}
                        psi_val = calcular_psi(dist_esp_col, dist_obs_col)
                        jsd_val = calcular_divergencia_js(dist_esp_col, dist_obs_col)

                        if psi_val >= psi_anom:
                            status_dr = "ANOMALIA"
                            detalhe_dr = (
                                f"Mudança severa de mix categórico (Distribution Drift) em `{col}`: "
                                f"PSI={psi_val:.3f} >= {psi_anom:.2f} (JSD={jsd_val:.3f})"
                            )
                        elif psi_val >= psi_warn:
                            status_dr = "WARNING"
                            detalhe_dr = (
                                f"Desvio moderado de mix categórico em `{col}`: "
                                f"PSI={psi_val:.3f} >= {psi_warn:.2f} (JSD={jsd_val:.3f})"
                            )
                        else:
                            status_dr = "OK"
                            detalhe_dr = (
                                f"Mix categórico estável em `{col}`: PSI={psi_val:.3f} < {psi_warn:.2f} "
                                f"(JSD={jsd_val:.3f})"
                            )

                        resultados.append(
                            ResultadoCheck(
                                run_id=run_id,
                                dt_referencia=dt_referencia,
                                ts_execucao=ts_execucao,
                                tabela=tabela,
                                particao=nome_part,
                                nivel_check="COLUNA",
                                tipo_check="drift_distribuicao",
                                coluna=col,
                                valor_observado=round(psi_val, 4),
                                valor_esperado=f"PSI < {psi_warn:.2f} (anom >= {psi_anom:.2f})",
                                status=status_dr,
                                detalhes=detalhe_dr,
                            )
                        )

        # ---------------------------------------------------------------------
        # 4. Intervalo Numérico ([min, max] Aprendido / Configurado)
        # ---------------------------------------------------------------------
        if cfg_int.get("ativo", True):
            regras_int = cfg_int.get("regras", {}) or {}
            origens_int = cfg_int.get("origens", {}) or {}
            tol_warn_int = float(cfg_int.get("tolerancia_warning_pct", 0.0))
            for col, limites_num in regras_int.items():
                if col not in df_part.columns or not isinstance(limites_num, dict):
                    continue
                origem_n = origens_int.get(col, "AUTO_APRENDIDO")
                tag_n = "aprendido" if origem_n == "AUTO_APRENDIDO" else "YAML"
                v_min = limites_num.get("min")
                v_max = limites_num.get("max")

                serie_num = pd.to_numeric(df_part[col], errors="coerce").dropna()
                mask_fora = pd.Series(False, index=serie_num.index)
                if v_min is not None:
                    mask_fora = mask_fora | (serie_num < float(v_min))
                if v_max is not None:
                    mask_fora = mask_fora | (serie_num > float(v_max))

                fora_intervalo = serie_num[mask_fora]
                qtd_fora_num = len(fora_intervalo)

                if qtd_fora_num > 0:
                    exemplos_num = fora_intervalo.unique()[:4].tolist()
                    if tol_warn_int > 0.0 and (qtd_fora_num / n_total) <= tol_warn_int:
                        status_n = "WARNING"
                    else:
                        status_n = "ANOMALIA"
                    detalhe_n = (
                        f"{qtd_fora_num} registros em `{col}` fora do intervalo {tag_n} [{v_min}, {v_max}]. "
                        f"Exemplos: {exemplos_num}"
                    )
                else:
                    status_n = "OK"
                    detalhe_n = (
                        f"Todos os valores de `{col}` estão dentro do intervalo {tag_n} [{v_min}, {v_max}]"
                    )

                resultados.append(
                    ResultadoCheck(
                        run_id=run_id,
                        dt_referencia=dt_referencia,
                        ts_execucao=ts_execucao,
                        tabela=tabela,
                        particao=nome_part,
                        nivel_check="COLUNA",
                        tipo_check="intervalo_numerico",
                        coluna=col,
                        valor_observado=qtd_fora_num,
                        valor_esperado=f"[{v_min}, {v_max}] ({tag_n})",
                        status=status_n,
                        detalhes=detalhe_n,
                    )
                )

        # ---------------------------------------------------------------------
        # 5. Formato / Regex Vetorizado em Pandas (Item 1.3)
        # ---------------------------------------------------------------------
        if cfg_reg.get("ativo", True):
            regras_reg = cfg_reg.get("regras", {}) or {}
            origens_reg = cfg_reg.get("origens", {}) or {}
            tol_warn_reg = float(cfg_reg.get("tolerancia_warning_pct", 0.0))
            for col, params_reg in regras_reg.items():
                if col not in df_part.columns or not isinstance(params_reg, dict):
                    continue
                origem_r = origens_reg.get(col, "AUTO_APRENDIDO")
                tag_r = "aprendido" if origem_r == "AUTO_APRENDIDO" else "YAML"
                padrao = str(params_reg.get("padrao", ".*"))
                rejeitar_rep = bool(
                    params_reg.get("rejeitar_digitos_repetidos", False)
                )

                serie_str = df_part[col].dropna().astype(str)
                if len(serie_str) > 0:
                    mask_match = serie_str.str.match(padrao, na=False)
                    if rejeitar_rep:
                        mask_rep = detectar_caracteres_repetidos_serie(serie_str)
                        mask_valido = mask_match & (~mask_rep)
                    else:
                        mask_valido = mask_match
                    invalidos_reg_s = serie_str[~mask_valido]
                    qtd_inv_reg = int(len(invalidos_reg_s))
                    exemplos_r = invalidos_reg_s.unique()[:4].tolist()
                else:
                    qtd_inv_reg = 0
                    exemplos_r = []

                if qtd_inv_reg > 0:
                    if tol_warn_reg > 0.0 and (qtd_inv_reg / n_total) <= tol_warn_reg:
                        status_r = "WARNING"
                    else:
                        status_r = "ANOMALIA"
                    detalhe_r = (
                        f"{qtd_inv_reg} valores com formato inválido em `{col}` "
                        f"(padrão {tag_r} `{padrao}`). Exemplos: {exemplos_r}"
                    )
                else:
                    status_r = "OK"
                    detalhe_r = (
                        f"100% dos valores preenchidos em `{col}` seguem o formato {tag_r} `{padrao}`"
                    )

                resultados.append(
                    ResultadoCheck(
                        run_id=run_id,
                        dt_referencia=dt_referencia,
                        ts_execucao=ts_execucao,
                        tabela=tabela,
                        particao=nome_part,
                        nivel_check="COLUNA",
                        tipo_check="formato_regex",
                        coluna=col,
                        valor_observado=qtd_inv_reg,
                        valor_esperado=f"Regex {padrao} ({tag_r})",
                        status=status_r,
                        detalhes=detalhe_r,
                    )
                )

        # ---------------------------------------------------------------------
        # 6. Dígito Verificador Módulo 11 (CPF / CNPJ, Item 2.6)
        # ---------------------------------------------------------------------
        if cfg_dv.get("ativo", False):
            regras_dv = cfg_dv.get("regras", {}) or {}
            origens_dv = cfg_dv.get("origens", {}) or {}
            for col, tipo_doc in regras_dv.items():
                if col not in df_part.columns:
                    continue
                doc_upper = str(tipo_doc).upper()
                origem_v = origens_dv.get(col, "AUTO_APRENDIDO")
                tag_v = "aprendido" if origem_v == "AUTO_APRENDIDO" else "YAML"
                s_val = df_part[col].dropna().astype(str).str.strip()
                if s_val.empty:
                    continue

                if doc_upper == "CPF":
                    # Avalia Módulo 11 apenas sobre os documentos que já passaram pela máscara de 11 dígitos não repetidos
                    mask_cand = s_val.str.match(r"^\d{11}$", na=False) & (
                        ~detectar_caracteres_repetidos_serie(s_val)
                    )
                    s_cand = s_val[mask_cand]
                    mask_ok = validar_cpf_serie(s_cand)
                elif doc_upper == "CNPJ":
                    mask_cand = s_val.str.match(r"^\d{14}$", na=False) & (
                        ~detectar_caracteres_repetidos_serie(s_val)
                    )
                    s_cand = s_val[mask_cand]
                    mask_ok = validar_cnpj_serie(s_cand)
                else:
                    continue

                inv_dv = s_cand[~mask_ok]
                qtd_inv_dv = int(len(inv_dv))
                if qtd_inv_dv > 0:
                    exemplos_dv = inv_dv.unique()[:4].tolist()
                    status_dv = "ANOMALIA"
                    detalhe_dv = (
                        f"{qtd_inv_dv} documento(s) em `{col}` com dígito verificador {doc_upper} "
                        f"(Módulo 11) inválido. Exemplos: {exemplos_dv}"
                    )
                else:
                    status_dv = "OK"
                    detalhe_dv = (
                        f"100% dos documentos com máscara válida em `{col}` possuem dígito verificador "
                        f"{doc_upper} (Módulo 11) íntegro ({tag_v})"
                    )

                resultados.append(
                    ResultadoCheck(
                        run_id=run_id,
                        dt_referencia=dt_referencia,
                        ts_execucao=ts_execucao,
                        tabela=tabela,
                        particao=nome_part,
                        nivel_check="COLUNA",
                        tipo_check="digito_verificador",
                        coluna=col,
                        valor_observado=qtd_inv_dv,
                        valor_esperado=f"Módulo 11 {doc_upper} válido ({tag_v})",
                        status=status_dv,
                        detalhes=detalhe_dv,
                    )
                )

        # ---------------------------------------------------------------------
        # 7. Consistência Entre Colunas / Cross-Column (Item 2.6)
        # ---------------------------------------------------------------------
        if cfg_cross.get("ativo", False):
            regras_cross = cfg_cross.get("regras", []) or []
            for regra_c in regras_cross:
                c_maior = str(regra_c.get("col_maior", ""))
                c_menor = str(regra_c.get("col_menor", ""))
                op = str(regra_c.get("operador", ">="))
                if c_maior not in df_part.columns or c_menor not in df_part.columns:
                    continue

                sa_raw = df_part[c_maior]
                sb_raw = df_part[c_menor]
                if pd.api.types.is_numeric_dtype(
                    sa_raw
                ) and pd.api.types.is_numeric_dtype(sb_raw):
                    sa = pd.to_numeric(sa_raw, errors="coerce")
                    sb = pd.to_numeric(sb_raw, errors="coerce")
                else:
                    sa = pd.to_datetime(sa_raw, errors="coerce", format="ISO8601")
                    sb = pd.to_datetime(sb_raw, errors="coerce", format="ISO8601")

                mask_ambos = sa.notna() & sb.notna()
                tot_pares = int(mask_ambos.sum())
                if tot_pares == 0:
                    continue
                sa_v = sa[mask_ambos]
                sb_v = sb[mask_ambos]

                if op == ">=":
                    mask_viol = sa_v < sb_v
                elif op == ">":
                    mask_viol = sa_v <= sb_v
                elif op == "==":
                    mask_viol = sa_v != sb_v
                else:
                    mask_viol = sa_v < sb_v

                qtd_viol = int(mask_viol.sum())
                chave_rel = f"{c_maior}{op}{c_menor}"
                if qtd_viol > 0:
                    idx_viol = sa_v[mask_viol].index[:3]
                    exemplos_v = [
                        f"{c_maior}={df_part.loc[i, c_maior]} vs {c_menor}={df_part.loc[i, c_menor]}"
                        for i in idx_viol
                    ]
                    status_cr = "ANOMALIA"
                    detalhe_cr = (
                        f"{qtd_viol}/{tot_pares} registros violam a regra relacional "
                        f"`{c_maior} {op} {c_menor}`. Exemplos: {exemplos_v}"
                    )
                else:
                    status_cr = "OK"
                    detalhe_cr = (
                        f"100% dos {tot_pares} pares preenchidos respeitam `{c_maior} {op} {c_menor}`"
                    )

                resultados.append(
                    ResultadoCheck(
                        run_id=run_id,
                        dt_referencia=dt_referencia,
                        ts_execucao=ts_execucao,
                        tabela=tabela,
                        particao=nome_part,
                        nivel_check="COLUNA",
                        tipo_check="consistencia_entre_colunas",
                        coluna=chave_rel,
                        valor_observado=qtd_viol,
                        valor_esperado=f"0 violações ({c_maior} {op} {c_menor})",
                        status=status_cr,
                        detalhes=detalhe_cr,
                    )
                )

    return resultados
