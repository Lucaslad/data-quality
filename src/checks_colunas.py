"""
Checks no nível de Colunas/Dados internos de cada partição (Micro).

Inclui:
1. Completude (percentual máximo de nulos por coluna).
2. Unicidade (chaves primárias duplicadas e linhas inteiras duplicadas).
3. Domínio (valores fora da lista de categorias permitidas).
4. Intervalo Numérico (valores negativos, zerados ou outliers fora de [min, max]).
5. Formato / Regex (validação de padrão e rejeição de dígitos repetidos como CPF '00000000000').
"""

from __future__ import annotations

import re
from typing import Any

import pandas as pd

from src.models import ResultadoCheck


def executar_checks_colunas(
    run_id: str,
    dt_referencia: str,
    ts_execucao: str,
    tabela: str,
    chaves_primarias: list[str],
    particoes_dfs: dict[str, pd.DataFrame],
    cfg_colunas: dict[str, Any],
) -> list[ResultadoCheck]:
    """Executa todos os checks de colunas configurados em cada partição."""
    resultados: list[ResultadoCheck] = []
    if not particoes_dfs:
        return resultados

    cfg_comp = cfg_colunas.get("completude", {})
    cfg_unic = cfg_colunas.get("unicidade", {})
    cfg_dom = cfg_colunas.get("dominio", {})
    cfg_int = cfg_colunas.get("intervalo_numerico", {})
    cfg_reg = cfg_colunas.get("formato_regex", {})

    for nome_part, df_part in sorted(particoes_dfs.items()):
        n_total = len(df_part)
        if n_total == 0:
            continue

        # ---------------------------------------------------------------------
        # 1. Completude (Nulos)
        # ---------------------------------------------------------------------
        if cfg_comp.get("ativo", False):
            limites = cfg_comp.get("limites_nulos_pct", {}) or {}
            for col, max_pct in limites.items():
                if col not in df_part.columns:
                    continue
                lim_max = float(max_pct)
                qtd_nulos = int(df_part[col].isna().sum())
                pct_nulos = qtd_nulos / n_total

                if pct_nulos > lim_max:
                    status = "ANOMALIA"
                    detalhe = (
                        f"Coluna `{col}` possui {qtd_nulos}/{n_total} nulos ({pct_nulos:.2%}), "
                        f"acima do limite permitido de {lim_max:.2%}"
                    )
                else:
                    status = "OK"
                    detalhe = (
                        f"Completude OK em `{col}`: {qtd_nulos}/{n_total} nulos "
                        f"({pct_nulos:.2%} <= {lim_max:.2%})"
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
                        valor_esperado=f"<= {lim_max:.4f}",
                        status=status,
                        detalhes=detalhe,
                    )
                )

        # ---------------------------------------------------------------------
        # 2. Unicidade (Chave Primária e Linha Completa)
        # ---------------------------------------------------------------------
        if cfg_unic.get("ativo", False):
            pks_presentes = [pk for pk in chaves_primarias if pk in df_part.columns]
            if cfg_unic.get("verificar_chave_primaria", True) and pks_presentes:
                df_sem_nulo_pk = df_part.dropna(subset=pks_presentes)
                qtd_dup_pk = int(df_sem_nulo_pk.duplicated(subset=pks_presentes).sum())
                pk_label = ",".join(pks_presentes)
                if qtd_dup_pk > 0:
                    status_pk = "ANOMALIA"
                    detalhe_pk = (
                        f"{qtd_dup_pk} registros duplicados na chave primária ({pk_label})"
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
                        valor_observado=qtd_dup_pk,
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
        # 3. Domínio (Categorias Válidas)
        # ---------------------------------------------------------------------
        if cfg_dom.get("ativo", False):
            regras_dom = cfg_dom.get("regras", {}) or {}
            for col, valores_validos in regras_dom.items():
                if col not in df_part.columns:
                    continue
                conjunto_valido = set(valores_validos)
                serie_preenchida = df_part[col].dropna()
                invalidos = serie_preenchida[~serie_preenchida.isin(conjunto_valido)]
                qtd_inv = len(invalidos)

                if qtd_inv > 0:
                    exemplos = invalidos.unique()[:4].tolist()
                    status_d = "ANOMALIA"
                    detalhe_d = (
                        f"{qtd_inv} valores fora do domínio permitido em `{col}`. "
                        f"Valores inválidos encontrados: {exemplos}"
                    )
                else:
                    status_d = "OK"
                    detalhe_d = f"Todos os valores preenchidos de `{col}` pertencem ao domínio válido"

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
                        valor_esperado=f"In {sorted(conjunto_valido)}",
                        status=status_d,
                        detalhes=detalhe_d,
                    )
                )

        # ---------------------------------------------------------------------
        # 4. Intervalo Numérico ([min, max])
        # ---------------------------------------------------------------------
        if cfg_int.get("ativo", False):
            regras_int = cfg_int.get("regras", {}) or {}
            for col, limites_num in regras_int.items():
                if col not in df_part.columns or not isinstance(limites_num, dict):
                    continue
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
                    status_n = "ANOMALIA"
                    detalhe_n = (
                        f"{qtd_fora_num} registros em `{col}` fora do intervalo [{v_min}, {v_max}]. "
                        f"Exemplos: {exemplos_num}"
                    )
                else:
                    status_n = "OK"
                    detalhe_n = (
                        f"Todos os valores de `{col}` estão dentro do intervalo [{v_min}, {v_max}]"
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
                        valor_esperado=f"[{v_min}, {v_max}]",
                        status=status_n,
                        detalhes=detalhe_n,
                    )
                )

        # ---------------------------------------------------------------------
        # 5. Formato / Regex
        # ---------------------------------------------------------------------
        if cfg_reg.get("ativo", False):
            regras_reg = cfg_reg.get("regras", {}) or {}
            for col, params_reg in regras_reg.items():
                if col not in df_part.columns or not isinstance(params_reg, dict):
                    continue
                padrao = str(params_reg.get("padrao", ".*"))
                rejeitar_rep = bool(params_reg.get("rejeitar_digitos_repetidos", False))
                regex_comp = re.compile(padrao)

                serie_str = df_part[col].dropna().astype(str)
                invalidos_reg = []
                for val in serie_str:
                    if not regex_comp.match(val):
                        invalidos_reg.append(val)
                    elif rejeitar_rep and len(set(val)) <= 1:
                        invalidos_reg.append(val)

                qtd_inv_reg = len(invalidos_reg)
                if qtd_inv_reg > 0:
                    exemplos_r = list(dict.fromkeys(invalidos_reg))[:4]
                    status_r = "ANOMALIA"
                    detalhe_r = (
                        f"{qtd_inv_reg} valores com formato inválido em `{col}` "
                        f"(padrão `{padrao}`). Exemplos: {exemplos_r}"
                    )
                else:
                    status_r = "OK"
                    detalhe_r = f"100% dos valores preenchidos em `{col}` seguem o formato `{padrao}`"

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
                        valor_esperado=f"Regex {padrao}",
                        status=status_r,
                        detalhes=detalhe_r,
                    )
                )

    return resultados
