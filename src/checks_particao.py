"""
Checks no nível de Partição (Macro), Contrato de Schema, Freshness/Lacunas,
Evolução entre Fotografias (Snapshots) e Detecção de Reprocessamento Histórico.

Inclui:
1. Consistência de Schema / Contrato (`consistencia_schema`): detecta colunas obrigatórias ausentes
   (`ANOMALIA` -> `BRONZE`), mudança incompatível de tipo primitivo (`ANOMALIA` -> `BRONZE`) e
   novas colunas não mapeadas (`WARNING` -> `SILVER`).
2. Freshness / SLA de Chegada e Lacunas de Partição (`freshness_particao`): valida se a partição
   esperada para `dt_referencia` chegou dentro do SLA e se há buracos na sequência de partições.
3. Volumetria Histórica da Partição (detecção robusta via Mediana + MAD, janela móvel, sazonalidade
   semanal Dia Útil vs. Fim de Semana e limiares de duplo estágio `WARNING` / `ANOMALIA`).
4. Evolução entre Fotografias e Auditoria de Reprocessamento Histórico.
5. Consistência de Data vs. Partição (descobre quais colunas de data pertencem à safra da partição).
"""

from __future__ import annotations

import datetime as dt
import json
from typing import Any

import pandas as pd

from src.models import ResultadoCheck
from src.snapshot_manager import SnapshotManager
from src.stats_utils import calcular_estatisticas_robustas_volume


def _calcular_estatisticas_robustas(valores: list[int]) -> tuple[float, float]:
    """Retorna (mediana, sigma_robusto_mad) da série de volumes de partição."""
    return calcular_estatisticas_robustas_volume(valores)


def _normalizar_tipo_schema(tipo: str) -> str:
    """Normaliza nomes de tipos do YAML ou pandas para as famílias semânticas do motor."""
    t = str(tipo).lower().strip()
    if t in ("numerico_int", "int", "int64", "int32", "integer", "bigint"):
        return "numerico_int"
    if t in ("numerico_float", "float", "float64", "float32", "double", "decimal", "numeric"):
        return "numerico_float"
    if t in ("string_cat", "str", "string", "object", "varchar", "char", "text", "category"):
        return "string_cat"
    if t in ("data_ts", "date", "datetime", "datetime_dtype", "timestamp", "datetime64[ns]"):
        return "data_ts"
    return t


def _tipos_schema_incompativeis(
    tipo_esperado_raw: str,
    tipo_atual_raw: str,
    tem_nulos: bool = False,
     tem_preenchidos: bool = True,
) -> bool:
    """
    Avalia se o tipo observado na partição é incompatível com o tipo esperado no contrato de schema.
    Considera compatível quando uma coluna inteira sofre upcast para float64 no Pandas devido à presença de NaN.
    """
    if not tem_preenchidos:
        return False
    t_esp = _normalizar_tipo_schema(tipo_esperado_raw)
    t_atu = _normalizar_tipo_schema(tipo_atual_raw)
    if t_esp == t_atu:
        return False
    if t_esp == "numerico_int" and t_atu == "numerico_float" and tem_nulos:
        return False
    return True


def _calcular_atraso_e_lacunas_particoes(
    nomes_particoes: list[str],
    tipo_particao: str,
    dt_referencia: str,
) -> tuple[int, str, list[str]]:
    """
    Calcula:
    - `atraso_periodos`: distância (em dias ou meses) entre a última partição disponível e `dt_referencia`.
    - `particao_esperada_ref`: nome da partição esperada para `dt_referencia`.
    - `lacunas`: lista de partições ausentes entre a primeira e a última partição existente.
    """
    dt_ref = dt.date.fromisoformat(dt_referencia)
    prefixo = nomes_particoes[0].split("=")[0] + "=" if "=" in nomes_particoes[0] else ""
    valores_existentes = [p.split("=")[-1] for p in nomes_particoes]
    conjunto_existentes = set(valores_existentes)

    lacunas: list[str] = []

    if tipo_particao == "diaria":
        part_esp = f"{prefixo}{dt_ref.strftime('%Y%m%d')}"
        datas_validas: list[dt.date] = []
        for v in valores_existentes:
            if len(v) == 8 and v.isdigit():
                try:
                    datas_validas.append(dt.date(int(v[:4]), int(v[4:6]), int(v[6:8])))
                except ValueError:
                    pass
        if not datas_validas:
            return 0, part_esp, []

        datas_validas.sort()
        dt_ini = datas_validas[0]
        dt_ult = datas_validas[-1]
        atraso = max(0, (dt_ref - dt_ult).days)

        cursor = dt_ini
        while cursor <= dt_ult:
            s_cur = cursor.strftime("%Y%m%d")
            if s_cur not in conjunto_existentes:
                lacunas.append(f"{prefixo}{s_cur}")
            cursor += dt.timedelta(days=1)

        return atraso, part_esp, lacunas

    # tipo_particao == "mensal"
    part_esp = f"{prefixo}{dt_ref.strftime('%Y%m')}"
    meses_validos: list[tuple[int, int]] = []
    for v in valores_existentes:
        if len(v) == 6 and v.isdigit():
            ano_v, mes_v = int(v[:4]), int(v[4:6])
            if 1 <= mes_v <= 12:
                meses_validos.append((ano_v, mes_v))
    if not meses_validos:
        return 0, part_esp, []

    meses_validos.sort()
    ano_ini, mes_ini = meses_validos[0]
    ano_ult, mes_ult = meses_validos[-1]
    atraso = max(0, (dt_ref.year - ano_ult) * 12 + (dt_ref.month - mes_ult))

    a_cur, m_cur = ano_ini, mes_ini
    while (a_cur, m_cur) <= (ano_ult, mes_ult):
        s_cur = f"{a_cur:04d}{m_cur:02d}"
        if s_cur not in conjunto_existentes:
            lacunas.append(f"{prefixo}{s_cur}")
        if m_cur == 12:
            a_cur += 1
            m_cur = 1
        else:
            m_cur += 1

    return atraso, part_esp, lacunas


def _selecionar_baseline_snapshot_particao(
    df_fotos_tab: pd.DataFrame,
    nome_part: str,
    dt_referencia: str,
    fp_atu: str,
    hash_atu: str,
) -> tuple[dict[str, Any] | None, bool]:
    """
    Seleciona a fotografia de referência (`row_ant`) para auditar a partição `nome_part`
    e retorna `(row_ant_dict, eh_baseline_gold)`.
    Prioriza o último snapshot `GOLD` conhecido da partição para impedir que reprocessamentos
    em quarentena (`BRONZE`) apaguem a referência histórica saudável.
    """
    if df_fotos_tab.empty:
        return None, False

    df_part_hist = df_fotos_tab[df_fotos_tab["particao"] == nome_part].sort_values(
        "dt_referencia"
    )
    if df_part_hist.empty:
        return None, False

    df_ant = df_part_hist[df_part_hist["dt_referencia"] < dt_referencia]
    df_mesmo_dia = df_part_hist[df_part_hist["dt_referencia"] == dt_referencia]

    # 1. Procura o último snapshot GOLD em datas anteriores (D-1, D-2...)
    if not df_ant.empty:
        if "camada_qualidade" in df_ant.columns:
            df_ant_gold = df_ant[df_ant["camada_qualidade"] == "GOLD"]
            if not df_ant_gold.empty:
                return df_ant_gold.iloc[-1].to_dict(), True
        ult_row = df_ant.iloc[-1].to_dict()
        if ult_row.get("baseline_gold_json"):
            try:
                bg = json.loads(str(ult_row["baseline_gold_json"]))
                if bg:
                    return bg, True
            except Exception:
                pass
        eh_gold = str(ult_row.get("camada_qualidade", "GOLD")) == "GOLD"
        return ult_row, eh_gold

    # 2. Caso só exista foto na mesma dt_referencia (re-execução no mesmo dia após reprocessamento)
    if not df_mesmo_dia.empty:
        row_dia = df_mesmo_dia.iloc[-1].to_dict()
        if row_dia.get("baseline_gold_json"):
            try:
                bg = json.loads(str(row_dia["baseline_gold_json"]))
                if bg:
                    return bg, True
            except Exception:
                pass
        fp_dia = str(row_dia.get("fingerprint_fisico", ""))
        hash_dia = str(row_dia.get("hash_conteudo", ""))
        if (fp_dia and fp_atu and fp_dia != fp_atu) or (
            hash_dia and hash_atu and hash_dia != hash_atu
        ):
            eh_gold = str(row_dia.get("camada_qualidade", "GOLD")) == "GOLD"
            return row_dia, eh_gold

    return None, False


def executar_checks_particao(
    run_id: str,
    dt_referencia: str,
    ts_execucao: str,
    tabela: str,
    tipo_particao: str,
    particoes_dfs: dict[str, pd.DataFrame],
    cfg_particao: dict[str, Any],
    df_fotos_particoes_hist: pd.DataFrame,
    perfis_particoes: dict[str, dict[str, Any]] | None = None,
    mapa_modo_avaliacao: dict[str, str] | None = None,
    aceitar_reprocessamento: bool = False,
) -> list[ResultadoCheck]:
    """Executa todos os checks configurados/aprendidos para o nível de partição e fotografia."""
    resultados: list[ResultadoCheck] = []

    perfis_uso: dict[str, dict[str, Any]] = {}
    if perfis_particoes:
        perfis_uso.update(perfis_particoes)
    else:
        for nome_p, df_p in sorted(particoes_dfs.items()):
            perfis_uso[nome_p] = SnapshotManager.construir_perfil_particao(
                nome_particao=nome_p,
                df_part=df_p,
                tipo_particao=tipo_particao,
            )

    if not perfis_uso:
        return resultados

    mapa_modo = mapa_modo_avaliacao or {}
    nomes_particoes = sorted(perfis_uso.keys())
    ultima_particao = nomes_particoes[-1]

    # -------------------------------------------------------------------------
    # 0.A Validação de Contrato e Schema Drift (`consistencia_schema`, Item 2.1)
    # -------------------------------------------------------------------------
    cfg_schema = cfg_particao.get("consistencia_schema", {})
    schema_esperado: dict[str, str] = cfg_schema.get("schema_esperado", {}) or {}
    if cfg_schema.get("ativo", True) and schema_esperado:
        for nome_part in nomes_particoes:
            perfil_atu = perfis_uso[nome_part]
            pdq = json.loads(str(perfil_atu.get("perfil_dq_json", "{}")))
            nulos_map = json.loads(str(perfil_atu.get("nulos_json", "{}")))
            preench_map = json.loads(str(perfil_atu.get("preenchidos_json", "{}")))

            cols_atuais = list(pdq.get("colunas", []))
            set_atuais = set(cols_atuais)
            tipos_atuais: dict[str, str] = pdq.get("tipos_colunas", {})

            cols_ausentes = [c for c in schema_esperado if c not in set_atuais]
            cols_incompativeis: list[str] = []
            for c_esp, t_esp in schema_esperado.items():
                if c_esp not in set_atuais:
                    continue
                t_atu = str(tipos_atuais.get(c_esp, "outro"))
                tem_nulos = int(nulos_map.get(c_esp, 0)) > 0
                tem_preench = int(preench_map.get(c_esp, 1)) > 0
                if _tipos_schema_incompativeis(
                    t_esp, t_atu, tem_nulos=tem_nulos, tem_preenchidos=tem_preench
                ):
                    cols_incompativeis.append(
                        f"{c_esp}(esperado={t_esp}, atual={t_atu})"
                    )

            cols_extras = [c for c in cols_atuais if c not in schema_esperado]

            if cols_ausentes or cols_incompativeis:
                status_sc = "ANOMALIA"
                partes_erro = []
                if cols_ausentes:
                    partes_erro.append(f"colunas ausentes={cols_ausentes}")
                if cols_incompativeis:
                    partes_erro.append(f"tipos incompatíveis={cols_incompativeis}")
                if cols_extras:
                    partes_erro.append(f"novas colunas={cols_extras}")
                detalhe_sc = (
                    f"Quebra de contrato de schema na partição `{nome_part}`: "
                    + "; ".join(partes_erro)
                )
                obs_sc = f"ausentes={len(cols_ausentes)}, incomp={len(cols_incompativeis)}"
            elif cols_extras:
                status_sc = "WARNING"
                detalhe_sc = (
                    f"Schema Drift leve em `{nome_part}`: {len(cols_extras)} nova(s) coluna(s) "
                    f"não mapeada(s) no contrato ({cols_extras})"
                )
                obs_sc = f"+{len(cols_extras)} coluna(s) extra(s)"
            else:
                status_sc = "OK"
                detalhe_sc = (
                    f"Schema 100% aderente ao contrato ({len(schema_esperado)} colunas validadas)"
                )
                obs_sc = f"{len(cols_atuais)} colunas OK"

            resultados.append(
                ResultadoCheck(
                    run_id=run_id,
                    dt_referencia=dt_referencia,
                    ts_execucao=ts_execucao,
                    tabela=tabela,
                    particao=nome_part,
                    nivel_check="PARTICAO",
                    tipo_check="consistencia_schema",
                    coluna="*",
                    valor_observado=obs_sc,
                    valor_esperado=f"{len(schema_esperado)} colunas do contrato",
                    status=status_sc,
                    detalhes=detalhe_sc,
                )
            )

    # -------------------------------------------------------------------------
    # 0.B Freshness / SLA de Chegada e Lacunas de Partição (`freshness_particao`, Item 2.2)
    # -------------------------------------------------------------------------
    cfg_fresh = cfg_particao.get("freshness_lacunas", {})
    if cfg_fresh.get("ativo", True):
        sla_max = int(cfg_fresh.get("sla_atraso_max_particoes", 0))
        verificar_lacunas = bool(cfg_fresh.get("verificar_lacunas", True))
        atraso_per, part_esp_ref, lacunas = _calcular_atraso_e_lacunas_particoes(
            nomes_particoes=nomes_particoes,
            tipo_particao=tipo_particao,
            dt_referencia=dt_referencia,
        )
        unid = "dia(s)" if tipo_particao == "diaria" else "mês(es)"
        lacunas_ativas = lacunas if verificar_lacunas else []

        if atraso_per > sla_max or lacunas_ativas:
            status_fr = "ANOMALIA"
            msgs_fr = []
            if atraso_per > sla_max:
                msgs_fr.append(
                    f"atraso de {atraso_per} {unid} vs dt_referencia={dt_referencia} "
                    f"(última partição={ultima_particao}, esperada={part_esp_ref}, SLA máx={sla_max})"
                )
            if lacunas_ativas:
                msgs_fr.append(
                    f"{len(lacunas_ativas)} lacuna(s) na sequência de partições: {lacunas_ativas[:5]}"
                )
            detalhe_fr = "Falha de Freshness/Continuidade: " + "; ".join(msgs_fr)
        else:
            status_fr = "OK"
            detalhe_fr = (
                f"Freshness e continuidade OK: última partição `{ultima_particao}` em dia "
                f"(atraso={atraso_per} {unid} <= SLA={sla_max}) e 0 lacunas na sequência ({len(nomes_particoes)} partições)"
            )

        resultados.append(
            ResultadoCheck(
                run_id=run_id,
                dt_referencia=dt_referencia,
                ts_execucao=ts_execucao,
                tabela=tabela,
                particao=ultima_particao,
                nivel_check="PARTICAO",
                tipo_check="freshness_particao",
                coluna="*",
                valor_observado=f"atraso={atraso_per}, lacunas={len(lacunas_ativas)}",
                valor_esperado=f"atraso<={sla_max}, lacunas=0 ({part_esp_ref})",
                status=status_fr,
                detalhes=detalhe_fr,
            )
        )

    # -------------------------------------------------------------------------
    # 1. Volumetria Histórica entre Partições (Sazonalidade + Duplo Estágio + Cold Start)
    # -------------------------------------------------------------------------
    cfg_vol = cfg_particao.get("volumetria_historica", {})
    desvio_rel_max = float(cfg_vol.get("desvio_relativo_max", 0.20))
    desvio_rel_warn = float(
        cfg_vol.get("desvio_relativo_warning", round(desvio_rel_max * 0.80, 4))
    )
    z_max = float(cfg_vol.get("z_score_robusto_max", 3.0))
    z_warn = float(cfg_vol.get("z_score_robusto_warning", max(2.0, z_max * 0.80)))
    sazonalidade_ativa = bool(cfg_vol.get("sazonalidade_ativa", False))
    baselines_sazonais: dict[str, dict[str, float]] = (
        cfg_vol.get("baselines_sazonais", {}) or {}
    )
    em_cold_start = bool(cfg_vol.get("em_cold_start", False))
    todas_contaminadas = bool(cfg_vol.get("todas_contaminadas", False))

    volumes = [int(p["qtd_registros"]) for p in perfis_uso.values()]
    med_calc, sig_calc = _calcular_estatisticas_robustas(volumes)
    mediana_global = float(cfg_vol.get("mediana_gold", med_calc))
    sigma_global = float(cfg_vol.get("sigma_gold", sig_calc))

    if cfg_vol.get("ativo", True):
        for nome_part in nomes_particoes:
            qtd = int(perfis_uso[nome_part]["qtd_registros"])

            # Seleção de baseline sazonal (Dia Útil vs Fim de Semana) quando aplicável (Item 2.5)
            mediana = mediana_global
            sigma_rob = sigma_global
            tag_sazonal = ""
            if sazonalidade_ativa and tipo_particao == "diaria" and baselines_sazonais:
                val_p = nome_part.split("=")[-1]
                if len(val_p) == 8 and val_p.isdigit():
                    try:
                        dt_p = dt.date(int(val_p[:4]), int(val_p[4:6]), int(val_p[6:8]))
                        chave_saz = "UTIL" if dt_p.weekday() < 5 else "FDS"
                        if chave_saz in baselines_sazonais:
                            mediana = float(baselines_sazonais[chave_saz]["mediana"])
                            sigma_rob = float(baselines_sazonais[chave_saz]["sigma"])
                            tag_sazonal = f" [{chave_saz}]"
                    except ValueError:
                        pass

            lim_inf = max(0, int(round(mediana * (1.0 - desvio_rel_max))))
            lim_sup = int(round(mediana * (1.0 + desvio_rel_max)))
            faixa_str = (
                f"[{lim_inf}, {lim_sup}] (mediana{tag_sazonal}={int(round(mediana))}, ±{desvio_rel_max:.0%})"
            )

            desvio_rel = (qtd - mediana) / mediana if mediana > 0 else 0.0
            z_rob = abs(qtd - mediana) / sigma_rob if sigma_rob > 0 else 0.0

            if qtd == 0:
                status = "ANOMALIA"
                detalhe = f"Partição vazia (0 registros vs mediana esperada de {mediana:.0f})"
            elif abs(desvio_rel) > desvio_rel_max and z_rob > z_max:
                direcao = "QUEDA BRUSCA" if desvio_rel < 0 else "PICO ANÔMALO"
                status = "ANOMALIA"
                detalhe = (
                    f"{direcao} de volumetria na partição: {qtd} linhas "
                    f"({desvio_rel:+.1%} vs mediana aprendida{tag_sazonal} de {mediana:.0f}, "
                    f"tol=±{desvio_rel_max:.0%}, z={z_rob:.1f})"
                )
            elif todas_contaminadas or (em_cold_start and len(nomes_particoes) < 2):
                status = "WARNING"
                detalhe = (
                    f"Volumetria em COLD START / sem histórico GOLD suficiente ({len(nomes_particoes)} partição): "
                    f"{qtd} linhas. Aguardando mais partições GOLD para consolidar baseline."
                )
            elif abs(desvio_rel) > desvio_rel_warn and z_rob > z_warn:
                direcao_w = "Queda moderada" if desvio_rel < 0 else "Elevação moderada"
                status = "WARNING"
                detalhe = (
                    f"{direcao_w} de volumetria na partição (alerta preventivo): {qtd} linhas "
                    f"({desvio_rel:+.1%} vs mediana aprendida{tag_sazonal} de {mediana:.0f}, "
                    f"warn=±{desvio_rel_warn:.0%}, max=±{desvio_rel_max:.0%}, z={z_rob:.1f})"
                )
            else:
                status = "OK"
                detalhe = (
                    f"Volumetria da partição estável: {qtd} linhas "
                    f"({desvio_rel:+.1%} vs mediana aprendida{tag_sazonal} de {mediana:.0f})"
                )

            resultados.append(
                ResultadoCheck(
                    run_id=run_id,
                    dt_referencia=dt_referencia,
                    ts_execucao=ts_execucao,
                    tabela=tabela,
                    particao=nome_part,
                    nivel_check="PARTICAO",
                    tipo_check="volumetria_historica",
                    coluna="*",
                    valor_observado=qtd,
                    valor_esperado=faixa_str,
                    status=status,
                    detalhes=detalhe,
                )
            )

    # -------------------------------------------------------------------------
    # 2. Evolução entre Fotografias & Auditoria de Reprocessamento Histórico
    # -------------------------------------------------------------------------
    cfg_foto = cfg_particao.get("evolucao_fotografias", {})
    lim_inf_global = max(0, int(round(mediana_global * (1.0 - desvio_rel_max))))
    if cfg_foto.get("ativo", True) and not df_fotos_particoes_hist.empty:
        df_fotos_tab = df_fotos_particoes_hist[
            df_fotos_particoes_hist["tabela"] == tabela
        ]
        tem_historico_anterior = not df_fotos_tab[
            df_fotos_tab["dt_referencia"] < dt_referencia
        ].empty
        queda_max_pct = float(cfg_foto.get("queda_max_registros_pct", 0.0))
        cols_evolucao = cfg_foto.get("colunas_evolucao_positiva", []) or []

        for nome_part in nomes_particoes:
            perfil_atu = perfis_uso[nome_part]
            qtd_atual = int(perfil_atu["qtd_registros"])
            fp_atu = str(perfil_atu.get("fingerprint_fisico", ""))
            hash_atu = str(perfil_atu.get("hash_conteudo", ""))
            modo_av = mapa_modo.get(nome_part, "SCAN_PARQUET")

            row_ant, eh_baseline_gold = _selecionar_baseline_snapshot_particao(
                df_fotos_tab=df_fotos_tab,
                nome_part=nome_part,
                dt_referencia=dt_referencia,
                fp_atu=fp_atu,
                hash_atu=hash_atu,
            )

            if row_ant is None:
                if tem_historico_anterior:
                    resultados.append(
                        ResultadoCheck(
                            run_id=run_id,
                            dt_referencia=dt_referencia,
                            ts_execucao=ts_execucao,
                            tabela=tabela,
                            particao=nome_part,
                            nivel_check="FOTOGRAFIA_DIFF",
                            tipo_check="evolucao_fotografias",
                            coluna=",".join(cols_evolucao) if cols_evolucao else "*",
                            valor_observado=qtd_atual,
                            valor_esperado="1ª fotografia da partição",
                            status="OK",
                            detalhes=(
                                f"Primeira fotografia registrada para a nova partição em {dt_referencia}: "
                                f"{qtd_atual} linhas (hash={hash_atu[:8]})"
                            ),
                        )
                    )
                continue

            dt_ant = str(row_ant.get("dt_referencia", dt_referencia))
            camada_ant = str(row_ant.get("camada_qualidade", "GOLD"))
            qtd_ant = int(row_ant["qtd_registros"])
            fp_ant = str(row_ant.get("fingerprint_fisico", ""))
            hash_ant = str(row_ant.get("hash_conteudo", ""))

            # Caso 2A: Partição histórica GOLD intocada (Cache GOLD — Parquet não lido)
            if modo_av == "CACHE_GOLD":
                resultados.append(
                    ResultadoCheck(
                        run_id=run_id,
                        dt_referencia=dt_referencia,
                        ts_execucao=ts_execucao,
                        tabela=tabela,
                        particao=nome_part,
                        nivel_check="FOTOGRAFIA_DIFF",
                        tipo_check="evolucao_fotografias",
                        coluna=",".join(cols_evolucao) if cols_evolucao else "*",
                        valor_observado=qtd_atual,
                        valor_esperado=f"== {qtd_ant} (Cache GOLD {dt_ant})",
                        status="OK",
                        detalhes=(
                            f"Partição GOLD histórica intocada vs foto {dt_ant}: {qtd_atual} linhas "
                            f"(fingerprint físico={fp_atu[:8]} inalterado; Parquet não relido via Cache GOLD)"
                        ),
                    )
                )
                continue

            # Caso 2B: Reprocessamento Físico Idempotente (arquivo mudou, mas hash_conteudo é idêntico)
            if fp_ant and fp_atu and fp_ant != fp_atu and hash_ant and hash_atu == hash_ant:
                resultados.append(
                    ResultadoCheck(
                        run_id=run_id,
                        dt_referencia=dt_referencia,
                        ts_execucao=ts_execucao,
                        tabela=tabela,
                        particao=nome_part,
                        nivel_check="FOTOGRAFIA_DIFF",
                        tipo_check="evolucao_fotografias",
                        coluna=",".join(cols_evolucao) if cols_evolucao else "*",
                        valor_observado=qtd_atual,
                        valor_esperado=f"== {qtd_ant} (hash={hash_ant[:8]})",
                        status="OK",
                        detalhes=(
                            f"REPROCESSAMENTO FÍSICO IDEMPOTENTE detectado vs foto {dt_ant}: "
                            f"arquivo Parquet foi regravado na origem (fp {fp_ant[:8]}->{fp_atu[:8]}), "
                            f"mas o conteúdo lógico permaneceu 100% idêntico (hash={hash_atu[:8]}, {qtd_atual} linhas)"
                        ),
                    )
                )
                continue

            # Caso 2C: Conteúdo lógico e físico 100% inalterados
            if hash_ant and hash_atu == hash_ant:
                resultados.append(
                    ResultadoCheck(
                        run_id=run_id,
                        dt_referencia=dt_referencia,
                        ts_execucao=ts_execucao,
                        tabela=tabela,
                        particao=nome_part,
                        nivel_check="FOTOGRAFIA_DIFF",
                        tipo_check="evolucao_fotografias",
                        coluna=",".join(cols_evolucao) if cols_evolucao else "*",
                        valor_observado=qtd_atual,
                        valor_esperado=f">= {qtd_ant} (foto {dt_ant})",
                        status="OK",
                        detalhes=(
                            f"Evolução consistente vs foto {dt_ant}: "
                            f"linhas {qtd_ant} -> {qtd_atual} (sem alterações de preenchimento ou conteúdo, hash={hash_atu[:8]})"
                        ),
                    )
                )
                continue

            # Caso 2D: O conteúdo lógico mudou (`hash_atu != hash_ant`)!
            diff_qtd = qtd_atual - qtd_ant
            var_pct = (diff_qtd / qtd_ant) if qtd_ant > 0 else 0.0
            preench_ant = json.loads(str(row_ant.get("preenchidos_json", "{}")))
            preench_atu = json.loads(str(perfil_atu.get("preenchidos_json", "{}")))

            # Se a partição estava em quarentena (BRONZE/SILVER) e nunca teve baseline GOLD:
            if not eh_baseline_gold:
                regressao_quarentena = False
                msgs_q: list[str] = []
                for col_c, p_atu_val in preench_atu.items():
                    if col_c not in preench_ant:
                        continue
                    p_ant_val = int(preench_ant[col_c])
                    delta_c = int(p_atu_val) - p_ant_val
                    if delta_c > 0 and col_c in cols_evolucao:
                        msgs_q.append(f"{col_c}: +{delta_c} novos preenchimentos")
                    elif delta_c < 0 and qtd_atual >= qtd_ant:
                        regressao_quarentena = True
                        msgs_q.append(
                            f"{col_c}: REGRESSÃO de {delta_c} preenchimentos ({p_ant_val} -> {p_atu_val})"
                        )

                if (
                    var_pct < -queda_max_pct and qtd_atual < lim_inf_global
                ) or regressao_quarentena:
                    status_f = "ANOMALIA"
                    detalhe_f = (
                        f"Perda/regressão detectada em relação à fotografia {dt_ant}: "
                        f"linhas {qtd_ant} -> {qtd_atual}. " + "; ".join(msgs_q)
                    )
                elif fp_ant and fp_atu and fp_ant != fp_atu and nome_part < ultima_particao:
                    status_f = "OK"
                    detalhe_f = (
                        f"REPROCESSAMENTO CORRETIVO detectado em partição de quarentena "
                        f"(estava em {camada_ant} na foto {dt_ant}): linhas {qtd_ant} -> {qtd_atual} "
                        f"(hash {hash_ant[:8]}->{hash_atu[:8]})"
                    )
                else:
                    status_f = "OK"
                    extra_q = (
                        f" ({'; '.join(msgs_q)})"
                        if msgs_q
                        else " (sem alterações de preenchimento)"
                    )
                    detalhe_f = (
                        f"Evolução consistente vs foto {dt_ant}: "
                        f"linhas {qtd_ant} -> {qtd_atual}{extra_q}"
                    )

                resultados.append(
                    ResultadoCheck(
                        run_id=run_id,
                        dt_referencia=dt_referencia,
                        ts_execucao=ts_execucao,
                        tabela=tabela,
                        particao=nome_part,
                        nivel_check="FOTOGRAFIA_DIFF",
                        tipo_check="evolucao_fotografias",
                        coluna=",".join(cols_evolucao) if cols_evolucao else "*",
                        valor_observado=qtd_atual,
                        valor_esperado=f">= {qtd_ant} (foto {dt_ant})",
                        status=status_f,
                        detalhes=detalhe_f,
                    )
                )
                continue

            # Caso 2E: Partição possui baseline GOLD (`eh_baseline_gold == True`) -> Auditoria completa!
            eh_safra_mensal_aberta = (
                tipo_particao == "mensal"
                and nome_part == ultima_particao
                and dt_ant < dt_referencia
                and diff_qtd > 0
            )

            mutacoes: list[str] = []
            evolucoes: list[str] = []

            # 1) Auditoria de variação de linhas
            if var_pct < -queda_max_pct:
                mutacoes.append(
                    f"queda de {abs(diff_qtd)} registros ({qtd_ant} -> {qtd_atual}, {var_pct:+.1%})"
                )
            elif diff_qtd > 0:
                if eh_safra_mensal_aberta:
                    evolucoes.append(f"+{diff_qtd} novos registros na safra aberta")
                else:
                    mutacoes.append(
                        f"adição inesperada de +{diff_qtd} registros em partição GOLD fechada ({qtd_ant} -> {qtd_atual})"
                    )

            # 2) Auditoria de preenchimento/nulos por coluna
            for col_c, p_atu_val in preench_atu.items():
                if col_c not in preench_ant:
                    continue
                p_ant_val = int(preench_ant[col_c])
                p_atu_int = int(p_atu_val)
                delta_col = p_atu_int - p_ant_val
                if delta_col < 0:
                    mutacoes.append(
                        f"{col_c}: REGRESSÃO de {delta_col} preenchimentos ({p_ant_val} -> {p_atu_int})"
                    )
                elif delta_col > 0:
                    if col_c in cols_evolucao:
                        evolucoes.append(f"{col_c}: +{delta_col} novos preenchimentos")
                    elif eh_safra_mensal_aberta and delta_col <= diff_qtd:
                        pass
                    else:
                        mutacoes.append(
                            f"{col_c}: preenchimento retroativo (+{delta_col}) em coluna imutável ({p_ant_val} -> {p_atu_int})"
                        )

            # 3) Auditoria de Somas Numéricas, Domínios e Hashes de Colunas
            if not eh_safra_mensal_aberta:
                somas_ant = json.loads(str(row_ant.get("somas_numericas_json", "{}")))
                somas_atu = json.loads(str(perfil_atu.get("somas_numericas_json", "{}")))
                for col_n, s_atu_v in somas_atu.items():
                    if col_n in somas_ant and col_n not in cols_evolucao:
                        s_ant_f = float(somas_ant[col_n])
                        s_atu_f = float(s_atu_v)
                        if abs(s_atu_f - s_ant_f) > 0.01:
                            var_s = (
                                (s_atu_f - s_ant_f) / abs(s_ant_f)
                                if s_ant_f != 0
                                else 0.0
                            )
                            mutacoes.append(
                                f"soma({col_n}) alterada de {s_ant_f:,.2f} para {s_atu_f:,.2f} ({var_s:+.2%})"
                            )

                doms_ant = json.loads(str(row_ant.get("dominios_json", "{}")))
                doms_atu = json.loads(str(perfil_atu.get("dominios_json", "{}")))
                for col_d, dom_atu_map in doms_atu.items():
                    if col_d in doms_ant and col_d not in cols_evolucao:
                        dom_ant_map = doms_ant[col_d]
                        if dom_atu_map != dom_ant_map:
                            todas_cats = sorted(
                                set(dom_ant_map.keys()) | set(dom_atu_map.keys())
                            )
                            diffs_cat = [
                                f"{k}:{dom_ant_map.get(k, 0)}->{dom_atu_map.get(k, 0)}"
                                for k in todas_cats
                                if dom_ant_map.get(k, 0) != dom_atu_map.get(k, 0)
                            ]
                            mutacoes.append(
                                f"mix de domínio alterado em `{col_d}` ({', '.join(diffs_cat[:4])})"
                            )

                hashes_col_ant = json.loads(
                    str(row_ant.get("hashes_colunas_json", "{}"))
                )
                hashes_col_atu = json.loads(
                    str(perfil_atu.get("hashes_colunas_json", "{}"))
                )
                for col_h, h_atu_c in hashes_col_atu.items():
                    if col_h not in hashes_col_ant:
                        continue
                    h_ant_c = hashes_col_ant[col_h]
                    if h_atu_c != h_ant_c:
                        delta_c = int(preench_atu.get(col_h, 0)) - int(
                            preench_ant.get(col_h, 0)
                        )
                        if col_h in cols_evolucao and delta_c > 0 and diff_qtd == 0:
                            continue
                        ja_reportada = any(col_h in m for m in mutacoes)
                        if not ja_reportada:
                            mutacoes.append(
                                f"valores internos alterados na coluna imutável `{col_h}` (hash {h_ant_c[:6]}->{h_atu_c[:6]})"
                            )

            if mutacoes:
                if aceitar_reprocessamento:
                    status_f = "OK"
                    detalhe_f = (
                        f"REPROCESSAMENTO HISTÓRICO ACEITO (--aceitar-reprocessamento) vs foto GOLD {dt_ant}: "
                        f"linhas {qtd_ant} -> {qtd_atual}. Alterações aceitas: "
                        + "; ".join(mutacoes)
                    )
                else:
                    status_f = "ANOMALIA"
                    detalhe_f = (
                        f"REPROCESSAMENTO HISTÓRICO / MUTAÇÃO DETECTADA vs foto GOLD {dt_ant}: "
                        f"linhas {qtd_ant} -> {qtd_atual}. "
                        + "; ".join(mutacoes)
                    )
            else:
                status_f = "OK"
                extra_info = (
                    f" ({'; '.join(evolucoes)})"
                    if evolucoes
                    else " (sem alterações de preenchimento)"
                )
                detalhe_f = (
                    f"Evolução consistente vs foto {dt_ant}: "
                    f"linhas {qtd_ant} -> {qtd_atual}{extra_info}"
                )

            resultados.append(
                ResultadoCheck(
                    run_id=run_id,
                    dt_referencia=dt_referencia,
                    ts_execucao=ts_execucao,
                    tabela=tabela,
                    particao=nome_part,
                    nivel_check="FOTOGRAFIA_DIFF",
                    tipo_check="evolucao_fotografias",
                    coluna=",".join(cols_evolucao) if cols_evolucao else "*",
                    valor_observado=qtd_atual,
                    valor_esperado=f">= {qtd_ant} (foto GOLD {dt_ant})",
                    status=status_f,
                    detalhes=detalhe_f,
                )
            )

    # -------------------------------------------------------------------------
    # 3. Consistência de Data com a Partição (Timeliness interna)
    # -------------------------------------------------------------------------
    cfg_cons = cfg_particao.get("consistencia_data_particao", {})
    if cfg_cons.get("ativo", True):
        colunas_data = cfg_cons.get("colunas_data_particao", []) or []
        for nome_part in nomes_particoes:
            perfil_atu = perfis_uso[nome_part]
            pdq = json.loads(str(perfil_atu.get("perfil_dq_json", "{}")))
            ad_map = pdq.get("aderencia_data_particao", {})

            for col_dt in colunas_data:
                if col_dt in ad_map:
                    info_dt = ad_map[col_dt]
                    total_validos = int(info_dt.get("total_validos", 0))
                    if total_validos == 0:
                        continue
                    qtd_fora = int(info_dt.get("qtd_fora", 0))
                    prefixo_esperado = str(info_dt.get("prefixo_esperado", ""))
                    exemplos = info_dt.get("exemplos_fora", [])
                elif nome_part in particoes_dfs and col_dt in particoes_dfs[nome_part].columns:
                    df_part = particoes_dfs[nome_part]
                    valor_particao = nome_part.split("=")[-1]
                    if tipo_particao == "mensal" and len(valor_particao) == 6:
                        prefixo_esperado = f"{valor_particao[:4]}-{valor_particao[4:6]}"
                    elif tipo_particao == "diaria" and len(valor_particao) == 8:
                        prefixo_esperado = (
                            f"{valor_particao[:4]}-{valor_particao[4:6]}-{valor_particao[6:8]}"
                        )
                    else:
                        prefixo_esperado = valor_particao
                    serie_str = df_part[col_dt].dropna().astype(str)
                    total_validos = len(serie_str)
                    if total_validos == 0:
                        continue
                    fora_particao = serie_str[~serie_str.str.startswith(prefixo_esperado)]
                    qtd_fora = len(fora_particao)
                    exemplos = fora_particao.unique()[:3].tolist()
                else:
                    continue

                if qtd_fora > 0:
                    status_dt = "ANOMALIA"
                    detalhe_dt = (
                        f"{qtd_fora} registros ({qtd_fora / total_validos:.1%}) com `{col_dt}` "
                        f"fora da partição `{nome_part}` (esperado prefixo aprendido `{prefixo_esperado}`, "
                        f"exemplos encontrados: {exemplos})"
                    )
                else:
                    status_dt = "OK"
                    detalhe_dt = (
                        f"100% dos registros ({total_validos}) possuem `{col_dt}` "
                        f"compatível com a partição `{nome_part}` (`{prefixo_esperado}`)"
                    )

                resultados.append(
                    ResultadoCheck(
                        run_id=run_id,
                        dt_referencia=dt_referencia,
                        ts_execucao=ts_execucao,
                        tabela=tabela,
                        particao=nome_part,
                        nivel_check="PARTICAO",
                        tipo_check="consistencia_data_particao",
                        coluna=col_dt,
                        valor_observado=qtd_fora,
                        valor_esperado=f"0 registros fora de {prefixo_esperado}",
                        status=status_dt,
                        detalhes=detalhe_dt,
                    )
                )

    return resultados
