"""
Motor de Auto-Aprendizado Blindado (Gold-Only Self-Learning Rules) para Data Quality.

Garante que o aprendizado estatístico do motor seja calculado EXCLUSIVAMENTE sobre
partições e fotografias saudáveis (classificadas como `GOLD`), impedindo que partições
com anomalias (`SILVER` ou `BRONZE`) contaminem os limiares aprendidos (*Data Poisoning*).

Fluxo em 2 estágios:
- Estágio 1 (Filtro de Qualidade para Treino): Identifica quais partições da tabela
  (e quais fotografias históricas) são 100% íntegras (`GOLD`), descartando do treino
  qualquer partição com duplicidade de chave, salto/queda de volume, nulos inesperados,
  categorias espúrias, outliers numéricos, datas fora da safra ou formato inválido.
- Estágio 2 (Calibração Fina sobre Base GOLD): Calcula todos os limites e regras
  exclusivamente sobre o subconjunto de partições e snapshots `GOLD`.
"""

from __future__ import annotations

import json
import re
from typing import Any

import numpy as np
import pandas as pd

REGEX_DATA_ISO = re.compile(r"^\d{4}-\d{2}-\d{2}")


def _eh_coluna_data_ou_ts(nome_col: str, serie: pd.Series) -> bool:
    """Identifica se uma coluna representa data ou timestamp pelo nome, tipo ou conteúdo."""
    nome_low = nome_col.lower()
    if nome_low.startswith(("dt_", "data_", "ts_", "timestamp_")):
        return True
    if pd.api.types.is_datetime64_any_dtype(serie):
        return True
    amostra = serie.dropna().astype(str).head(50)
    if len(amostra) == 0:
        return False
    taxa_iso = sum(1 for v in amostra if REGEX_DATA_ISO.match(v)) / len(amostra)
    return taxa_iso >= 0.80


def _mediana_e_sigma_mad(valores: list[float]) -> tuple[float, float]:
    """Calcula mediana e desvio padrão robusto baseado no MAD (Median Absolute Deviation)."""
    if not valores:
        return 0.0, 0.0
    arr = np.array(valores, dtype=float)
    med = float(np.median(arr))
    mad = float(np.median(np.abs(arr - med)))
    sigma_rob = mad * 1.4826
    return med, sigma_rob


def _filtrar_particoes_gold_para_treino(
    particoes_dfs: dict[str, pd.DataFrame],
    chaves_primarias: list[str],
    tipo_particao: str,
) -> list[str]:
    """
    Estágio 1: Identifica quais partições da tabela estão 100% saudáveis (candidatas `GOLD`)
    para que partições `SILVER` ou `BRONZE` não contaminem a calibração dos limiares.
    """
    nomes_particoes = sorted(
        p for p, df in particoes_dfs.items() if len(df) > 0
    )
    if len(nomes_particoes) <= 1:
        return nomes_particoes

    dfs_todos = [particoes_dfs[p] for p in nomes_particoes]
    n_parts = len(nomes_particoes)
    todas_colunas = list(dfs_todos[0].columns)

    particoes_contaminadas: set[str] = set()

    # 1. Verifica duplicidade de chave primária ou de linha inteira
    for p in nomes_particoes:
        df_p = particoes_dfs[p]
        if int(df_p.duplicated().sum()) > 0:
            particoes_contaminadas.add(p)
            continue
        pks_pres = [k for k in chaves_primarias if k in df_p.columns]
        if pks_pres:
            if int(df_p.dropna(subset=pks_pres).duplicated(subset=pks_pres).sum()) > 0:
                particoes_contaminadas.add(p)

    # 2. Verifica quedas ou picos de volumetria na partição
    volumes = [float(len(particoes_dfs[p])) for p in nomes_particoes]
    med_vol, sigma_vol_raw = _mediana_e_sigma_mad(volumes)
    sigma_vol = max(sigma_vol_raw, med_vol * 0.03, 1.0)
    for p in nomes_particoes:
        qtd = float(len(particoes_dfs[p]))
        desvio_rel = abs(qtd - med_vol) / med_vol if med_vol > 0 else 0.0
        z_rob = abs(qtd - med_vol) / sigma_vol
        if desvio_rel > 0.15 and z_rob > 3.0:
            particoes_contaminadas.add(p)

    # 3. Verifica nulos em colunas que têm mediana 0% de nulos
    for col in todas_colunas:
        if col.lower().startswith(("ts_", "dt_atualizacao")):
            continue
        taxas_nulos = [
            float(particoes_dfs[p][col].isna().sum()) / len(particoes_dfs[p])
            for p in nomes_particoes
        ]
        if float(np.median(taxas_nulos)) == 0.0:
            for p, tx in zip(nomes_particoes, taxas_nulos):
                if tx > 0.0:
                    particoes_contaminadas.add(p)

    # 4. Verifica consistência de data com o prefixo da partição
    for col in todas_colunas:
        if not _eh_coluna_data_ou_ts(col, dfs_todos[0][col]):
            continue
        if pd.api.types.is_datetime64_any_dtype(dfs_todos[0][col]):
            continue
        aderencias_part: dict[str, float] = {}
        for p in nomes_particoes:
            val_part = p.split("=")[-1]
            pref = (
                f"{val_part[:4]}-{val_part[4:6]}"
                if (tipo_particao == "mensal" and len(val_part) == 6)
                else (
                    f"{val_part[:4]}-{val_part[4:6]}-{val_part[6:8]}"
                    if (tipo_particao == "diaria" and len(val_part) == 8)
                    else val_part
                )
            )
            s_str = particoes_dfs[p][col].dropna().astype(str)
            if len(s_str) > 0:
                aderencias_part[p] = float(s_str.str.startswith(pref).mean())
        if aderencias_part and float(np.median(list(aderencias_part.values()))) >= 0.995:
            for p, ad in aderencias_part.items():
                if ad < 1.0:
                    particoes_contaminadas.add(p)

    # 5. Verifica categorias espúrias (presentes em < 50% das partições)
    limiar_presenca = max(1, int(np.ceil(n_parts * 0.50)))
    for col in todas_colunas:
        if col in chaves_primarias or col.lower().startswith("id_"):
            continue
        if _eh_coluna_data_ou_ts(col, dfs_todos[0][col]):
            continue
        s_am = dfs_todos[0][col]
        if not (
            pd.api.types.is_string_dtype(s_am)
            or pd.api.types.is_object_dtype(s_am)
            or isinstance(s_am.dtype, pd.CategoricalDtype)
        ):
            continue
        cards = [particoes_dfs[p][col].dropna().nunique() for p in nomes_particoes]
        if not cards or float(np.median(cards)) > 25:
            continue
        contagem_parts_cat: dict[Any, int] = {}
        for p in nomes_particoes:
            for v in particoes_dfs[p][col].dropna().unique():
                contagem_parts_cat[v] = contagem_parts_cat.get(v, 0) + 1
        cats_validas = {
            v for v, cnt in contagem_parts_cat.items() if cnt >= limiar_presenca
        }
        if cats_validas:
            for p in nomes_particoes:
                vals_p = set(particoes_dfs[p][col].dropna().unique())
                if not vals_p.issubset(cats_validas):
                    particoes_contaminadas.add(p)

    # 6. Verifica valores negativos/zero indevidos ou outliers numéricos extremos
    for col in todas_colunas:
        if col in chaves_primarias or col.lower().startswith("id_"):
            continue
        if _eh_coluna_data_ou_ts(col, dfs_todos[0][col]):
            continue
        s_am = dfs_todos[0][col]
        if not pd.api.types.is_numeric_dtype(s_am) or pd.api.types.is_bool_dtype(s_am):
            continue
        mins_p = {}
        logs_max_p = {}
        for p in nomes_particoes:
            s_n = pd.to_numeric(particoes_dfs[p][col], errors="coerce").dropna()
            if len(s_n) > 0:
                mins_p[p] = float(s_n.min())
                logs_max_p[p] = float(np.log10(max(1.0, abs(float(s_n.max())))))
        if mins_p:
            med_min, sig_min_raw = _mediana_e_sigma_mad(list(mins_p.values()))
            sig_min = max(sig_min_raw, abs(med_min) * 0.05, 1.0)
            med_lmax, sig_lmax_raw = _mediana_e_sigma_mad(list(logs_max_p.values()))
            sig_lmax = max(sig_lmax_raw, 0.01)
            for p in nomes_particoes:
                if p not in mins_p:
                    continue
                if med_min > 0 and mins_p[p] <= 0:
                    particoes_contaminadas.add(p)
                elif abs(mins_p[p] - med_min) > 3.5 * sig_min:
                    particoes_contaminadas.add(p)
                elif abs(logs_max_p[p] - med_lmax) > 4.5 * sig_lmax:
                    particoes_contaminadas.add(p)

    particoes_gold = [p for p in nomes_particoes if p not in particoes_contaminadas]
    return particoes_gold if particoes_gold else nomes_particoes


def aprender_regras_tabela(
    run_id: str,
    dt_referencia: str,
    ts_execucao: str,
    tabela: str,
    tipo_particao: str,
    chaves_primarias_cfg: list[str] | str | None,
    particoes_dfs: dict[str, pd.DataFrame],
    df_fotos_particoes_hist: pd.DataFrame,
    checks_cfg: dict[str, Any],
) -> dict[str, Any]:
    """
    Aprende todas as regras e limiares de Data Quality para a tabela informada,
    utilizando EXCLUSIVAMENTE as partições e fotografias saudáveis (`GOLD`).
    """
    registros_silver: list[dict[str, Any]] = []

    def _registrar_regra(
        nivel: str,
        tipo: str,
        coluna: str,
        parametro_aprendido: str,
        origem: str,
        detalhes: str,
    ) -> None:
        registros_silver.append(
            {
                "run_id": run_id,
                "dt_referencia": dt_referencia,
                "ts_execucao": ts_execucao,
                "tabela": tabela,
                "nivel_check": nivel,
                "tipo_check": tipo,
                "coluna": coluna,
                "regra_aprendida": parametro_aprendido,
                "origem_regra": origem,
                "detalhes_aprendizado": detalhes,
            }
        )

    nomes_todas_particoes = sorted(
        p for p, df in particoes_dfs.items() if len(df) > 0
    )
    n_total_particoes = len(nomes_todas_particoes)
    if n_total_particoes == 0:
        return {"registros_regras_silver": []}

    todas_colunas = list(particoes_dfs[nomes_todas_particoes[0]].columns)

    # =========================================================================
    # 1. Auto-descoberta de Chaves Primárias
    # =========================================================================
    if isinstance(chaves_primarias_cfg, list):
        pks_yaml = [
            str(k)
            for k in chaves_primarias_cfg
            if str(k).lower() not in ("auto", "")
        ]
    elif (
        isinstance(chaves_primarias_cfg, str)
        and chaves_primarias_cfg.lower() not in ("auto", "")
    ):
        pks_yaml = [chaves_primarias_cfg]
    else:
        pks_yaml = []

    if pks_yaml:
        chaves_primarias = pks_yaml
        origem_pk = "MANUAL_YAML"
        detalhe_pk = f"Chave primária definida no YAML: {chaves_primarias}"
    else:
        chaves_primarias = []
        for col in todas_colunas:
            col_low = col.lower()
            if not (col_low.startswith("id_") or col_low.endswith("_id")):
                continue
            taxas_unicidade = []
            taxas_nulos = []
            valores_globais = []
            for p in nomes_todas_particoes:
                df_p = particoes_dfs[p]
                if col not in df_p.columns or len(df_p) == 0:
                    continue
                n_nulos = int(df_p[col].isna().sum())
                taxas_nulos.append(n_nulos / len(df_p))
                s_val = df_p[col].dropna()
                if len(s_val) > 0:
                    taxas_unicidade.append(s_val.nunique() / len(s_val))
                    valores_globais.extend(s_val.astype(str).tolist())

            if (
                taxas_unicidade
                and max(taxas_nulos) == 0.0
                and float(np.median(taxas_unicidade)) == 1.0
            ):
                unicidade_global = (
                    len(set(valores_globais)) / len(valores_globais)
                    if valores_globais
                    else 0.0
                )
                if unicidade_global >= 0.80:
                    chaves_primarias.append(col)
                    break

        origem_pk = "AUTO_APRENDIDO"
        detalhe_pk = (
            f"Chave primária auto-descoberta (100% única nas partições GOLD e 0% nulos): {chaves_primarias}"
        )

    # =========================================================================
    # Estágio 1 -> Seleciona APENAS as partições e fotografias GOLD para o treino!
    # =========================================================================
    nomes_particoes_gold = _filtrar_particoes_gold_para_treino(
        particoes_dfs=particoes_dfs,
        chaves_primarias=chaves_primarias,
        tipo_particao=tipo_particao,
    )
    dfs_gold = [particoes_dfs[p] for p in nomes_particoes_gold]
    n_gold = len(dfs_gold)
    tag_treino_gold = f"({n_gold}/{n_total_particoes} partições GOLD)"

    # Filtra histórico de fotografias de partição apenas para partições GOLD
    df_fotos_gold = pd.DataFrame()
    if not df_fotos_particoes_hist.empty:
        df_f_tab = df_fotos_particoes_hist[
            (df_fotos_particoes_hist["tabela"] == tabela)
            & (df_fotos_particoes_hist["dt_referencia"] < dt_referencia)
        ].copy()
        if not df_f_tab.empty:
            if "camada_qualidade" in df_f_tab.columns:
                df_f_gold = df_f_tab[df_f_tab["camada_qualidade"] == "GOLD"]
                df_fotos_gold = (
                    df_f_gold
                    if not df_f_gold.empty
                    else df_f_tab[df_f_tab["particao"].isin(nomes_particoes_gold)]
                )
            else:
                df_fotos_gold = df_f_tab[
                    df_f_tab["particao"].isin(nomes_particoes_gold)
                ]

    _registrar_regra(
        nivel="COLUNA",
        tipo="unicidade_chave_primaria",
        coluna=",".join(chaves_primarias) if chaves_primarias else "*",
        parametro_aprendido=f"PK={chaves_primarias} (0 duplicatas)",
        origem=origem_pk,
        detalhes=f"{detalhe_pk} {tag_treino_gold}",
    )

    # =========================================================================
    # 2. Aprendizado de Volumetria Histórica (Exclusivo sobre Partições GOLD)
    # =========================================================================
    cfg_part = checks_cfg.get("particao", {}) or {}
    cfg_vol = cfg_part.get("volumetria_historica", {}) or {}

    volumes_gold = [len(df_p) for df_p in dfs_gold]
    med_vol, sigma_vol_raw = _mediana_e_sigma_mad([float(v) for v in volumes_gold])
    sigma_vol = max(sigma_vol_raw, med_vol * 0.03, 1.0)
    cv_robusto_vol = sigma_vol / med_vol if med_vol > 0 else 0.05

    desvio_rel_yaml = cfg_vol.get("desvio_relativo_max")
    if desvio_rel_yaml is not None and str(desvio_rel_yaml).lower() != "auto":
        desvio_rel_max = float(desvio_rel_yaml)
        origem_vol = "MANUAL_YAML"
    else:
        desvio_rel_max = float(
            np.clip(round(max(4.5 * cv_robusto_vol, 0.15), 4), 0.15, 0.35)
        )
        origem_vol = "AUTO_APRENDIDO"

    z_yaml = cfg_vol.get("z_score_robusto_max")
    if z_yaml is not None and str(z_yaml).lower() != "auto":
        z_max = float(z_yaml)
    else:
        z_max = 3.0

    lim_inf_vol = max(0, int(round(med_vol * (1.0 - desvio_rel_max))))
    lim_sup_vol = int(round(med_vol * (1.0 + desvio_rel_max)))
    _registrar_regra(
        nivel="PARTICAO",
        tipo="volumetria_historica",
        coluna="*",
        parametro_aprendido=f"[{lim_inf_vol}, {lim_sup_vol}] (±{desvio_rel_max:.0%}, z<={z_max:.1f})",
        origem=origem_vol,
        detalhes=(
            f"Aprendido de {tag_treino_gold}: mediana={med_vol:.0f} linhas, "
            f"sigma_MAD={sigma_vol:.1f} (CV={cv_robusto_vol:.1%}) -> tolerância ±{desvio_rel_max:.0%}"
        ),
    )

    # =========================================================================
    # 3. Aprendizado de Evolução entre Fotografias (Exclusivo sobre Fotos GOLD)
    # =========================================================================
    cfg_foto = cfg_part.get("evolucao_fotografias", {}) or {}
    cols_ev_yaml = cfg_foto.get("colunas_evolucao_positiva")

    cols_evolucao_aprendidas: list[str] = []
    cols_imutaveis_aprendidas: list[str] = []

    if not df_fotos_gold.empty:
        df_hist_ord = df_fotos_gold.sort_values(["particao", "dt_referencia"])
        crescimento_por_col: dict[str, int] = {c: 0 for c in todas_colunas}
        for _, grp_part in df_hist_ord.groupby("particao"):
            if len(grp_part) < 2:
                continue
            linhas_part = grp_part.to_dict("records")
            for i in range(len(linhas_part) - 1):
                qtd_ant = int(linhas_part[i]["qtd_registros"])
                qtd_seg = int(linhas_part[i + 1]["qtd_registros"])
                delta_linhas = max(0, qtd_seg - qtd_ant)
                p_ant = json.loads(str(linhas_part[i]["preenchidos_json"]))
                p_seg = json.loads(str(linhas_part[i + 1]["preenchidos_json"]))
                for c in todas_colunas:
                    delta_col = int(p_seg.get(c, 0)) - int(p_ant.get(c, 0))
                    if delta_col > delta_linhas:
                        crescimento_por_col[c] = crescimento_por_col.get(c, 0) + 1

        for c in todas_colunas:
            if crescimento_por_col.get(c, 0) > 0:
                cols_evolucao_aprendidas.append(c)
            else:
                cols_imutaveis_aprendidas.append(c)

    if isinstance(cols_ev_yaml, list):
        cols_evolucao_final = [
            str(c) for c in cols_ev_yaml if str(c).lower() != "auto"
        ]
        origem_foto = "MANUAL_YAML"
    else:
        cols_evolucao_final = cols_evolucao_aprendidas
        origem_foto = "AUTO_APRENDIDO"

    queda_yaml = cfg_foto.get("queda_max_registros_pct")
    if queda_yaml is not None and str(queda_yaml).lower() != "auto":
        queda_max_pct = float(queda_yaml)
    else:
        queda_max_pct = 0.0

    permitir_att_yaml = cfg_foto.get("permitir_atualizacao_particoes_passadas")
    if permitir_att_yaml is not None and str(permitir_att_yaml).lower() != "auto":
        permitir_att = bool(permitir_att_yaml)
    else:
        permitir_att = len(cols_evolucao_final) > 0

    _registrar_regra(
        nivel="FOTOGRAFIA_DIFF",
        tipo="evolucao_fotografias",
        coluna=",".join(cols_evolucao_final) if cols_evolucao_final else "*",
        parametro_aprendido=(
            f"queda_max=0%, evolucao_positiva={cols_evolucao_final}, "
            f"imutaveis={len(cols_imutaveis_aprendidas)} cols"
        ),
        origem=origem_foto,
        detalhes=(
            f"Aprendido via snapshots GOLD {tag_treino_gold}: "
            f"evolução positiva={cols_evolucao_final}; atualização no passado={permitir_att}"
        ),
    )

    # =========================================================================
    # 4. Aprendizado de Consistência de Data vs. Partição (Sobre Partições GOLD)
    # =========================================================================
    cfg_cons = cfg_part.get("consistencia_data_particao", {}) or {}
    cols_dt_part_yaml = cfg_cons.get("colunas_data_particao")

    aderencia_particao_por_col: dict[str, float] = {}
    cols_dt_part_aprendidas: list[str] = []

    for col in todas_colunas:
        if not _eh_coluna_data_ou_ts(col, dfs_gold[0][col]):
            continue
        if pd.api.types.is_datetime64_any_dtype(dfs_gold[0][col]):
            continue

        taxas_aderencia = []
        for nome_part in nomes_particoes_gold:
            df_p = particoes_dfs[nome_part]
            if col not in df_p.columns or len(df_p) == 0:
                continue
            val_part = nome_part.split("=")[-1]
            if tipo_particao == "mensal" and len(val_part) == 6:
                pref = f"{val_part[:4]}-{val_part[4:6]}"
            elif tipo_particao == "diaria" and len(val_part) == 8:
                pref = f"{val_part[:4]}-{val_part[4:6]}-{val_part[6:8]}"
            else:
                pref = val_part

            s_str = df_p[col].dropna().astype(str)
            if len(s_str) == 0:
                continue
            aderencia = float(s_str.str.startswith(pref).mean())
            taxas_aderencia.append(aderencia)

        if taxas_aderencia:
            med_aderencia = float(np.median(taxas_aderencia))
            aderencia_particao_por_col[col] = med_aderencia
            if med_aderencia >= 0.995:
                cols_dt_part_aprendidas.append(col)

    if isinstance(cols_dt_part_yaml, list):
        cols_dt_part_final = [
            str(c) for c in cols_dt_part_yaml if str(c).lower() != "auto"
        ]
        origem_dt_part = "MANUAL_YAML"
    else:
        cols_dt_part_final = cols_dt_part_aprendidas
        origem_dt_part = "AUTO_APRENDIDO"

    _registrar_regra(
        nivel="PARTICAO",
        tipo="consistencia_data_particao",
        coluna=",".join(cols_dt_part_final) if cols_dt_part_final else "nenhuma",
        parametro_aprendido=f"colunas_safra={cols_dt_part_final}",
        origem=origem_dt_part,
        detalhes=(
            f"Aderência nas partições GOLD {tag_treino_gold}: "
            + ", ".join(f"{c}={v:.1%}" for c, v in aderencia_particao_por_col.items())
        ),
    )

    # =========================================================================
    # 5. Auto-descoberta de Colunas para Maturação de Datas de Evento
    # =========================================================================
    cfg_mat = checks_cfg.get("maturacao_datas", {}) or {}
    mat_ativo_cfg = cfg_mat.get("ativo", "auto")
    cols_mat_cfg = cfg_mat.get("colunas")

    cols_mat_aprendidas: dict[str, dict[str, Any]] = {}
    if tipo_particao == "mensal" and mat_ativo_cfg is not False:
        df_concat_gold = pd.concat(dfs_gold, ignore_index=True)
        for col in todas_colunas:
            if not _eh_coluna_data_ou_ts(col, df_concat_gold[col]):
                continue
            if pd.api.types.is_datetime64_any_dtype(df_concat_gold[col]):
                continue
            s_datas = pd.to_datetime(
                df_concat_gold[col], errors="coerce", format="ISO8601"
            ).dt.date.dropna()
            if len(s_datas) == 0:
                continue
            dias_do_mes_distintos = len({d.day for d in s_datas})
            if dias_do_mes_distintos >= 20:
                cols_mat_aprendidas[col] = {
                    "ativo": True,
                    "janela_avaliacao_dias": "auto",
                    "escopo_particoes": "todas",
                    "tolerancia_desvio_curva": "auto",
                    "min_fotos_aprendizado": 2,
                }

    if isinstance(cols_mat_cfg, dict) and cols_mat_cfg:
        cols_mat_final: dict[str, dict[str, Any]] = {}
        for col_m, p_m in cols_mat_cfg.items():
            if isinstance(p_m, bool):
                if p_m:
                    cols_mat_final[col_m] = cols_mat_aprendidas.get(
                        col_m,
                        {
                            "ativo": True,
                            "janela_avaliacao_dias": "auto",
                            "escopo_particoes": "todas",
                            "tolerancia_desvio_curva": "auto",
                            "min_fotos_aprendizado": 2,
                        },
                    )
            elif isinstance(p_m, dict) and p_m.get("ativo", True):
                cols_mat_final[col_m] = {
                    "ativo": True,
                    "janela_avaliacao_dias": p_m.get("janela_avaliacao_dias", "auto"),
                    "escopo_particoes": p_m.get("escopo_particoes", "todas"),
                    "tolerancia_desvio_curva": p_m.get("tolerancia_desvio_curva", "auto"),
                    "min_fotos_aprendizado": int(p_m.get("min_fotos_aprendizado", 2)),
                }
        origem_mat_cols = "MANUAL_YAML"
    else:
        cols_mat_final = cols_mat_aprendidas
        origem_mat_cols = "AUTO_APRENDIDO"

    mat_ativo_final = (
        bool(mat_ativo_cfg)
        if isinstance(mat_ativo_cfg, bool)
        else (len(cols_mat_final) > 0)
    )

    if mat_ativo_final and cols_mat_final:
        _registrar_regra(
            nivel="MATURACAO_EVENTO",
            tipo="colunas_monitoradas",
            coluna=",".join(cols_mat_final.keys()),
            parametro_aprendido=f"colunas={list(cols_mat_final.keys())} (treino blindado GOLD)",
            origem=origem_mat_cols,
            detalhes=(
                f"Colunas de evento diário contínuo identificadas nas partições GOLD "
                f"{tag_treino_gold}: {list(cols_mat_final.keys())}"
            ),
        )

    # =========================================================================
    # 6. Aprendizado de Completude (Exclusivo sobre Partições e Fotos GOLD)
    # =========================================================================
    cfg_cols = checks_cfg.get("colunas", {}) or {}
    cfg_comp = cfg_cols.get("completude", {}) or {}
    limites_nulos_yaml = cfg_comp.get("limites_nulos_pct")
    if not isinstance(limites_nulos_yaml, dict):
        limites_nulos_yaml = {}

    limites_nulos_final: dict[str, float] = {}
    origens_nulos: dict[str, str] = {}

    for col in todas_colunas:
        if col.lower().startswith(("ts_", "dt_atualizacao")) and col not in limites_nulos_yaml:
            continue

        if col in limites_nulos_yaml and str(limites_nulos_yaml[col]).lower() != "auto":
            lim = float(limites_nulos_yaml[col])
            limites_nulos_final[col] = lim
            origens_nulos[col] = "MANUAL_YAML"
            _registrar_regra(
                nivel="COLUNA",
                tipo="completude",
                coluna=col,
                parametro_aprendido=f"<= {lim:.2%}",
                origem="MANUAL_YAML",
                detalhes=f"Limite de nulos definido no YAML para `{col}`: {lim:.2%}",
            )
            continue

        taxas_nulos_gold = []
        for df_p in dfs_gold:
            if col in df_p.columns and len(df_p) > 0:
                taxas_nulos_gold.append(float(df_p[col].isna().sum()) / len(df_p))

        if not df_fotos_gold.empty:
            for _, r_h in df_fotos_gold.iterrows():
                qtd_h = int(r_h["qtd_registros"])
                if qtd_h > 0:
                    nulos_dict = json.loads(str(r_h["nulos_json"]))
                    if col in nulos_dict:
                        taxas_nulos_gold.append(float(nulos_dict[col]) / qtd_h)

        med_nulos, sigma_nulos = _mediana_e_sigma_mad(taxas_nulos_gold)

        if med_nulos == 0.0:
            lim_aprendido = 0.0
            detalhe_nulo = (
                f"0.00% de nulos nas partições GOLD {tag_treino_gold} -> tolerância zero aprendida (<= 0.00%)"
            )
        else:
            max_gold = max(taxas_nulos_gold)
            # Se a coluna também está em maturação, considera folga natural para a safra aberta
            folga_maturacao = 0.10 if col in cols_mat_final else 0.06
            lim_aprendido = float(
                min(1.0, round(max_gold + max(3.0 * sigma_nulos, folga_maturacao), 4))
            )
            detalhe_nulo = (
                f"Aprendido das partições GOLD {tag_treino_gold} em `{col}`: "
                f"mediana={med_nulos:.2%}, máx_gold={max_gold:.2%} -> limite <= {lim_aprendido:.2%}"
            )

        limites_nulos_final[col] = lim_aprendido
        origens_nulos[col] = "AUTO_APRENDIDO"
        _registrar_regra(
            nivel="COLUNA",
            tipo="completude",
            coluna=col,
            parametro_aprendido=f"<= {lim_aprendido:.2%}",
            origem="AUTO_APRENDIDO",
            detalhes=detalhe_nulo,
        )

    # =========================================================================
    # 7. Aprendizado de Domínio Categórico (Exclusivo sobre Partições GOLD)
    # =========================================================================
    cfg_dom = cfg_cols.get("dominio", {}) or {}
    regras_dom_yaml = cfg_dom.get("regras")
    if not isinstance(regras_dom_yaml, dict):
        regras_dom_yaml = {}

    regras_dom_final: dict[str, list[Any]] = {}
    distribuicao_dom_esperada: dict[str, dict[str, float]] = {}
    origens_dom: dict[str, str] = {}

    for col in todas_colunas:
        if col in chaves_primarias or col.lower().startswith("id_"):
            continue
        if _eh_coluna_data_ou_ts(col, dfs_gold[0][col]):
            continue

        if col in regras_dom_yaml and isinstance(regras_dom_yaml[col], list):
            regras_dom_final[col] = sorted(regras_dom_yaml[col])
            origens_dom[col] = "MANUAL_YAML"
            _registrar_regra(
                nivel="COLUNA",
                tipo="dominio",
                coluna=col,
                parametro_aprendido=f"In {regras_dom_final[col]}",
                origem="MANUAL_YAML",
                detalhes=f"Domínio definido no YAML para `{col}`",
            )
            continue

        s_amostra = dfs_gold[0][col]
        if not (
            pd.api.types.is_string_dtype(s_amostra)
            or pd.api.types.is_object_dtype(s_amostra)
            or isinstance(s_amostra.dtype, pd.CategoricalDtype)
        ):
            continue

        cardinalidades = []
        razoes_unicos = []
        presenca_particoes_gold: dict[Any, int] = {}
        participacoes_por_valor: dict[Any, list[float]] = {}

        for df_p in dfs_gold:
            s_val = df_p[col].dropna()
            if len(s_val) == 0:
                continue
            n_uniq = s_val.nunique()
            cardinalidades.append(n_uniq)
            razoes_unicos.append(n_uniq / len(s_val))

            vc = s_val.value_counts(normalize=True)
            for val_cat, prop in vc.items():
                presenca_particoes_gold[val_cat] = (
                    presenca_particoes_gold.get(val_cat, 0) + 1
                )
                participacoes_por_valor.setdefault(val_cat, []).append(float(prop))

        if not cardinalidades:
            continue

        med_card = float(np.median(cardinalidades))
        med_razao = float(np.median(razoes_unicos))

        if med_card <= 25 and med_razao < 0.25:
            limiar_gold = max(1, int(np.ceil(n_gold * 0.50)))
            categorias_aprendidas = []
            mix_col: dict[str, float] = {}

            for val_cat, qtd_parts_presente in presenca_particoes_gold.items():
                med_prop = float(np.median(participacoes_por_valor[val_cat]))
                if qtd_parts_presente >= limiar_gold and med_prop >= 0.005:
                    categorias_aprendidas.append(val_cat)
                    mix_col[str(val_cat)] = round(med_prop, 4)

            if categorias_aprendidas:
                categorias_ordenadas = sorted(categorias_aprendidas)
                regras_dom_final[col] = categorias_ordenadas
                distribuicao_dom_esperada[col] = mix_col
                origens_dom[col] = "AUTO_APRENDIDO"

                resumo_mix = ", ".join(
                    f"{k}:{v:.0%}" for k, v in sorted(mix_col.items())
                )
                _registrar_regra(
                    nivel="COLUNA",
                    tipo="dominio",
                    coluna=col,
                    parametro_aprendido=f"In {categorias_ordenadas}",
                    origem="AUTO_APRENDIDO",
                    detalhes=(
                        f"Aprendido exclusivamente das partições GOLD {tag_treino_gold}. "
                        f"Mix histórico: [{resumo_mix}]"
                    ),
                )

    # =========================================================================
    # 8. Aprendizado de Intervalo Numérico (Exclusivo sobre Partições GOLD)
    # =========================================================================
    cfg_int = cfg_cols.get("intervalo_numerico", {}) or {}
    regras_int_yaml = cfg_int.get("regras")
    if not isinstance(regras_int_yaml, dict):
        regras_int_yaml = {}

    regras_int_final: dict[str, dict[str, float | int]] = {}
    origens_int: dict[str, str] = {}

    for col in todas_colunas:
        if col in chaves_primarias or col.lower().startswith("id_"):
            continue
        if _eh_coluna_data_ou_ts(col, dfs_gold[0][col]):
            continue

        if col in regras_int_yaml and isinstance(regras_int_yaml[col], dict):
            lim_cfg = regras_int_yaml[col]
            if (
                lim_cfg.get("min") is not None
                and str(lim_cfg.get("min")).lower() != "auto"
                and lim_cfg.get("max") is not None
                and str(lim_cfg.get("max")).lower() != "auto"
            ):
                regras_int_final[col] = {
                    "min": lim_cfg["min"],
                    "max": lim_cfg["max"],
                }
                origens_int[col] = "MANUAL_YAML"
                _registrar_regra(
                    nivel="COLUNA",
                    tipo="intervalo_numerico",
                    coluna=col,
                    parametro_aprendido=f"[{lim_cfg['min']}, {lim_cfg['max']}]",
                    origem="MANUAL_YAML",
                    detalhes=f"Intervalo numérico definido no YAML para `{col}`",
                )
                continue

        s_amostra = dfs_gold[0][col]
        if not pd.api.types.is_numeric_dtype(s_amostra) or pd.api.types.is_bool_dtype(
            s_amostra
        ):
            continue

        eh_inteiro = pd.api.types.is_integer_dtype(s_amostra.dtype)
        mins_gold: list[float] = []
        maxs_gold: list[float] = []

        for df_p in dfs_gold:
            s_num = pd.to_numeric(df_p[col], errors="coerce").dropna()
            if len(s_num) == 0:
                continue
            mins_gold.append(float(s_num.min()))
            maxs_gold.append(float(s_num.max()))

        if not mins_gold:
            continue

        min_estavel = min(mins_gold)
        max_estavel = max(maxs_gold)
        _, sigma_min_raw = _mediana_e_sigma_mad(mins_gold)
        sigma_min = max(sigma_min_raw, abs(min_estavel) * 0.05, 1.0)

        # Inferência do limite inferior (v_min) sobre partições GOLD
        if min_estavel > 0:
            if not eh_inteiro:
                v_min: float | int = 0.01
            elif min_estavel >= 100:
                v_min = int(np.floor(min_estavel / 50.0) * 50)
            else:
                v_min = 0
        else:
            v_min_calc = min_estavel - 3.0 * sigma_min
            v_min = int(round(v_min_calc)) if eh_inteiro else round(v_min_calc, 2)

        # Inferência do limite superior (v_max) sobre partições GOLD
        cv_max_gold = (
            float(np.std(maxs_gold)) / float(np.mean(maxs_gold))
            if np.mean(maxs_gold) > 0
            else 0.0
        )
        if eh_inteiro:
            fator_margem = 1.02 if max_estavel < 500 else 1.0
            passo = 50.0 if max_estavel >= 50 else 10.0
            v_max: float | int = int(
                np.ceil((max_estavel * fator_margem) / passo) * passo
            )
        else:
            if cv_max_gold < 0.05:
                v_max = round(np.ceil((max_estavel * 1.05) / 1000.0) * 1000.0, 2)
            else:
                v_max = round(np.ceil((max_estavel * 1.25) / 1000.0) * 1000.0, 2)

        if col in regras_int_yaml and isinstance(regras_int_yaml[col], dict):
            if (
                regras_int_yaml[col].get("min") is not None
                and str(regras_int_yaml[col].get("min")).lower() != "auto"
            ):
                v_min = regras_int_yaml[col]["min"]
            if (
                regras_int_yaml[col].get("max") is not None
                and str(regras_int_yaml[col].get("max")).lower() != "auto"
            ):
                v_max = regras_int_yaml[col]["max"]

        regras_int_final[col] = {"min": v_min, "max": v_max}
        origens_int[col] = "AUTO_APRENDIDO"
        _registrar_regra(
            nivel="COLUNA",
            tipo="intervalo_numerico",
            coluna=col,
            parametro_aprendido=f"[{v_min}, {v_max}]",
            origem="AUTO_APRENDIDO",
            detalhes=(
                f"Aprendido das partições GOLD {tag_treino_gold} em `{col}`: "
                f"min_gold={min_estavel}, max_gold={max_estavel} -> [{v_min}, {v_max}]"
            ),
        )

    # =========================================================================
    # 9. Aprendizado de Formato / Regex (Exclusivo sobre Partições GOLD)
    # =========================================================================
    cfg_reg = cfg_cols.get("formato_regex", {}) or {}
    regras_reg_yaml = cfg_reg.get("regras")
    if not isinstance(regras_reg_yaml, dict):
        regras_reg_yaml = {}

    regras_reg_final: dict[str, dict[str, Any]] = {}
    origens_reg: dict[str, str] = {}

    for col in todas_colunas:
        if col in chaves_primarias or col.lower().startswith("id_"):
            continue
        if col in regras_dom_final or col in regras_int_final:
            continue
        if _eh_coluna_data_ou_ts(col, dfs_gold[0][col]):
            continue

        if col in regras_reg_yaml and isinstance(regras_reg_yaml[col], dict):
            padrao_y = regras_reg_yaml[col].get("padrao")
            if padrao_y and str(padrao_y).lower() != "auto":
                regras_reg_final[col] = {
                    "padrao": str(padrao_y),
                    "rejeitar_digitos_repetidos": bool(
                        regras_reg_yaml[col].get("rejeitar_digitos_repetidos", True)
                    ),
                }
                origens_reg[col] = "MANUAL_YAML"
                _registrar_regra(
                    nivel="COLUNA",
                    tipo="formato_regex",
                    coluna=col,
                    parametro_aprendido=f"Regex {padrao_y}",
                    origem="MANUAL_YAML",
                    detalhes=f"Padrão Regex definido no YAML para `{col}`",
                )
                continue

        s_amostra = dfs_gold[0][col]
        if not (
            pd.api.types.is_string_dtype(s_amostra)
            or pd.api.types.is_object_dtype(s_amostra)
        ):
            continue

        comprimentos_medianos = []
        taxas_digitos_fixos = []
        taxas_nao_repetidos = []

        for df_p in dfs_gold:
            s_str = df_p[col].dropna().astype(str)
            if len(s_str) == 0:
                continue
            lens = s_str.str.len()
            tam_med = int(np.median(lens))
            comprimentos_medianos.append(tam_med)
            taxa_dig = float((s_str.str.isdigit() & (lens == tam_med)).mean())
            taxas_digitos_fixos.append(taxa_dig)
            taxa_nao_rep = float((s_str.apply(lambda x: len(set(x)) > 1)).mean())
            taxas_nao_repetidos.append(taxa_nao_rep)

        if taxas_digitos_fixos and float(np.median(taxas_digitos_fixos)) >= 0.98:
            tam_aprendido = int(np.median(comprimentos_medianos))
            padrao_aprendido = f"^\\d{{{tam_aprendido}}}$"
            rejeitar_rep_aprendido = float(np.median(taxas_nao_repetidos)) >= 0.995

            regras_reg_final[col] = {
                "padrao": padrao_aprendido,
                "rejeitar_digitos_repetidos": rejeitar_rep_aprendido,
            }
            origens_reg[col] = "AUTO_APRENDIDO"
            _registrar_regra(
                nivel="COLUNA",
                tipo="formato_regex",
                coluna=col,
                parametro_aprendido=(
                    f"Regex {padrao_aprendido} (rejeitar_repetidos={rejeitar_rep_aprendido})"
                ),
                origem="AUTO_APRENDIDO",
                detalhes=(
                    f"Aprendido das partições GOLD {tag_treino_gold} para `{col}`: "
                    f"{tam_aprendido} dígitos numéricos (aderência={np.median(taxas_digitos_fixos):.2%})"
                ),
            )

    return {
        "chaves_primarias": chaves_primarias,
        "particoes_gold_treino": nomes_particoes_gold,
        "origem_chaves_primarias": origem_pk,
        "particao": {
            "volumetria_historica": {
                "ativo": cfg_vol.get("ativo", True),
                "mediana_gold": med_vol,
                "sigma_gold": sigma_vol,
                "desvio_relativo_max": desvio_rel_max,
                "z_score_robusto_max": z_max,
                "origem": origem_vol,
            },
            "evolucao_fotografias": {
                "ativo": cfg_foto.get("ativo", True),
                "permitir_atualizacao_particoes_passadas": permitir_att,
                "queda_max_registros_pct": queda_max_pct,
                "colunas_evolucao_positiva": cols_evolucao_final,
                "colunas_imutaveis": cols_imutaveis_aprendidas,
                "origem": origem_foto,
            },
            "consistencia_data_particao": {
                "ativo": cfg_cons.get("ativo", True),
                "colunas_data_particao": cols_dt_part_final,
                "origem": origem_dt_part,
            },
        },
        "maturacao_datas": {
            "ativo": mat_ativo_final,
            "colunas": cols_mat_final,
            "origem": origem_mat_cols,
        },
        "colunas": {
            "completude": {
                "ativo": cfg_comp.get("ativo", True),
                "limites_nulos_pct": limites_nulos_final,
                "origens": origens_nulos,
            },
            "unicidade": {
                "ativo": cfg_cols.get("unicidade", {}).get("ativo", True),
                "verificar_chave_primaria": cfg_cols.get("unicidade", {}).get(
                    "verificar_chave_primaria", True
                ),
                "verificar_linha_completa": cfg_cols.get("unicidade", {}).get(
                    "verificar_linha_completa", True
                ),
                "origem": origem_pk,
            },
            "dominio": {
                "ativo": cfg_dom.get("ativo", True),
                "regras": regras_dom_final,
                "distribuicao_esperada": distribuicao_dom_esperada,
                "origens": origens_dom,
            },
            "intervalo_numerico": {
                "ativo": cfg_int.get("ativo", True),
                "regras": regras_int_final,
                "origens": origens_int,
            },
            "formato_regex": {
                "ativo": cfg_reg.get("ativo", True) if regras_reg_final else False,
                "regras": regras_reg_final,
                "origens": origens_reg,
            },
        },
        "registros_regras_silver": registros_silver,
    }
