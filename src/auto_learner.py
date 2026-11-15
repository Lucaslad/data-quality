"""
Motor de Auto-Aprendizado Blindado (Gold-Only Self-Learning Rules) para Data Quality.

Garante que o aprendizado estatístico do motor seja calculado EXCLUSIVAMENTE sobre
partições e fotografias saudáveis (classificadas como `GOLD`), impedindo que partições
com anomalias (`SILVER` ou `BRONZE`) contaminem os limiares aprendidos (*Data Poisoning*)
e evitando vazamento temporal (*Data Leakage*) da partição recém-chegada.

Suporta **Modo Incremental de Produção e Streaming**:
- Opera integralmente sobre os perfis estatísticos (`perfis_particoes`), sem exigir
  que todos os DataFrames Parquet permaneçam carregados em memória RAM.
"""

from __future__ import annotations

from collections import Counter
import datetime as dt
from itertools import combinations
import json
import re
from typing import Any

import numpy as np
import pandas as pd

from src.snapshot_manager import (
    SnapshotManager,
    eh_coluna_id_candidata,
    eh_coluna_id_primaria_estrita,
)
from src.stats_utils import (
    calcular_passo_arredondamento_dinamico,
    mediana_e_sigma_mad,
)

REGEX_DATA_ISO = re.compile(r"^\d{4}-\d{2}-\d{2}")


def _mediana_e_sigma_mad(valores: list[float]) -> tuple[float, float]:
    """Wrapper mantendo compatibilidade direta com chamadas internas."""
    return mediana_e_sigma_mad(valores)


def _extrair_data_particao_diaria(nome_particao: str) -> dt.date | None:
    """Extrai `datetime.date` de uma partição no formato `col=YYYYMMDD`."""
    val = nome_particao.split("=")[-1].strip()
    if len(val) == 8 and val.isdigit():
        try:
            return dt.date(int(val[:4]), int(val[4:6]), int(val[6:8]))
        except ValueError:
            return None
    return None


def _extrair_perfis_deserializados(
    perfis_particoes: dict[str, dict[str, Any]],
) -> dict[str, dict[str, Any]]:
    """Deserializa os campos JSON dos perfis de partição para acesso rápido no aprendizado."""
    resultado: dict[str, dict[str, Any]] = {}
    for nome_p, perf in perfis_particoes.items():
        qtd = int(perf.get("qtd_registros", 0))
        if qtd <= 0:
            continue
        resultado[nome_p] = {
            "qtd_registros": qtd,
            "preenchidos": json.loads(str(perf.get("preenchidos_json", "{}"))),
            "nulos": json.loads(str(perf.get("nulos_json", "{}"))),
            "somas_num": json.loads(str(perf.get("somas_numericas_json", "{}"))),
            "mins_num": json.loads(str(perf.get("mins_numericos_json", "{}"))),
            "maxs_num": json.loads(str(perf.get("maxs_numericos_json", "{}"))),
            "dominios": json.loads(str(perf.get("dominios_json", "{}"))),
            "perfil_dq": json.loads(str(perf.get("perfil_dq_json", "{}"))),
        }
    return resultado


def _descobrir_chaves_primarias_automaticamente(
    todas_colunas: list[str],
    nomes_particoes_ref: list[str],
    perfis_des: dict[str, dict[str, Any]],
    particoes_dfs: dict[str, pd.DataFrame],
) -> list[str]:
    """
    Auto-descobre a chave primária (simples ou composta) avaliando:
    1. Colunas estritas de ID (`id_*`, `*_id`, `id`) 100% únicas e com 0% de nulos.
    2. Outras colunas candidatas (`cod_*`, `num_*`, `nr_*`, `pk_*`, `chave_*`) 100% únicas e 0% nulos.
    3. Combinações compostas (pares de colunas candidatas) que juntas sejam 100% únicas e sem nulos.
    """
    if not nomes_particoes_ref:
        return []

    def _coluna_unica_sem_nulos(col: str) -> bool:
        taxas_unicidade: list[float] = []
        taxas_nulos: list[float] = []
        for p in nomes_particoes_ref:
            info_p = perfis_des[p]
            qtd_p = info_p["qtd_registros"]
            if qtd_p <= 0:
                continue
            unic_c = info_p["perfil_dq"].get("unicidade_cols", {}).get(col)
            if unic_c is not None:
                n_nulos = int(unic_c.get("n_nulos", 0))
                n_unicos = int(unic_c.get("n_unicos", 0))
            elif p in particoes_dfs and col in particoes_dfs[p].columns:
                s = particoes_dfs[p][col]
                n_nulos = int(s.isna().sum())
                n_unicos = int(s.dropna().nunique())
            else:
                continue

            taxas_nulos.append(n_nulos / qtd_p)
            n_preench = qtd_p - n_nulos
            if n_preench > 0:
                taxas_unicidade.append(n_unicos / n_preench)

        return (
            bool(taxas_unicidade)
            and float(np.median(taxas_nulos)) == 0.0
            and float(np.median(taxas_unicidade)) == 1.0
        )

    # Passo 1: Prioriza colunas `id_*` / `*_id`
    cols_estritas = [c for c in todas_colunas if eh_coluna_id_primaria_estrita(c)]
    for col in cols_estritas:
        if _coluna_unica_sem_nulos(col):
            return [col]

    # Passo 2: Avalia colunas candidatas expandidas (`cod_*`, `num_*`, `nr_*`, `pk_*`, etc.)
    cols_expandidas = [
        c
        for c in todas_colunas
        if eh_coluna_id_candidata(c) and c not in cols_estritas
    ]
    for col in cols_expandidas:
        if _coluna_unica_sem_nulos(col):
            return [col]

    # Passo 3: Avalia chave primária composta (pares de colunas candidatas)
    todas_candidatas = (cols_estritas + cols_expandidas)[:6]
    for col_a, col_b in combinations(todas_candidatas, 2):
        chave_par = f"{col_a},{col_b}"
        taxas_nulos_par: list[float] = []
        taxas_unic_par: list[float] = []
        for p in nomes_particoes_ref:
            info_p = perfis_des[p]
            qtd_p = info_p["qtd_registros"]
            if qtd_p <= 0:
                continue
            u_comp = info_p["perfil_dq"].get("unicidade_composta", {}).get(chave_par)
            if u_comp is not None:
                n_nulos = int(u_comp.get("n_nulos", 0))
                n_unicos = int(u_comp.get("n_unicos", 0))
            elif (
                p in particoes_dfs
                and col_a in particoes_dfs[p].columns
                and col_b in particoes_dfs[p].columns
            ):
                df_p = particoes_dfs[p]
                n_nulos = int(df_p[[col_a, col_b]].isna().any(axis=1).sum())
                df_sn = df_p.dropna(subset=[col_a, col_b])
                qtd_dup = int(df_sn.duplicated(subset=[col_a, col_b]).sum())
                n_unicos = int(len(df_sn) - qtd_dup)
            else:
                continue

            taxas_nulos_par.append(n_nulos / qtd_p)
            n_preench = qtd_p - n_nulos
            if n_preench > 0:
                taxas_unic_par.append(n_unicos / n_preench)

        if (
            taxas_unic_par
            and float(np.median(taxas_nulos_par)) == 0.0
            and float(np.median(taxas_unic_par)) == 1.0
        ):
            return [col_a, col_b]

    return []


def _filtrar_particoes_gold_para_treino(
    perfis_des: dict[str, dict[str, Any]],
    particoes_dfs: dict[str, pd.DataFrame],
    chaves_primarias: list[str],
    dt_referencia: str | None = None,
    tabela: str | None = None,
    df_fotos_particoes_hist: pd.DataFrame | None = None,
) -> tuple[list[str], bool]:
    """
    Estágio 1: Identifica quais partições da tabela estão 100% saudáveis (candidatas `GOLD`)
    para que partições `SILVER` ou `BRONZE` não contaminem a calibração dos limiares.

    Prevenção de *Data Leakage* Temporal (Item 1.4):
    - Prioriza partições históricas já validadas (`dt_referencia < dt_referencia_atual`) ou
      anteriores à última partição recém-chegada para estimar as medianas e MAD de referência.
    - Retorna `(particoes_gold, em_cold_start_ou_fallback)`. Se todas as partições estiverem
      contaminadas ou houver menos de 2 partições GOLD, sinaliza `em_cold_start_ou_fallback = True`.
    """
    nomes_particoes = sorted(perfis_des.keys())
    if not nomes_particoes:
        return [], True

    prim_dq = perfis_des[nomes_particoes[0]]["perfil_dq"]
    todas_colunas: list[str] = prim_dq.get("colunas", [])
    tipos_colunas: dict[str, str] = prim_dq.get("tipos_colunas", {})

    # Determina o subconjunto de referência histórica (sem a partição recém-chegada) para calcular
    # medianas/MAD sem vazamento temporal (data leakage)
    particoes_hist_gold_conhecidas: list[str] = []
    if (
        df_fotos_particoes_hist is not None
        and not df_fotos_particoes_hist.empty
        and tabela
        and dt_referencia
    ):
        df_h = df_fotos_particoes_hist[
            (df_fotos_particoes_hist["tabela"] == tabela)
            & (df_fotos_particoes_hist["dt_referencia"] < dt_referencia)
        ]
        if not df_h.empty and "camada_qualidade" in df_h.columns:
            ult_por_p = df_h.sort_values("dt_referencia").groupby("particao").last()
            particoes_hist_gold_conhecidas = [
                str(p)
                for p, row in ult_por_p.iterrows()
                if str(row.get("camada_qualidade", "")) == "GOLD"
                and str(p) in perfis_des
                and str(p) < nomes_particoes[-1]
            ]

    if len(particoes_hist_gold_conhecidas) >= 2:
        particoes_base_stats = sorted(particoes_hist_gold_conhecidas)
    elif len(nomes_particoes) >= 3:
        # Exclui a última partição (recém-chegada) do cálculo inicial de mediana/MAD
        particoes_base_stats = nomes_particoes[:-1]
    else:
        particoes_base_stats = nomes_particoes

    particoes_contaminadas: set[str] = set()

    # 1. Verifica duplicidade ou NULOS em chave primária (simples ou composta) ou linha inteira
    for p in nomes_particoes:
        info_p = perfis_des[p]
        pdq = info_p["perfil_dq"]
        if int(pdq.get("qtd_dup_linha", 0)) > 0:
            particoes_contaminadas.add(p)
            continue

        pks_pres = [pk for pk in chaves_primarias if pk in pdq.get("colunas", todas_colunas)]
        if not pks_pres:
            continue

        # Nulo em qualquer coluna de PK contamina a partição imediatamente (Item 1.1)
        tem_nulo_pk = any(
            int(info_p["nulos"].get(pk, 0)) > 0 for pk in pks_pres
        )
        if tem_nulo_pk:
            particoes_contaminadas.add(p)
            continue

        if len(pks_pres) == 1:
            pk = pks_pres[0]
            unic_cols = pdq.get("unicidade_cols", {})
            if pk in unic_cols and int(unic_cols[pk].get("qtd_dup", 0)) > 0:
                particoes_contaminadas.add(p)
            elif p in particoes_dfs and pk in particoes_dfs[p].columns:
                df_p = particoes_dfs[p]
                if int(df_p.dropna(subset=[pk]).duplicated(subset=[pk]).sum()) > 0:
                    particoes_contaminadas.add(p)
        else:
            # Chave primária composta (Item 1.2): avalia duplicidade conjunta da tupla
            chave_comp = ",".join(pks_pres)
            u_comp = pdq.get("unicidade_composta", {}).get(chave_comp)
            if u_comp is not None:
                if int(u_comp.get("qtd_dup", 0)) > 0 or int(u_comp.get("n_nulos", 0)) > 0:
                    particoes_contaminadas.add(p)
            elif p in particoes_dfs and all(k in particoes_dfs[p].columns for k in pks_pres):
                df_p = particoes_dfs[p]
                if int(df_p.dropna(subset=pks_pres).duplicated(subset=pks_pres).sum()) > 0:
                    particoes_contaminadas.add(p)

    #Se só há 1 partição na tabela, retorna após checagem intrínseca de PK/duplicatas (Cold Start)
    if len(nomes_particoes) == 1:
        particoes_gold_un = [
            p for p in nomes_particoes if p not in particoes_contaminadas
        ]
        return particoes_gold_un, True

    # 2. Verifica quedas ou picos de volumetria na partição (usando estatísticas livres de leakage
    # e respeitando eventual sazonalidade semanal Dia Útil vs. Fim de Semana)
    volumes_ref = [float(perfis_des[p]["qtd_registros"]) for p in particoes_base_stats]
    med_vol, sigma_vol_raw = _mediana_e_sigma_mad(volumes_ref)
    sigma_vol = max(sigma_vol_raw, med_vol * 0.03, 1.0)

    vols_util_all: list[float] = []
    vols_fds_all: list[float] = []
    datas_part_map: dict[str, dt.date] = {}
    for p in nomes_particoes:
        dt_p = _extrair_data_particao_diaria(p)
        if dt_p is not None:
            datas_part_map[p] = dt_p
            v_p = float(perfis_des[p]["qtd_registros"])
            if dt_p.weekday() < 5:
                vols_util_all.append(v_p)
            else:
                vols_fds_all.append(v_p)

    usar_sazonal_filtro = False
    med_util_f, sig_util_f = med_vol, sigma_vol
    med_fds_f, sig_fds_f = med_vol, sigma_vol
    if len(vols_util_all) >= 2 and len(vols_fds_all) >= 2:
        mu, su_raw = _mediana_e_sigma_mad(vols_util_all)
        mf, sf_raw = _mediana_e_sigma_mad(vols_fds_all)
        if med_vol > 0 and (abs(mu - mf) / med_vol) >= 0.15:
            usar_sazonal_filtro = True
            med_util_f = mu
            sig_util_f = max(su_raw, mu * 0.03, 1.0)
            med_fds_f = mf
            sig_fds_f = max(sf_raw, mf * 0.03, 1.0)

    for p in nomes_particoes:
        qtd = float(perfis_des[p]["qtd_registros"])
        if usar_sazonal_filtro and p in datas_part_map:
            if datas_part_map[p].weekday() < 5:
                m_ref, s_ref = med_util_f, sig_util_f
            else:
                m_ref, s_ref = med_fds_f, sig_fds_f
        else:
            m_ref, s_ref = med_vol, sigma_vol

        desvio_rel = abs(qtd - m_ref) / m_ref if m_ref > 0 else 0.0
        z_rob = abs(qtd - m_ref) / s_ref
        if desvio_rel > 0.15 and z_rob > 3.0:
            particoes_contaminadas.add(p)

    # 3. Verifica nulos em colunas que têm mediana 0% de nulos no histórico base
    for col in todas_colunas:
        if col.lower().startswith(("ts_", "dt_atualizacao")):
            continue
        taxas_nulos_ref = [
            float(perfis_des[p]["nulos"].get(col, 0)) / perfis_des[p]["qtd_registros"]
            for p in particoes_base_stats
        ]
        if taxas_nulos_ref and float(np.median(taxas_nulos_ref)) == 0.0:
            for p in nomes_particoes:
                tx = (
                    float(perfis_des[p]["nulos"].get(col, 0))
                    / perfis_des[p]["qtd_registros"]
                )
                if tx > 0.0:
                    particoes_contaminadas.add(p)

    # 4. Verifica consistência de data com o prefixo da partição
    for col in todas_colunas:
        if tipos_colunas.get(col) != "data_ts":
            continue
        aderencias_ref: list[float] = []
        aderencias_todas: dict[str, float] = {}
        for p in nomes_particoes:
            ad_col = (
                perfis_des[p]["perfil_dq"]
                .get("aderencia_data_particao", {})
                .get(col, {})
            )
            if int(ad_col.get("total_validos", 0)) > 0:
                val_ad = float(ad_col.get("aderencia", 1.0))
                aderencias_todas[p] = val_ad
                if p in particoes_base_stats:
                    aderencias_ref.append(val_ad)
        if aderencias_ref and float(np.median(aderencias_ref)) >= 0.995:
            for p, ad in aderencias_todas.items():
                if ad < 1.0:
                    particoes_contaminadas.add(p)

    # 5. Verifica categorias espúrias (presentes em < 50% das partições de referência)
    n_ref = len(particoes_base_stats)
    limiar_presenca = max(1, int(np.ceil(n_ref * 0.50)))
    for col in todas_colunas:
        if col in chaves_primarias or eh_coluna_id_primaria_estrita(col):
            continue
        if tipos_colunas.get(col) != "string_cat":
            continue
        cards = [
            len(perfis_des[p]["dominios"].get(col, {}))
            for p in particoes_base_stats
            if col in perfis_des[p]["dominios"]
        ]
        if not cards or float(np.median(cards)) > 25:
            continue
        contagem_parts_cat: dict[str, int] = {}
        for p in particoes_base_stats:
            for v in perfis_des[p]["dominios"].get(col, {}).keys():
                contagem_parts_cat[v] = contagem_parts_cat.get(v, 0) + 1
        cats_validas = {
            v for v, cnt in contagem_parts_cat.items() if cnt >= limiar_presenca
        }
        if cats_validas:
            for p in nomes_particoes:
                vals_p = set(perfis_des[p]["dominios"].get(col, {}).keys())
                if vals_p and not vals_p.issubset(cats_validas):
                    particoes_contaminadas.add(p)

    # 6. Verifica valores negativos/zero indevidos ou outliers numéricos extremos
    for col in todas_colunas:
        if col in chaves_primarias or eh_coluna_id_primaria_estrita(col):
            continue
        if tipos_colunas.get(col) not in ("numerico_int", "numerico_float"):
            continue
        mins_p: dict[str, float] = {}
        logs_max_p: dict[str, float] = {}
        for p in nomes_particoes:
            if col in perfis_des[p]["mins_num"] and col in perfis_des[p]["maxs_num"]:
                v_min_p = float(perfis_des[p]["mins_num"][col])
                v_max_p = float(perfis_des[p]["maxs_num"][col])
                mins_p[p] = v_min_p
                logs_max_p[p] = float(np.log10(max(1.0, abs(v_max_p))))
        mins_ref = [mins_p[p] for p in particoes_base_stats if p in mins_p]
        logs_ref = [logs_max_p[p] for p in particoes_base_stats if p in logs_max_p]
        if mins_ref:
            med_min, sig_min_raw = _mediana_e_sigma_mad(mins_ref)
            sig_min = max(sig_min_raw, abs(med_min) * 0.05, 1.0)
            med_lmax, sig_lmax_raw = _mediana_e_sigma_mad(logs_ref)
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
    if not particoes_gold:
        # Todas as partições estão contaminadas -> sinaliza fallback/cold-start
        return [], True

    em_cold_start = len(particoes_gold) < 2
    return particoes_gold, em_cold_start


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
    perfis_particoes: dict[str, dict[str, Any]] | None = None,
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

    perfis_brutos: dict[str, dict[str, Any]] = {}
    if perfis_particoes:
        perfis_brutos.update(perfis_particoes)
    else:
        for nome_p, df_p in sorted(particoes_dfs.items()):
            if len(df_p) > 0:
                perfis_brutos[nome_p] = SnapshotManager.construir_perfil_particao(
                    nome_particao=nome_p,
                    df_part=df_p,
                    tipo_particao=tipo_particao,
                )

    perfis_des = _extrair_perfis_deserializados(perfis_brutos)
    nomes_todas_particoes = sorted(perfis_des.keys())
    n_total_particoes = len(nomes_todas_particoes)
    if n_total_particoes == 0:
        return {"registros_regras_silver": []}

    prim_dq = perfis_des[nomes_todas_particoes[0]]["perfil_dq"]
    todas_colunas: list[str] = prim_dq.get("colunas", [])
    tipos_colunas: dict[str, str] = prim_dq.get("tipos_colunas", {})

    # Partições de referência (excluindo a última se houver histórico) para auto-descoberta de PK sem leakage
    particoes_ref_pk = (
        nomes_todas_particoes[:-1]
        if len(nomes_todas_particoes) >= 2
        else nomes_todas_particoes
    )

    # =========================================================================
    # 1. Auto-descoberta de Chaves Primárias (Simples e Compostas)
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
        chaves_primarias = _descobrir_chaves_primarias_automaticamente(
            todas_colunas=todas_colunas,
            nomes_particoes_ref=particoes_ref_pk,
            perfis_des=perfis_des,
            particoes_dfs=particoes_dfs,
        )
        origem_pk = "AUTO_APRENDIDO"
        detalhe_pk = (
            f"Chave primária auto-descoberta (100% única nas partições GOLD e 0% nulos): {chaves_primarias}"
        )

    # =========================================================================
    # Estágio 1 -> Seleciona APENAS as partições e fotografias GOLD para o treino!
    # =========================================================================
    nomes_particoes_gold_raw, em_cold_start = _filtrar_particoes_gold_para_treino(
        perfis_des=perfis_des,
        particoes_dfs=particoes_dfs,
        chaves_primarias=chaves_primarias,
        dt_referencia=dt_referencia,
        tabela=tabela,
        df_fotos_particoes_hist=df_fotos_particoes_hist,
    )

    todas_contaminadas = len(nomes_particoes_gold_raw) == 0
    nomes_particoes_gold = (
        nomes_particoes_gold_raw
        if nomes_particoes_gold_raw
        else nomes_todas_particoes
    )
    n_gold = len(nomes_particoes_gold_raw)
    if todas_contaminadas:
        tag_treino_gold = (
            f"(ALERTA COLD START: 0/{n_total_particoes} partições GOLD; usando fallback conservador)"
        )
    elif em_cold_start:
        tag_treino_gold = f"(COLD START: {n_gold}/{n_total_particoes} partição GOLD)"
    else:
        tag_treino_gold = f"({n_gold}/{n_total_particoes} partições GOLD)"

    # Filtra histórico de fotografias de partição apenas para partições GOLD (< dt_referencia)
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
        parametro_aprendido=f"PK={chaves_primarias} (0 duplicatas, 0 nulos)",
        origem=origem_pk,
        detalhes=f"{detalhe_pk} {tag_treino_gold}",
    )

    # =========================================================================
    # 1.B Aprendizado de Schema Esperado / Contrato (Item 2.1: Schema Drift)
    # =========================================================================
    cfg_part = checks_cfg.get("particao", {}) or {}
    cfg_cols = checks_cfg.get("colunas", {}) or {}
    cfg_schema = (
        cfg_part.get("consistencia_schema")
        or cfg_cols.get("consistencia_schema")
        or {}
    )
    schema_yaml = cfg_schema.get("schema_esperado")
    if isinstance(schema_yaml, dict) and schema_yaml:
        schema_esperado = {str(k): str(v) for k, v in schema_yaml.items()}
        origem_schema = "MANUAL_YAML"
    else:
        contagem_col_gold: Counter[str] = Counter()
        tipos_por_col_gold: dict[str, Counter[str]] = {}
        for p_g in nomes_particoes_gold:
            pdq_g = perfis_des[p_g]["perfil_dq"]
            cols_g = pdq_g.get("colunas", [])
            t_map_g = pdq_g.get("tipos_colunas", {})
            for c_g in cols_g:
                contagem_col_gold[c_g] += 1
                tipos_por_col_gold.setdefault(c_g, Counter())[
                    str(t_map_g.get(c_g, "outro"))
                ] += 1

        limiar_schema = max(1, int(np.ceil(len(nomes_particoes_gold) * 0.50)))
        schema_esperado = {}
        for c_g, cnt_g in contagem_col_gold.items():
            if cnt_g >= limiar_schema:
                tipo_modal = tipos_por_col_gold[c_g].most_common(1)[0][0]
                schema_esperado[c_g] = tipo_modal
        origem_schema = "AUTO_APRENDIDO"

    _registrar_regra(
        nivel="PARTICAO",
        tipo="consistencia_schema",
        coluna="*",
        parametro_aprendido=f"{len(schema_esperado)} colunas ({', '.join(f'{k}:{v}' for k, v in list(schema_esperado.items())[:5])}...)",
        origem=origem_schema,
        detalhes=f"Contrato de schema aprendido de {tag_treino_gold}: {len(schema_esperado)} colunas obrigatórias",
    )

    # =========================================================================
    # 2. Aprendizado de Volumetria Histórica (Janela Móvel + Sazonalidade Semanal)
    # =========================================================================
    cfg_vol = cfg_part.get("volumetria_historica", {}) or {}
    janela_hist_cfg = cfg_vol.get("janela_historico_particoes", "auto")
    if janela_hist_cfg is not None and str(janela_hist_cfg).lower() != "auto":
        janela_hist = max(1, int(janela_hist_cfg))
    else:
        janela_hist = 30 if tipo_particao == "diaria" else 12

    particoes_gold_janela = nomes_particoes_gold[-janela_hist:]

    volumes_gold = [
        float(perfis_des[p]["qtd_registros"]) for p in particoes_gold_janela
    ]
    med_vol, sigma_vol_raw = _mediana_e_sigma_mad(volumes_gold)
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

    desvio_warn_yaml = cfg_vol.get("desvio_relativo_warning")
    if desvio_warn_yaml is not None and str(desvio_warn_yaml).lower() != "auto":
        desvio_rel_warning = float(desvio_warn_yaml)
    else:
        desvio_rel_warning = round(desvio_rel_max * 0.80, 4)

    z_yaml = cfg_vol.get("z_score_robusto_max")
    if z_yaml is not None and str(z_yaml).lower() != "auto":
        z_max = float(z_yaml)
    else:
        z_max = 3.0

    z_warn_yaml = cfg_vol.get("z_score_robusto_warning")
    if z_warn_yaml is not None and str(z_warn_yaml).lower() != "auto":
        z_warning = float(z_warn_yaml)
    else:
        z_warning = round(max(2.0, z_max * 0.80), 2)

    # Sazonalidade Semanal (Dia Útil vs. Fim de Semana) para partições diárias (Item 2.5)
    sazonalidade_cfg = cfg_vol.get("sazonalidade_semanal", "auto")
    baselines_sazonais: dict[str, dict[str, float]] = {}
    sazonalidade_ativa = False

    if tipo_particao == "diaria" and sazonalidade_cfg is not False:
        vols_util: list[float] = []
        vols_fds: list[float] = []
        for p_g in particoes_gold_janela:
            dt_p = _extrair_data_particao_diaria(p_g)
            if dt_p is None:
                continue
            v_p = float(perfis_des[p_g]["qtd_registros"])
            if dt_p.weekday() < 5:
                vols_util.append(v_p)
            else:
                vols_fds.append(v_p)

        if len(vols_util) >= 2 and len(vols_fds) >= 2:
            med_u, sig_u_raw = _mediana_e_sigma_mad(vols_util)
            med_f, sig_f_raw = _mediana_e_sigma_mad(vols_fds)
            sig_u = max(sig_u_raw, med_u * 0.03, 1.0)
            sig_f = max(sig_f_raw, med_f * 0.03, 1.0)
            baselines_sazonais = {
                "UTIL": {"mediana": med_u, "sigma": sig_u},
                "FDS": {"mediana": med_f, "sigma": sig_f},
            }
            diff_sazonal_rel = (
                abs(med_u - med_f) / med_vol if med_vol > 0 else 0.0
            )
            if isinstance(sazonalidade_cfg, bool):
                sazonalidade_ativa = sazonalidade_cfg
            else:
                sazonalidade_ativa = diff_sazonal_rel >= 0.15

    lim_inf_vol = max(0, int(round(med_vol * (1.0 - desvio_rel_max))))
    lim_sup_vol = int(round(med_vol * (1.0 + desvio_rel_max)))
    info_sazonal_str = (
        f", sazonalidade=ATIVA(UTIL={baselines_sazonais['UTIL']['mediana']:.0f}, FDS={baselines_sazonais['FDS']['mediana']:.0f})"
        if sazonalidade_ativa and baselines_sazonais
        else ""
    )
    _registrar_regra(
        nivel="PARTICAO",
        tipo="volumetria_historica",
        coluna="*",
        parametro_aprendido=(
            f"[{lim_inf_vol}, {lim_sup_vol}] (±{desvio_rel_max:.0%}, z<={z_max:.1f}, janela={janela_hist})"
        ),
        origem=origem_vol,
        detalhes=(
            f"Aprendido de {tag_treino_gold} (janela={len(particoes_gold_janela)}/{janela_hist}): "
            f"mediana={med_vol:.0f} linhas, sigma_MAD={sigma_vol:.1f} (CV={cv_robusto_vol:.1%}) "
            f"-> tolerância ±{desvio_rel_max:.0%}{info_sazonal_str}"
        ),
    )

    # =========================================================================
    # 2.B Configuração de Freshness / SLA e Lacunas de Partição (Item 2.2)
    # =========================================================================
    cfg_fresh = cfg_part.get("freshness_lacunas", {}) or {}
    fresh_ativo = bool(cfg_fresh.get("ativo", True))
    sla_cfg = cfg_fresh.get("sla_atraso_max_particoes", "auto")
    if sla_cfg is not None and str(sla_cfg).lower() != "auto":
        sla_max_part = int(sla_cfg)
        origem_fresh = "MANUAL_YAML"
    else:
        sla_max_part = 0
        origem_fresh = "AUTO_APRENDIDO"

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

        ult_fotos_gold = df_hist_ord.groupby("particao").last().reset_index()
        for _, r_ult in ult_fotos_gold.iterrows():
            p_nome = str(r_ult["particao"])
            if p_nome not in nomes_particoes_gold:
                continue
            qtd_ant = int(r_ult["qtd_registros"])
            qtd_atu = int(perfis_des[p_nome]["qtd_registros"])
            delta_linhas = max(0, qtd_atu - qtd_ant)
            p_ant = json.loads(str(r_ult["preenchidos_json"]))
            p_atu = perfis_des[p_nome]["preenchidos"]
            for c in todas_colunas:
                delta_col = int(p_atu.get(c, 0)) - int(p_ant.get(c, 0))
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
        if tipos_colunas.get(col) != "data_ts":
            continue

        taxas_aderencia = []
        for nome_part in nomes_particoes_gold:
            ad_info = (
                perfis_des[nome_part]["perfil_dq"]
                .get("aderencia_data_particao", {})
                .get(col, {})
            )
            if int(ad_info.get("total_validos", 0)) > 0:
                taxas_aderencia.append(float(ad_info.get("aderencia", 1.0)))

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
        for col in todas_colunas:
            if tipos_colunas.get(col) != "data_ts":
                continue
            dias_unicos: set[int] = set()
            for nome_part in nomes_particoes_gold:
                ad_info = (
                    perfis_des[nome_part]["perfil_dq"]
                    .get("aderencia_data_particao", {})
                    .get(col, {})
                )
                for d_int in ad_info.get("dias_mes_distintos", []):
                    dias_unicos.add(int(d_int))
            if len(dias_unicos) >= 20:
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
    cfg_comp = cfg_cols.get("completude", {}) or {}
    limites_nulos_yaml = cfg_comp.get("limites_nulos_pct")
    if not isinstance(limites_nulos_yaml, dict):
        limites_nulos_yaml = {}

    limites_nulos_final: dict[str, float] = {}
    origens_nulos: dict[str, str] = {}

    for col in todas_colunas:
        if (
            col.lower().startswith(("ts_", "dt_atualizacao"))
            and col not in limites_nulos_yaml
        ):
            continue

        if (
            col in limites_nulos_yaml
            and str(limites_nulos_yaml[col]).lower() != "auto"
        ):
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

        # Chaves primárias nunca podem ter nulos (Item 1.1)
        if col in chaves_primarias:
            limites_nulos_final[col] = 0.0
            origens_nulos[col] = "AUTO_APRENDIDO"
            _registrar_regra(
                nivel="COLUNA",
                tipo="completude",
                coluna=col,
                parametro_aprendido="<= 0.00% (PK)",
                origem="AUTO_APRENDIDO",
                detalhes=f"Coluna `{col}` faz parte da chave primária -> tolerância zero a nulos",
            )
            continue

        taxas_nulos_gold = []
        for p_gold in nomes_particoes_gold:
            info_g = perfis_des[p_gold]
            qtd_g = info_g["qtd_registros"]
            if qtd_g > 0 and col in info_g["nulos"]:
                taxas_nulos_gold.append(float(info_g["nulos"][col]) / qtd_g)

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
    # 7. Aprendizado de Domínio Categórico e Mix para Distribution Drift (Item 2.3)
    # =========================================================================
    cfg_dom = cfg_cols.get("dominio", {}) or {}
    regras_dom_yaml = cfg_dom.get("regras")
    if not isinstance(regras_dom_yaml, dict):
        regras_dom_yaml = {}

    regras_dom_final: dict[str, list[Any]] = {}
    distribuicao_dom_esperada: dict[str, dict[str, float]] = {}
    origens_dom: dict[str, str] = {}

    for col in todas_colunas:
        if col in chaves_primarias or eh_coluna_id_primaria_estrita(col):
            continue
        if tipos_colunas.get(col) != "string_cat":
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

        cardinalidades = []
        razoes_unicos = []
        presenca_particoes_gold: dict[str, int] = {}
        participacoes_por_valor: dict[str, list[float]] = {}

        for p_gold in nomes_particoes_gold:
            dom_p = perfis_des[p_gold]["dominios"].get(col, {})
            if not dom_p:
                continue
            tot_val = sum(int(v) for v in dom_p.values())
            if tot_val == 0:
                continue
            n_uniq = len(dom_p)
            cardinalidades.append(n_uniq)
            razoes_unicos.append(n_uniq / tot_val)

            for val_cat, cnt in dom_p.items():
                prop = float(cnt) / tot_val
                presenca_particoes_gold[val_cat] = (
                    presenca_particoes_gold.get(val_cat, 0) + 1
                )
                participacoes_por_valor.setdefault(val_cat, []).append(prop)

        if not cardinalidades:
            continue

        med_card = float(np.median(cardinalidades))
        med_razao = float(np.median(razoes_unicos))

        if med_card <= 25 and med_razao < 0.25:
            limiar_gold = max(1, int(np.ceil(len(nomes_particoes_gold) * 0.50)))
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
    # 8. Aprendizado de Intervalo Numérico com Escala Dinâmica (Item 1.6)
    # =========================================================================
    cfg_int = cfg_cols.get("intervalo_numerico", {}) or {}
    regras_int_yaml = cfg_int.get("regras")
    if not isinstance(regras_int_yaml, dict):
        regras_int_yaml = {}

    regras_int_final: dict[str, dict[str, float | int]] = {}
    origens_int: dict[str, str] = {}

    for col in todas_colunas:
        if col in chaves_primarias or eh_coluna_id_primaria_estrita(col):
            continue
        tipo_c = tipos_colunas.get(col, "")
        if tipo_c not in ("numerico_int", "numerico_float"):
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

        eh_inteiro = tipo_c == "numerico_int"
        mins_gold: list[float] = []
        maxs_gold: list[float] = []

        for p_gold in nomes_particoes_gold:
            info_g = perfis_des[p_gold]
            if col in info_g["mins_num"] and col in info_g["maxs_num"]:
                mins_gold.append(float(info_g["mins_num"][col]))
                maxs_gold.append(float(info_g["maxs_num"][col]))

        if not mins_gold:
            continue

        min_estavel = min(mins_gold)
        max_estavel = max(maxs_gold)
        _, sigma_min_raw = _mediana_e_sigma_mad(mins_gold)
        sigma_min = max(sigma_min_raw, abs(min_estavel) * 0.05, 1.0 if eh_inteiro else 0.001)

        # Passo dinâmico de arredondamento baseado na ordem de grandeza (log10) (Item 1.6)
        passo_float, casas_float = calcular_passo_arredondamento_dinamico(max_estavel)

        # Inferência do limite inferior (v_min) sobre partições GOLD
        if min_estavel > 0:
            if not eh_inteiro:
                if max_estavel >= 10.0 and min_estavel >= 0.01:
                    v_min: float | int = 0.01
                else:
                    v_min = round(
                        max(0.0, float(np.floor((min_estavel * 0.90) / passo_float) * passo_float)),
                        casas_float,
                    )
            elif min_estavel >= 100:
                v_min = int(np.floor(min_estavel / 50.0) * 50)
            else:
                v_min = 0
        else:
            v_min_calc = min_estavel - 3.0 * sigma_min
            v_min = (
                int(round(v_min_calc))
                if eh_inteiro
                else round(v_min_calc, casas_float)
            )

        # Inferência do limite superior (v_max) sobre partições GOLD
        cv_max_gold = (
            float(np.std(maxs_gold)) / float(np.mean(maxs_gold))
            if np.mean(maxs_gold) > 0
            else 0.0
        )
        if eh_inteiro:
            fator_margem = 1.02 if max_estavel < 500 else 1.0
            passo_int = 50.0 if max_estavel >= 50 else 10.0
            v_max: float | int = int(
                np.ceil((max_estavel * fator_margem) / passo_int) * passo_int
            )
        else:
            fator_float = 1.05 if cv_max_gold < 0.05 else 1.25
            v_max_bruto = round(
                float(np.ceil((max_estavel * fator_float) / passo_float) * passo_float),
                casas_float,
            )
            # Se for proporção/taxa contida em [0, 1] com topo próximo de 1.0, respeita teto 1.0
            if 0.0 <= min_estavel and 0.50 <= max_estavel <= 1.0:
                v_max = min(1.0, v_max_bruto)
            else:
                v_max = v_max_bruto

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
    # 9. Aprendizado de Formato / Regex e Dígito Verificador Módulo 11 (Itens 1.3 e 2.6)
    # =========================================================================
    cfg_reg = cfg_cols.get("formato_regex", {}) or {}
    regras_reg_yaml = cfg_reg.get("regras")
    if not isinstance(regras_reg_yaml, dict):
        regras_reg_yaml = {}

    regras_reg_final: dict[str, dict[str, Any]] = {}
    origens_reg: dict[str, str] = {}

    cfg_dv = cfg_cols.get("digito_verificador", {}) or {}
    regras_dv_yaml = cfg_dv.get("regras")
    if not isinstance(regras_dv_yaml, dict):
        regras_dv_yaml = {}
    regras_dv_final: dict[str, str] = {}
    origens_dv: dict[str, str] = {}

    for col in todas_colunas:
        if col in chaves_primarias or eh_coluna_id_primaria_estrita(col):
            continue
        if col in regras_dom_final or col in regras_int_final:
            continue
        if tipos_colunas.get(col) != "string_cat":
            continue

        if col in regras_dv_yaml and str(regras_dv_yaml[col]).upper() in ("CPF", "CNPJ"):
            regras_dv_final[col] = str(regras_dv_yaml[col]).upper()
            origens_dv[col] = "MANUAL_YAML"

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

        comprimentos_medianos = []
        taxas_digitos_fixos = []
        taxas_nao_repetidos = []
        taxas_cpf_mod11 = []
        taxas_cnpj_mod11 = []

        for p_gold in nomes_particoes_gold:
            reg_info = (
                perfis_des[p_gold]["perfil_dq"]
                .get("regex_str_cols", {})
                .get(col, {})
            )
            if reg_info:
                comprimentos_medianos.append(int(reg_info["tam_med"]))
                taxas_digitos_fixos.append(float(reg_info["taxa_dig"]))
                taxas_nao_repetidos.append(float(reg_info["taxa_nao_rep"]))
                if "taxa_cpf_mod11" in reg_info:
                    taxas_cpf_mod11.append(float(reg_info["taxa_cpf_mod11"]))
                if "taxa_cnpj_mod11" in reg_info:
                    taxas_cnpj_mod11.append(float(reg_info["taxa_cnpj_mod11"]))

        if taxas_digitos_fixos and float(np.median(taxas_digitos_fixos)) >= 0.98:
            tam_aprendido = int(np.median(comprimentos_medianos))
            padrao_aprendido = f"^\\d{{{tam_aprendido}}}$"
            rejeitar_rep_aprendido = float(np.median(taxas_nao_repetidos)) >= 0.995

            regras_reg_final[col] = {
                "padrao": padrao_aprendido,
                "rejeitar_digitos_repetidos": rejeitar_rep_aprendido,
                "tam_esperado": tam_aprendido,
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

            # Se nas partições GOLD os dígitos obedecem ao Módulo 11 (CPF ou CNPJ), aprende a regra!
            if col not in regras_dv_final:
                if (
                    tam_aprendido == 11
                    and taxas_cpf_mod11
                    and float(np.median(taxas_cpf_mod11)) >= 0.98
                ):
                    regras_dv_final[col] = "CPF"
                    origens_dv[col] = "AUTO_APRENDIDO"
                    _registrar_regra(
                        nivel="COLUNA",
                        tipo="digito_verificador",
                        coluna=col,
                        parametro_aprendido="Módulo 11 (CPF)",
                        origem="AUTO_APRENDIDO",
                        detalhes=(
                            f"Aprendido das partições GOLD {tag_treino_gold}: `{col}` "
                            f"adere ao cálculo de dígito verificador Módulo 11 de CPF"
                        ),
                    )
                elif (
                    tam_aprendido == 14
                    and taxas_cnpj_mod11
                    and float(np.median(taxas_cnpj_mod11)) >= 0.98
                ):
                    regras_dv_final[col] = "CNPJ"
                    origens_dv[col] = "AUTO_APRENDIDO"
                    _registrar_regra(
                        nivel="COLUNA",
                        tipo="digito_verificador",
                        coluna=col,
                        parametro_aprendido="Módulo 11 (CNPJ)",
                        origem="AUTO_APRENDIDO",
                        detalhes=(
                            f"Aprendido das partições GOLD {tag_treino_gold}: `{col}` "
                            f"adere ao cálculo de dígito verificador Módulo 11 de CNPJ"
                        ),
                    )

    # =========================================================================
    # 10. Aprendizado de Consistência Entre Colunas / Cross-Column (Item 2.6)
    # =========================================================================
    cfg_cross = cfg_cols.get("consistencia_entre_colunas", {}) or {}
    regras_cross_yaml = cfg_cross.get("regras")
    regras_cross_final: list[dict[str, str]] = []
    origem_cross = "AUTO_APRENDIDO"

    if isinstance(regras_cross_yaml, list) and regras_cross_yaml:
        for item_c in regras_cross_yaml:
            if (
                isinstance(item_c, dict)
                and item_c.get("col_maior")
                and item_c.get("col_menor")
            ):
                regras_cross_final.append(
                    {
                        "col_maior": str(item_c["col_maior"]),
                        "col_menor": str(item_c["col_menor"]),
                        "operador": str(item_c.get("operador", ">=")),
                    }
                )
        origem_cross = "MANUAL_YAML"
    elif cfg_cross.get("ativo", True) is not False:
        # Auto-aprende relações de precedência temporal 100% respeitadas nas partições GOLD
        aderencias_rel_gold: dict[str, list[float]] = {}
        pares_totais_rel: dict[str, list[int]] = {}
        meta_rel: dict[str, tuple[str, str]] = {}

        for p_gold in nomes_particoes_gold:
            rels_p = perfis_des[p_gold]["perfil_dq"].get("relacoes_colunas", {})
            for chave_rel, info_rel in rels_p.items():
                tot_p = int(info_rel.get("total_pares", 0))
                if tot_p >= 10:
                    aderencias_rel_gold.setdefault(chave_rel, []).append(
                        float(info_rel.get("taxa_aderencia", 0.0))
                    )
                    pares_totais_rel.setdefault(chave_rel, []).append(tot_p)
                    meta_rel[chave_rel] = (
                        str(info_rel["col_maior"]),
                        str(info_rel["col_menor"]),
                    )

        for chave_rel, taxas_r in sorted(aderencias_rel_gold.items()):
            # Precisa estar presente na maioria das partições GOLD e ter 100% de aderência
            if (
                len(taxas_r) >= max(1, int(np.ceil(len(nomes_particoes_gold) * 0.50)))
                and min(taxas_r) == 1.0
            ):
                c_maior, c_menor = meta_rel[chave_rel]
                chave_inversa = f"{c_menor}>={c_maior}"
                taxas_inv = aderencias_rel_gold.get(chave_inversa, [0.0])
                # Evita aprender relação trivial onde ambas são sempre idênticas
                if float(np.median(taxas_inv)) < 0.99:
                    regras_cross_final.append(
                        {
                            "col_maior": c_maior,
                            "col_menor": c_menor,
                            "operador": ">=",
                        }
                    )
                    _registrar_regra(
                        nivel="COLUNA",
                        tipo="consistencia_entre_colunas",
                        coluna=f"{c_maior}>={c_menor}",
                        parametro_aprendido=f"{c_maior} >= {c_menor}",
                        origem="AUTO_APRENDIDO",
                        detalhes=(
                            f"Relação temporal aprendida das partições GOLD {tag_treino_gold}: "
                            f"100% dos registros respeitam `{c_maior} >= {c_menor}`"
                        ),
                    )

    return {
        "chaves_primarias": chaves_primarias,
        "particoes_gold_treino": nomes_particoes_gold_raw,
        "em_cold_start": em_cold_start,
        "todas_particoes_contaminadas": todas_contaminadas,
        "origem_chaves_primarias": origem_pk,
        "particao": {
            "consistencia_schema": {
                "ativo": cfg_schema.get("ativo", True),
                "schema_esperado": schema_esperado,
                "origem": origem_schema,
            },
            "freshness_lacunas": {
                "ativo": fresh_ativo,
                "sla_atraso_max_particoes": sla_max_part,
                "verificar_lacunas": bool(cfg_fresh.get("verificar_lacunas", True)),
                "origem": origem_fresh,
            },
            "volumetria_historica": {
                "ativo": cfg_vol.get("ativo", True),
                "mediana_gold": med_vol,
                "sigma_gold": sigma_vol,
                "desvio_relativo_max": desvio_rel_max,
                "desvio_relativo_warning": desvio_rel_warning,
                "z_score_robusto_max": z_max,
                "z_score_robusto_warning": z_warning,
                "janela_historico_particoes": janela_hist,
                "sazonalidade_ativa": sazonalidade_ativa,
                "baselines_sazonais": baselines_sazonais,
                "em_cold_start": em_cold_start,
                "todas_contaminadas": todas_contaminadas,
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
                "tolerancia_warning_pct": float(
                    cfg_comp.get("tolerancia_warning_pct", 0.0)
                ),
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
                "verificar_drift_mix": bool(cfg_dom.get("verificar_drift_mix", True)),
                "psi_warning": float(cfg_dom.get("psi_warning", 0.25)),
                "psi_anomalia": float(cfg_dom.get("psi_anomalia", 0.50)),
                "tolerancia_warning_pct": float(
                    cfg_dom.get("tolerancia_warning_pct", 0.0)
                ),
                "origens": origens_dom,
            },
            "intervalo_numerico": {
                "ativo": cfg_int.get("ativo", True),
                "regras": regras_int_final,
                "tolerancia_warning_pct": float(
                    cfg_int.get("tolerancia_warning_pct", 0.0)
                ),
                "origens": origens_int,
            },
            "formato_regex": {
                "ativo": cfg_reg.get("ativo", True) if regras_reg_final else False,
                "regras": regras_reg_final,
                "tolerancia_warning_pct": float(
                    cfg_reg.get("tolerancia_warning_pct", 0.0)
                ),
                "origens": origens_reg,
            },
            "digito_verificador": {
                "ativo": cfg_dv.get("ativo", True) if regras_dv_final else False,
                "regras": regras_dv_final,
                "origens": origens_dv,
            },
            "consistencia_entre_colunas": {
                "ativo": cfg_cross.get("ativo", True) if regras_cross_final else False,
                "regras": regras_cross_final,
                "origem": origem_cross,
            },
        },
        "registros_regras_silver": registros_silver,
    }
