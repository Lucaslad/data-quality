"""
Checks no nível de Partição (Macro) e evolução entre Fotografias (Snapshots).

Inclui:
1. Volumetria Histórica da Partição (detecção robusta via Mediana + MAD de quedas e picos).
2. Evolução entre Fotografias de Partições (monitora atualizações em partições passadas,
   como novos `dt_cadastro_chave_pix`, e alerta perdas/regressões inesperadas de dados).
3. Consistência de Data vs. Partição (verifica se colunas de data pertencem à safra da partição).
"""

from __future__ import annotations

import json
from typing import Any

import numpy as np
import pandas as pd

from src.models import ResultadoCheck


def _calcular_estatisticas_robustas(valores: list[int]) -> tuple[float, float]:
    """Retorna (mediana, sigma_robusto_mad) da série de volumes de partição."""
    arr = np.array(valores, dtype=float)
    mediana = float(np.median(arr))
    mad = float(np.median(np.abs(arr - mediana)))
    # Fator 1.4826 converte MAD para equivalente de desvio padrão normal
    sigma_robusto = max(mad * 1.4826, mediana * 0.03, 1.0)
    return mediana, sigma_robusto


def executar_checks_particao(
    run_id: str,
    dt_referencia: str,
    ts_execucao: str,
    tabela: str,
    tipo_particao: str,
    particoes_dfs: dict[str, pd.DataFrame],
    cfg_particao: dict[str, Any],
    df_fotos_particoes_hist: pd.DataFrame,
) -> list[ResultadoCheck]:
    """Executa todos os checks configurados para o nível de partição."""
    resultados: list[ResultadoCheck] = []
    if not particoes_dfs:
        return resultados

    # -------------------------------------------------------------------------
    # 1. Volumetria Histórica entre Partições
    # -------------------------------------------------------------------------
    cfg_vol = cfg_particao.get("volumetria_historica", {})
    if cfg_vol.get("ativo", False):
        desvio_rel_max = float(cfg_vol.get("desvio_relativo_max", 0.30))
        z_max = float(cfg_vol.get("z_score_robusto_max", 3.0))

        volumes = [len(df) for df in particoes_dfs.values()]
        mediana, sigma_rob = _calcular_estatisticas_robustas(volumes)

        lim_inf = max(0, int(round(mediana * (1.0 - desvio_rel_max))))
        lim_sup = int(round(mediana * (1.0 + desvio_rel_max)))
        faixa_str = f"[{lim_inf}, {lim_sup}] (mediana={int(round(mediana))})"

        for nome_part, df_part in sorted(particoes_dfs.items()):
            qtd = len(df_part)
            desvio_rel = (qtd - mediana) / mediana if mediana > 0 else 0.0
            z_rob = abs(qtd - mediana) / sigma_rob

            if abs(desvio_rel) > desvio_rel_max and z_rob > z_max:
                direcao = "QUEDA BRUSCA" if desvio_rel < 0 else "PICO ANÔMALO"
                status = "ANOMALIA"
                detalhe = (
                    f"{direcao} de volumetria na partição: {qtd} linhas "
                    f"({desvio_rel:+.1%} vs mediana histórica de {mediana:.0f}, z={z_rob:.1f})"
                )
            else:
                status = "OK"
                detalhe = (
                    f"Volumetria da partição estável: {qtd} linhas "
                    f"({desvio_rel:+.1%} vs mediana de {mediana:.0f})"
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
    # 2. Evolução entre Fotografias (Snapshots de Partições Passadas e Atuais)
    # -------------------------------------------------------------------------
    cfg_foto = cfg_particao.get("evolucao_fotografias", {})
    if cfg_foto.get("ativo", False) and not df_fotos_particoes_hist.empty:
        df_ant_tab = df_fotos_particoes_hist[
            (df_fotos_particoes_hist["tabela"] == tabela)
            & (df_fotos_particoes_hist["dt_referencia"] < dt_referencia)
        ]
        if not df_ant_tab.empty:
            ult_dt_ref = str(df_ant_tab["dt_referencia"].max())
            df_ultima_foto = df_ant_tab[df_ant_tab["dt_referencia"] == ult_dt_ref]
            mapa_foto_ant = {
                str(row["particao"]): row for _, row in df_ultima_foto.iterrows()
            }

            queda_max_pct = float(cfg_foto.get("queda_max_registros_pct", 0.0))
            cols_evolucao = cfg_foto.get("colunas_evolucao_positiva", []) or []

            for nome_part, df_part in sorted(particoes_dfs.items()):
                if nome_part not in mapa_foto_ant:
                    continue

                row_ant = mapa_foto_ant[nome_part]
                qtd_ant = int(row_ant["qtd_registros"])
                qtd_atual = len(df_part)
                diff_qtd = qtd_atual - qtd_ant
                var_pct = (diff_qtd / qtd_ant) if qtd_ant > 0 else 0.0

                preench_ant = json.loads(str(row_ant["preenchidos_json"]))
                msgs_cols = []
                regressao_coluna = False

                for col_ev in cols_evolucao:
                    if col_ev in df_part.columns and col_ev in preench_ant:
                        p_ant = int(preench_ant[col_ev])
                        p_atu = int(df_part[col_ev].notna().sum())
                        delta_col = p_atu - p_ant
                        if delta_col > 0:
                            msgs_cols.append(f"{col_ev}: +{delta_col} novos preenchimentos")
                        elif delta_col < 0:
                            regressao_coluna = True
                            msgs_cols.append(
                                f"{col_ev}: REGRESSÃO de {delta_col} preenchimentos ({p_ant} -> {p_atu})"
                            )

                if var_pct < -queda_max_pct or regressao_coluna:
                    status_f = "ANOMALIA"
                    detalhe_f = (
                        f"Perda/regressão detectada em relação à fotografia {ult_dt_ref}: "
                        f"linhas {qtd_ant} -> {qtd_atual}. " + "; ".join(msgs_cols)
                    )
                else:
                    status_f = "OK"
                    extra_info = (
                        f" ({'; '.join(msgs_cols)})"
                        if msgs_cols
                        else " (sem alterações de preenchimento)"
                    )
                    detalhe_f = (
                        f"Evolução consistente vs foto {ult_dt_ref}: "
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
                        valor_esperado=f">= {qtd_ant} (foto {ult_dt_ref})",
                        status=status_f,
                        detalhes=detalhe_f,
                    )
                )

    # -------------------------------------------------------------------------
    # 3. Consistência de Data com a Partição (Timeliness interna)
    # -------------------------------------------------------------------------
    cfg_cons = cfg_particao.get("consistencia_data_particao", {})
    if cfg_cons.get("ativo", False):
        colunas_data = cfg_cons.get("colunas_data_particao", []) or []
        for nome_part, df_part in sorted(particoes_dfs.items()):
            valor_particao = nome_part.split("=")[-1]
            if tipo_particao == "mensal" and len(valor_particao) == 6:
                prefixo_esperado = f"{valor_particao[:4]}-{valor_particao[4:6]}"
            elif tipo_particao == "diaria" and len(valor_particao) == 8:
                prefixo_esperado = (
                    f"{valor_particao[:4]}-{valor_particao[4:6]}-{valor_particao[6:8]}"
                )
            else:
                prefixo_esperado = valor_particao

            for col_dt in colunas_data:
                if col_dt not in df_part.columns:
                    continue
                serie_str = df_part[col_dt].dropna().astype(str)
                total_validos = len(serie_str)
                if total_validos == 0:
                    continue

                fora_particao = serie_str[~serie_str.str.startswith(prefixo_esperado)]
                qtd_fora = len(fora_particao)

                if qtd_fora > 0:
                    exemplos = fora_particao.unique()[:3].tolist()
                    status_dt = "ANOMALIA"
                    detalhe_dt = (
                        f"{qtd_fora} registros ({qtd_fora / total_validos:.1%}) com `{col_dt}` "
                        f"fora da partição `{nome_part}` (esperado prefixo `{prefixo_esperado}`, "
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
