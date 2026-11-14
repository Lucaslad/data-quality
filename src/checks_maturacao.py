"""
Check inteligente de Maturação / Atraso de Datas de Evento (`maturacao_datas`).

O motor aprende SOZINHO tanto:
1. Quantos dias cada coluna demora para estabilizar (`lag_estabilizacao_aprendido`),
   acompanhando a mesma `data_evento` através de fotografias consecutivas (`lag -> lag + 1`)
   até o ponto em que ela para de crescer e atinge o patamar maduro.
2. Qual é o percentual esperado (`fator_maturacao_esperado`) em cada dia de atraso (`lag = 0, 1, ...`).
"""

from __future__ import annotations

from typing import Any

import pandas as pd

from src.models import ResultadoCheck
from src.snapshot_manager import SnapshotManager


def executar_checks_maturacao(
    run_id: str,
    dt_referencia: str,
    ts_execucao: str,
    tabela: str,
    particoes_dfs: dict[str, pd.DataFrame],
    cfg_maturacao: dict[str, Any],
    snapshot_mgr: SnapshotManager,
) -> list[ResultadoCheck]:
    """Executa os checks de maturação/atraso de colunas de data com aprendizado 100% automático."""
    resultados: list[ResultadoCheck] = []
    if not cfg_maturacao.get("ativo", False) or not particoes_dfs:
        return resultados

    colunas_cfg = cfg_maturacao.get("colunas", {}) or {}
    if not colunas_cfg:
        return resultados

    df_todas_particoes = pd.concat(list(particoes_dfs.values()), ignore_index=True)
    ultima_particao_nome = sorted(particoes_dfs.keys())[-1]

    for col_data, params in colunas_cfg.items():
        if isinstance(params, bool):
            if not params:
                continue
            params = {}
        elif isinstance(params, dict) and not params.get("ativo", True):
            continue

        escopo = str(params.get("escopo_particoes", "todas")).lower()
        df_alvo = (
            df_todas_particoes
            if escopo == "todas"
            else particoes_dfs[ultima_particao_nome]
        )
        particao_label = "TODAS_PARTICOES" if escopo == "todas" else ultima_particao_nome

        if col_data not in df_alvo.columns:
            continue

        janela_avaliacao = int(params.get("janela_avaliacao_dias", 7))
        tolerancia_curva = float(params.get("tolerancia_desvio_curva", 0.35))
        min_fotos = int(params.get("min_fotos_aprendizado", 2))

        # 1. Aprende automaticamente o lag de estabilização e a curva usando fotos anteriores
        aprendizado = snapshot_mgr.atualizar_e_obter_curva_aprendida(
            tabela=tabela,
            coluna_data=col_data,
            dt_referencia_atual=dt_referencia,
        )
        qtd_fotos_hist = int(aprendizado["qtd_fotos_historico"])
        lag_estabilizacao = int(aprendizado["lag_estabilizacao_aprendido"])
        curva_aprendida: dict[int, dict[str, Any]] = aprendizado["curva_por_lag"]
        tem_aprendizado = qtd_fotos_hist >= min_fotos

        # 2. Registra a fotografia da execução atual na camada Bronze
        df_foto_atual = snapshot_mgr.registrar_foto_eventos_data(
            run_id=run_id,
            dt_referencia=dt_referencia,
            ts_execucao=ts_execucao,
            tabela=tabela,
            coluna_data=col_data,
            df_completo=df_alvo,
        )

        # Atualiza o artefato Silver incluindo a foto recém-tirada para próximas execuções
        snapshot_mgr.atualizar_e_obter_curva_aprendida(
            tabela=tabela,
            coluna_data=col_data,
            dt_referencia_atual="9999-12-31",
        )

        # Registra um check informativo sintetizando o atraso descoberto para a coluna
        if tem_aprendizado:
            resumo_curva = ", ".join(
                f"lag {k}d={curva_aprendida[k]['fator_maturacao_esperado']:.0%}"
                for k in range(0, lag_estabilizacao + 1)
                if k in curva_aprendida
            )
            detalhe_aprendizado = (
                f"Motor aprendeu sozinho (em {qtd_fotos_hist} fotos) que `{col_data}` "
                f"leva {lag_estabilizacao} dia(s) para estabilizar ({resumo_curva})"
            )
        else:
            detalhe_aprendizado = (
                f"Aprendizado em coleta para `{col_data}` ({qtd_fotos_hist}/{min_fotos} fotos)"
            )

        resultados.append(
            ResultadoCheck(
                run_id=run_id,
                dt_referencia=dt_referencia,
                ts_execucao=ts_execucao,
                tabela=tabela,
                particao=particao_label,
                nivel_check="MATURACAO_EVENTO",
                tipo_check="diagnostico_atraso_aprendido",
                coluna=col_data,
                valor_observado=f"{lag_estabilizacao}d",
                valor_esperado="Auto-aprendido via snapshots",
                status="OK" if tem_aprendizado else "WARNING",
                detalhes=detalhe_aprendizado,
            )
        )

        df_janela = df_foto_atual[
            df_foto_atual["lag_dias"] <= janela_avaliacao
        ].sort_values("lag_dias")

        for _, row in df_janela.iterrows():
            lag = int(row["lag_dias"])
            dt_ev = str(row["data_evento"])
            qtd_obs = int(row["qtd_registros"])
            vol_maduro = float(row["volume_maduro_referencia"])
            fator_obs = float(row["fator_maturacao"])

            info_hist = curva_aprendida.get(lag)

            # Caso A: Data dentro da janela de atraso DESCOBERTA pelo motor (lag < lag_estabilizacao)
            if tem_aprendizado and lag < lag_estabilizacao and info_hist is not None:
                fator_esp = float(info_hist["fator_maturacao_esperado"])
                qtd_esp_lag = vol_maduro * fator_esp
                fator_min_lag = max(
                    0.05, fator_esp * (1.0 - tolerancia_curva) - 0.08
                )
                qtd_min_lag = max(1, int(round(vol_maduro * fator_min_lag)))

                if fator_obs >= fator_min_lag:
                    status = "OK"
                    detalhe = (
                        f"Data {dt_ev} (lag={lag}d < atraso_aprendido={lag_estabilizacao}d): "
                        f"{qtd_obs} casos vs ref madura de {vol_maduro:.0f}/dia ({fator_obs:.0%}). "
                        f"Queda normal p/ lag={lag}d segundo aprendizado ({fator_esp:.0%} / ~{qtd_esp_lag:.0f} casos)"
                    )
                else:
                    status = "ANOMALIA"
                    detalhe = (
                        f"Queda anormal em data recente {dt_ev} (lag={lag}d < atraso_aprendido={lag_estabilizacao}d): "
                        f"{qtd_obs} casos ({fator_obs:.0%}), ABAIXO até mesmo da curva aprendida para lag={lag}d "
                        f"(esperado ~{fator_esp:.0%} / mín {qtd_min_lag} casos)"
                    )
                esperado_str = (
                    f"~{qtd_esp_lag:.0f} casos (curva lag={lag}d: {fator_esp:.0%})"
                )

            # Caso B: Cold start (ainda sem fotos suficientes para saber se há atraso normal)
            elif not tem_aprendizado:
                fator_min_maduro = 1.0 - tolerancia_curva
                if fator_obs < fator_min_maduro:
                    status = "WARNING"
                    detalhe = (
                        f"Queda em {dt_ev} (lag={lag}d): {qtd_obs} casos ({fator_obs:.0%} de {vol_maduro:.0f}). "
                        f"Aguardando fotos para aprender atraso ({qtd_fotos_hist}/{min_fotos})"
                    )
                else:
                    status = "OK"
                    detalhe = f"Data {dt_ev} (lag={lag}d) estável: {qtd_obs} casos"
                esperado_str = f"Em aprendizado ({qtd_fotos_hist}/{min_fotos} fotos)"

            # Caso C: Data que JÁ ATINGIU OU PASSOU do atraso aprendido (lag >= lag_estabilizacao)
            else:
                fator_esp_maduro = (
                    float(info_hist["fator_maturacao_esperado"])
                    if info_hist is not None
                    else 1.0
                )
                fator_base = max(0.85, min(1.05, fator_esp_maduro))
                fator_min_maduro = fator_base * (1.0 - tolerancia_curva)
                qtd_min_maduro = max(1, int(round(vol_maduro * fator_min_maduro)))

                if fator_obs < fator_min_maduro:
                    status = "ANOMALIA"
                    detalhe = (
                        f"QUEDA REAL em data já madura ({dt_ev}, lag={lag}d >= atraso_aprendido={lag_estabilizacao}d): "
                        f"apenas {qtd_obs} casos ({fator_obs:.0%} da ref madura de {vol_maduro:.0f}/dia; "
                        f"mínimo esperado={qtd_min_maduro})"
                    )
                else:
                    status = "OK"
                    detalhe = (
                        f"Data madura {dt_ev} (lag={lag}d >= atraso_aprendido={lag_estabilizacao}d) consolidada: "
                        f"{qtd_obs} casos ({fator_obs:.0%} da ref madura de {vol_maduro:.0f}/dia)"
                    )
                esperado_str = f">= {qtd_min_maduro} casos (ref madura={vol_maduro:.0f})"

            resultados.append(
                ResultadoCheck(
                    run_id=run_id,
                    dt_referencia=dt_referencia,
                    ts_execucao=ts_execucao,
                    tabela=tabela,
                    particao=particao_label,
                    nivel_check="MATURACAO_EVENTO",
                    tipo_check=f"maturacao_lag_{lag}d",
                    coluna=f"{col_data}[{dt_ev}]",
                    valor_observado=qtd_obs,
                    valor_esperado=esperado_str,
                    status=status,
                    detalhes=detalhe,
                )
            )

    return resultados
