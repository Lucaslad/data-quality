"""
Gerenciador de fotografias (Snapshots) de execução e aprendizado automático de atraso/maturação.

Responsabilidades:
1. Camada Bronze (`dados/saida/bronze/`):
   - Salvar fotografias de cada partição a cada execução (`snapshots_particoes.parquet`).
   - Salvar fotografias da distribuição de datas de evento (`snapshots_eventos_data.parquet`),
     registrando quantos eventos existem para cada `lag_dias = (dt_referencia - data_evento).days`.
2. Camada Silver (`dados/saida/silver/`):
   - Acompanhar a mesma `data_evento` através de fotografias consecutivas (`lag -> lag + 1`)
     para descobrir AUTOMATICAMENTE:
     a) Quantos dias de atraso cada coluna leva para estabilizar (`lag_estabilizacao_aprendido`),
        sem precisar configurar esse número manualmente no YAML.
     b) A curva de preenchimento esperado (`fator_maturacao_esperado`) em cada `lag_dias`.
"""

from __future__ import annotations

import datetime as dt
import json
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd


class SnapshotManager:
    """Gerencia a persistência de fotografias (Bronze) e o aprendizado automático de lag (Silver)."""

    def __init__(self, diretorio_saida: Path) -> None:
        self.diretorio_saida = Path(diretorio_saida)
        self.bronze_dir = self.diretorio_saida / "bronze"
        self.silver_dir = self.diretorio_saida / "silver"
        self.gold_dir = self.diretorio_saida / "gold"

        for d in (self.bronze_dir, self.silver_dir, self.gold_dir):
            d.mkdir(parents=True, exist_ok=True)

        self.arq_fotos_particoes = self.bronze_dir / "snapshots_particoes.parquet"
        self.arq_fotos_eventos = self.bronze_dir / "snapshots_eventos_data.parquet"
        self.arq_curva_aprendida = self.silver_dir / "curva_maturacao_aprendida.parquet"

    def carregar_fotos_particoes(self) -> pd.DataFrame:
        if self.arq_fotos_particoes.exists():
            return pd.read_parquet(self.arq_fotos_particoes)
        return pd.DataFrame()

    def carregar_fotos_eventos(self) -> pd.DataFrame:
        if self.arq_fotos_eventos.exists():
            return pd.read_parquet(self.arq_fotos_eventos)
        return pd.DataFrame()

    def registrar_foto_particoes(
        self,
        run_id: str,
        dt_referencia: str,
        ts_execucao: str,
        tabela: str,
        particoes_dfs: dict[str, pd.DataFrame],
    ) -> pd.DataFrame:
        """
        Registra uma fotografia de todas as partições da tabela na camada Bronze.
        Armazena a quantidade de linhas e a contagem de valores não-nulos por coluna,
        permitindo auditar atualizações em partições passadas entre execuções.
        """
        registros = []
        for nome_particao, df_part in sorted(particoes_dfs.items()):
            qtd_linhas = len(df_part)
            preenchidos_por_col = {
                col: int(df_part[col].notna().sum()) for col in df_part.columns
            }
            nulos_por_col = {
                col: int(df_part[col].isna().sum()) for col in df_part.columns
            }
            registros.append(
                {
                    "run_id": run_id,
                    "dt_referencia": dt_referencia,
                    "ts_execucao": ts_execucao,
                    "tabela": tabela,
                    "particao": nome_particao,
                    "qtd_registros": int(qtd_linhas),
                    "preenchidos_json": json.dumps(
                        preenchidos_por_col, ensure_ascii=False
                    ),
                    "nulos_json": json.dumps(nulos_por_col, ensure_ascii=False),
                }
            )

        df_novo = pd.DataFrame(registros)
        df_hist = self.carregar_fotos_particoes()

        if not df_hist.empty:
            mask_manter = ~(
                (df_hist["tabela"] == tabela)
                & (df_hist["dt_referencia"] == dt_referencia)
            )
            df_hist = df_hist[mask_manter]
            df_final = pd.concat([df_hist, df_novo], ignore_index=True)
        else:
            df_final = df_novo

        df_final.to_parquet(
            self.arq_fotos_particoes, engine="pyarrow", compression="snappy", index=False
        )
        return df_novo

    def registrar_foto_eventos_data(
        self,
        run_id: str,
        dt_referencia: str,
        ts_execucao: str,
        tabela: str,
        coluna_data: str,
        df_completo: pd.DataFrame,
        janela_captura_dias: int = 25,
    ) -> pd.DataFrame:
        """
        Tira uma fotografia da volumetria diária agrupada por `coluna_data`.
        Não assume nenhum `max_lag_dias` fixo: usa a janela consolidada histórica (dias 10 a 25)
        como linha de base inicial e salva os volumes de `lag = 0` a `janela_captura_dias`.
        """
        dt_ref = dt.date.fromisoformat(dt_referencia)

        serie_datas = pd.to_datetime(
            df_completo[coluna_data], errors="coerce", format="ISO8601"
        ).dt.date
        serie_valida = serie_datas.dropna()

        contagem_por_data = serie_valida.value_counts().to_dict()

        # Usa a janela de cauda longa (lag 8 a janela_captura_dias) para estimar o patamar estabilizado
        volumes_cauda_estavel: list[int] = []
        for lag in range(8, janela_captura_dias + 1):
            d_alvo = dt_ref - dt.timedelta(days=lag)
            qtd = int(contagem_por_data.get(d_alvo, 0))
            if qtd > 0:
                volumes_cauda_estavel.append(qtd)

        if not volumes_cauda_estavel:
            # Fallback caso a base tenha menos de 8 dias de histórico
            volumes_cauda_estavel = [
                int(v) for v in contagem_por_data.values() if int(v) > 0
            ]

        volume_maduro_ref = (
            float(np.median(volumes_cauda_estavel)) if volumes_cauda_estavel else 1.0
        )

        registros = []
        for lag in range(0, janela_captura_dias + 1):
            d_evento = dt_ref - dt.timedelta(days=lag)
            qtd = int(contagem_por_data.get(d_evento, 0))
            fator = round(qtd / volume_maduro_ref, 4) if volume_maduro_ref > 0 else 0.0
            registros.append(
                {
                    "run_id": run_id,
                    "dt_referencia": dt_referencia,
                    "ts_execucao": ts_execucao,
                    "tabela": tabela,
                    "coluna_data": coluna_data,
                    "data_evento": d_evento.isoformat(),
                    "lag_dias": int(lag),
                    "qtd_registros": qtd,
                    "volume_maduro_referencia": round(volume_maduro_ref, 2),
                    "fator_maturacao": fator,
                }
            )

        df_novo = pd.DataFrame(registros)
        df_hist = self.carregar_fotos_eventos()

        if not df_hist.empty:
            mask_manter = ~(
                (df_hist["tabela"] == tabela)
                & (df_hist["coluna_data"] == coluna_data)
                & (df_hist["dt_referencia"] == dt_referencia)
            )
            df_hist = df_hist[mask_manter]
            df_final = pd.concat([df_hist, df_novo], ignore_index=True)
        else:
            df_final = df_novo

        df_final.to_parquet(
            self.arq_fotos_eventos, engine="pyarrow", compression="snappy", index=False
        )
        return df_novo

    def atualizar_e_obter_curva_aprendida(
        self,
        tabela: str,
        coluna_data: str,
        dt_referencia_atual: str,
        max_lag_busca: int = 10,
    ) -> dict[str, Any]:
        """
        Aprende AUTOMATICAMENTE a partir das fotografias anteriores:
        1. Quantos dias de atraso a coluna leva para estabilizar (`lag_estabilizacao_aprendido`),
           comparando a evolução da MESMA `data_evento` entre fotografias consecutivas (`lag -> lag + 1`).
        2. O `fator_maturacao_esperado` para cada `lag_dias`.

        Retorna:
        {
            "qtd_fotos_historico": int,
            "lag_estabilizacao_aprendido": int,
            "curva_por_lag": dict[int, dict[str, Any]],
        }
        """
        df_fotos = self.carregar_fotos_eventos()
        if df_fotos.empty:
            return {
                "qtd_fotos_historico": 0,
                "lag_estabilizacao_aprendido": 0,
                "curva_por_lag": {},
            }

        df_treino = df_fotos[
            (df_fotos["tabela"] == tabela)
            & (df_fotos["coluna_data"] == coluna_data)
            & (df_fotos["dt_referencia"] < dt_referencia_atual)
        ].copy()

        if df_treino.empty:
            return {
                "qtd_fotos_historico": 0,
                "lag_estabilizacao_aprendido": 0,
                "curva_por_lag": {},
            }

        qtd_fotos_total = int(df_treino["dt_referencia"].nunique())

        # Pareia a MESMA data_evento em fotos consecutivas: lag -> lag + 1
        df_lag_atual = df_treino[
            ["data_evento", "lag_dias", "qtd_registros"]
        ].rename(columns={"qtd_registros": "qtd_lag"})

        df_lag_seguinte = df_treino[
            ["data_evento", "lag_dias", "qtd_registros"]
        ].copy()
        df_lag_seguinte["lag_dias"] = df_lag_seguinte["lag_dias"] - 1
        df_lag_seguinte = df_lag_seguinte.rename(
            columns={"qtd_registros": "qtd_lag_mais_1"}
        )

        df_transicoes = pd.merge(
            df_lag_atual,
            df_lag_seguinte,
            on=["data_evento", "lag_dias"],
            how="inner",
        )
        df_transicoes["delta_qtd"] = (
            df_transicoes["qtd_lag_mais_1"] - df_transicoes["qtd_lag"]
        )
        df_transicoes["taxa_crescimento"] = df_transicoes["delta_qtd"] / np.maximum(
            df_transicoes["qtd_lag"], 1
        )

        crescimento_por_lag: dict[int, tuple[float, float]] = {}
        if not df_transicoes.empty:
            for lag_t, grp_t in df_transicoes.groupby("lag_dias"):
                med_delta = float(np.median(grp_t["delta_qtd"].to_numpy()))
                med_taxa = float(np.median(grp_t["taxa_crescimento"].to_numpy()))
                crescimento_por_lag[int(lag_t)] = (med_delta, med_taxa)

        curva_por_lag: dict[int, dict[str, Any]] = {}
        for lag, grupo in df_treino.groupby("lag_dias"):
            lag_int = int(lag)
            fatores = grupo["fator_maturacao"].astype(float).to_numpy()
            qtds = grupo["qtd_registros"].astype(float).to_numpy()
            n_fotos_lag = int(grupo["dt_referencia"].nunique())

            fator_medio = float(np.median(fatores))
            fator_std = float(np.std(fatores)) if len(fatores) > 1 else 0.05
            qtd_media = float(np.median(qtds))
            med_delta, med_taxa = crescimento_por_lag.get(lag_int, (0.0, 0.0))

            curva_por_lag[lag_int] = {
                "tabela": tabela,
                "coluna_data": coluna_data,
                "lag_dias": lag_int,
                "qtd_fotos_aprendizado": n_fotos_lag,
                "fator_maturacao_esperado": round(fator_medio, 4),
                "fator_maturacao_std": round(fator_std, 4),
                "qtd_registros_mediana_historica": round(qtd_media, 2),
                "delta_registros_proxima_foto": round(med_delta, 2),
                "taxa_crescimento_proxima_foto": round(med_taxa, 4),
            }

        # Descoberta AUTOMÁTICA do lag de estabilização da coluna:
        # Verifica até qual lag_dias a mesma data_evento ainda cresce na foto seguinte
        # (delta >= 1.0 registro ou taxa > 3%) ou ainda está abaixo de 88% do volume maduro.
        lag_estabilizacao = 0
        for lag_cand in range(0, max_lag_busca):
            info = curva_por_lag.get(lag_cand)
            if info is None:
                continue
            delta_prox = float(info["delta_registros_proxima_foto"])
            taxa_prox = float(info["taxa_crescimento_proxima_foto"])
            fator_esp = float(info["fator_maturacao_esperado"])

            ainda_cresce_entre_fotos = (delta_prox >= 1.0) or (taxa_prox > 0.03)
            ainda_abaixo_patamar_maduro = fator_esp < 0.88

            if ainda_cresce_entre_fotos or ainda_abaixo_patamar_maduro:
                lag_estabilizacao = lag_cand + 1
            else:
                # Atingiu estabilização neste lag_cand
                break

        linhas_silver = []
        for lag_int, info in sorted(curva_por_lag.items()):
            info["lag_estabilizacao_aprendido"] = int(lag_estabilizacao)
            info["estagio"] = (
                "EM_MATURACAO" if lag_int < lag_estabilizacao else "MADURO"
            )
            linhas_silver.append(info)

        if linhas_silver:
            df_curva_nova = pd.DataFrame(linhas_silver)
            if self.arq_curva_aprendida.exists():
                df_curva_ant = pd.read_parquet(self.arq_curva_aprendida)
                mask = ~(
                    (df_curva_ant["tabela"] == tabela)
                    & (df_curva_ant["coluna_data"] == coluna_data)
                )
                df_curva_final = pd.concat(
                    [df_curva_ant[mask], df_curva_nova], ignore_index=True
                )
            else:
                df_curva_final = df_curva_nova

            df_curva_final.to_parquet(
                self.arq_curva_aprendida,
                engine="pyarrow",
                compression="snappy",
                index=False,
            )

        return {
            "qtd_fotos_historico": qtd_fotos_total,
            "lag_estabilizacao_aprendido": int(lag_estabilizacao),
            "curva_por_lag": curva_por_lag,
        }
