"""
Gerenciador de fotografias (Snapshots) de execução, metadados e aprendizado automático blindado (GOLD).

Responsabilidades:
1. Diretório de Metadados do Motor (`dados/saida/_metadados_dq/`):
   - Salvar fotografias de cada partição a cada execução (`snapshots_particoes.parquet`),
     incluindo a `camada_qualidade` (`GOLD`, `SILVER`, `BRONZE`) da partição.
   - Salvar fotografias da distribuição de datas de evento (`snapshots_eventos_data.parquet`),
     descontando duplicatas de chave primária e marcando datas saudáveis vs. quarentena.
   - Salvar a curva de maturação aprendida (`curva_maturacao_aprendida.parquet`), calculada
     EXCLUSIVAMENTE sobre datas de evento saudáveis (`GOLD`).
   - Salvar todas as regras e limiares aprendidos pelo motor (`regras_aprendidas.parquet`).
2. Diretórios de Dados Classificados (`dados/saida/bronze/`, `dados/saida/silver/`, `dados/saida/gold/`):
   - Mantidos exclusivamente para receber as cópias das tabelas e suas respectivas partições
     de acordo com a classificação de qualidade de cada partição.
"""

from __future__ import annotations

import datetime as dt
import json
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd


class SnapshotManager:
    """Gerencia a persistência de fotografias, regras aprendidas (GOLD-only) e pastas Medallion."""

    def __init__(self, diretorio_saida: Path) -> None:
        self.diretorio_saida = Path(diretorio_saida)
        self.bronze_dir = self.diretorio_saida / "bronze"
        self.silver_dir = self.diretorio_saida / "silver"
        self.gold_dir = self.diretorio_saida / "gold"
        self.metadados_dir = self.diretorio_saida / "_metadados_dq"

        for d in (self.bronze_dir, self.silver_dir, self.gold_dir, self.metadados_dir):
            d.mkdir(parents=True, exist_ok=True)

        self.arq_fotos_particoes = self.metadados_dir / "snapshots_particoes.parquet"
        self.arq_fotos_eventos = self.metadados_dir / "snapshots_eventos_data.parquet"
        self.arq_curva_aprendida = self.metadados_dir / "curva_maturacao_aprendida.parquet"
        self.arq_regras_aprendidas = self.metadados_dir / "regras_aprendidas.parquet"

    def carregar_fotos_particoes(self) -> pd.DataFrame:
        if self.arq_fotos_particoes.exists():
            return pd.read_parquet(self.arq_fotos_particoes)
        return pd.DataFrame()

    def carregar_fotos_eventos(self) -> pd.DataFrame:
        if self.arq_fotos_eventos.exists():
            return pd.read_parquet(self.arq_fotos_eventos)
        return pd.DataFrame()

    def carregar_regras_aprendidas(self) -> pd.DataFrame:
        if self.arq_regras_aprendidas.exists():
            return pd.read_parquet(self.arq_regras_aprendidas)
        return pd.DataFrame()

    def registrar_regras_aprendidas(
        self,
        tabela: str,
        dt_referencia: str,
        registros_regras: list[dict[str, Any]],
    ) -> pd.DataFrame:
        """Persiste em `_metadados_dq/` todas as regras auto-aprendidas (e overrides) da tabela."""
        if not registros_regras:
            return pd.DataFrame()

        df_novo = pd.DataFrame(registros_regras)
        df_hist = self.carregar_regras_aprendidas()

        if not df_hist.empty:
            mask_manter = ~(
                (df_hist["tabela"] == tabela)
                & (df_hist["dt_referencia"] == dt_referencia)
            )
            df_final = pd.concat([df_hist[mask_manter], df_novo], ignore_index=True)
        else:
            df_final = df_novo

        df_final.to_parquet(
            self.arq_regras_aprendidas,
            engine="pyarrow",
            compression="snappy",
            index=False,
        )
        return df_novo

    def registrar_foto_particoes(
        self,
        run_id: str,
        dt_referencia: str,
        ts_execucao: str,
        tabela: str,
        particoes_dfs: dict[str, pd.DataFrame],
        mapa_camada_particoes: dict[str, str] | None = None,
    ) -> pd.DataFrame:
        """
        Registra uma fotografia de todas as partições da tabela em `_metadados_dq/`,
        anotando a `camada_qualidade` (`GOLD`, `SILVER`, `BRONZE`) de cada partição para
        que o aprendizado futuro utilize apenas partições `GOLD`.
        """
        mapa_camada = mapa_camada_particoes or {}
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
                    "camada_qualidade": mapa_camada.get(nome_particao, "GOLD"),
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
        chaves_primarias: list[str] | None = None,
        janela_captura_dias: int = 25,
    ) -> pd.DataFrame:
        """
        Tira uma fotografia da volumetria diária agrupada por `coluna_data`.
        Remove duplicatas de chave primária/linha antes da contagem para que partições
        BRONZE com duplicatas injetadas não inflem artificialmente a referência madura.
        """
        dt_ref = dt.date.fromisoformat(dt_referencia)

        pks_validas = [
            k for k in (chaves_primarias or []) if k in df_completo.columns
        ]
        if pks_validas:
            df_base = df_completo.drop_duplicates(subset=pks_validas)
        else:
            df_base = df_completo.drop_duplicates()

        serie_datas = pd.to_datetime(
            df_base[coluna_data], errors="coerce", format="ISO8601"
        ).dt.date
        serie_valida = serie_datas.dropna()

        contagem_por_data = serie_valida.value_counts().to_dict()

        volumes_cauda_estavel: list[int] = []
        for lag in range(8, janela_captura_dias + 1):
            d_alvo = dt_ref - dt.timedelta(days=lag)
            qtd = int(contagem_por_data.get(d_alvo, 0))
            if qtd > 0:
                volumes_cauda_estavel.append(qtd)

        if not volumes_cauda_estavel:
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
        Aprende AUTOMATICAMENTE a partir das fotografias anteriores, filtrando APENAS
        datas de evento saudáveis (`GOLD`) para evitar que datas com falha de ingestão
        contaminem a curva esperada (`fator_maturacao_esperado`) ou o `lag_estabilizacao_aprendido`.
        """
        df_fotos = self.carregar_fotos_eventos()
        if df_fotos.empty:
            return {
                "qtd_fotos_historico": 0,
                "lag_estabilizacao_aprendido": 0,
                "janela_avaliacao_aprendida": 5,
                "tolerancia_desvio_aprendida": 0.35,
                "curva_por_lag": {},
            }

        df_treino_bruto = df_fotos[
            (df_fotos["tabela"] == tabela)
            & (df_fotos["coluna_data"] == coluna_data)
            & (df_fotos["dt_referencia"] < dt_referencia_atual)
        ].copy()

        if df_treino_bruto.empty:
            return {
                "qtd_fotos_historico": 0,
                "lag_estabilizacao_aprendido": 0,
                "janela_avaliacao_aprendida": 5,
                "tolerancia_desvio_aprendida": 0.35,
                "curva_por_lag": {},
            }

        qtd_fotos_total = int(df_treino_bruto["dt_referencia"].nunique())

        # Filtra datas de evento contaminadas/anômalas (quarentena) do conjunto de treino:
        # Uma data_evento que teve 0 registros no lag 0, ou fator < 0.25 no lag >= 1,
        # ou fator < 0.55 no lag >= 2 sofreu falha de ingestão e não deve ensinar a curva!
        datas_contaminadas: set[str] = set()
        for _, r_ev in df_treino_bruto.iterrows():
            lag_ev = int(r_ev["lag_dias"])
            fator_ev = float(r_ev["fator_maturacao"])
            if lag_ev == 0 and fator_ev <= 0.02:
                datas_contaminadas.add(str(r_ev["data_evento"]))
            elif lag_ev == 1 and fator_ev < 0.25:
                datas_contaminadas.add(str(r_ev["data_evento"]))
            elif lag_ev >= 2 and fator_ev < 0.55:
                datas_contaminadas.add(str(r_ev["data_evento"]))

        df_treino_gold = df_treino_bruto[
            ~df_treino_bruto["data_evento"].isin(datas_contaminadas)
        ].copy()
        df_treino = df_treino_gold if not df_treino_gold.empty else df_treino_bruto

        # Pareia a MESMA data_evento em fotos consecutivas (apenas eventos GOLD): lag -> lag + 1
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
        stds_maduros: list[float] = []

        for lag, grupo in df_treino.groupby("lag_dias"):
            lag_int = int(lag)
            fatores = grupo["fator_maturacao"].astype(float).to_numpy()
            qtds = grupo["qtd_registros"].astype(float).to_numpy()
            n_fotos_lag = int(grupo["dt_referencia"].nunique())

            fator_medio = float(np.median(fatores))
            fator_std = float(np.std(fatores)) if len(fatores) > 1 else 0.05
            if lag_int >= 8:
                stds_maduros.append(fator_std)

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

        lag_estabilizacao = 0
        for lag_cand in range(0, max_lag_busca):
            info = curva_por_lag.get(lag_cand)
            if info is None:
                continue
            delta_prox = float(info["delta_registros_proxima_foto"])
            taxa_prox = float(info["taxa_crescimento_proxima_foto"])
            fator_esp = float(info["fator_maturacao_esperado"])

            ainda_cresce_entre_fotos = (delta_prox >= 1.0) or (taxa_prox > 0.03)
            sem_transicao_mas_incompleto = (
                lag_cand not in crescimento_por_lag and fator_esp < 0.88
            )

            if ainda_cresce_entre_fotos or sem_transicao_mas_incompleto:
                lag_estabilizacao = lag_cand + 1
            else:
                break

        janela_aprendida = max(5, int(lag_estabilizacao) + 5)

        std_ref_maduro = float(np.median(stds_maduros)) if stds_maduros else 0.08
        tolerancia_aprendida = float(
            np.clip(round(max(3.0 * std_ref_maduro, 0.30), 2), 0.25, 0.38)
        )

        linhas_silver = []
        for lag_int, info in sorted(curva_por_lag.items()):
            info["lag_estabilizacao_aprendido"] = int(lag_estabilizacao)
            info["janela_avaliacao_aprendida"] = int(janela_aprendida)
            info["tolerancia_desvio_aprendida"] = float(tolerancia_aprendida)
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
            "janela_avaliacao_aprendida": int(janela_aprendida),
            "tolerancia_desvio_aprendida": float(tolerancia_aprendida),
            "curva_por_lag": curva_por_lag,
        }
