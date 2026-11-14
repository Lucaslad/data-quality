"""
Orquestrador principal do Motor de Data Quality.

Lê o arquivo YAML de configuração, carrega as partições Parquet da pasta `dados/origem/`,
executa os checks de Partição, Maturação de Datas (aprendizado de atraso via fotografias)
e Colunas, e grava os artefatos nas camadas Bronze, Silver e Gold em `dados/saida/`.
"""

from __future__ import annotations

import datetime as dt
from pathlib import Path
from typing import Any
import uuid

import pandas as pd

from src.checks_colunas import executar_checks_colunas
from src.checks_maturacao import executar_checks_maturacao
from src.checks_particao import executar_checks_particao
from src.config_loader import carregar_configuracao
from src.models import ResultadoCheck
from src.snapshot_manager import SnapshotManager


class DataQualityEngine:
    """Motor de Data Quality configurável via YAML com suporte a aprendizado por fotografias."""

    def __init__(self, caminho_config: str | Path, base_project_dir: Path | None = None) -> None:
        self.caminho_config = Path(caminho_config).resolve()
        self.base_dir = (
            Path(base_project_dir).resolve()
            if base_project_dir
            else self.caminho_config.parent.parent
        )
        self.config = carregar_configuracao(self.caminho_config)

        cfg_geral = self.config.get("configuracoes_gerais", {})
        self.dir_origem = self.base_dir / cfg_geral.get("diretorio_origem", "dados/origem")
        self.dir_saida = self.base_dir / cfg_geral.get("diretorio_saida", "dados/saida")
        self.dt_referencia_padrao = str(
            cfg_geral.get("data_referencia_padrao", "2026-10-30")
        )

        self.snapshot_mgr = SnapshotManager(self.dir_saida)

    def _carregar_particoes_tabela(
        self, caminho_tabela: Path, coluna_particao: str
    ) -> dict[str, pd.DataFrame]:
        """Carrega todas as partições estilo Hive (`col=valor/*.parquet`) de uma tabela."""
        particoes: dict[str, pd.DataFrame] = {}
        if not caminho_tabela.exists():
            return particoes

        prefixo = f"{coluna_particao}="
        for pasta_part in sorted(caminho_tabela.iterdir()):
            if not pasta_part.is_dir() or not pasta_part.name.startswith(prefixo):
                continue
            arquivos_parquet = sorted(pasta_part.glob("*.parquet"))
            if not arquivos_parquet:
                continue
            dfs = [pd.read_parquet(arq) for arq in arquivos_parquet]
            particoes[pasta_part.name] = pd.concat(dfs, ignore_index=True)

        return particoes

    def executar(
        self,
        dt_referencia: str | None = None,
        filtro_tabelas: list[str] | None = None,
        verbose: bool = True,
    ) -> dict[str, Any]:
        """
        Executa o pipeline completo de Data Quality para a `dt_referencia` informada.
        Retorna um dicionário com os DataFrames gerados nas camadas Silver e Gold.
        """
        dt_ref = dt_referencia or self.dt_referencia_padrao
        run_id = f"dq-{dt_ref.replace('-', '')}-{uuid.uuid4().hex[:8]}"
        ts_execucao = dt.datetime.now().isoformat(timespec="seconds")

        tabelas_cfg = self.config.get("tabelas", {}) or {}
        todos_resultados: list[ResultadoCheck] = []

        if verbose:
            print(f"\n{'=' * 78}")
            print(f"Iniciando Motor de Data Quality | run_id={run_id} | dt_referencia={dt_ref}")
            print(f"{'=' * 78}")

        df_fotos_part_hist = self.snapshot_mgr.carregar_fotos_particoes()

        for nome_tabela, cfg_tab in tabelas_cfg.items():
            if not isinstance(cfg_tab, dict):
                continue
            if not cfg_tab.get("ativo", True):
                if verbose:
                    print(f"[-] Tabela `{nome_tabela}` desativada no YAML. Pulando...")
                continue
            if filtro_tabelas and nome_tabela not in filtro_tabelas:
                continue

            caminho_rel = cfg_tab.get("caminho_relativo", nome_tabela)
            caminho_tab = self.dir_origem / caminho_rel
            col_particao = str(cfg_tab.get("coluna_particao", "anomes"))
            tipo_particao = str(cfg_tab.get("tipo_particao", "mensal"))
            chaves_primarias = cfg_tab.get("chaves_primarias", []) or []

            particoes_dfs = self._carregar_particoes_tabela(caminho_tab, col_particao)
            if not particoes_dfs:
                if verbose:
                    print(f"[!] Nenhuma partição encontrada para `{nome_tabela}` em {caminho_tab}")
                continue

            if verbose:
                total_linhas = sum(len(df) for df in particoes_dfs.values())
                print(
                    f"\n[+] Avaliando `{nome_tabela}` "
                    f"({len(particoes_dfs)} partições, {total_linhas} registros totais)..."
                )

            checks_cfg = cfg_tab.get("checks", {}) or {}

            # 1. Checks de Partição (Volumetria Histórica, Evolução de Fotografias, Data vs Partição)
            res_part = executar_checks_particao(
                run_id=run_id,
                dt_referencia=dt_ref,
                ts_execucao=ts_execucao,
                tabela=nome_tabela,
                tipo_particao=tipo_particao,
                particoes_dfs=particoes_dfs,
                cfg_particao=checks_cfg.get("particao", {}),
                df_fotos_particoes_hist=df_fotos_part_hist,
            )
            todos_resultados.extend(res_part)

            # 2. Registra a fotografia das partições da execução atual na camada Bronze
            self.snapshot_mgr.registrar_foto_particoes(
                run_id=run_id,
                dt_referencia=dt_ref,
                ts_execucao=ts_execucao,
                tabela=nome_tabela,
                particoes_dfs=particoes_dfs,
            )

            # 3. Checks de Maturação / Atraso de Eventos (Aprendizado de Curva de Lag via Fotografias)
            res_mat = executar_checks_maturacao(
                run_id=run_id,
                dt_referencia=dt_ref,
                ts_execucao=ts_execucao,
                tabela=nome_tabela,
                particoes_dfs=particoes_dfs,
                cfg_maturacao=checks_cfg.get("maturacao_datas", {}),
                snapshot_mgr=self.snapshot_mgr,
            )
            todos_resultados.extend(res_mat)

            # 4. Checks de Colunas / Dados internos da partição
            res_cols = executar_checks_colunas(
                run_id=run_id,
                dt_referencia=dt_ref,
                ts_execucao=ts_execucao,
                tabela=nome_tabela,
                chaves_primarias=chaves_primarias,
                particoes_dfs=particoes_dfs,
                cfg_colunas=checks_cfg.get("colunas", {}),
            )
            todos_resultados.extend(res_cols)

        df_resultados = pd.DataFrame([r.to_dict() for r in todos_resultados])
        artefatos = self._persistir_camadas_silver_e_gold(
            df_resultados=df_resultados,
            dt_referencia=dt_ref,
            run_id=run_id,
        )

        if verbose:
            self._imprimir_resumo_console(df_resultados, artefatos["df_resumo_gold"])

        return artefatos

    def _persistir_camadas_silver_e_gold(
        self,
        df_resultados: pd.DataFrame,
        dt_referencia: str,
        run_id: str,
    ) -> dict[str, Any]:
        """Persiste os resultados detalhados na camada Silver e indicadores/alertas na Gold."""
        arq_silver_checks = self.snapshot_mgr.silver_dir / "resultados_checks_detalhados.parquet"
        arq_gold_resumo = self.snapshot_mgr.gold_dir / "resumo_qualidade_tabelas.parquet"
        arq_gold_alertas = self.snapshot_mgr.gold_dir / "alertas_anomalias.parquet"
        arq_gold_relatorio = self.snapshot_mgr.gold_dir / "relatorio_ultima_execucao.md"

        if df_resultados.empty:
            return {
                "df_resultados_silver": df_resultados,
                "df_resumo_gold": pd.DataFrame(),
                "df_alertas_gold": pd.DataFrame(),
            }

        # --- Camada Silver: Histórico de todos os checks executados ---
        if arq_silver_checks.exists():
            df_ant = pd.read_parquet(arq_silver_checks)
            # Substitui execuções anteriores da mesma dt_referencia e tabela para idempotência
            tabelas_exec = df_resultados["tabela"].unique().tolist()
            mask_manter = ~(
                (df_ant["dt_referencia"] == dt_referencia)
                & (df_ant["tabela"].isin(tabelas_exec))
            )
            df_silver_total = pd.concat(
                [df_ant[mask_manter], df_resultados], ignore_index=True
            )
        else:
            df_silver_total = df_resultados

        df_silver_total.to_parquet(
            arq_silver_checks, engine="pyarrow", compression="snappy", index=False
        )

        # --- Camada Gold 1: Resumo e Score de Qualidade por Tabela e Nível ---
        linhas_resumo = []
        for (tabela, nivel), grp in df_resultados.groupby(["tabela", "nivel_check"]):
            total = len(grp)
            qtd_ok = int((grp["status"] == "OK").sum())
            qtd_warn = int((grp["status"] == "WARNING").sum())
            qtd_anom = int((grp["status"] == "ANOMALIA").sum())
            score = round((qtd_ok / total) * 100.0, 2) if total > 0 else 100.0

            linhas_resumo.append(
                {
                    "run_id": run_id,
                    "dt_referencia": dt_referencia,
                    "tabela": tabela,
                    "nivel_check": nivel,
                    "total_checks": total,
                    "qtd_ok": qtd_ok,
                    "qtd_warning": qtd_warn,
                    "qtd_anomalia": qtd_anom,
                    "score_qualidade_pct": score,
                    "status_geral": (
                        "CRITICO"
                        if qtd_anom > 0
                        else ("ATENCAO" if qtd_warn > 0 else "SAUDAVEL")
                    ),
                }
            )

        df_resumo_gold = pd.DataFrame(linhas_resumo)
        df_resumo_gold.to_parquet(
            arq_gold_resumo, engine="pyarrow", compression="snappy", index=False
        )

        # --- Camada Gold 2: Alertas Ativos (Anomalias e Warnings) ---
        df_alertas_gold = df_resultados[
            df_resultados["status"].isin(["ANOMALIA", "WARNING"])
        ].copy()
        df_alertas_gold.to_parquet(
            arq_gold_alertas, engine="pyarrow", compression="snappy", index=False
        )

        # --- Camada Gold 3: Relatório Markdown da última execução ---
        self._gerar_relatorio_markdown(
            arq_gold_relatorio, dt_referencia, run_id, df_resumo_gold, df_resultados
        )

        return {
            "df_resultados_silver": df_resultados,
            "df_resumo_gold": df_resumo_gold,
            "df_alertas_gold": df_alertas_gold,
        }

    def _gerar_relatorio_markdown(
        self,
        caminho_md: Path,
        dt_referencia: str,
        run_id: str,
        df_resumo: pd.DataFrame,
        df_resultados: pd.DataFrame,
    ) -> None:
        linhas = [
            f"# Relatório Executivo de Data Quality ({dt_referencia})",
            f"- **Run ID:** `{run_id}`",
            f"- **Total de Verificações Executadas:** `{len(df_resultados)}`",
            "",
            "## 1. Resumo por Tabela e Nível de Check",
            "",
            "| Tabela | Nível | Total Checks | OK | Warning | Anomalia | Score (%) | Status |",
            "|---|---|---:|---:|---:|---:|---:|---|",
        ]
        for _, r in df_resumo.iterrows():
            linhas.append(
                f"| `{r['tabela']}` | `{r['nivel_check']}` | {r['total_checks']} | "
                f"{r['qtd_ok']} | {r['qtd_warning']} | {r['qtd_anomalia']} | "
                f"{r['score_qualidade_pct']:.1f}% | **{r['status_geral']}** |"
            )

        df_mat = df_resultados[df_resultados["nivel_check"] == "MATURACAO_EVENTO"]
        if not df_mat.empty:
            linhas.extend(
                [
                    "",
                    "## 2. Análise de Maturação / Atraso de Datas (`dt_cadastro_chave_pix`)",
                    "",
                    "| Tabela | Coluna[Data Evento] | Check (Lag) | Observado | Esperado (Curva Aprendida) | Status | Detalhes |",
                    "|---|---|---|---:|---|---|---|",
                ]
            )
            for _, r in df_mat.iterrows():
                linhas.append(
                    f"| `{r['tabela']}` | `{r['coluna']}` | `{r['tipo_check']}` | "
                    f"`{r['valor_observado']}` | `{r['valor_esperado']}` | **{r['status']}** | {r['detalhes']} |"
                )

        df_anom = df_resultados[df_resultados["status"] == "ANOMALIA"]
        if not df_anom.empty:
            linhas.extend(
                [
                    "",
                    "## 3. Detalhamento de Anomalias Detectadas",
                    "",
                    "| Tabela | Partição | Nível | Check | Coluna | Observado | Esperado | Detalhes |",
                    "|---|---|---|---|---|---|---|---|",
                ]
            )
            for _, r in df_anom.iterrows():
                linhas.append(
                    f"| `{r['tabela']}` | `{r['particao']}` | `{r['nivel_check']}` | "
                    f"`{r['tipo_check']}` | `{r['coluna']}` | `{r['valor_observado']}` | "
                    f"`{r['valor_esperado']}` | {r['detalhes']} |"
                )

        caminho_md.write_text("\n".join(linhas) + "\n", encoding="utf-8")

    def _imprimir_resumo_console(
        self, df_resultados: pd.DataFrame, df_resumo_gold: pd.DataFrame
    ) -> None:
        if df_resumo_gold.empty:
            print("\nNenhum resultado gerado.")
            return

        print(f"\n{'-' * 78}")
        print("RESUMO DE QUALIDADE (CAMADA GOLD):")
        print(f"{'-' * 78}")
        print(
            df_resumo_gold[
                [
                    "tabela",
                    "nivel_check",
                    "total_checks",
                    "qtd_ok",
                    "qtd_warning",
                    "qtd_anomalia",
                    "score_qualidade_pct",
                    "status_geral",
                ]
            ].to_string(index=False)
        )

        df_mat = df_resultados[df_resultados["nivel_check"] == "MATURACAO_EVENTO"]
        if not df_mat.empty:
            print(f"\n{'-' * 78}")
            print("DESTAQUE: CHECK DE MATURAÇÃO / ATRASO DE EVENTO (dt_cadastro_chave_pix):")
            print(f"{'-' * 78}")
            for _, row in df_mat.iterrows():
                print(
                    f"  [{row['status']:8s}] {row['tabela']} | {row['coluna']} ({row['tipo_check']}): "
                    f"{row['detalhes']}"
                )
