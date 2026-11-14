"""
Orquestrador principal do Motor de Data Quality.

Fluxo completo:
1. Lê o arquivo YAML de configuração (`config/dq_config.yaml`).
2. Carrega as partições Parquet da pasta `dados/origem/`.
3. Aprende automaticamente as regras e limiares de cada tabela/coluna (`auto_learner.py` e
   `snapshot_manager.py`).
4. Executa os checks de Partição, Maturação de Datas e Colunas.
5. Classifica cada partição individualmente em `GOLD`, `SILVER` ou `BRONZE` e copia
   os arquivos Parquet da partição exclusivamente para a respectiva camada em:
   - `dados/saida/gold/<tabela>/<particao>/` (Partições 100% OK)
   - `dados/saida/silver/<tabela>/<particao>/` (Partições com estrutura/PK/volumetria íntegras, mas com alertas/anomalias leves de coluna)
   - `dados/saida/bronze/<tabela>/<particao>/` (Partições em quarentena por falha crítica: PK duplicada, queda/pico de volume, falha de maturação ou múltiplas falhas combinadas)
6. Grava todos os artefatos de controle, snapshots, regras aprendidas e relatórios em
   `dados/saida/_metadados_dq/`.
"""

from __future__ import annotations

import datetime as dt
from pathlib import Path
import re
import shutil
from typing import Any
import uuid

import pandas as pd

from src.auto_learner import aprender_regras_tabela
from src.checks_colunas import executar_checks_colunas
from src.checks_maturacao import executar_checks_maturacao
from src.checks_particao import executar_checks_particao
from src.config_loader import carregar_configuracao
from src.models import ResultadoCheck
from src.snapshot_manager import SnapshotManager

CHECKS_ESTRUTURAIS_CRITICOS = {
    "unicidade_chave_primaria",
    "unicidade_linha_completa",
    "volumetria_historica",
    "evolucao_fotografias",
}
LIMITE_MAX_ANOMALIAS_SILVER = 6


class DataQualityEngine:
    """Motor de Data Quality com auto-aprendizado e roteamento de partições para Bronze, Silver e Gold."""

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

    def _limpar_arquivos_soltos_camadas(self) -> None:
        """
        Garante que as pastas `bronze/`, `silver/` e `gold/` contenham apenas
        as subpastas das tabelas e partições copiadas (e `.gitkeep`), mantendo
        os arquivos de metadados exclusivamente em `_metadados_dq/`.
        """
        for pasta_camada in (
            self.snapshot_mgr.bronze_dir,
            self.snapshot_mgr.silver_dir,
            self.snapshot_mgr.gold_dir,
        ):
            if not pasta_camada.exists():
                continue
            for item in pasta_camada.iterdir():
                if item.is_file() and item.name != ".gitkeep":
                    item.unlink()

    def _classificar_e_rotear_particoes_tabela(
        self,
        run_id: str,
        dt_referencia: str,
        ts_execucao: str,
        tabela: str,
        coluna_particao: str,
        tipo_particao: str,
        caminho_origem_tabela: Path,
        particoes_dfs: dict[str, pd.DataFrame],
        resultados_tabela: list[ResultadoCheck],
        copiar_arquivos: bool = True,
    ) -> list[dict[str, Any]]:
        """
        Classifica cada partição da tabela em `GOLD`, `SILVER` ou `BRONZE` e copia
        os arquivos Parquet da partição exclusivamente para a pasta da sua camada.
        """
        nomes_particoes = sorted(particoes_dfs.keys())
        if not nomes_particoes:
            return []

        ultima_particao = nomes_particoes[-1]

        # Agrupa os resultados de checks por partição (incluindo eventos de maturação da safra)
        checks_por_particao: dict[str, list[ResultadoCheck]] = {
            p: [] for p in nomes_particoes
        }
        regex_dt_evento = re.compile(r"\[(\d{4})-(\d{2})-(\d{2})\]")

        for r in resultados_tabela:
            if r.particao in checks_por_particao:
                checks_por_particao[r.particao].append(r)
            elif r.particao == "TODAS_PARTICOES":
                m = regex_dt_evento.search(r.coluna)
                if m:
                    ano, mes, dia = m.group(1), m.group(2), m.group(3)
                    chave_cand = (
                        f"{coluna_particao}={ano}{mes}"
                        if tipo_particao == "mensal"
                        else f"{coluna_particao}={ano}{mes}{dia}"
                    )
                    alvo = (
                        chave_cand
                        if chave_cand in checks_por_particao
                        else ultima_particao
                    )
                    checks_por_particao[alvo].append(r)
                else:
                    checks_por_particao[ultima_particao].append(r)

        # Se for copiar arquivos, limpa diretórios antigos desta tabela nas 3 camadas
        # para garantir que cada partição fique EXCLUSIVAMENTE na sua classificação atual.
        if copiar_arquivos:
            for dir_camada in (
                self.snapshot_mgr.bronze_dir,
                self.snapshot_mgr.silver_dir,
                self.snapshot_mgr.gold_dir,
            ):
                pasta_tab_antiga = dir_camada / tabela
                if pasta_tab_antiga.exists():
                    shutil.rmtree(pasta_tab_antiga)

        registros_classificacao: list[dict[str, Any]] = []

        for nome_part in nomes_particoes:
            lista_r = checks_por_particao.get(nome_part, [])
            n_checks = len(lista_r)
            n_ok = sum(1 for r in lista_r if r.status == "OK")
            n_warn = sum(1 for r in lista_r if r.status == "WARNING")
            n_anom = sum(1 for r in lista_r if r.status == "ANOMALIA")
            score_part = round((n_ok / n_checks) * 100.0, 2) if n_checks > 0 else 100.0

            anomalias_part = [r for r in lista_r if r.status == "ANOMALIA"]
            falhas_estruturais = [
                r
                for r in anomalias_part
                if r.tipo_check in CHECKS_ESTRUTURAIS_CRITICOS
                or r.nivel_check == "MATURACAO_EVENTO"
            ]

            # Regra de Classificação Exclusiva:
            # 1. GOLD: 0 anomalias e 0 warnings (100% saudável)
            # 2. BRONZE: quebra crítica (PK duplicada, queda/pico de volumetria, regressão de foto,
            #    quebra real de maturação) ou excesso de anomalias (> LIMITE_MAX_ANOMALIAS_SILVER)
            # 3. SILVER: sem quebra de PK/volumetria/maturação, mas com warning ou desvios pontuais de coluna
            if n_anom == 0 and n_warn == 0:
                camada = "GOLD"
                pasta_base_camada = self.snapshot_mgr.gold_dir
                motivo = "100% dos checks aprovados (0 anomalias, 0 warnings)"
            elif len(falhas_estruturais) > 0 or n_anom > LIMITE_MAX_ANOMALIAS_SILVER:
                camada = "BRONZE"
                pasta_base_camada = self.snapshot_mgr.bronze_dir
                tipos_crit = sorted(
                    {r.tipo_check for r in (falhas_estruturais or anomalias_part)}
                )
                motivo = (
                    f"Quarentena / Falha crítica ({n_anom} anomalia(s): "
                    f"{', '.join(tipos_crit[:4])})"
                )
            else:
                camada = "SILVER"
                pasta_base_camada = self.snapshot_mgr.silver_dir
                tipos_leves = sorted(
                    {
                        f"{r.tipo_check}({r.coluna})"
                        for r in lista_r
                        if r.status in ("ANOMALIA", "WARNING")
                    }
                )
                motivo = (
                    f"Estrutura/PK/Volumetria íntegras; desvios restritos a coluna/warning "
                    f"({n_anom} anomalia(s), {n_warn} warning(s): {', '.join(tipos_leves[:4])})"
                )

            pasta_origem_part = caminho_origem_tabela / nome_part
            pasta_destino_part = pasta_base_camada / tabela / nome_part
            caminho_rel_destino = (
                f"dados/saida/{camada.lower()}/{tabela}/{nome_part}"
            )

            qtd_arquivos_copiados = 0
            if copiar_arquivos and pasta_origem_part.exists():
                pasta_destino_part.mkdir(parents=True, exist_ok=True)
                for arq_pq in sorted(pasta_origem_part.glob("*.parquet")):
                    shutil.copy2(arq_pq, pasta_destino_part / arq_pq.name)
                    qtd_arquivos_copiados += 1

            registros_classificacao.append(
                {
                    "run_id": run_id,
                    "dt_referencia": dt_referencia,
                    "ts_execucao": ts_execucao,
                    "tabela": tabela,
                    "particao": nome_part,
                    "qtd_registros": len(particoes_dfs[nome_part]),
                    "total_checks": n_checks,
                    "qtd_ok": n_ok,
                    "qtd_warning": n_warn,
                    "qtd_anomalia": n_anom,
                    "score_qualidade_pct": score_part,
                    "camada_destino": camada,
                    "caminho_destino": caminho_rel_destino,
                    "arquivos_copiados": qtd_arquivos_copiados,
                    "motivo_classificacao": motivo,
                }
            )

        return registros_classificacao

    def executar(
        self,
        dt_referencia: str | None = None,
        filtro_tabelas: list[str] | None = None,
        verbose: bool = True,
        copiar_particoes: bool = True,
    ) -> dict[str, Any]:
        """
        Executa o pipeline completo de Data Quality para a `dt_referencia` informada,
        classificando e copiando cada partição para `gold/`, `silver/` ou `bronze/`.
        """
        dt_ref = dt_referencia or self.dt_referencia_padrao
        run_id = f"dq-{dt_ref.replace('-', '')}-{uuid.uuid4().hex[:8]}"
        ts_execucao = dt.datetime.now().isoformat(timespec="seconds")

        if copiar_particoes:
            self._limpar_arquivos_soltos_camadas()

        tabelas_cfg = self.config.get("tabelas", {}) or {}
        todos_resultados: list[ResultadoCheck] = []
        todas_regras_aprendidas: list[dict[str, Any]] = []
        todas_classificacoes_particoes: list[dict[str, Any]] = []

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
            chaves_primarias_cfg = cfg_tab.get("chaves_primarias", []) or []

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

            # 0. Auto-Aprendizado de Regras e Limiares (Estatísticas Robustas + Snapshots)
            regras_aprendidas = aprender_regras_tabela(
                run_id=run_id,
                dt_referencia=dt_ref,
                ts_execucao=ts_execucao,
                tabela=nome_tabela,
                tipo_particao=tipo_particao,
                chaves_primarias_cfg=chaves_primarias_cfg,
                particoes_dfs=particoes_dfs,
                df_fotos_particoes_hist=df_fotos_part_hist,
                checks_cfg=checks_cfg,
            )
            registros_regras_tab = regras_aprendidas.get("registros_regras_silver", [])
            todas_regras_aprendidas.extend(registros_regras_tab)
            self.snapshot_mgr.registrar_regras_aprendidas(
                tabela=nome_tabela,
                dt_referencia=dt_ref,
                registros_regras=registros_regras_tab,
            )

            resultados_tabela: list[ResultadoCheck] = []

            # 1. Checks de Partição (Volumetria Histórica, Evolução de Fotografias, Data vs Partição)
            res_part = executar_checks_particao(
                run_id=run_id,
                dt_referencia=dt_ref,
                ts_execucao=ts_execucao,
                tabela=nome_tabela,
                tipo_particao=tipo_particao,
                particoes_dfs=particoes_dfs,
                cfg_particao=regras_aprendidas["particao"],
                df_fotos_particoes_hist=df_fotos_part_hist,
            )
            resultados_tabela.extend(res_part)

            # 2. Checks de Maturação / Atraso de Eventos (Aprendizado de Curva de Lag via Fotografias GOLD)
            res_mat = executar_checks_maturacao(
                run_id=run_id,
                dt_referencia=dt_ref,
                ts_execucao=ts_execucao,
                tabela=nome_tabela,
                particoes_dfs=particoes_dfs,
                cfg_maturacao=regras_aprendidas["maturacao_datas"],
                snapshot_mgr=self.snapshot_mgr,
                chaves_primarias=regras_aprendidas["chaves_primarias"],
            )
            resultados_tabela.extend(res_mat)

            # 3. Checks de Colunas / Dados internos da partição
            res_cols = executar_checks_colunas(
                run_id=run_id,
                dt_referencia=dt_ref,
                ts_execucao=ts_execucao,
                tabela=nome_tabela,
                chaves_primarias=regras_aprendidas["chaves_primarias"],
                particoes_dfs=particoes_dfs,
                cfg_colunas=regras_aprendidas["colunas"],
            )
            resultados_tabela.extend(res_cols)
            todos_resultados.extend(resultados_tabela)

            # 4. Classifica cada partição e copia para `gold/`, `silver/` ou `bronze/`
            classificacoes_tab = self._classificar_e_rotear_particoes_tabela(
                run_id=run_id,
                dt_referencia=dt_ref,
                ts_execucao=ts_execucao,
                tabela=nome_tabela,
                coluna_particao=col_particao,
                tipo_particao=tipo_particao,
                caminho_origem_tabela=caminho_tab,
                particoes_dfs=particoes_dfs,
                resultados_tabela=resultados_tabela,
                copiar_arquivos=copiar_particoes,
            )
            todas_classificacoes_particoes.extend(classificacoes_tab)

            # 5. Registra a fotografia das partições marcada com sua camada_qualidade (GOLD/SILVER/BRONZE)
            mapa_camada = {
                r["particao"]: r["camada_destino"] for r in classificacoes_tab
            }
            self.snapshot_mgr.registrar_foto_particoes(
                run_id=run_id,
                dt_referencia=dt_ref,
                ts_execucao=ts_execucao,
                tabela=nome_tabela,
                particoes_dfs=particoes_dfs,
                mapa_camada_particoes=mapa_camada,
            )

        df_resultados = pd.DataFrame([r.to_dict() for r in todos_resultados])
        df_regras_silver = pd.DataFrame(todas_regras_aprendidas)
        df_classificacao = pd.DataFrame(todas_classificacoes_particoes)

        artefatos = self._persistir_metadados_e_relatorios(
            df_resultados=df_resultados,
            df_regras_silver=df_regras_silver,
            df_classificacao=df_classificacao,
            dt_referencia=dt_ref,
            run_id=run_id,
        )

        if verbose:
            self._imprimir_resumo_console(
                df_resultados,
                artefatos["df_resumo_gold"],
                df_classificacao,
            )

        return artefatos

    def _persistir_metadados_e_relatorios(
        self,
        df_resultados: pd.DataFrame,
        df_regras_silver: pd.DataFrame,
        df_classificacao: pd.DataFrame,
        dt_referencia: str,
        run_id: str,
    ) -> dict[str, Any]:
        """Persiste resultados, classificações de partições e relatórios em `_metadados_dq/`."""
        meta_dir = self.snapshot_mgr.metadados_dir
        arq_checks = meta_dir / "resultados_checks_detalhados.parquet"
        arq_classificacao = meta_dir / "classificacao_particoes.parquet"
        arq_resumo = meta_dir / "resumo_qualidade_tabelas.parquet"
        arq_alertas = meta_dir / "alertas_anomalias.parquet"
        arq_relatorio = meta_dir / "relatorio_ultima_execucao.md"

        if df_resultados.empty:
            return {
                "df_resultados_silver": df_resultados,
                "df_regras_silver": df_regras_silver,
                "df_classificacao_particoes": df_classificacao,
                "df_resumo_gold": pd.DataFrame(),
                "df_alertas_gold": pd.DataFrame(),
            }

        # --- 1. Histórico de todos os checks executados ---
        if arq_checks.exists():
            df_ant = pd.read_parquet(arq_checks)
            tabelas_exec = df_resultados["tabela"].unique().tolist()
            mask_manter = ~(
                (df_ant["dt_referencia"] == dt_referencia)
                & (df_ant["tabela"].isin(tabelas_exec))
            )
            df_checks_total = pd.concat(
                [df_ant[mask_manter], df_resultados], ignore_index=True
            )
        else:
            df_checks_total = df_resultados

        df_checks_total.to_parquet(
            arq_checks, engine="pyarrow", compression="snappy", index=False
        )

        # --- 2. Histórico de classificação e roteamento de partições ---
        if not df_classificacao.empty:
            if arq_classificacao.exists():
                df_c_ant = pd.read_parquet(arq_classificacao)
                tabelas_exec = df_classificacao["tabela"].unique().tolist()
                mask_c = ~(
                    (df_c_ant["dt_referencia"] == dt_referencia)
                    & (df_c_ant["tabela"].isin(tabelas_exec))
                )
                df_c_total = pd.concat(
                    [df_c_ant[mask_c], df_classificacao], ignore_index=True
                )
            else:
                df_c_total = df_classificacao

            df_c_total.to_parquet(
                arq_classificacao, engine="pyarrow", compression="snappy", index=False
            )

        # --- 3. Resumo e Score de Qualidade por Tabela e Nível ---
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
            arq_resumo, engine="pyarrow", compression="snappy", index=False
        )

        # --- 4. Alertas Ativos (Anomalias e Warnings) ---
        df_alertas_gold = df_resultados[
            df_resultados["status"].isin(["ANOMALIA", "WARNING"])
        ].copy()
        df_alertas_gold.to_parquet(
            arq_alertas, engine="pyarrow", compression="snappy", index=False
        )

        # --- 5. Relatório Markdown da última execução ---
        self._gerar_relatorio_markdown(
            arq_relatorio,
            dt_referencia,
            run_id,
            df_resumo_gold,
            df_resultados,
            df_regras_silver,
            df_classificacao,
        )

        return {
            "df_resultados_silver": df_resultados,
            "df_regras_silver": df_regras_silver,
            "df_classificacao_particoes": df_classificacao,
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
        df_regras_silver: pd.DataFrame,
        df_classificacao: pd.DataFrame,
    ) -> None:
        linhas = [
            f"# Relatório Executivo de Data Quality ({dt_referencia})",
            f"- **Run ID:** `{run_id}`",
            f"- **Total de Verificações Executadas:** `{len(df_resultados)}`",
            f"- **Total de Regras Aprendidas/Ativas:** `{len(df_regras_silver)}`",
            f"- **Total de Partições Classificadas e Copiadas:** `{len(df_classificacao)}`",
            "",
            "## 1. Roteamento de Partições para as Camadas (`gold/`, `silver/`, `bronze/`)",
            "",
            "| Tabela | Total Partições | GOLD (100% OK) | SILVER (Falha Leve / Warning) | BRONZE (Quarentena / Crítico) |",
            "|---|---:|---:|---:|---:|",
        ]

        if not df_classificacao.empty:
            for tabela, grp_c in df_classificacao.groupby("tabela"):
                n_tot = len(grp_c)
                n_g = int((grp_c["camada_destino"] == "GOLD").sum())
                n_s = int((grp_c["camada_destino"] == "SILVER").sum())
                n_b = int((grp_c["camada_destino"] == "BRONZE").sum())
                linhas.append(
                    f"| `{tabela}` | {n_tot} | **{n_g}** | **{n_s}** | **{n_b}** |"
                )

            linhas.extend(
                [
                    "",
                    "### 1.1. Detalhamento por Partição Copiada",
                    "",
                    "| Tabela | Partição | Linhas | Score (%) | Camada Destino | Pasta Destino | Motivo da Classificação |",
                    "|---|---|---:|---:|---|---|---|",
                ]
            )
            for _, rc in df_classificacao.iterrows():
                linhas.append(
                    f"| `{rc['tabela']}` | `{rc['particao']}` | {rc['qtd_registros']} | "
                    f"{rc['score_qualidade_pct']:.1f}% | **{rc['camada_destino']}** | "
                    f"`{rc['caminho_destino']}` | {rc['motivo_classificacao']} |"
                )

        linhas.extend(
            [
                "",
                "## 2. Resumo por Tabela e Nível de Check",
                "",
                "| Tabela | Nível | Total Checks | OK | Warning | Anomalia | Score (%) | Status |",
                "|---|---|---:|---:|---:|---:|---:|---|",
            ]
        )
        for _, r in df_resumo.iterrows():
            linhas.append(
                f"| `{r['tabela']}` | `{r['nivel_check']}` | {r['total_checks']} | "
                f"{r['qtd_ok']} | {r['qtd_warning']} | {r['qtd_anomalia']} | "
                f"{r['score_qualidade_pct']:.1f}% | **{r['status_geral']}** |"
            )

        if not df_regras_silver.empty:
            linhas.extend(
                [
                    "",
                    "## 3. Regras e Limiares Aprendidos pelo Motor",
                    "",
                    "| Tabela | Nível | Tipo de Regra | Coluna | Regra Aprendida | Origem | Detalhes |",
                    "|---|---|---|---|---|---|---|",
                ]
            )
            for _, r in df_regras_silver.iterrows():
                linhas.append(
                    f"| `{r['tabela']}` | `{r['nivel_check']}` | `{r['tipo_check']}` | "
                    f"`{r['coluna']}` | `{r['regra_aprendida']}` | **{r['origem_regra']}** | "
                    f"{r['detalhes_aprendizado']} |"
                )

        df_mat = df_resultados[df_resultados["nivel_check"] == "MATURACAO_EVENTO"]
        if not df_mat.empty:
            linhas.extend(
                [
                    "",
                    "## 4. Análise de Maturação / Atraso de Datas",
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
                    "## 5. Detalhamento de Anomalias Detectadas",
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
        self,
        df_resultados: pd.DataFrame,
        df_resumo_gold: pd.DataFrame,
        df_classificacao: pd.DataFrame,
    ) -> None:
        if df_resumo_gold.empty:
            print("\nNenhum resultado gerado.")
            return

        if not df_classificacao.empty:
            print(f"\n{'-' * 78}")
            print("ROTEAMENTO DE PARTIÇÕES (PASTAS GOLD / SILVER / BRONZE):")
            print(f"{'-' * 78}")
            resumo_rot = []
            for tabela, grp in df_classificacao.groupby("tabela"):
                resumo_rot.append(
                    {
                        "tabela": tabela,
                        "total_particoes": len(grp),
                        "GOLD": int((grp["camada_destino"] == "GOLD").sum()),
                        "SILVER": int((grp["camada_destino"] == "SILVER").sum()),
                        "BRONZE": int((grp["camada_destino"] == "BRONZE").sum()),
                    }
                )
            print(pd.DataFrame(resumo_rot).to_string(index=False))

            df_nao_gold = df_classificacao[df_classificacao["camada_destino"] != "GOLD"]
            if not df_nao_gold.empty:
                print("\nPartições roteadas para SILVER ou BRONZE:")
                for _, rc in df_nao_gold.iterrows():
                    print(
                        f"  [{rc['camada_destino']:6s}] {rc['tabela']}/{rc['particao']} "
                        f"-> {rc['caminho_destino']} | {rc['motivo_classificacao']}"
                    )

        print(f"\n{'-' * 78}")
        print("RESUMO DE QUALIDADE POR TABELA E NÍVEL:")
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
            print("DESTAQUE: CHECK DE MATURAÇÃO / ATRASO DE EVENTO (AUTO-APRENDIDO):")
            print(f"{'-' * 78}")
            for _, row in df_mat.iterrows():
                print(
                    f"  [{row['status']:8s}] {row['tabela']} | {row['coluna']} ({row['tipo_check']}): "
                    f"{row['detalhes']}"
                )
