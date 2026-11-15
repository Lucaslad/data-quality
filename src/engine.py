"""
Orquestrador principal do Motor de Data Quality.

Fluxo completo (Modo Incremental + Streaming de Memória + Auditoria de Reprocessamento + AWS Glue/S3):
1. Lê e valida o arquivo YAML de configuração (`config/dq_config.yaml`).
2. Inspeciona os metadados físicos (`mtime`/`ETag`, `size`, `fingerprint_fisico`) via `StorageBackend`:
   - Lê apenas as partições da **Janela Ativa**, partições novas, partições anteriormente em
     quarentena (`SILVER`/`BRONZE`) e partições históricas cujo arquivo físico mudou (`REPROCESSAMENTO_DETECTADO`).
   - Pula a leitura de Parquet de partições históricas `GOLD` intocadas (`CACHE_GOLD`).
   - No modo *Streaming* (`modo_streaming_memoria`), extrai o perfil completo de cada partição
     e libera o `pd.DataFrame` da memória imediatamente ($O(1)$ partições em RAM).
3. Aprende automaticamente as regras e limiares de cada tabela/coluna (`auto_learner.py`)
   sobre o histórico `GOLD` livre de vazamento temporal (*Data Leakage*).
4. Executa os checks de Partição (Schema, Freshness/Lacunas, Volumetria Sazonal, Evolução/Reprocessamento,
   Data vs Partição), Colunas (Completude, Unicidade Simples/Composta, Domínio, Distribution Drift PSI,
   Intervalo Numérico Dinâmico, Regex Vetorizado, Dígito Verificador Módulo 11, Cross-Column) e Maturação.
5. Calcula o Score Ponderado de Qualidade, classifica cada partição em `GOLD`, `SILVER` ou `BRONZE`,
   roteia os arquivos via `StorageBackend` e sincroniza o catálogo (`GlueCatalogIntegrator`).
6. Grava todos os artefatos com histórico deduplicado e particionado em `dados/saida/_metadados_dq/`.
"""

from __future__ import annotations

import datetime as dt
import logging
from pathlib import Path
import re
from typing import Any
import uuid

import pandas as pd

from src.auto_learner import aprender_regras_tabela
from src.catalog_integration import GlueCatalogIntegrator
from src.checks_colunas import (
    cache_gold_suporta_regras_atuais,
    executar_checks_colunas,
)
from src.checks_maturacao import executar_checks_maturacao
from src.checks_particao import executar_checks_particao
from src.config_loader import carregar_configuracao
from src.logging_utils import configurar_logger_dq
from src.models import ResultadoCheck
from src.snapshot_manager import SnapshotManager
from src.storage_backend import StorageBackend, obter_storage_backend

CHECKS_ESTRUTURAIS_CRITICOS = {
    "consistencia_schema",
    "freshness_particao",
    "unicidade_chave_primaria",
    "unicidade_linha_completa",
    "volumetria_historica",
    "evolucao_fotografias",
}
LIMITE_MAX_ANOMALIAS_SILVER = 6

PESOS_CHECKS_PADRAO: dict[str, float] = {
    "consistencia_schema": 3.0,
    "unicidade_chave_primaria": 3.0,
    "volumetria_historica": 3.0,
    "evolucao_fotografias": 3.0,
    "unicidade_linha_completa": 2.5,
    "freshness_particao": 2.5,
    "consistencia_data_particao": 2.0,
    "completude": 1.5,
    "dominio": 1.0,
    "drift_distribuicao": 1.0,
    "intervalo_numerico": 1.0,
    "formato_regex": 1.0,
    "digito_verificador": 1.0,
    "consistencia_entre_colunas": 1.5,
}
TETO_SCORE_BRONZE_PADRAO = 65.0


class DataQualityEngine:
    """Motor de Data Quality com leitura incremental, streaming, detecção de reprocessamento e roteamento Medallion."""

    def __init__(
        self,
        caminho_config: str | Path,
        base_project_dir: Path | None = None,
        storage_backend: StorageBackend | None = None,
        glue_client: Any = None,
    ) -> None:
        self.caminho_config = Path(caminho_config).resolve()
        self.base_dir = (
            Path(base_project_dir).resolve()
            if base_project_dir
            else self.caminho_config.parent.parent
        )
        self.config = carregar_configuracao(self.caminho_config)

        cfg_geral = self.config.get("configuracoes_gerais", {}) or {}
        self.dir_origem = self.base_dir / cfg_geral.get(
            "diretorio_origem", "dados/origem"
        )
        self.dir_saida = self.base_dir / cfg_geral.get(
            "diretorio_saida", "dados/saida"
        )
        self.dt_referencia_padrao = str(
            cfg_geral.get("data_referencia_padrao", "2026-10-30")
        )
        self.leitura_incremental = bool(
            cfg_geral.get(
                "leitura_incremental",
                cfg_geral.get("modo_incremental_producao", True),
            )
        )
        self.janela_diaria = int(
            cfg_geral.get("janela_leitura_recente_diaria", 5)
        )
        self.janela_mensal = int(
            cfg_geral.get("janela_leitura_recente_mensal", 2)
        )
        self.modo_streaming_memoria = bool(
            cfg_geral.get("modo_streaming_memoria", False)
        )
        self.particionar_metadados = bool(
            cfg_geral.get("particionar_metadados", True)
        )

        # Configurações de roteamento e score ponderado (Item 2.4)
        cfg_rot = cfg_geral.get("roteamento_e_score", {}) or {}
        crit_yaml = cfg_rot.get("checks_estruturais_criticos")
        if isinstance(crit_yaml, list) and crit_yaml:
            self.checks_estruturais_criticos = {str(x) for x in crit_yaml}
        else:
            self.checks_estruturais_criticos = set(CHECKS_ESTRUTURAIS_CRITICOS)

        self.limite_max_anomalias_silver = int(
            cfg_rot.get(
                "limite_max_anomalias_silver", LIMITE_MAX_ANOMALIAS_SILVER
            )
        )
        self.teto_score_bronze = float(
            cfg_rot.get("teto_score_bronze", TETO_SCORE_BRONZE_PADRAO)
        )
        self.pesos_checks = dict(PESOS_CHECKS_PADRAO)
        pesos_yaml = cfg_rot.get("pesos_checks")
        if isinstance(pesos_yaml, dict):
            for k_p, v_p in pesos_yaml.items():
                self.pesos_checks[str(k_p)] = float(v_p)

        # Logger estruturado (Item 3.4)
        self.logger = configurar_logger_dq(
            formato=cfg_geral.get("formato_log"),
            nivel=cfg_geral.get("nivel_log", logging.INFO),
        )

        # Abstração de Storage e Catálogo (Itens 4.3 e 4.4)
        self.storage = storage_backend or obter_storage_backend(self.dir_origem)
        self.snapshot_mgr = SnapshotManager(
            self.dir_saida,
            storage_backend=self.storage,
            particionar_metadados=self.particionar_metadados,
        )
        self.catalog_integrator = GlueCatalogIntegrator(
            cfg_catalogo=cfg_geral.get("integracao_catalogo"),
            metadados_dir=self.snapshot_mgr.metadados_dir,
            glue_client=glue_client,
        )

    def _obter_peso_check(self, r: ResultadoCheck) -> float:
        """Retorna o peso de criticidade de um check para cálculo do score ponderado."""
        if r.tipo_check in self.pesos_checks:
            return float(self.pesos_checks[r.tipo_check])
        if r.nivel_check == "MATURACAO_EVENTO":
            return 3.0 if r.tipo_check != "diagnostico_atraso_aprendido" else 1.0
        return 1.0

    def calcular_score_ponderado(
        self,
        checks: list[ResultadoCheck],
        tem_falha_estrutural: bool = False,
    ) -> float:
        """
        Calcula o score de qualidade ponderado pelo peso/criticidade de cada check (Item 2.4):
        - `OK` = 100% do peso
        - `WARNING` = 50% do peso
        - `ANOMALIA` = 0% do peso
        Aplica teto máximo (`teto_score_bronze`) quando há falha estrutural crítica (`BRONZE`).
        """
        if not checks:
            return 100.0
        soma_pesos = 0.0
        soma_obtida = 0.0
        for r in checks:
            peso = self._obter_peso_check(r)
            soma_pesos += peso
            if r.status == "OK":
                soma_obtida += peso
            elif r.status == "WARNING":
                soma_obtida += 0.5 * peso

        score = (
            round((soma_obtida / soma_pesos) * 100.0, 2)
            if soma_pesos > 0
            else 100.0
        )
        if tem_falha_estrutural:
            score = min(score, self.teto_score_bronze)
        return round(score, 2)

    @staticmethod
    def _extrair_hints_cfg_tabela(
        cfg_tab: dict[str, Any],
    ) -> tuple[list[str], dict[str, str]]:
        """Extrai hints de chave primária e regex customizado do YAML para o construtor de perfil."""
        pks_raw = cfg_tab.get("chaves_primarias", [])
        if isinstance(pks_raw, list):
            pks_hint = [
                str(k) for k in pks_raw if str(k).lower() not in ("auto", "")
            ]
        elif isinstance(pks_raw, str) and pks_raw.lower() not in ("auto", ""):
            pks_hint = [pks_raw]
        else:
            pks_hint = []

        regex_custom: dict[str, str] = {}
        regras_reg = (
            cfg_tab.get("checks", {})
            .get("colunas", {})
            .get("formato_regex", {})
            .get("regras", {})
        )
        if isinstance(regras_reg, dict):
            for col_r, val_r in regras_reg.items():
                if isinstance(val_r, dict) and val_r.get("padrao"):
                    p_str = str(val_r["padrao"])
                    if p_str.lower() != "auto":
                        regex_custom[str(col_r)] = p_str

        return pks_hint, regex_custom

    def _inspecionar_e_carregar_particoes_tabela(
        self,
        tabela: str,
        caminho_tabela: Path,
        coluna_particao: str,
        tipo_particao: str,
        dt_referencia: str,
        df_fotos_part_hist: pd.DataFrame,
        forcar_scan_completo: bool = False,
        usar_streaming: bool = False,
        pks_hint: list[str] | None = None,
        regras_regex_custom: dict[str, str] | None = None,
    ) -> tuple[
        dict[str, pd.DataFrame],
        dict[str, dict[str, Any]],
        dict[str, dict[str, Any]],
        dict[str, str],
    ]:
        """
        Inspeciona os metadados físicos das partições via `StorageBackend` e decide quais
        precisam ter o Parquet lido vs. quais partições históricas GOLD intocadas podem usar o cache (`CACHE_GOLD`).
        Quando `usar_streaming=True` (Item 4.1), extrai o perfil estatístico completo de cada partição
        e libera o `pd.DataFrame` da memória imediatamente, mantendo apenas `perfis_particoes`.
        """
        particoes_dfs: dict[str, pd.DataFrame] = {}
        perfis_particoes: dict[str, dict[str, Any]] = {}
        perfis_cache_gold: dict[str, dict[str, Any]] = {}
        mapa_modo_avaliacao: dict[str, str] = {}

        prefixo = f"{coluna_particao}="
        nomes_todas = self.storage.listar_particoes(caminho_tabela, prefixo)
        if not nomes_todas:
            return (
                particoes_dfs,
                perfis_particoes,
                perfis_cache_gold,
                mapa_modo_avaliacao,
            )

        ultima_particao = nomes_todas[-1]
        n_janela = (
            self.janela_diaria if tipo_particao == "diaria" else self.janela_mensal
        )
        particoes_janela_recente = set(nomes_todas[-n_janela:])

        # Mapeia último snapshot de cada partição para comparar o fingerprint físico
        snap_por_part: dict[str, dict[str, Any]] = {}
        if not df_fotos_part_hist.empty:
            df_tab_h = df_fotos_part_hist[
                (df_fotos_part_hist["tabela"] == tabela)
                & (df_fotos_part_hist["dt_referencia"] <= dt_referencia)
            ].sort_values("dt_referencia")
            df_ant = df_tab_h[df_tab_h["dt_referencia"] < dt_referencia]
            base_busca = df_ant if not df_ant.empty else df_tab_h
            for _, row_h in base_busca.iterrows():
                snap_por_part[str(row_h["particao"])] = row_h.to_dict()

            df_dia = df_tab_h[df_tab_h["dt_referencia"] == dt_referencia]
            snap_mesmo_dia: dict[str, dict[str, Any]] = {
                str(r["particao"]): r.to_dict() for _, r in df_dia.iterrows()
            }
        else:
            snap_mesmo_dia = {}

        usar_incremental = self.leitura_incremental and not forcar_scan_completo

        for nome_part in nomes_todas:
            pasta_part = caminho_tabela / nome_part
            meta_fisica = self.storage.inspecionar_metadados_fisicos(pasta_part)
            fp_atual = str(meta_fisica.get("fingerprint_fisico", ""))

            row_snap = snap_por_part.get(nome_part)
            row_dia = snap_mesmo_dia.get(nome_part)

            fp_snap = str(row_snap.get("fingerprint_fisico", "")) if row_snap else ""
            fp_dia = str(row_dia.get("fingerprint_fisico", "")) if row_dia else ""

            mudou_fisicamente_vs_ant = bool(
                fp_snap and fp_atual and fp_atual != fp_snap
            )
            mudou_fisicamente_intraday = bool(
                fp_dia and fp_atual and fp_atual != fp_dia
            )

            if not usar_incremental:
                modo = "LEITURA_COMPLETA"
            elif row_snap is None and row_dia is None:
                modo = "NOVA_PARTICAO"
            elif mudou_fisicamente_intraday or (
                mudou_fisicamente_vs_ant and nome_part < ultima_particao
            ):
                modo = "REPROCESSAMENTO_DETECTADO"
            else:
                ref_cache = row_snap if row_snap is not None else row_dia
                camada_ref = (
                    str(ref_cache.get("camada_qualidade", "GOLD"))
                    if ref_cache
                    else "BRONZE"
                )
                pdq_str = (
                    str(ref_cache.get("perfil_dq_json", "")) if ref_cache else ""
                )
                tem_perfil_valido = bool(
                    pdq_str and pdq_str not in ("", "{}", "null", "None")
                )

                if camada_ref != "GOLD":
                    modo = "QUARENTENA"
                elif nome_part in particoes_janela_recente or not tem_perfil_valido:
                    modo = "JANELA_RECENTE"
                elif not mudou_fisicamente_vs_ant:
                    modo = "CACHE_GOLD"
                else:
                    modo = "REPROCESSAMENTO_DETECTADO"

            mapa_modo_avaliacao[nome_part] = modo

            if modo == "CACHE_GOLD":
                ref_cache = row_snap if row_snap is not None else row_dia
                assert ref_cache is not None
                perfil_cached = {
                    "particao": nome_part,
                    "qtd_registros": int(ref_cache["qtd_registros"]),
                    "qtd_arquivos": int(meta_fisica.get("qtd_arquivos", 1)),
                    "tamanho_bytes": int(meta_fisica.get("tamanho_bytes", 0)),
                    "max_mtime_ns": int(meta_fisica.get("max_mtime_ns", 0)),
                    "fingerprint_fisico": fp_atual,
                    "hash_conteudo": str(ref_cache.get("hash_conteudo", "")),
                    "hashes_colunas_json": str(
                        ref_cache.get("hashes_colunas_json", "{}")
                    ),
                    "preenchidos_json": str(ref_cache.get("preenchidos_json", "{}")),
                    "nulos_json": str(ref_cache.get("nulos_json", "{}")),
                    "somas_numericas_json": str(
                        ref_cache.get("somas_numericas_json", "{}")
                    ),
                    "mins_numericos_json": str(
                        ref_cache.get("mins_numericos_json", "{}")
                    ),
                    "maxs_numericos_json": str(
                        ref_cache.get("maxs_numericos_json", "{}")
                    ),
                    "dominios_json": str(ref_cache.get("dominios_json", "{}")),
                    "perfil_dq_json": str(ref_cache.get("perfil_dq_json", "{}")),
                }
                perfis_particoes[nome_part] = perfil_cached
                perfis_cache_gold[nome_part] = perfil_cached
            else:
                df_part = self.storage.ler_particao_parquet(pasta_part)
                perfis_particoes[nome_part] = (
                    self.snapshot_mgr.construir_perfil_particao(
                        nome_particao=nome_part,
                        df_part=df_part,
                        tipo_particao=tipo_particao,
                        meta_fisica=meta_fisica,
                        chaves_primarias_hint=pks_hint,
                        regras_regex_custom=regras_regex_custom,
                    )
                )
                if not usar_streaming:
                    particoes_dfs[nome_part] = df_part
                else:
                    del df_part

        return (
            particoes_dfs,
            perfis_particoes,
            perfis_cache_gold,
            mapa_modo_avaliacao,
        )

    def _revalidar_cache_gold_contra_regras(
        self,
        caminho_tabela: Path,
        tipo_particao: str,
        regras_aprendidas: dict[str, Any],
        particoes_dfs: dict[str, pd.DataFrame],
        perfis_particoes: dict[str, dict[str, Any]],
        perfis_cache_gold: dict[str, dict[str, Any]],
        mapa_modo_avaliacao: dict[str, str],
        usar_streaming: bool,
        regras_regex_custom: dict[str, str] | None = None,
    ) -> None:
        """
        Verifica se alguma partição em `CACHE_GOLD` precisa ser relida do Parquet por conta de
        uma nova regra manual introduzida no YAML (ex.: nova chave composta ou novo Regex customizado
        sem estatística correspondente no cache, Itens 1.2 e 1.3).
        """
        if not perfis_cache_gold:
            return
        pks = regras_aprendidas.get("chaves_primarias", []) or []
        cfg_cols = regras_aprendidas.get("colunas", {}) or {}

        for nome_part in list(perfis_cache_gold.keys()):
            perfil_c = perfis_cache_gold[nome_part]
            if not cache_gold_suporta_regras_atuais(perfil_c, pks, cfg_cols):
                pasta_part = caminho_tabela / nome_part
                meta_fisica = self.storage.inspecionar_metadados_fisicos(pasta_part)
                df_part = self.storage.ler_particao_parquet(pasta_part)
                novo_perfil = self.snapshot_mgr.construir_perfil_particao(
                    nome_particao=nome_part,
                    df_part=df_part,
                    tipo_particao=tipo_particao,
                    meta_fisica=meta_fisica,
                    chaves_primarias_hint=pks,
                    regras_regex_custom=regras_regex_custom,
                )
                perfis_particoes[nome_part] = novo_perfil
                del perfis_cache_gold[nome_part]
                mapa_modo_avaliacao[nome_part] = "SCAN_NOVA_REGRA_YAML"
                if not usar_streaming:
                    particoes_dfs[nome_part] = df_part
                else:
                    del df_part

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
        perfis_particoes: dict[str, dict[str, Any]],
        mapa_modo_avaliacao: dict[str, str],
        resultados_tabela: list[ResultadoCheck],
        chaves_primarias: list[str] | None = None,
        copiar_arquivos: bool = True,
    ) -> list[dict[str, Any]]:
        """
        Classifica cada partição da tabela em `GOLD`, `SILVER` ou `BRONZE` usando score ponderado,
        copia os arquivos Parquet via `StorageBackend` e sincroniza o catálogo (`GlueCatalogIntegrator`).
        """
        nomes_particoes = sorted(perfis_particoes.keys())
        if not nomes_particoes:
            return []

        ultima_particao = nomes_particoes[-1]
        set_pks = set(chaves_primarias or [])

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

        # Remove pastas de partições obsoletas que não existem mais na origem
        if copiar_arquivos:
            conjunto_ativas = set(nomes_particoes)
            for dir_camada in (
                self.snapshot_mgr.bronze_dir,
                self.snapshot_mgr.silver_dir,
                self.snapshot_mgr.gold_dir,
            ):
                pasta_tab = dir_camada / tabela
                if pasta_tab.exists():
                    for sub in pasta_tab.iterdir():
                        if sub.is_dir() and sub.name not in conjunto_ativas:
                            self.storage.remover_particao(sub)

        registros_classificacao: list[dict[str, Any]] = []
        mapa_dirs_camada = {
            "GOLD": self.snapshot_mgr.gold_dir,
            "SILVER": self.snapshot_mgr.silver_dir,
            "BRONZE": self.snapshot_mgr.bronze_dir,
        }

        for nome_part in nomes_particoes:
            lista_r = checks_por_particao.get(nome_part, [])
            n_checks = len(lista_r)
            n_ok = sum(1 for r in lista_r if r.status == "OK")
            n_warn = sum(1 for r in lista_r if r.status == "WARNING")
            n_anom = sum(1 for r in lista_r if r.status == "ANOMALIA")

            anomalias_part = [r for r in lista_r if r.status == "ANOMALIA"]
            # Item 1.1: Qualquer anomalia em check estrutural crítico, maturação ou nulo em PK vai para BRONZE
            falhas_estruturais = [
                r
                for r in anomalias_part
                if r.tipo_check in self.checks_estruturais_criticos
                or r.nivel_check == "MATURACAO_EVENTO"
                or (r.tipo_check == "completude" and r.coluna in set_pks)
            ]
            tem_falha_estrutural = (
                len(falhas_estruturais) > 0
                or n_anom > self.limite_max_anomalias_silver
            )
            score_part = self.calcular_score_ponderado(
                lista_r, tem_falha_estrutural=tem_falha_estrutural
            )

            if n_anom == 0 and n_warn == 0:
                camada = "GOLD"
                pasta_base_camada = self.snapshot_mgr.gold_dir
                motivo = "100% dos checks aprovados (0 anomalias, 0 warnings)"
            elif tem_falha_estrutural:
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

            modo_av = mapa_modo_avaliacao.get(nome_part, "SCAN_PARQUET")
            pasta_origem_part = caminho_origem_tabela / nome_part
            pasta_destino_part = pasta_base_camada / tabela / nome_part
            caminho_rel_destino = (
                f"dados/saida/{camada.lower()}/{tabela}/{nome_part}"
            )

            qtd_arquivos_copiados = 0
            if copiar_arquivos:
                # Garante exclusividade: remove a partição das outras 2 camadas se existir
                for nome_cam_outra, dir_cam_outra in mapa_dirs_camada.items():
                    if nome_cam_outra == camada:
                        continue
                    pasta_outra = dir_cam_outra / tabela / nome_part
                    self.storage.remover_particao(pasta_outra)

                # Se for CACHE_GOLD e o arquivo já estiver em GOLD, evita recopiar
                n_arqs_dest = self.storage.contar_arquivos_parquet(
                    pasta_destino_part
                )
                if modo_av == "CACHE_GOLD" and camada == "GOLD" and n_arqs_dest > 0:
                    qtd_arquivos_copiados = n_arqs_dest
                elif self.storage.existe(pasta_origem_part):
                    qtd_arquivos_copiados = self.storage.copiar_particao(
                        pasta_origem_part, pasta_destino_part
                    )

            registros_classificacao.append(
                {
                    "run_id": run_id,
                    "dt_referencia": dt_referencia,
                    "ts_execucao": ts_execucao,
                    "tabela": tabela,
                    "particao": nome_part,
                    "modo_avaliacao": modo_av,
                    "qtd_registros": int(perfis_particoes[nome_part]["qtd_registros"]),
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

        # Hook pós-roteamento: sincroniza partições no catálogo (Glue Data Catalog / Iceberg / Manifesto, Item 4.4)
        self.catalog_integrator.sincronizar_particoes_tabela(
            run_id=run_id,
            dt_referencia=dt_referencia,
            ts_execucao=ts_execucao,
            tabela=tabela,
            coluna_particao=coluna_particao,
            classificacoes_tabela=registros_classificacao,
        )

        return registros_classificacao

    def executar(
        self,
        dt_referencia: str | None = None,
        filtro_tabelas: list[str] | None = None,
        verbose: bool = True,
        copiar_particoes: bool = True,
        aceitar_reprocessamento: bool = False,
        forcar_scan_completo: bool = False,
        modo_streaming: bool | None = None,
    ) -> dict[str, Any]:
        """
        Executa o pipeline completo de Data Quality para a `dt_referencia` informada,
        classificando e copiando cada partição para `gold/`, `silver/` ou `bronze/`.
        """
        dt_ref = dt_referencia or self.dt_referencia_padrao
        run_id = f"dq-{dt_ref.replace('-', '')}-{uuid.uuid4().hex[:8]}"
        ts_execucao = dt.datetime.now().isoformat(timespec="seconds")
        usar_streaming = (
            self.modo_streaming_memoria
            if modo_streaming is None
            else bool(modo_streaming)
        )

        if copiar_particoes:
            self._limpar_arquivos_soltos_camadas()

        tabelas_cfg = self.config.get("tabelas", {}) or {}
        todos_resultados: list[ResultadoCheck] = []
        todas_regras_aprendidas: list[dict[str, Any]] = []
        todas_classificacoes_particoes: list[dict[str, Any]] = []

        if verbose:
            modo_str = (
                "LEITURA_COMPLETA"
                if (forcar_scan_completo or not self.leitura_incremental)
                else "LEITURA_INCREMENTAL"
            )
            if usar_streaming:
                modo_str += "+STREAMING"
            self.logger.info("=" * 78)
            self.logger.info(
                "Iniciando Motor de Data Quality | run_id=%s | dt_referencia=%s | modo=%s",
                run_id,
                dt_ref,
                modo_str,
                extra={"run_id": run_id, "dt_referencia": dt_ref, "evento": "INICIO_EXECUCAO"},
            )
            if aceitar_reprocessamento:
                self.logger.info(
                    "  [!] Flag --aceitar-reprocessamento ATIVA: mutações em partições "
                    "históricas serão aceitas no baseline caso passem nos checks intrínsecos."
                )
            self.logger.info("=" * 78)

        df_fotos_part_hist = self.snapshot_mgr.carregar_fotos_particoes()

        for nome_tabela, cfg_tab in tabelas_cfg.items():
            if not isinstance(cfg_tab, dict):
                continue
            if not cfg_tab.get("ativo", True):
                if verbose:
                    self.logger.info(
                        "[-] Tabela `%s` desativada no YAML. Pulando...", nome_tabela
                    )
                continue
            if filtro_tabelas and nome_tabela not in filtro_tabelas:
                continue

            caminho_rel = cfg_tab.get("caminho_relativo", nome_tabela)
            caminho_tab = self.dir_origem / caminho_rel
            col_particao = str(cfg_tab.get("coluna_particao", "anomes"))
            tipo_particao = str(cfg_tab.get("tipo_particao", "mensal"))
            chaves_primarias_cfg = cfg_tab.get("chaves_primarias", []) or []
            pks_hint, regras_regex_custom = self._extrair_hints_cfg_tabela(cfg_tab)

            (
                particoes_dfs,
                perfis_particoes,
                perfis_cache_gold,
                mapa_modo_avaliacao,
            ) = self._inspecionar_e_carregar_particoes_tabela(
                tabela=nome_tabela,
                caminho_tabela=caminho_tab,
                coluna_particao=col_particao,
                tipo_particao=tipo_particao,
                dt_referencia=dt_ref,
                df_fotos_part_hist=df_fotos_part_hist,
                forcar_scan_completo=forcar_scan_completo,
                usar_streaming=usar_streaming,
                pks_hint=pks_hint,
                regras_regex_custom=regras_regex_custom,
            )

            if not perfis_particoes:
                if verbose:
                    self.logger.warning(
                        "[!] Nenhuma partição encontrada para `%s` em %s",
                        nome_tabela,
                        caminho_tab,
                    )
                continue

            checks_cfg = cfg_tab.get("checks", {}) or {}

            # 0. Auto-Aprendizado de Regras e Limiares (Estatísticas Robustas + Snapshots + Cache GOLD)
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
                perfis_particoes=perfis_particoes,
            )

            # Invalida CACHE_GOLD apenas para partições afetadas por eventual nova regra manual no YAML (Itens 1.2 e 1.3)
            self._revalidar_cache_gold_contra_regras(
                caminho_tabela=caminho_tab,
                tipo_particao=tipo_particao,
                regras_aprendidas=regras_aprendidas,
                particoes_dfs=particoes_dfs,
                perfis_particoes=perfis_particoes,
                perfis_cache_gold=perfis_cache_gold,
                mapa_modo_avaliacao=mapa_modo_avaliacao,
                usar_streaming=usar_streaming,
                regras_regex_custom=regras_regex_custom,
            )

            if verbose:
                total_linhas = sum(
                    int(p["qtd_registros"]) for p in perfis_particoes.values()
                )
                n_total_p = len(perfis_particoes)
                n_cache = len(perfis_cache_gold)
                n_lidas = n_total_p - n_cache
                reproc_parts = [
                    p
                    for p, m in mapa_modo_avaliacao.items()
                    if m == "REPROCESSAMENTO_DETECTADO"
                ]
                self.logger.info(
                    "\n[+] Avaliando `%s` (%d partições / %d linhas | Parquet lidos: %d, Cache GOLD: %d)...",
                    nome_tabela,
                    n_total_p,
                    total_linhas,
                    n_lidas,
                    n_cache,
                    extra={"run_id": run_id, "tabela": nome_tabela},
                )
                if reproc_parts:
                    self.logger.warning(
                        "    [!] REPROCESSAMENTO HISTÓRICO DETECTADO em %d partição(ões): %s",
                        len(reproc_parts),
                        reproc_parts,
                    )

            registros_regras_tab = regras_aprendidas.get(
                "registros_regras_silver", []
            )
            todas_regras_aprendidas.extend(registros_regras_tab)
            self.snapshot_mgr.registrar_regras_aprendidas(
                tabela=nome_tabela,
                dt_referencia=dt_ref,
                registros_regras=registros_regras_tab,
            )

            resultados_tabela: list[ResultadoCheck] = []
            pks_tabela = regras_aprendidas.get("chaves_primarias", []) or []

            # 1. Checks de Partição (Schema, Freshness/Lacunas, Volumetria, Evolução/Reprocessamento, Data vs Partição)
            res_part = executar_checks_particao(
                run_id=run_id,
                dt_referencia=dt_ref,
                ts_execucao=ts_execucao,
                tabela=nome_tabela,
                tipo_particao=tipo_particao,
                particoes_dfs=particoes_dfs,
                cfg_particao=regras_aprendidas["particao"],
                df_fotos_particoes_hist=df_fotos_part_hist,
                perfis_particoes=perfis_particoes,
                mapa_modo_avaliacao=mapa_modo_avaliacao,
                aceitar_reprocessamento=aceitar_reprocessamento,
            )
            resultados_tabela.extend(res_part)

            # 2. Checks de Colunas / Dados internos da partição (Parquet lidos + Cache GOLD + Streaming)
            res_cols = executar_checks_colunas(
                run_id=run_id,
                dt_referencia=dt_ref,
                ts_execucao=ts_execucao,
                tabela=nome_tabela,
                chaves_primarias=pks_tabela,
                particoes_dfs=particoes_dfs,
                cfg_colunas=regras_aprendidas["colunas"],
                perfis_cache_gold=perfis_cache_gold,
                perfis_particoes=perfis_particoes,
            )
            resultados_tabela.extend(res_cols)

            # Identifica partições com falha estrutural crítica para não contaminar a curva de maturação (Item 1.5)
            set_pks_tab = set(pks_tabela)
            particoes_contaminadas_estruturais = {
                r.particao
                for r in resultados_tabela
                if r.status == "ANOMALIA"
                and (
                    r.tipo_check in self.checks_estruturais_criticos
                    or (r.tipo_check == "completude" and r.coluna in set_pks_tab)
                )
            }

            # 3. Checks de Maturação / Atraso de Eventos (Aprendizado de Curva de Lag via Fotografias GOLD)
            res_mat = executar_checks_maturacao(
                run_id=run_id,
                dt_referencia=dt_ref,
                ts_execucao=ts_execucao,
                tabela=nome_tabela,
                particoes_dfs=particoes_dfs,
                cfg_maturacao=regras_aprendidas["maturacao_datas"],
                snapshot_mgr=self.snapshot_mgr,
                chaves_primarias=pks_tabela,
                perfis_cache_gold=perfis_cache_gold,
                perfis_particoes=perfis_particoes,
                particoes_contaminadas_estruturais=particoes_contaminadas_estruturais,
            )
            resultados_tabela.extend(res_mat)
            todos_resultados.extend(resultados_tabela)

            # Libera DataFrames após avaliação da tabela para minimizar pegada de RAM
            particoes_dfs.clear()

            # 4. Classifica cada partição, roteia para `gold/`, `silver/` ou `bronze/` e sincroniza catálogo
            classificacoes_tab = self._classificar_e_rotear_particoes_tabela(
                run_id=run_id,
                dt_referencia=dt_ref,
                ts_execucao=ts_execucao,
                tabela=nome_tabela,
                coluna_particao=col_particao,
                tipo_particao=tipo_particao,
                caminho_origem_tabela=caminho_tab,
                perfis_particoes=perfis_particoes,
                mapa_modo_avaliacao=mapa_modo_avaliacao,
                resultados_tabela=resultados_tabela,
                chaves_primarias=pks_tabela,
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
                particoes_dfs=None,
                mapa_camada_particoes=mapa_camada,
                perfis_particoes=perfis_particoes,
                mapa_modo_avaliacao=mapa_modo_avaliacao,
                tipo_particao=tipo_particao,
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
            todos_objetos_resultados=todos_resultados,
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
        todos_objetos_resultados: list[ResultadoCheck] | None = None,
    ) -> dict[str, Any]:
        """
        Persiste resultados, classificações de partições, resumos e alertas em `_metadados_dq/`,
        mantendo histórico completo deduplicado por `(dt_referencia, tabela)` (Item 3.4)
        e cópias particionadas por tabela/data (Item 4.2).
        """
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

        tabelas_exec = df_resultados["tabela"].unique().tolist()

        # --- 1. Histórico de todos os checks executados ---
        for tab_e, grp_e in df_resultados.groupby("tabela"):
            self.snapshot_mgr._salvar_metadado_particionado(
                "resultados_checks_detalhados", str(tab_e), dt_referencia, grp_e
            )

        if arq_checks.exists():
            df_ant = pd.read_parquet(arq_checks)
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
            for tab_c, grp_c in df_classificacao.groupby("tabela"):
                self.snapshot_mgr._salvar_metadado_particionado(
                    "classificacao_particoes", str(tab_c), dt_referencia, grp_c
                )
            if arq_classificacao.exists():
                df_c_ant = pd.read_parquet(arq_classificacao)
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

        # --- 3. Resumo e Score Ponderado de Qualidade por Tabela e Nível (Histórico Deduplicado, Itens 2.4 e 3.4) ---
        objs_por_tab_nivel: dict[tuple[str, str], list[ResultadoCheck]] = {}
        if todos_objetos_resultados:
            for obj_r in todos_objetos_resultados:
                objs_por_tab_nivel.setdefault(
                    (obj_r.tabela, obj_r.nivel_check), []
                ).append(obj_r)

        linhas_resumo = []
        for (tabela, nivel), grp in df_resultados.groupby(["tabela", "nivel_check"]):
            total = len(grp)
            qtd_ok = int((grp["status"] == "OK").sum())
            qtd_warn = int((grp["status"] == "WARNING").sum())
            qtd_anom = int((grp["status"] == "ANOMALIA").sum())
            objs_grp = objs_por_tab_nivel.get((str(tabela), str(nivel)))
            if objs_grp:
                score = self.calcular_score_ponderado(
                    objs_grp, tem_falha_estrutural=False
                )
            else:
                score = (
                    round(((qtd_ok + 0.5 * qtd_warn) / total) * 100.0, 2)
                    if total > 0
                    else 100.0
                )

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
        for tab_r, grp_r in df_resumo_gold.groupby("tabela"):
            self.snapshot_mgr._salvar_metadado_particionado(
                "resumo_qualidade_tabelas", str(tab_r), dt_referencia, grp_r
            )

        if arq_resumo.exists():
            df_r_ant = pd.read_parquet(arq_resumo)
            mask_r = ~(
                (df_r_ant["dt_referencia"] == dt_referencia)
                & (df_r_ant["tabela"].isin(tabelas_exec))
            )
            df_resumo_total = pd.concat(
                [df_r_ant[mask_r], df_resumo_gold], ignore_index=True
            )
        else:
            df_resumo_total = df_resumo_gold

        df_resumo_total.to_parquet(
            arq_resumo, engine="pyarrow", compression="snappy", index=False
        )

        # --- 4. Alertas Ativos (Anomalias e Warnings com Histórico Deduplicado, Item 3.4) ---
        df_alertas_gold = df_resultados[
            df_resultados["status"].isin(["ANOMALIA", "WARNING"])
        ].copy()
        if not df_alertas_gold.empty:
            for tab_a, grp_a in df_alertas_gold.groupby("tabela"):
                self.snapshot_mgr._salvar_metadado_particionado(
                    "alertas_anomalias", str(tab_a), dt_referencia, grp_a
                )

        if arq_alertas.exists():
            df_a_ant = pd.read_parquet(arq_alertas)
            mask_a = ~(
                (df_a_ant["dt_referencia"] == dt_referencia)
                & (df_a_ant["tabela"].isin(tabelas_exec))
            )
            df_alertas_total = pd.concat(
                [df_a_ant[mask_a], df_alertas_gold], ignore_index=True
            )
        else:
            df_alertas_total = df_alertas_gold

        df_alertas_total.to_parquet(
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
            "## 1. Roteamento de Partições e Leitura Incremental (`gold/`, `silver/`, `bronze/`)",
            "",
            "| Tabela | Total Partições | Lidas (Parquet Scan) | Cache GOLD (0 I/O) | Reproc. Físico | GOLD | SILVER | BRONZE |",
            "|---|---:|---:|---:|---:|---:|---:|---:|",
        ]

        if not df_classificacao.empty:
            for tabela, grp_c in df_classificacao.groupby("tabela"):
                n_tot = len(grp_c)
                n_cache = int(
                    (grp_c.get("modo_avaliacao", "") == "CACHE_GOLD").sum()
                )
                n_scan = n_tot - n_cache
                n_reproc = int(
                    (
                        grp_c.get("modo_avaliacao", "")
                        == "REPROCESSAMENTO_DETECTADO"
                    ).sum()
                )
                n_g = int((grp_c["camada_destino"] == "GOLD").sum())
                n_s = int((grp_c["camada_destino"] == "SILVER").sum())
                n_b = int((grp_c["camada_destino"] == "BRONZE").sum())
                linhas.append(
                    f"| `{tabela}` | {n_tot} | {n_scan} | {n_cache} | **{n_reproc}** | **{n_g}** | **{n_s}** | **{n_b}** |"
                )

            linhas.extend(
                [
                    "",
                    "### 1.1. Detalhamento por Partição Classificada",
                    "",
                    "| Tabela | Partição | Modo de Avaliação | Linhas | Score (%) | Camada Destino | Pasta Destino | Motivo da Classificação |",
                    "|---|---|---|---:|---:|---|---|---|",
                ]
            )
            for _, rc in df_classificacao.iterrows():
                modo_av = rc.get("modo_avaliacao", "SCAN_PARQUET")
                linhas.append(
                    f"| `{rc['tabela']}` | `{rc['particao']}` | `{modo_av}` | {rc['qtd_registros']} | "
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

        df_anom = df_resultados[
            df_resultados["status"].isin(["ANOMALIA", "WARNING"])
        ]
        if not df_anom.empty:
            linhas.extend(
                [
                    "",
                    "## 5. Detalhamento de Anomalias e Warnings Detectados",
                    "",
                    "| Tabela | Partição | Nível | Check | Coluna | Status | Observado | Esperado | Detalhes |",
                    "|---|---|---|---|---|---|---|---|---|",
                ]
            )
            for _, r in df_anom.iterrows():
                linhas.append(
                    f"| `{r['tabela']}` | `{r['particao']}` | `{r['nivel_check']}` | "
                    f"`{r['tipo_check']}` | `{r['coluna']}` | **{r['status']}** | `{r['valor_observado']}` | "
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
            self.logger.info("\nNenhum resultado gerado.")
            return

        if not df_classificacao.empty:
            self.logger.info("\n%s", "-" * 78)
            self.logger.info(
                "ROTEAMENTO DE PARTIÇÕES E EFICIÊNCIA DE LEITURA (GOLD / SILVER / BRONZE):"
            )
            self.logger.info("%s", "-" * 78)
            resumo_rot = []
            for tabela, grp in df_classificacao.groupby("tabela"):
                n_tot = len(grp)
                n_cache = int(
                    (grp.get("modo_avaliacao", "") == "CACHE_GOLD").sum()
                )
                n_reproc = int(
                    (
                        grp.get("modo_avaliacao", "")
                        == "REPROCESSAMENTO_DETECTADO"
                    ).sum()
                )
                resumo_rot.append(
                    {
                        "tabela": tabela,
                        "total": n_tot,
                        "lidas_parquet": n_tot - n_cache,
                        "cache_gold": n_cache,
                        "reprocessadas": n_reproc,
                        "GOLD": int((grp["camada_destino"] == "GOLD").sum()),
                        "SILVER": int((grp["camada_destino"] == "SILVER").sum()),
                        "BRONZE": int((grp["camada_destino"] == "BRONZE").sum()),
                    }
                )
            self.logger.info("%s", pd.DataFrame(resumo_rot).to_string(index=False))

            df_reproc = df_classificacao[
                df_classificacao.get("modo_avaliacao", "")
                == "REPROCESSAMENTO_DETECTADO"
            ]
            if not df_reproc.empty:
                self.logger.info("\nAuditoria de Partições Históricas Reprocessadas:")
                for _, rp in df_reproc.iterrows():
                    chk_f = df_resultados[
                        (df_resultados["tabela"] == rp["tabela"])
                        & (df_resultados["particao"] == rp["particao"])
                        & (df_resultados["tipo_check"] == "evolucao_fotografias")
                    ]
                    det_f = (
                        str(chk_f.iloc[0]["detalhes"])
                        if not chk_f.empty
                        else rp["motivo_classificacao"]
                    )
                    self.logger.info(
                        "  [%-6s] %s/%s -> %s",
                        rp["camada_destino"],
                        rp["tabela"],
                        rp["particao"],
                        det_f,
                    )

            df_nao_gold = df_classificacao[
                df_classificacao["camada_destino"] != "GOLD"
            ]
            if not df_nao_gold.empty:
                self.logger.info("\nPartições roteadas para SILVER ou BRONZE:")
                for _, rc in df_nao_gold.iterrows():
                    self.logger.info(
                        "  [%-6s] %s/%s (%s) -> %s | %s",
                        rc["camada_destino"],
                        rc["tabela"],
                        rc["particao"],
                        rc.get("modo_avaliacao", "SCAN"),
                        rc["caminho_destino"],
                        rc["motivo_classificacao"],
                    )

        self.logger.info("\n%s", "-" * 78)
        self.logger.info("RESUMO DE QUALIDADE POR TABELA E NÍVEL:")
        self.logger.info("%s", "-" * 78)
        self.logger.info(
            "%s",
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
            ].to_string(index=False),
        )

        df_mat = df_resultados[df_resultados["nivel_check"] == "MATURACAO_EVENTO"]
        if not df_mat.empty:
            self.logger.info("\n%s", "-" * 78)
            self.logger.info(
                "DESTAQUE: CHECK DE MATURAÇÃO / ATRASO DE EVENTO (AUTO-APRENDIDO):"
            )
            self.logger.info("%s", "-" * 78)
            for _, row in df_mat.iterrows():
                self.logger.info(
                    "  [%-8s] %s | %s (%s): %s",
                    row["status"],
                    row["tabela"],
                    row["coluna"],
                    row["tipo_check"],
                    row["detalhes"],
                )
