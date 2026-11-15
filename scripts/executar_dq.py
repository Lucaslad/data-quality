"""
Script de execução do Motor de Data Quality.

Exemplos de uso:
  # Executa com simulação prévia de fotografias (D-4 a D-1) para treinar o aprendizado e avalia D (2026-10-30):
  python scripts/executar_dq.py --simular-historico-fotos

  # Execução incremental padrão na data de referência do YAML (usa Cache GOLD para partições intocadas):
  python scripts/executar_dq.py

  # Simula 4 cenários de Reprocessamento Histórico (Idempotente, Mutação Silenciosa, Expurgo e Correção):
  python scripts/executar_dq.py --simular-reprocessamento

  # Aceita intencionalmente mutações de um reprocessamento histórico/backfill legítimo no baseline GOLD:
  python scripts/executar_dq.py --aceitar-reprocessamento

  # Força leitura completa de todos os arquivos Parquet (desativa cache incremental):
  python scripts/executar_dq.py --forcar-scan-completo
"""

from __future__ import annotations

import argparse
import datetime as dt
from pathlib import Path
import sys

ROOT_DIR = Path(__file__).resolve().parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from scripts.gerar_dados_ficticios import (  # noqa: E402
    gerar_bases_para_data,
    simular_cenarios_reprocessamento_historico,
)
from src.engine import DataQualityEngine  # noqa: E402


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Executa o Motor de Data Quality configurável via YAML."
    )
    parser.add_argument(
        "--config",
        type=str,
        default=str(ROOT_DIR / "config" / "dq_config.yaml"),
        help="Caminho para o arquivo YAML de configuração.",
    )
    parser.add_argument(
        "--dt-referencia",
        type=str,
        default=None,
        help="Data de referência da execução (formato YYYY-MM-DD). Se omitida, usa a do YAML.",
    )
    parser.add_argument(
        "--tabela",
        type=str,
        nargs="*",
        default=None,
        help="Lista opcional de tabelas específicas para executar.",
    )
    parser.add_argument(
        "--simular-historico-fotos",
        action="store_true",
        help=(
            "Simula execuções nos 4 dias anteriores à data de referência para tirar "
            "fotografias (snapshots) e consolidar o aprendizado da curva de atraso."
        ),
    )
    parser.add_argument(
        "--simular-reprocessamento",
        action="store_true",
        help=(
            "Injeta 4 cenários reais de reprocessamento de partições históricas "
            "(idempotente, mutação silenciosa de valores, expurgo de linhas e correção de BRONZE) "
            "para demonstrar a detecção automática via Fingerprint Físico e Lógico."
        ),
    )
    parser.add_argument(
        "--aceitar-reprocessamento",
        action="store_true",
        help=(
            "Autoriza que mutações em partições históricas GOLD (ex.: backfill/expurgo intencional) "
            "sejam aceitas e atualizem o baseline GOLD caso passem nos demais checks de qualidade."
        ),
    )
    parser.add_argument(
        "--forcar-scan-completo",
        action="store_true",
        help="Desativa o cache incremental e força a leitura física de todos os arquivos Parquet.",
    )
    parser.add_argument(
        "--streaming",
        action="store_true",
        help="Ativa processamento em Streaming de partições (libera DataFrames da RAM após extrair perfil).",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    engine = DataQualityEngine(caminho_config=args.config, base_project_dir=ROOT_DIR)
    dt_ref_str = args.dt_referencia or engine.dt_referencia_padrao
    dt_ref = dt.date.fromisoformat(dt_ref_str)

    if args.simular_historico_fotos:
        print(
            "\n[MODO SIMULAÇÃO DE FOTOGRAFIAS] Gerando snapshots históricos (D-4 a D-1) "
            "para aprendizado automático do atraso e evolução de partições passadas..."
        )
        for arq_ant in (
            engine.snapshot_mgr.arq_fotos_particoes,
            engine.snapshot_mgr.arq_fotos_eventos,
            engine.snapshot_mgr.arq_curva_aprendida,
            engine.snapshot_mgr.arq_regras_aprendidas,
        ):
            if arq_ant.exists():
                arq_ant.unlink()

        for dias_atras in (4, 3, 2, 1):
            dt_hist = dt_ref - dt.timedelta(days=dias_atras)
            gerar_bases_para_data(dt_hist, verbose=False)
            print(f"  -> Tirando fotografia da execução em {dt_hist.isoformat()}...")
            engine.executar(
                dt_referencia=dt_hist.isoformat(),
                filtro_tabelas=args.tabela,
                verbose=False,
                copiar_particoes=False,
            )

        # Restaura a base de origem para a data de referência final (D)
        gerar_bases_para_data(dt_ref, verbose=False)

    if args.simular_reprocessamento:
        # Garante que exista um snapshot baseline prévio antes de injetar o reprocessamento
        if not engine.snapshot_mgr.arq_fotos_particoes.exists():
            gerar_bases_para_data(dt_ref, verbose=False)
            engine.executar(
                dt_referencia=dt_ref.isoformat(),
                filtro_tabelas=args.tabela,
                verbose=False,
                copiar_particoes=True,
            )
        simular_cenarios_reprocessamento_historico(verbose=True)

    engine.executar(
        dt_referencia=dt_ref.isoformat(),
        filtro_tabelas=args.tabela,
        verbose=True,
        copiar_particoes=True,
        aceitar_reprocessamento=args.aceitar_reprocessamento,
        forcar_scan_completo=args.forcar_scan_completo,
        modo_streaming=True if args.streaming else None,
    )


if __name__ == "__main__":
    main()
