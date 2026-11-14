"""
Script de execução do Motor de Data Quality.

Exemplos de uso:
  # Executa com simulação prévia de fotografias (D-4 a D-1) para treinar o aprendizado de atraso e avalia D (2026-10-30):
  python scripts/executar_dq.py --simular-historico-fotos

  # Execução simples na data de referência padrão do YAML:
  python scripts/executar_dq.py

  # Filtrando apenas uma tabela específica:
  python scripts/executar_dq.py --tabela tabela_particao_mensal
"""

from __future__ import annotations

import argparse
import datetime as dt
from pathlib import Path
import sys

ROOT_DIR = Path(__file__).resolve().parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from scripts.gerar_dados_ficticios import gerar_bases_para_data  # noqa: E402
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
            )

        # Restaura a base de origem para a data de referência final (D)
        gerar_bases_para_data(dt_ref, verbose=False)

    engine.executar(
        dt_referencia=dt_ref.isoformat(),
        filtro_tabelas=args.tabela,
        verbose=True,
    )


if __name__ == "__main__":
    main()
