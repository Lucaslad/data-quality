"""Testes unitários para `src/checks_maturacao.py` e filtro estatístico de lags em `src/snapshot_manager.py`."""

from __future__ import annotations

from pathlib import Path
import tempfile
import unittest

import pandas as pd

from src.checks_maturacao import executar_checks_maturacao
from src.snapshot_manager import SnapshotManager


class TestChecksMaturacao(unittest.TestCase):
    """Valida curva de maturação, detecção estatística por lag e proteção contra contaminação BRONZE."""

    def test_filtro_estatistico_preserva_atraso_natural_zero_no_lag0(self) -> None:
        # Item 1.5: Se um evento tem atraso natural de 1 dia (lag=0 sempre tem fator 0.0),
        # o filtro estatístico NÃO deve descartar as datas de lag=0 como contaminadas!
        with tempfile.TemporaryDirectory() as tmp:
            mgr = SnapshotManager(Path(tmp))
            registros = []
            for idx_foto, dt_ref in enumerate(["2026-10-27", "2026-10-28", "2026-10-29"]):
                for lag in range(0, 10):
                    fator = 0.0 if lag == 0 else (0.60 if lag == 1 else 1.0)
                    qtd = int(fator * 100)
                    registros.append(
                        {
                            "run_id": f"r-{idx_foto}",
                            "dt_referencia": dt_ref,
                            "ts_execucao": f"{dt_ref}T10:00:00",
                            "tabela": "tab_atraso_natural",
                            "coluna_data": "dt_evento",
                            "data_evento": f"2026-10-{27 + idx_foto - lag:02d}",
                            "lag_dias": lag,
                            "qtd_registros": qtd,
                            "volume_maduro_referencia": 100.0,
                            "fator_maturacao": fator,
                        }
                    )
            df_hist = pd.DataFrame(registros)
            df_hist.to_parquet(mgr.arq_fotos_eventos, index=False)

            curva = mgr.atualizar_e_obter_curva_aprendida(
                tabela="tab_atraso_natural",
                coluna_data="dt_evento",
                dt_referencia_atual="2026-10-30",
            )
            self.assertEqual(curva["lag_estabilizacao_aprendido"], 2)
            self.assertEqual(
                curva["curva_por_lag"][0]["fator_maturacao_esperado"], 0.0
            )
            self.assertAlmostEqual(
                curva["curva_por_lag"][1]["fator_maturacao_esperado"], 0.60
            )

    def test_exclusao_particoes_bronze_historicas_na_maturacao(self) -> None:
        # Item 1.5: Partição histórica em BRONZE (< ultima_particao) é filtrada antes de gerar foto de eventos
        with tempfile.TemporaryDirectory() as tmp:
            mgr = SnapshotManager(Path(tmp))
            particoes_dfs = {
                # Partição histórica contaminada com data corrompida em outubro
                "anomes=202609": pd.DataFrame(
                    {
                        "id_fatura": [f"B-{i}" for i in range(50)],
                        "dt_pix": ["2026-10-20"] * 50,
                    }
                ),
                # Partição atual saudável
                "anomes=202610": pd.DataFrame(
                    {
                        "id_fatura": [f"G-{i}" for i in range(10)],
                        "dt_pix": ["2026-10-20"] * 10,
                    }
                ),
            }
            res = executar_checks_maturacao(
                run_id="r-mat",
                dt_referencia="2026-10-30",
                ts_execucao="2026-10-30T10:00:00",
                tabela="tab_mat",
                particoes_dfs=particoes_dfs,
                cfg_maturacao={"ativo": True, "colunas": {"dt_pix": True}},
                snapshot_mgr=mgr,
                chaves_primarias=["id_fatura"],
                particoes_contaminadas_estruturais={"anomes=202609"},
            )
            self.assertTrue(len(res) > 0)
            df_ev = mgr.carregar_fotos_eventos()
            row_20 = df_ev[df_ev["data_evento"] == "2026-10-20"].iloc[0]
            # Deve contar apenas os 10 registros da partição saudável 202610, ignorando os 50 da partição BRONZE 202609
            self.assertEqual(int(row_20["qtd_registros"]), 10)


if __name__ == "__main__":
    unittest.main()
