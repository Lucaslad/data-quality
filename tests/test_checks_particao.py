"""Testes unitários para `src/checks_particao.py`."""

from __future__ import annotations

import unittest

import pandas as pd

from src.checks_particao import executar_checks_particao


class TestChecksParticao(unittest.TestCase):
    """Valida checks de Schema Drift, Freshness/Lacunas, Volumetria Sazonal/Duplo Estágio e Evolução."""

    def test_consistencia_schema_missing_incompatible_and_extra_column(self) -> None:
        # Item 2.1: Coluna ausente -> ANOMALIA; Tipo incompatível -> ANOMALIA; Nova coluna -> WARNING
        schema_contrato = {
            "id_cliente": "numerico_int",
            "valor": "numerico_float",
            "uf": "string_cat",
        }
        cfg_particao = {
            "consistencia_schema": {
                "ativo": True,
                "schema_esperado": schema_contrato,
            },
            "freshness_lacunas": {"ativo": False},
            "volumetria_historica": {"ativo": False},
            "evolucao_fotografias": {"ativo": False},
            "consistencia_data_particao": {"ativo": False},
        }
        particoes_dfs = {
            "anomes=202605": pd.DataFrame(
                {"id_cliente": [1, 2], "valor": [10.5, 20.0], "uf": ["SP", "RJ"]}
            ),
            # Nova coluna extra -> WARNING
            "anomes=202606": pd.DataFrame(
                {
                    "id_cliente": [1, 2],
                    "valor": [10.5, 20.0],
                    "uf": ["SP", "RJ"],
                    "col_nova": ["A", "B"],
                }
            ),
            # Coluna `valor` mudou de float para string -> ANOMALIA
            "anomes=202607": pd.DataFrame(
                {"id_cliente": [1, 2], "valor": ["DEZ", "VINTE"], "uf": ["SP", "RJ"]}
            ),
            # Coluna `uf` desapareceu -> ANOMALIA
            "anomes=202608": pd.DataFrame(
                {"id_cliente": [1, 2], "valor": [10.5, 20.0]}
            ),
        }

        res = executar_checks_particao(
            run_id="r-schema",
            dt_referencia="2026-08-30",
            ts_execucao="2026-08-30T10:00:00",
            tabela="tab_schema",
            tipo_particao="mensal",
            particoes_dfs=particoes_dfs,
            cfg_particao=cfg_particao,
            df_fotos_particoes_hist=pd.DataFrame(),
        )
        por_part = {r.particao: r for r in res if r.tipo_check == "consistencia_schema"}
        self.assertEqual(por_part["anomes=202605"].status, "OK")
        self.assertEqual(por_part["anomes=202606"].status, "WARNING")
        self.assertEqual(por_part["anomes=202607"].status, "ANOMALIA")
        self.assertEqual(por_part["anomes=202608"].status, "ANOMALIA")

    def test_freshness_sla_e_lacunas_sequencia(self) -> None:
        # Item 2.2: Detecta atraso na chegada da partição de dt_referencia e lacuna intermediária
        cfg_particao = {
            "consistencia_schema": {"ativo": False},
            "freshness_lacunas": {
                "ativo": True,
                "sla_atraso_max_particoes": 0,
                "verificar_lacunas": True,
            },
            "volumetria_historica": {"ativo": False},
            "evolucao_fotografias": {"ativo": False},
            "consistencia_data_particao": {"ativo": False},
        }
        # Falta 20261002 (lacuna) e última partição é 20261003, mas dt_referencia é 2026-10-05 (atraso=2)
        particoes_dfs = {
            "anomesdia=20261001": pd.DataFrame({"id": [1]}),
            "anomesdia=20261003": pd.DataFrame({"id": [2]}),
        }
        res = executar_checks_particao(
            run_id="r-fresh",
            dt_referencia="2026-10-05",
            ts_execucao="2026-10-05T10:00:00",
            tabela="tab_fresh",
            tipo_particao="diaria",
            particoes_dfs=particoes_dfs,
            cfg_particao=cfg_particao,
            df_fotos_particoes_hist=pd.DataFrame(),
        )
        chk_fresh = [r for r in res if r.tipo_check == "freshness_particao"][0]
        self.assertEqual(chk_fresh.status, "ANOMALIA")
        self.assertIn("anomesdia=20261002", chk_fresh.detalhes)
        self.assertIn("atraso de 2 dia(s)", chk_fresh.detalhes)

    def test_volumetria_duplo_estagio_warning_vs_anomalia(self) -> None:
        # Item 2.4: Desvio entre warning (16%) e max (20%) gera WARNING; acima de 20% gera ANOMALIA
        cfg_particao = {
            "consistencia_schema": {"ativo": False},
            "freshness_lacunas": {"ativo": False},
            "volumetria_historica": {
                "ativo": True,
                "mediana_gold": 100.0,
                "sigma_gold": 3.0,
                "desvio_relativo_warning": 0.15,
                "desvio_relativo_max": 0.20,
                "z_score_robusto_warning": 2.0,
                "z_score_robusto_max": 3.0,
            },
            "evolucao_fotografias": {"ativo": False},
            "consistencia_data_particao": {"ativo": False},
        }
        particoes_dfs = {
            "anomes=202605": pd.DataFrame({"id": range(100)}),  # 0% -> OK
            "anomes=202606": pd.DataFrame({"id": range(82)}),   # -18% -> WARNING
            "anomes=202607": pd.DataFrame({"id": range(50)}),   # -50% -> ANOMALIA
        }
        res = executar_checks_particao(
            run_id="r-vol",
            dt_referencia="2026-07-30",
            ts_execucao="2026-07-30T10:00:00",
            tabela="tab_vol",
            tipo_particao="mensal",
            particoes_dfs=particoes_dfs,
            cfg_particao=cfg_particao,
            df_fotos_particoes_hist=pd.DataFrame(),
        )
        por_part = {r.particao: r.status for r in res if r.tipo_check == "volumetria_historica"}
        self.assertEqual(por_part["anomes=202605"], "OK")
        self.assertEqual(por_part["anomes=202606"], "WARNING")
        self.assertEqual(por_part["anomes=202607"], "ANOMALIA")


if __name__ == "__main__":
    unittest.main()
