"""Testes unitários para `src/auto_learner.py` e `src/stats_utils.py`."""

from __future__ import annotations

import unittest

import pandas as pd

from src.auto_learner import aprender_regras_tabela
from src.stats_utils import (
    calcular_divergencia_js,
    calcular_passo_arredondamento_dinamico,
    calcular_psi,
    gerar_dv_cnpj,
    gerar_dv_cpf,
    mediana_e_sigma_mad,
    validar_cnpj_serie,
    validar_cpf_serie,
)


class TestAutoLearnerAndStats(unittest.TestCase):
    """Valida auto-descoberta de regras, escala dinâmica, sazonalidade e prevenção de data leakage."""

    def test_mediana_e_sigma_mad_com_mad_zero(self) -> None:
        med, sig = mediana_e_sigma_mad([100.0, 100.0, 100.0], sigma_min_rel=0.03, sigma_min_abs=1.0)
        self.assertEqual(med, 100.0)
        self.assertEqual(sig, 3.0)

    def test_escala_dinamica_limites_decimais_baixa_magnitude(self) -> None:
        # Item 1.6: Coluna de alíquota/probabilidade entre 0.01 e 0.18 não deve arredondar para 1000.0
        passo_peq, casas_peq = calcular_passo_arredondamento_dinamico(0.18)
        self.assertAlmostEqual(passo_peq, 0.01)
        self.assertGreaterEqual(casas_peq, 2)

        passo_grande, _ = calcular_passo_arredondamento_dinamico(15000.0)
        self.assertEqual(passo_grande, 1000.0)

        particoes = {}
        for idx, m in enumerate(["202605", "202606", "202607"]):
            particoes[f"anomes={m}"] = pd.DataFrame(
                {
                    "id_contrato": [f"C-{m}-{i}" for i in range(50)],
                    "taxa_juros": [round(0.02 + (i % 10) * 0.015, 4) for i in range(50)],
                    "prob_default": [round(0.05 + (i % 10) * 0.08, 4) for i in range(50)],
                }
            )

        regras = aprender_regras_tabela(
            run_id="run-1",
            dt_referencia="2026-07-30",
            ts_execucao="2026-07-30T10:00:00",
            tabela="tab_taxas",
            tipo_particao="mensal",
            chaves_primarias_cfg="auto",
            particoes_dfs=particoes,
            df_fotos_particoes_hist=pd.DataFrame(),
            checks_cfg={},
        )
        lim_taxa = regras["colunas"]["intervalo_numerico"]["regras"]["taxa_juros"]
        self.assertLessEqual(lim_taxa["max"], 1.0)
        self.assertGreaterEqual(lim_taxa["min"], 0.0)
        self.assertLessEqual(lim_taxa["min"], 0.02)

        lim_prob = regras["colunas"]["intervalo_numerico"]["regras"]["prob_default"]
        self.assertLessEqual(lim_prob["max"], 1.0)

    def test_auto_descoberta_pk_prefixos_expandidos_e_chave_composta(self) -> None:
        # Item 1.2: Auto-descoberta de chave simples `cod_*` e de chave composta `(cod_agencia, num_conta)`
        particoes_cod = {
            f"anomes=20260{m}": pd.DataFrame(
                {
                    "cod_operacao": [f"OP-{m}-{i}" for i in range(30)],
                    "valor": [100.0 + i for i in range(30)],
                }
            )
            for m in (5, 6, 7)
        }
        regras_cod = aprender_regras_tabela(
            run_id="r1",
            dt_referencia="2026-07-30",
            ts_execucao="2026-07-30T10:00:00",
            tabela="tab_cod",
            tipo_particao="mensal",
            chaves_primarias_cfg="auto",
            particoes_dfs=particoes_cod,
            df_fotos_particoes_hist=pd.DataFrame(),
            checks_cfg={},
        )
        self.assertEqual(regras_cod["chaves_primarias"], ["cod_operacao"])

        # Chave composta onde `cod_agencia` e `num_conta` se repetem isoladamente, mas o par é único
        particoes_comp = {}
        for m in (5, 6, 7):
            particoes_comp[f"anomes=20260{m}"] = pd.DataFrame(
                {
                    "cod_agencia": ["0001", "0001", "0002", "0002"],
                    "num_conta": ["10", "20", "10", "20"],
                    "saldo": [100.0, 200.0, 300.0, 400.0],
                }
            )
        regras_comp = aprender_regras_tabela(
            run_id="r2",
            dt_referencia="2026-07-30",
            ts_execucao="2026-07-30T10:00:00",
            tabela="tab_composta",
            tipo_particao="mensal",
            chaves_primarias_cfg="auto",
            particoes_dfs=particoes_comp,
            df_fotos_particoes_hist=pd.DataFrame(),
            checks_cfg={},
        )
        self.assertEqual(regras_comp["chaves_primarias"], ["cod_agencia", "num_conta"])

    def test_prevencao_data_leakage_e_cold_start(self) -> None:
        # Item 1.4: Partição recém-chegada anômala não deve deslocar mediana histórica
        particoes = {
            "anomes=202605": pd.DataFrame({"id_tx": [f"1-{i}" for i in range(100)], "v": [10.0] * 100}),
            "anomes=202606": pd.DataFrame({"id_tx": [f"2-{i}" for i in range(102)], "v": [10.0] * 102}),
            "anomes=202607": pd.DataFrame({"id_tx": [f"3-{i}" for i in range(98)], "v": [10.0] * 98}),
            # Última partição chega com queda severa (15 linhas)
            "anomes=202608": pd.DataFrame({"id_tx": [f"4-{i}" for i in range(15)], "v": [10.0] * 15}),
        }
        regras = aprender_regras_tabela(
            run_id="r3",
            dt_referencia="2026-08-30",
            ts_execucao="2026-08-30T10:00:00",
            tabela="tab_leak",
            tipo_particao="mensal",
            chaves_primarias_cfg="auto",
            particoes_dfs=particoes,
            df_fotos_particoes_hist=pd.DataFrame(),
            checks_cfg={},
        )
        self.assertNotIn("anomes=202608", regras["particoes_gold_treino"])
        self.assertAlmostEqual(regras["particao"]["volumetria_historica"]["mediana_gold"], 100.0, delta=2.0)

    def test_aprendizado_sazonalidade_cross_column_e_mod11(self) -> None:
        # Item 2.5 e 2.6: Sazonalidade semanal + Cross-Column + Módulo 11 CPF
        cpfs_validos = [f"1234567{i:02d}" + gerar_dv_cpf(f"1234567{i:02d}") for i in range(10, 40)]
        self.assertTrue(validar_cpf_serie(pd.Series(cpfs_validos)).all())

        cnpjs_validos = [f"12345678000{i:01d}" + gerar_dv_cnpj(f"12345678000{i:01d}") for i in range(1, 5)]
        self.assertTrue(validar_cnpj_serie(pd.Series(cnpjs_validos)).all())

        particoes_diarias = {}
        # Dias 2026-10-05 (Seg) a 2026-10-11 (Dom)
        for dia in range(5, 12):
            dt_s = f"202610{dia:02d}"
            dt_iso = f"2026-10-{dia:02d}"
            eh_fds = dia in (10, 11)
            n_rows = 80 if eh_fds else 200
            particoes_diarias[f"anomesdia={dt_s}"] = pd.DataFrame(
                {
                    "id_evento": [f"E-{dia}-{i}" for i in range(n_rows)],
                    "cpf_cliente": [cpfs_validos[i % len(cpfs_validos)] for i in range(n_rows)],
                    "dt_inicio": [dt_iso] * n_rows,
                    "dt_liquidacao": [f"2026-10-{dia + 1:02d}"] * n_rows,
                }
            )

        regras = aprender_regras_tabela(
            run_id="r4",
            dt_referencia="2026-10-11",
            ts_execucao="2026-10-11T10:00:00",
            tabela="tab_sazonal",
            tipo_particao="diaria",
            chaves_primarias_cfg="auto",
            particoes_dfs=particoes_diarias,
            df_fotos_particoes_hist=pd.DataFrame(),
            checks_cfg={},
        )
        vol_cfg = regras["particao"]["volumetria_historica"]
        self.assertTrue(vol_cfg["sazonalidade_ativa"])
        self.assertEqual(vol_cfg["baselines_sazonais"]["UTIL"]["mediana"], 200.0)
        self.assertEqual(vol_cfg["baselines_sazonais"]["FDS"]["mediana"], 80.0)

        self.assertEqual(regras["colunas"]["digito_verificador"]["regras"].get("cpf_cliente"), "CPF")
        regras_cross = regras["colunas"]["consistencia_entre_colunas"]["regras"]
        self.assertTrue(
            any(
                r["col_maior"] == "dt_liquidacao" and r["col_menor"] == "dt_inicio"
                for r in regras_cross
            )
        )

    def test_psi_e_divergencia_js(self) -> None:
        dist_base = {"PIX": 0.50, "TED": 0.30, "BOLETO": 0.20}
        dist_igual = {"PIX": 500, "TED": 300, "BOLETO": 200}
        dist_mudanca = {"PIX": 50, "TED": 150, "BOLETO": 800}

        self.assertAlmostEqual(calcular_psi(dist_base, dist_igual), 0.0, places=4)
        self.assertAlmostEqual(calcular_divergencia_js(dist_base, dist_igual), 0.0, places=4)
        self.assertGreater(calcular_psi(dist_base, dist_mudanca), 0.50)


if __name__ == "__main__":
    unittest.main()
