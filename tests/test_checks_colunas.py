"""Testes unitários para `src/checks_colunas.py`."""

from __future__ import annotations

import unittest

import pandas as pd

from src.checks_colunas import (
    cache_gold_suporta_regras_atuais,
    executar_checks_colunas,
)
from src.snapshot_manager import SnapshotManager
from src.stats_utils import gerar_dv_cpf


class TestChecksColunas(unittest.TestCase):
    """Valida PK com nulos, PK composta no cache, Regex vetorizado, PSI Drift, Módulo 11 e Cross-Column."""

    def test_pk_com_nulos_reprova_unicidade_chave_primaria(self) -> None:
        # Item 1.1: Partição com PK nula (mesmo sem duplicatas entre as preenchidas) deve falhar em unicidade_chave_primaria
        df_pk_nula = pd.DataFrame(
            {
                "id_fatura": ["F1", "F2", None, "F4"],
                "valor": [10.0, 20.0, 30.0, 40.0],
            }
        )
        cfg_colunas = {
            "completude": {"ativo": True, "limites_nulos_pct": {"id_fatura": 0.0}},
            "unicidade": {
                "ativo": True,
                "verificar_chave_primaria": True,
                "verificar_linha_completa": True,
            },
        }
        # Teste 1: Em memória
        res_mem = executar_checks_colunas(
            run_id="r-pk-mem",
            dt_referencia="2026-10-30",
            ts_execucao="2026-10-30T10:00:00",
            tabela="tab_pk",
            chaves_primarias=["id_fatura"],
            particoes_dfs={"anomes=202610": df_pk_nula},
            cfg_colunas=cfg_colunas,
        )
        chk_pk_mem = [r for r in res_mem if r.tipo_check == "unicidade_chave_primaria"][0]
        self.assertEqual(chk_pk_mem.status, "ANOMALIA")
        self.assertIn("NULO", chk_pk_mem.detalhes)

        # Teste 2: Via Cache GOLD / Perfil
        perfil = SnapshotManager.construir_perfil_particao(
            "anomes=202610", df_pk_nula, "mensal", chaves_primarias_hint=["id_fatura"]
        )
        res_cache = executar_checks_colunas(
            run_id="r-pk-cache",
            dt_referencia="2026-10-30",
            ts_execucao="2026-10-30T10:00:00",
            tabela="tab_pk",
            chaves_primarias=["id_fatura"],
            particoes_dfs={},
            cfg_colunas=cfg_colunas,
            perfis_cache_gold={"anomes=202610": perfil},
        )
        chk_pk_cache = [r for r in res_cache if r.tipo_check == "unicidade_chave_primaria"][0]
        self.assertEqual(chk_pk_cache.status, "ANOMALIA")

    def test_chave_primaria_composta_no_cache_gold(self) -> None:
        # Item 1.2: Colunas individuais têm repetição, mas a tupla (id_pedido, num_item) é 100% única
        df_comp = pd.DataFrame(
            {
                "id_pedido": ["P1", "P1", "P2", "P2"],
                "num_item": [1, 2, 1, 2],
                "valor": [10.0, 20.0, 30.0, 40.0],
            }
        )
        perfil = SnapshotManager.construir_perfil_particao(
            "anomes=202610",
            df_comp,
            "mensal",
            chaves_primarias_hint=["id_pedido", "num_item"],
        )
        cfg_colunas = {
            "unicidade": {
                "ativo": True,
                "verificar_chave_primaria": True,
                "verificar_linha_completa": False,
            }
        }
        res_cache = executar_checks_colunas(
            run_id="r-comp-ok",
            dt_referencia="2026-10-30",
            ts_execucao="2026-10-30T10:00:00",
            tabela="tab_comp",
            chaves_primarias=["id_pedido", "num_item"],
            particoes_dfs={},
            cfg_colunas=cfg_colunas,
            perfis_cache_gold={"anomes=202610": perfil},
        )
        self.assertEqual(res_cache[0].status, "OK")
        self.assertEqual(res_cache[0].valor_observado, 0)

    def test_regex_vetorizado_e_invalidacao_cache_gold(self) -> None:
        # Item 1.3: Validação de regex vetorizada e detecção de nova regra manual no YAML
        df_reg = pd.DataFrame(
            {
                "id_cliente": [1, 2, 3],
                "cpf": ["12345678901", "00000000000", "ABC"],
            }
        )
        cfg_colunas = {
            "formato_regex": {
                "ativo": True,
                "regras": {
                    "cpf": {
                        "padrao": r"^\d{11}$",
                        "rejeitar_digitos_repetidos": True,
                    }
                },
            }
        }
        res = executar_checks_colunas(
            run_id="r-reg",
            dt_referencia="2026-10-30",
            ts_execucao="2026-10-30T10:00:00",
            tabela="tab_reg",
            chaves_primarias=["id_cliente"],
            particoes_dfs={"anomes=202610": df_reg},
            cfg_colunas=cfg_colunas,
        )
        chk_reg = [r for r in res if r.tipo_check == "formato_regex"][0]
        self.assertEqual(chk_reg.status, "ANOMALIA")
        self.assertEqual(chk_reg.valor_observado, 2)  # "00000000000" e "ABC"

        # Verifica invalidação de CACHE_GOLD quando um padrão customizado arbitrário é inserido no YAML
        df_limpo = pd.DataFrame(
            {"id_cliente": [1, 2], "cpf": ["12345678901", "98765432100"]}
        )
        perfil_limpo = SnapshotManager.construir_perfil_particao(
            "anomes=202605", df_limpo, "mensal"
        )
        cfg_nova_regra_yaml = {
            "formato_regex": {
                "ativo": True,
                "regras": {"cpf": {"padrao": r"^BR-\d{11}$"}},
            }
        }
        self.assertFalse(
            cache_gold_suporta_regras_atuais(
                perfil_limpo, ["id_cliente"], cfg_nova_regra_yaml
            )
        )

    def test_distribution_drift_psi_mod11_e_cross_column(self) -> None:
        # Itens 2.3 e 2.6: Distribution Drift PSI, Dígito Verificador Módulo 11 e Cross-Column
        cpf_ok = "123456789" + gerar_dv_cpf("123456789")
        cpf_erro_dv = "123456789" + ("00" if gerar_dv_cpf("123456789") != "00" else "99")

        # Mix histórico era 90% PIX e 10% BOLETO, mas na partição atual virou 10% PIX e 90% BOLETO
        df_drift = pd.DataFrame(
            {
                "id_tx": [f"T-{i}" for i in range(40)],
                "canal": ["PIX"] * 4 + ["BOLETO"] * 36,
                "cpf": [cpf_ok] * 38 + [cpf_erro_dv] * 2,
                "dt_abertura": ["2026-10-10"] * 39 + ["2026-10-20"],
                "dt_encerramento": ["2026-10-15"] * 39 + ["2026-10-05"],  # 1 violação: encerramento < abertura
            }
        )
        cfg_colunas = {
            "dominio": {
                "ativo": True,
                "regras": {"canal": ["PIX", "BOLETO"]},
                "distribuicao_esperada": {"canal": {"PIX": 0.90, "BOLETO": 0.10}},
                "verificar_drift_mix": True,
                "psi_warning": 0.25,
                "psi_anomalia": 0.50,
            },
            "digito_verificador": {
                "ativo": True,
                "regras": {"cpf": "CPF"},
            },
            "consistencia_entre_colunas": {
                "ativo": True,
                "regras": [
                    {
                        "col_maior": "dt_encerramento",
                        "col_menor": "dt_abertura",
                        "operador": ">=",
                    }
                ],
            },
        }

        res = executar_checks_colunas(
            run_id="r-adv",
            dt_referencia="2026-10-30",
            ts_execucao="2026-10-30T10:00:00",
            tabela="tab_adv",
            chaves_primarias=["id_tx"],
            particoes_dfs={"anomes=202610": df_drift},
            cfg_colunas=cfg_colunas,
        )
        por_tipo = {r.tipo_check: r for r in res}
        self.assertEqual(por_tipo["dominio"].status, "OK")
        self.assertEqual(por_tipo["drift_distribuicao"].status, "ANOMALIA")
        self.assertEqual(por_tipo["digito_verificador"].status, "ANOMALIA")
        self.assertEqual(por_tipo["digito_verificador"].valor_observado, 2)
        self.assertEqual(por_tipo["consistencia_entre_colunas"].status, "ANOMALIA")
        self.assertEqual(por_tipo["consistencia_entre_colunas"].valor_observado, 1)


if __name__ == "__main__":
    unittest.main()
