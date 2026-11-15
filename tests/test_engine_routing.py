"""Testes integrados para `src/engine.py`, `src/storage_backend.py`, `src/catalog_integration.py` e `src/config_loader.py`."""

from __future__ import annotations

import datetime as dt
import io
from pathlib import Path
import tempfile
import unittest

import pandas as pd

from src.config_loader import validar_configuracao
from src.engine import DataQualityEngine
from src.storage_backend import S3StorageBackend


class FakeS3Body:
    def __init__(self, data: bytes) -> None:
        self._data = data

    def read(self) -> bytes:
        return self._data


class FakeS3Client:
    """Mock leve de cliente S3 (boto3) em memória para testar `S3StorageBackend`."""

    def __init__(self) -> None:
        self.objects: dict[tuple[str, str], dict[str, object]] = {}

    def put_object(self, Bucket: str, Key: str, Body: bytes) -> None:
        self.objects[(Bucket, Key)] = {
            "Key": Key,
            "Size": len(Body),
            "ETag": f'"{hash(Body)}"',
            "LastModified": dt.datetime(2026, 10, 30, 12, 0, 0),
            "Body": Body,
        }

    def get_object(self, Bucket: str, Key: str) -> dict[str, object]:
        obj = self.objects[(Bucket, Key)]
        return {"Body": FakeS3Body(obj["Body"])}  # type: ignore[arg-type]

    def list_objects_v2(self, Bucket: str, Prefix: str = "", **kwargs: object) -> dict[str, object]:
        contents = [
            {
                "Key": k,
                "Size": v["Size"],
                "ETag": v["ETag"],
                "LastModified": v["LastModified"],
            }
            for (b, k), v in sorted(self.objects.items())
            if b == Bucket and k.startswith(Prefix)
        ]
        return {"Contents": contents, "IsTruncated": False}

    def copy_object(
        self, Bucket: str, Key: str, CopySource: dict[str, str]
    ) -> None:
        src_obj = self.objects[(CopySource["Bucket"], CopySource["Key"])]
        self.put_object(Bucket, Key, src_obj["Body"])  # type: ignore[arg-type]

    def delete_objects(
        self, Bucket: str, Delete: dict[str, list[dict[str, str]]]
    ) -> None:
        for item in Delete.get("Objects", []):
            self.objects.pop((Bucket, item["Key"]), None)


class TestEngineRoutingAndArchitecture(unittest.TestCase):
    """Valida roteamento Medallion, score ponderado, modo streaming, histórico deduplicado e S3Backend."""

    def test_roteamento_pk_nula_vai_para_bronze_e_modo_streaming(self) -> None:
        # Itens 1.1, 2.4, 3.4, 4.1 e 4.2:
        # - Partição com PK nula deve ir para BRONZE com score penalizado (<= teto_score_bronze)
        # - Modo Streaming libera DataFrames e produz mesma classificação
        # - Histórico de resumos e alertas é preservado entre execuções
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            origem = root / "dados" / "origem" / "tab_clientes"
            for m in ("202605", "202606", "202607"):
                p_dir = origem / f"anomes={m}"
                p_dir.mkdir(parents=True, exist_ok=True)
                df_ok = pd.DataFrame(
                    {
                        "id_cliente": [f"C-{m}-{i}" for i in range(60)],
                        "valor": [100.0 + i for i in range(60)],
                        "uf": ["SP" if i % 2 == 0 else "RJ" for i in range(60)],
                    }
                )
                df_ok.to_parquet(p_dir / "part-0.parquet", index=False)

            # Partição 202608 tem apenas 1 nulo na PK `id_cliente` e nenhum outro erro
            p_anom = origem / "anomes=202608"
            p_anom.mkdir(parents=True, exist_ok=True)
            ids_202608: list[str | None] = [f"C-202608-{i}" for i in range(60)]
            ids_202608[0] = None
            df_pk_nula = pd.DataFrame(
                {
                    "id_cliente": ids_202608,
                    "valor": [100.0 + i for i in range(60)],
                    "uf": ["SP" if i % 2 == 0 else "RJ" for i in range(60)],
                }
            )
            df_pk_nula.to_parquet(p_anom / "part-0.parquet", index=False)

            cfg_yaml = root / "config" / "dq_config.yaml"
            cfg_yaml.parent.mkdir(parents=True, exist_ok=True)
            cfg_yaml.write_text(
                """
configuracoes_gerais:
  diretorio_origem: "dados/origem"
  diretorio_saida: "dados/saida"
  data_referencia_padrao: "2026-08-30"
  leitura_incremental: true
  modo_streaming_memoria: true
  particionar_metadados: true

tabelas:
  tab_clientes:
    ativo: true
    caminho_relativo: "tab_clientes"
    coluna_particao: "anomes"
    tipo_particao: "mensal"
    chaves_primarias: "auto"
    checks:
      particao:
        volumetria_historica:
          ativo: true
      colunas:
        completude:
          ativo: true
        unicidade:
          ativo: true
""",
                encoding="utf-8",
            )

            engine = DataQualityEngine(caminho_config=cfg_yaml, base_project_dir=root)
            art1 = engine.executar(dt_referencia="2026-08-29", verbose=False)
            art2 = engine.executar(dt_referencia="2026-08-30", verbose=False)

            df_class = art2["df_classificacao_particoes"]
            mapa_camada = dict(zip(df_class["particao"], df_class["camada_destino"]))
            self.assertEqual(mapa_camada["anomes=202605"], "GOLD")
            self.assertEqual(mapa_camada["anomes=202606"], "GOLD")
            self.assertEqual(mapa_camada["anomes=202607"], "GOLD")
            # Partição com nulo na PK foi bloqueada em BRONZE (Item 1.1)!
            self.assertEqual(mapa_camada["anomes=202608"], "BRONZE")

            score_bronze = float(
                df_class[df_class["particao"] == "anomes=202608"]["score_qualidade_pct"].iloc[0]
            )
            self.assertLessEqual(score_bronze, 65.0)

            # Verifica que o histórico de resumos e alertas manteve AMBAS as datas (2026-08-29 e 2026-08-30) (Item 3.4)
            df_resumo_hist = pd.read_parquet(
                engine.snapshot_mgr.metadados_dir / "resumo_qualidade_tabelas.parquet"
            )
            self.assertEqual(
                set(df_resumo_hist["dt_referencia"].unique()),
                {"2026-08-29", "2026-08-30"},
            )
            df_alertas_hist = pd.read_parquet(
                engine.snapshot_mgr.metadados_dir / "alertas_anomalias.parquet"
            )
            self.assertEqual(
                set(df_alertas_hist["dt_referencia"].unique()),
                {"2026-08-29", "2026-08-30"},
            )

            # Verifica metadados particionados (Item 4.2) e manifesto de catálogo (Item 4.4)
            self.assertTrue(
                (
                    engine.snapshot_mgr.particionado_dir
                    / "snapshots_particoes"
                    / "tabela=tab_clientes"
                    / "dt_referencia=2026-08-30"
                    / "part-00000.snappy.parquet"
                ).exists()
            )
            self.assertTrue(
                engine.catalog_integrator.arq_manifesto_catalogo.exists()
            )
            self.assertIsNotNone(art1)

    def test_s3_storage_backend_com_mock(self) -> None:
        # Item 4.3: Testa S3StorageBackend (ListObjectsV2 fingerprint, leitura, cópia e remoção)
        fake_s3 = FakeS3Client()
        s3_backend = S3StorageBackend(s3_client=fake_s3)

        df_orig = pd.DataFrame({"id_tx": ["T1", "T2"], "valor": [10.0, 20.0]})
        buf = io.BytesIO()
        df_orig.to_parquet(buf, index=False)
        fake_s3.put_object(
            Bucket="meu-bucket",
            Key="origem/tab/anomes=202610/part-0.parquet",
            Body=buf.getvalue(),
        )

        particoes = s3_backend.listar_particoes("s3://meu-bucket/origem/tab", "anomes=")
        self.assertEqual(particoes, ["anomes=202610"])

        meta = s3_backend.inspecionar_metadados_fisicos(
            "s3://meu-bucket/origem/tab/anomes=202610"
        )
        self.assertEqual(meta["qtd_arquivos"], 1)
        self.assertTrue(len(meta["fingerprint_fisico"]) == 16)

        df_lido = s3_backend.ler_particao_parquet(
            "s3://meu-bucket/origem/tab/anomes=202610"
        )
        self.assertEqual(len(df_lido), 2)

        copiados = s3_backend.copiar_particao(
            "s3://meu-bucket/origem/tab/anomes=202610",
            "s3://meu-bucket/gold/tab/anomes=202610",
        )
        self.assertEqual(copiados, 1)
        self.assertEqual(
            s3_backend.contar_arquivos_parquet(
                "s3://meu-bucket/gold/tab/anomes=202610"
            ),
            1,
        )

        s3_backend.remover_particao("s3://meu-bucket/gold/tab/anomes=202610")
        self.assertEqual(
            s3_backend.contar_arquivos_parquet(
                "s3://meu-bucket/gold/tab/anomes=202610"
            ),
            0,
        )

    def test_validacao_schema_yaml(self) -> None:
        # Item 3.3: Detecta chaves com typo no YAML e levanta ValueError para tipo_particao inválido
        avisos = validar_configuracao(
            {
                "configuracoes_gerais": {"chave_inexistente_typo": 123},
                "tabelas": {
                    "minha_tab": {
                        "tipo_particao": "mensal",
                        "chave_errada": True,
                    }
                },
            },
            estrito=False,
        )
        self.assertEqual(len(avisos), 2)

        with self.assertRaises(ValueError):
            validar_configuracao(
                {
                    "tabelas": {
                        "minha_tab": {"tipo_particao": "semanal_invalido"}
                    }
                }
            )


if __name__ == "__main__":
    unittest.main()
