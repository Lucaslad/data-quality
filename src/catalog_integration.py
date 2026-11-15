"""
Integração pós-roteamento com AWS Glue Data Catalog e Tabelas Transacionais (Apache Iceberg).

Responsabilidades:
1. Sincronizar partições promovidas para `GOLD`, `SILVER` e `BRONZE` nos respectivos databases do
   AWS Glue Data Catalog (`batch_create_partition` / `batch_delete_partition`), garantindo visibilidade
   imediata no Amazon Athena / Redshift Spectrum / EMR.
2. Suportar geração de metadados transacionais e statements para tabelas Apache Iceberg (tags/branches de qualidade).
3. Persistir manifesto local/S3 auditável (`_metadados_dq/catalogo_particoes_registradas.parquet`).
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd


class GlueCatalogIntegrator:
    """Gerencia o registro de partições roteadas no AWS Glue Data Catalog / Iceberg."""

    def __init__(
        self,
        cfg_catalogo: dict[str, Any] | None,
        metadados_dir: Path,
        glue_client: Any = None,
    ) -> None:
        cfg = cfg_catalogo or {}
        self.ativo = bool(cfg.get("ativo", False))
        self.tipo = str(cfg.get("tipo", "glue")).lower()  # glue | iceberg | local_manifest
        self.databases = {
            "GOLD": str(cfg.get("database_gold", "dq_gold_db")),
            "SILVER": str(cfg.get("database_silver", "dq_silver_db")),
            "BRONZE": str(cfg.get("database_bronze", "dq_bronze_db")),
        }
        self.metadados_dir = Path(metadados_dir)
        self.arq_manifesto_catalogo = (
            self.metadados_dir / "catalogo_particoes_registradas.parquet"
        )
        self._glue_client = glue_client

    @property
    def glue(self) -> Any:
        if self._glue_client is None:
            import boto3  # type: ignore

            self._glue_client = boto3.client("glue")
        return self._glue_client

    def sincronizar_particoes_tabela(
        self,
        run_id: str,
        dt_referencia: str,
        ts_execucao: str,
        tabela: str,
        coluna_particao: str,
        classificacoes_tabela: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        """
        Registra cada partição classificada no catálogo correspondente à sua camada (`GOLD`, `SILVER`, `BRONZE`)
        e remove de camadas anteriores caso tenha sido promovida ou rebaixada.
        """
        if not classificacoes_tabela:
            return []

        registros_catalogo: list[dict[str, Any]] = []

        for item in classificacoes_tabela:
            nome_part = str(item["particao"])
            valor_part = nome_part.split("=", 1)[-1]
            camada_dest = str(item["camada_destino"])
            caminho_dest = str(item["caminho_destino"])
            db_destino = self.databases.get(camada_dest, f"dq_{camada_dest.lower()}_db")

            status_sync = "MANIFESTO_LOCAL"
            ddl_ou_acao = (
                f"ALTER TABLE {db_destino}.{tabela} ADD IF NOT EXISTS "
                f"PARTITION ({coluna_particao}='{valor_part}') LOCATION '{caminho_dest}'"
            )

            if self.ativo and self.tipo == "glue":
                try:
                    # 1. Registra/atualiza na camada de destino
                    self.glue.batch_create_partition(
                        DatabaseName=db_destino,
                        TableName=tabela,
                        PartitionInputList=[
                            {
                                "Values": [valor_part],
                                "StorageDescriptor": {
                                    "Location": caminho_dest,
                                    "InputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
                                    "OutputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
                                    "SerdeInfo": {
                                        "SerializationLibrary": "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe",
                                        "Parameters": {"serialization.format": "1"},
                                    },
                                },
                                "Parameters": {
                                    "dq_run_id": run_id,
                                    "dq_dt_referencia": dt_referencia,
                                    "dq_camada": camada_dest,
                                    "dq_score_pct": str(item.get("score_qualidade_pct", 100.0)),
                                },
                            }
                        ],
                    )
                    # 2. Remove das outras 2 camadas para garantir exclusividade no catálogo
                    for outra_camada, outro_db in self.databases.items():
                        if outra_camada == camada_dest:
                            continue
                        try:
                            self.glue.batch_delete_partition(
                                DatabaseName=outro_db,
                                TableName=tabela,
                                PartitionsToDelete=[{"Values": [valor_part]}],
                            )
                        except Exception:
                            pass
                    status_sync = "SINCRONIZADO_GLUE"
                except Exception as exc:
                    status_sync = f"ERRO_GLUE: {type(exc).__name__}"

            elif self.ativo and self.tipo == "iceberg":
                tag_iceberg = f"dq_{camada_dest.lower()}_{dt_referencia.replace('-', '')}"
                ddl_ou_acao = (
                    f"ALTER TABLE {db_destino}.{tabela} CREATE OR REPLACE TAG `{tag_iceberg}` "
                    f"/* particao={nome_part}, camada={camada_dest} */"
                )
                status_sync = "TAG_ICEBERG_PREPARADA"

            registros_catalogo.append(
                {
                    "run_id": run_id,
                    "dt_referencia": dt_referencia,
                    "ts_execucao": ts_execucao,
                    "tabela": tabela,
                    "particao": nome_part,
                    "coluna_particao": coluna_particao,
                    "valor_particao": valor_part,
                    "camada_catalogo": camada_dest,
                    "database_catalogo": db_destino,
                    "location": caminho_dest,
                    "formato_catalogo": self.tipo.upper(),
                    "status_sincronizacao": status_sync,
                    "operacao_catalogo": ddl_ou_acao,
                }
            )

        self._persistir_manifesto(tabela, dt_referencia, registros_catalogo)
        return registros_catalogo

    def _persistir_manifesto(
        self,
        tabela: str,
        dt_referencia: str,
        novos_registros: list[dict[str, Any]],
    ) -> None:
        if not novos_registros:
            return
        df_novo = pd.DataFrame(novos_registros)
        if self.arq_manifesto_catalogo.exists():
            try:
                df_ant = pd.read_parquet(self.arq_manifesto_catalogo)
                mask = ~(
                    (df_ant["tabela"] == tabela)
                    & (df_ant["dt_referencia"] == dt_referencia)
                )
                df_final = pd.concat([df_ant[mask], df_novo], ignore_index=True)
            except Exception:
                df_final = df_novo
        else:
            df_final = df_novo

        self.metadados_dir.mkdir(parents=True, exist_ok=True)
        df_final.to_parquet(
            self.arq_manifesto_catalogo,
            engine="pyarrow",
            compression="snappy",
            index=False,
        )
