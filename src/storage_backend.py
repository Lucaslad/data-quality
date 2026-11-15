"""
Camada de Abstração de Storage (`StorageBackend`) para execução local e no AWS Glue / Amazon S3.

Implementações:
1. `LocalStorageBackend`: opera sobre disco local usando `pathlib.Path`, `os.stat`, `shutil.copy2` e `shutil.rmtree`.
2. `S3StorageBackend`: opera sobre URIs `s3://bucket/prefix` usando `boto3` / `pyarrow`:
   - `inspecionar_metadados_fisicos`: utiliza `ListObjectsV2` (`ETag` + `ContentLength` + `LastModified`)
     sem baixar arquivos Parquet para calcular o `fingerprint_fisico`.
   - `copiar_particao` / `remover_particao`: utiliza `s3:CopyObject` e `s3:DeleteObjects` nativos na nuvem.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
import hashlib
import io
from pathlib import Path
import shutil
from typing import Any

import pandas as pd


def eh_uri_s3(caminho: str | Path) -> bool:
    """Retorna True se o caminho for uma URI S3 (`s3://` ou `s3a://`)."""
    s = str(caminho).strip()
    return s.startswith(("s3://", "s3a://"))


def parse_s3_uri(uri: str | Path) -> tuple[str, str]:
    """Separa `s3://bucket/prefixo/caminho` em `(bucket, key_sem_barra_inicial)`."""
    s = str(uri).strip()
    for scheme in ("s3://", "s3a://"):
        if s.startswith(scheme):
            s = s[len(scheme) :]
            break
    partes = s.split("/", 1)
    bucket = partes[0]
    key = partes[1] if len(partes) > 1 else ""
    return bucket, key.strip("/")


class StorageBackend(ABC):
    """Interface abstrata de armazenamento para leitura, inspeção de metadados e roteamento Medallion."""

    @abstractmethod
    def existe(self, caminho: str | Path) -> bool:
        """Verifica se o arquivo ou diretório existe."""

    @abstractmethod
    def listar_particoes(
        self, caminho_tabela: str | Path, prefixo_particao: str
    ) -> list[str]:
        """Lista os nomes das subpastas de partição (`<coluna_particao>=<valor>`) que contêm arquivos Parquet."""

    @abstractmethod
    def inspecionar_metadados_fisicos(
        self, caminho_particao: str | Path
    ) -> dict[str, Any]:
        """
        Inspeciona os metadados físicos dos arquivos Parquet da partição sem ler o conteúdo lógico.
        Retorna `qtd_arquivos`, `tamanho_bytes`, `max_mtime_ns`, `arquivos_nomes` e `fingerprint_fisico`.
        """

    @abstractmethod
    def ler_particao_parquet(self, caminho_particao: str | Path) -> pd.DataFrame:
        """Lê e concatena todos os arquivos `.parquet` de uma partição em um único DataFrame."""

    @abstractmethod
    def contar_arquivos_parquet(self, caminho_particao: str | Path) -> int:
        """Retorna a quantidade de arquivos `.parquet` existentes na partição."""

    @abstractmethod
    def copiar_particao(
        self, origem_particao: str | Path, destino_particao: str | Path
    ) -> int:
        """Copia de forma limpa os arquivos Parquet de `origem_particao` para `destino_particao`."""

    @abstractmethod
    def remover_particao(self, caminho_particao: str | Path) -> None:
        """Remove todos os arquivos de uma partição."""

    @abstractmethod
    def ler_parquet(self, caminho_arquivo: str | Path) -> pd.DataFrame:
        """Lê um arquivo Parquet individual."""

    @abstractmethod
    def escrever_parquet(
        self, df: pd.DataFrame, caminho_arquivo: str | Path
    ) -> None:
        """Escreve um DataFrame em um arquivo Parquet com compressão Snappy."""


class LocalStorageBackend(StorageBackend):
    """Implementação em sistema de arquivos local (`pathlib` + `shutil`)."""

    def existe(self, caminho: str | Path) -> bool:
        return Path(caminho).exists()

    def listar_particoes(
        self, caminho_tabela: str | Path, prefixo_particao: str
    ) -> list[str]:
        pasta_tab = Path(caminho_tabela)
        if not pasta_tab.exists():
            return []
        nomes: list[str] = []
        for sub in sorted(pasta_tab.iterdir()):
            if not sub.is_dir() or not sub.name.startswith(prefixo_particao):
                continue
            if any(sub.glob("*.parquet")):
                nomes.append(sub.name)
        return nomes

    def inspecionar_metadados_fisicos(
        self, caminho_particao: str | Path
    ) -> dict[str, Any]:
        pasta = Path(caminho_particao)
        if not pasta.exists():
            return {
                "qtd_arquivos": 0,
                "tamanho_bytes": 0,
                "max_mtime_ns": 0,
                "arquivos_nomes": "",
                "fingerprint_fisico": "",
            }
        arquivos = sorted(pasta.glob("*.parquet"))
        if not arquivos:
            return {
                "qtd_arquivos": 0,
                "tamanho_bytes": 0,
                "max_mtime_ns": 0,
                "arquivos_nomes": "",
                "fingerprint_fisico": "",
            }

        pecas_hash: list[str] = []
        nomes: list[str] = []
        tamanho_total = 0
        max_mtime = 0
        for arq in arquivos:
            st = arq.stat()
            tamanho_total += int(st.st_size)
            mtime_ns = int(st.st_mtime_ns)
            if mtime_ns > max_mtime:
                max_mtime = mtime_ns
            nomes.append(arq.name)
            pecas_hash.append(f"{arq.name}:{st.st_size}:{mtime_ns}")

        assinatura = "|".join(pecas_hash)
        fp_fisico = hashlib.sha256(assinatura.encode("utf-8")).hexdigest()[:16]
        return {
            "qtd_arquivos": len(arquivos),
            "tamanho_bytes": tamanho_total,
            "max_mtime_ns": max_mtime,
            "arquivos_nomes": ",".join(nomes),
            "fingerprint_fisico": fp_fisico,
        }

    def ler_particao_parquet(self, caminho_particao: str | Path) -> pd.DataFrame:
        pasta = Path(caminho_particao)
        arquivos = sorted(pasta.glob("*.parquet"))
        if not arquivos:
            return pd.DataFrame()
        dfs = [pd.read_parquet(arq) for arq in arquivos]
        return pd.concat(dfs, ignore_index=True)

    def contar_arquivos_parquet(self, caminho_particao: str | Path) -> int:
        pasta = Path(caminho_particao)
        if not pasta.exists():
            return 0
        return len(list(pasta.glob("*.parquet")))

    def copiar_particao(
        self, origem_particao: str | Path, destino_particao: str | Path
    ) -> int:
        pasta_orig = Path(origem_particao)
        pasta_dest = Path(destino_particao)
        if not pasta_orig.exists():
            return 0
        if pasta_dest.exists():
            shutil.rmtree(pasta_dest)
        pasta_dest.mkdir(parents=True, exist_ok=True)
        copiados = 0
        for arq_pq in sorted(pasta_orig.glob("*.parquet")):
            shutil.copy2(arq_pq, pasta_dest / arq_pq.name)
            copiados += 1
        return copiados

    def remover_particao(self, caminho_particao: str | Path) -> None:
        pasta = Path(caminho_particao)
        if pasta.exists():
            shutil.rmtree(pasta)
            pasta_pai = pasta.parent
            if pasta_pai.exists() and not any(pasta_pai.iterdir()):
                try:
                    pasta_pai.rmdir()
                except OSError:
                    pass

    def ler_parquet(self, caminho_arquivo: str | Path) -> pd.DataFrame:
        return pd.read_parquet(Path(caminho_arquivo))

    def escrever_parquet(
        self, df: pd.DataFrame, caminho_arquivo: str | Path
    ) -> None:
        arq = Path(caminho_arquivo)
        arq.parent.mkdir(parents=True, exist_ok=True)
        df.to_parquet(arq, engine="pyarrow", compression="snappy", index=False)


class S3StorageBackend(StorageBackend):
    """
    Implementação para Amazon S3 (`s3://bucket/prefix`), compatível com AWS Glue Jobs.
    Usa `ETag` + `ContentLength` via `ListObjectsV2` para fingerprint físico e `CopyObject`
    para promoção entre camadas Bronze, Silver e Gold sem tráfego de rede local.
    """

    def __init__(self, s3_client: Any = None, region_name: str | None = None) -> None:
        self._s3_client = s3_client
        self._region_name = region_name

    @property
    def s3(self) -> Any:
        if self._s3_client is None:
            import boto3  # type: ignore

            self._s3_client = boto3.client("s3", region_name=self._region_name)
        return self._s3_client

    def _listar_objetos_parquet(self, uri: str | Path) -> list[dict[str, Any]]:
        bucket, prefix = parse_s3_uri(uri)
        prefix_busca = f"{prefix}/" if prefix else ""
        objetos: list[dict[str, Any]] = []
        continuation_token: str | None = None

        while True:
            kwargs: dict[str, Any] = {"Bucket": bucket, "Prefix": prefix_busca}
            if continuation_token:
                kwargs["ContinuationToken"] = continuation_token
            resp = self.s3.list_objects_v2(**kwargs)
            for obj in resp.get("Contents", []) or []:
                key = str(obj.get("Key", ""))
                if key.endswith(".parquet"):
                    objetos.append(obj)
            if resp.get("IsTruncated"):
                continuation_token = resp.get("NextContinuationToken")
            else:
                break

        return sorted(objetos, key=lambda o: str(o.get("Key", "")))

    def existe(self, caminho: str | Path) -> bool:
        bucket, key = parse_s3_uri(caminho)
        if key.endswith(".parquet"):
            resp = self.s3.list_objects_v2(Bucket=bucket, Prefix=key, MaxKeys=1)
            return any(
                str(o.get("Key", "")) == key for o in (resp.get("Contents", []) or [])
            )
        return len(self._listar_objetos_parquet(caminho)) > 0

    def listar_particoes(
        self, caminho_tabela: str | Path, prefixo_particao: str
    ) -> list[str]:
        bucket, prefix = parse_s3_uri(caminho_tabela)
        prefix_base = f"{prefix}/" if prefix else ""
        objetos = self._listar_objetos_parquet(caminho_tabela)
        particoes: set[str] = set()
        for obj in objetos:
            key = str(obj.get("Key", ""))
            rel = key[len(prefix_base) :] if key.startswith(prefix_base) else key
            partes = rel.split("/")
            if len(partes) >= 2 and partes[0].startswith(prefixo_particao):
                particoes.add(partes[0])
        return sorted(particoes)

    def inspecionar_metadados_fisicos(
        self, caminho_particao: str | Path
    ) -> dict[str, Any]:
        objetos = self._listar_objetos_parquet(caminho_particao)
        if not objetos:
            return {
                "qtd_arquivos": 0,
                "tamanho_bytes": 0,
                "max_mtime_ns": 0,
                "arquivos_nomes": "",
                "fingerprint_fisico": "",
            }

        pecas_hash: list[str] = []
        nomes: list[str] = []
        tamanho_total = 0
        max_mtime_ns = 0

        for obj in objetos:
            key = str(obj.get("Key", ""))
            nome_arq = key.rsplit("/", 1)[-1]
            size = int(obj.get("Size", 0))
            etag = str(obj.get("ETag", "")).strip('"')
            last_mod = obj.get("LastModified")
            mtime_ns = (
                int(last_mod.timestamp() * 1_000_000_000)
                if hasattr(last_mod, "timestamp")
                else 0
            )
            tamanho_total += size
            if mtime_ns > max_mtime_ns:
                max_mtime_ns = mtime_ns
            nomes.append(nome_arq)
            # No S3, ETag + ContentLength identificam o objeto fisicamente de forma determinística
            pecas_hash.append(f"{nome_arq}:{size}:{etag or mtime_ns}")

        assinatura = "|".join(pecas_hash)
        fp_fisico = hashlib.sha256(assinatura.encode("utf-8")).hexdigest()[:16]
        return {
            "qtd_arquivos": len(objetos),
            "tamanho_bytes": tamanho_total,
            "max_mtime_ns": max_mtime_ns,
            "arquivos_nomes": ",".join(nomes),
            "fingerprint_fisico": fp_fisico,
        }

    def ler_particao_parquet(self, caminho_particao: str | Path) -> pd.DataFrame:
        bucket, _ = parse_s3_uri(caminho_particao)
        objetos = self._listar_objetos_parquet(caminho_particao)
        if not objetos:
            return pd.DataFrame()
        dfs: list[pd.DataFrame] = []
        for obj in objetos:
            key = str(obj["Key"])
            resp = self.s3.get_object(Bucket=bucket, Key=key)
            raw_bytes = resp["Body"].read()
            dfs.append(pd.read_parquet(io.BytesIO(raw_bytes)))
        return pd.concat(dfs, ignore_index=True)

    def contar_arquivos_parquet(self, caminho_particao: str | Path) -> int:
        return len(self._listar_objetos_parquet(caminho_particao))

    def copiar_particao(
        self, origem_particao: str | Path, destino_particao: str | Path
    ) -> int:
        bucket_orig, _ = parse_s3_uri(origem_particao)
        bucket_dest, prefix_dest = parse_s3_uri(destino_particao)
        objetos_orig = self._listar_objetos_parquet(origem_particao)
        if not objetos_orig:
            return 0

        # Remove objetos antigos no destino antes de copiar
        self.remover_particao(destino_particao)

        copiados = 0
        for obj in objetos_orig:
            key_orig = str(obj["Key"])
            nome_arq = key_orig.rsplit("/", 1)[-1]
            key_dest = f"{prefix_dest}/{nome_arq}" if prefix_dest else nome_arq
            self.s3.copy_object(
                Bucket=bucket_dest,
                Key=key_dest,
                CopySource={"Bucket": bucket_orig, "Key": key_orig},
            )
            copiados += 1
        return copiados

    def remover_particao(self, caminho_particao: str | Path) -> None:
        bucket, _ = parse_s3_uri(caminho_particao)
        objetos = self._listar_objetos_parquet(caminho_particao)
        if not objetos:
            return
        delete_keys = [{"Key": str(o["Key"])} for o in objetos]
        self.s3.delete_objects(Bucket=bucket, Delete={"Objects": delete_keys})

    def ler_parquet(self, caminho_arquivo: str | Path) -> pd.DataFrame:
        bucket, key = parse_s3_uri(caminho_arquivo)
        resp = self.s3.get_object(Bucket=bucket, Key=key)
        return pd.read_parquet(io.BytesIO(resp["Body"].read()))

    def escrever_parquet(
        self, df: pd.DataFrame, caminho_arquivo: str | Path
    ) -> None:
        bucket, key = parse_s3_uri(caminho_arquivo)
        buf = io.BytesIO()
        df.to_parquet(buf, engine="pyarrow", compression="snappy", index=False)
        self.s3.put_object(Bucket=bucket, Key=key, Body=buf.getvalue())


def obter_storage_backend(
    caminho_referencia: str | Path = "",
    s3_client: Any = None,
    region_name: str | None = None,
) -> StorageBackend:
    """Factory que retorna `S3StorageBackend` para URIs `s3://` ou `LocalStorageBackend` para caminhos locais."""
    if eh_uri_s3(caminho_referencia):
        return S3StorageBackend(s3_client=s3_client, region_name=region_name)
    return LocalStorageBackend()
