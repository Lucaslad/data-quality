"""
Gerenciador de fotografias (Snapshots) de execução, Fingerprints Físicos/Lógicos e Aprendizado Blindado (GOLD).

Responsabilidades:
1. Diretório de Metadados do Motor (`dados/saida/_metadados_dq/`):
   - Inspecionar metadados físicos dos arquivos Parquet (`mtime`/`ETag`, `size`, `qtd_arquivos`, `fingerprint_fisico`)
     via `StorageBackend` sem precisar ler os dados, viabilizando leitura incremental Local e no S3.
   - Calcular o Fingerprint Lógico e Perfil Estatístico completo de cada partição (`hash_conteudo`,
     `hashes_colunas_json`, `somas_numericas_json`, `dominios_json`, `mins/maxs`, `perfil_dq_json`),
     incluindo unicidade de chaves compostas, validação Módulo 11 (CPF/CNPJ), relações entre colunas
     e amostras para permitir processamento em *Streaming* (sem manter todos os DataFrames na RAM).
   - Salvar os metadados tanto de forma particionada por `tabela=<nome>/dt_referencia=<YYYY-MM-DD>/`
     (evitando *race conditions* em jobs concorrentes no AWS Glue) quanto consolidada.
2. Diretórios de Dados Classificados (`dados/saida/bronze/`, `dados/saida/silver/`, `dados/saida/gold/`):
   - Mantidos exclusivamente para receber as cópias das tabelas e suas respectivas partições
     de acordo com a classificação de qualidade de cada partição.
"""

from __future__ import annotations

import datetime as dt
import hashlib
from itertools import combinations
import json
from pathlib import Path
import re
from typing import Any

import numpy as np
import pandas as pd

from src.stats_utils import (
    detectar_caracteres_repetidos_serie,
    mediana_e_sigma_mad,
    validar_cnpj_serie,
    validar_cpf_serie,
)
from src.storage_backend import StorageBackend, obter_storage_backend

REGEX_DATA_ISO = re.compile(r"^\d{4}-\d{2}-\d{2}")

PREFIXOS_ID_CANDIDATOS = (
    "id_",
    "cod_",
    "cd_",
    "num_",
    "nr_",
    "pk_",
    "chave_",
    "seq_",
)
SUFIXOS_ID_CANDIDATOS = (
    "_id",
    "_cod",
    "_cd",
    "_pk",
    "_key",
    "_num",
    "_nr",
)
NOMES_ID_EXATOS = {"id", "codigo", "pk", "chave"}


def eh_coluna_id_primaria_estrita(nome_col: str) -> bool:
    """Identifica colunas com nomenclatura clássica de ID (`id_*` ou `*_id`)."""
    col_low = nome_col.lower().strip()
    return col_low.startswith("id_") or col_low.endswith("_id") or col_low == "id"


def eh_coluna_id_candidata(nome_col: str) -> bool:
    """
    Identifica colunas candidatas a chave primária (simples ou composta), abrangendo
    prefixos/sufixos `id_`, `_id`, `cod_`, `num_`, `nr_`, `pk_`, `chave_`, etc.
    """
    col_low = nome_col.lower().strip()
    if col_low in NOMES_ID_EXATOS:
        return True
    return col_low.startswith(PREFIXOS_ID_CANDIDATOS) or col_low.endswith(
        SUFIXOS_ID_CANDIDATOS
    )


def eh_coluna_data_ou_ts(nome_col: str, serie: pd.Series) -> bool:
    """Identifica se uma coluna representa data ou timestamp pelo nome, tipo ou conteúdo."""
    nome_low = nome_col.lower()
    if nome_low.startswith(("dt_", "data_", "ts_", "timestamp_")):
        return True
    if pd.api.types.is_datetime64_any_dtype(serie):
        return True
    amostra = serie.dropna().astype(str).head(50)
    if len(amostra) == 0:
        return False
    taxa_iso = float(amostra.str.match(REGEX_DATA_ISO, na=False).mean())
    return taxa_iso >= 0.80


class SnapshotManager:
    """Gerencia a persistência de fotografias, fingerprints de reprocessamento e pastas Medallion."""

    def __init__(
        self,
        diretorio_saida: str | Path,
        storage_backend: StorageBackend | None = None,
        particionar_metadados: bool = True,
    ) -> None:
        self.diretorio_saida = Path(diretorio_saida)
        self.storage = storage_backend or obter_storage_backend(diretorio_saida)
        self.particionar_metadados = particionar_metadados

        self.bronze_dir = self.diretorio_saida / "bronze"
        self.silver_dir = self.diretorio_saida / "silver"
        self.gold_dir = self.diretorio_saida / "gold"
        self.metadados_dir = self.diretorio_saida / "_metadados_dq"
        self.particionado_dir = self.metadados_dir / "particionado"

        for d in (
            self.bronze_dir,
            self.silver_dir,
            self.gold_dir,
            self.metadados_dir,
            self.particionado_dir,
        ):
            d.mkdir(parents=True, exist_ok=True)

        self.arq_fotos_particoes = self.metadados_dir / "snapshots_particoes.parquet"
        self.arq_fotos_eventos = self.metadados_dir / "snapshots_eventos_data.parquet"
        self.arq_curva_aprendida = (
            self.metadados_dir / "curva_maturacao_aprendida.parquet"
        )
        self.arq_regras_aprendidas = self.metadados_dir / "regras_aprendidas.parquet"

    def _salvar_metadado_particionado(
        self,
        nome_artefato: str,
        tabela: str,
        dt_referencia: str,
        df_lote: pd.DataFrame,
    ) -> Path | None:
        """
        Persiste o lote de metadados isolado em:
        `_metadados_dq/particionado/<nome_artefato>/tabela=<tabela>/dt_referencia=<dt_referencia>/part-00000.snappy.parquet`
        Evita condições de corrida quando múltiplos jobs do AWS Glue processam tabelas em paralelo no S3.
        """
        if not self.particionar_metadados or df_lote.empty:
            return None
        pasta_part = (
            self.particionado_dir
            / nome_artefato
            / f"tabela={tabela}"
            / f"dt_referencia={dt_referencia}"
        )
        pasta_part.mkdir(parents=True, exist_ok=True)
        arq_part = pasta_part / "part-00000.snappy.parquet"
        df_lote.to_parquet(
            arq_part, engine="pyarrow", compression="snappy", index=False
        )
        return arq_part

    @staticmethod
    def inspecionar_metadados_fisicos_particao(
        pasta_particao: Path,
        storage_backend: StorageBackend | None = None,
    ) -> dict[str, Any]:
        """
        Inspeciona APENAS os metadados físicos dos arquivos Parquet da partição
        (equivalente a `s3:ListObjectsV2` / `os.stat`), sem ler o conteúdo dos arquivos.
        """
        backend = storage_backend or obter_storage_backend(pasta_particao)
        return backend.inspecionar_metadados_fisicos(pasta_particao)

    @staticmethod
    def construir_perfil_particao(
        nome_particao: str,
        df_part: pd.DataFrame,
        tipo_particao: str,
        meta_fisica: dict[str, Any] | None = None,
        chaves_primarias_hint: list[str] | None = None,
        regras_regex_custom: dict[str, str] | None = None,
    ) -> dict[str, Any]:
        """
        Calcula o Fingerprint Lógico e Perfil Estatístico completo de uma partição lida em memória:
        - `hash_conteudo` e `hashes_colunas_json` (ordenados de forma determinística pela PK/ID)
        - `somas_numericas_json`, `mins_numericos_json`, `maxs_numericos_json`
        - `dominios_json` (contagem por categoria)
        - `preenchidos_json` e `nulos_json`
        - `perfil_dq_json`: métricas completas pré-agregadas (unicidade individual e composta,
          aderência de data, regex vetorizado, validação Módulo 11 CPF/CNPJ, relações entre colunas
          e amostras de valores extremos) permitindo avaliação 100% em *Streaming* ou via `CACHE_GOLD`.
        """
        mf = meta_fisica or {
            "qtd_arquivos": 1,
            "tamanho_bytes": 0,
            "max_mtime_ns": 0,
            "arquivos_nomes": "",
            "fingerprint_fisico": "",
        }
        qtd_linhas = int(len(df_part))
        colunas = list(df_part.columns)

        # Identifica colunas candidatas a chave para ordenação determinística e unicidade composta
        cols_id_estritas = [c for c in colunas if eh_coluna_id_primaria_estrita(c)]
        cols_id_candidatas = [c for c in colunas if eh_coluna_id_candidata(c)]
        cols_ord = (
            [c for c in (chaves_primarias_hint or []) if c in colunas]
            or cols_id_estritas
            or cols_id_candidatas
        )

        if qtd_linhas > 1 and cols_ord:
            try:
                df_ord = df_part.sort_values(
                    by=cols_ord, kind="mergesort", na_position="first"
                ).reset_index(drop=True)
            except Exception:
                df_ord = df_part.reset_index(drop=True)
        else:
            df_ord = df_part.reset_index(drop=True)

        hashes_colunas: dict[str, str] = {}
        pecas_hash_tabela: list[str] = []
        for col in colunas:
            try:
                arr_h = pd.util.hash_pandas_object(
                    df_ord[col], index=False
                ).to_numpy()
                h_col = hashlib.sha256(arr_h.tobytes()).hexdigest()[:16]
            except Exception:
                s_fallback = df_ord[col].astype(str).fillna("<NA>")
                h_col = hashlib.sha256(
                    "|".join(s_fallback).encode("utf-8")
                ).hexdigest()[:16]
            hashes_colunas[col] = h_col
            pecas_hash_tabela.append(f"{col}:{h_col}")

        hash_conteudo = hashlib.sha256(
            "|".join(pecas_hash_tabela).encode("utf-8")
        ).hexdigest()[:16]

        preenchidos_por_col = {col: int(df_part[col].notna().sum()) for col in colunas}
        nulos_por_col = {col: int(df_part[col].isna().sum()) for col in colunas}

        somas_numericas: dict[str, float] = {}
        mins_numericos: dict[str, float] = {}
        maxs_numericos: dict[str, float] = {}
        dominios_por_col: dict[str, dict[str, int]] = {}

        tipos_colunas: dict[str, str] = {}
        dtypes_brutos: dict[str, str] = {col: str(df_part[col].dtype) for col in colunas}
        unicidade_cols: dict[str, dict[str, int]] = {}
        unicidade_composta: dict[str, dict[str, int]] = {}
        aderencia_data_particao: dict[str, dict[str, Any]] = {}
        regex_str_cols: dict[str, dict[str, Any]] = {}
        eventos_por_data: dict[str, dict[str, int]] = {}
        amostras_numericas: dict[str, list[float]] = {}

        val_particao = nome_particao.split("=")[-1]
        if tipo_particao == "mensal" and len(val_particao) == 6:
            prefixo_esperado = f"{val_particao[:4]}-{val_particao[4:6]}"
        elif tipo_particao == "diaria" and len(val_particao) == 8:
            prefixo_esperado = (
                f"{val_particao[:4]}-{val_particao[4:6]}-{val_particao[6:8]}"
            )
        else:
            prefixo_esperado = val_particao

        # Deduplicação por chave primária candidata para contagem de eventos de maturação
        if cols_ord and qtd_linhas > 0:
            df_dedup_pk = df_part.drop_duplicates(subset=[cols_ord[0]])
        else:
            df_dedup_pk = df_part.drop_duplicates()

        colunas_data_identificadas: list[str] = []

        for col in colunas:
            serie = df_part[col]
            eh_id_estrito = eh_coluna_id_primaria_estrita(col)
            eh_id_cand = eh_coluna_id_candidata(col)
            eh_dt = eh_coluna_data_ou_ts(col, serie)

            # Calcula unicidade para todas as colunas candidatas a ID (ou todas as colunas de forma rápida)
            s_sem_nulo = serie.dropna()
            if eh_id_cand or col in (chaves_primarias_hint or []):
                n_unicos_col = int(s_sem_nulo.nunique())
                unicidade_cols[col] = {
                    "n_nulos": int(nulos_por_col[col]),
                    "n_unicos": n_unicos_col,
                    "qtd_dup": int(len(s_sem_nulo) - n_unicos_col),
                }

            if pd.api.types.is_datetime64_any_dtype(serie):
                tipos_colunas[col] = "datetime_dtype"
                colunas_data_identificadas.append(col)
                continue

            if eh_dt:
                tipos_colunas[col] = "data_ts"
                colunas_data_identificadas.append(col)
                s_str = s_sem_nulo.astype(str)
                total_val = len(s_str)
                if total_val > 0:
                    fora_mask = ~s_str.str.startswith(prefixo_esperado)
                    fora_vals = s_str[fora_mask]
                    qtd_fora = int(len(fora_vals))
                    aderencia = float((total_val - qtd_fora) / total_val)
                    exemplos_fora = fora_vals.unique()[:3].tolist()
                else:
                    qtd_fora = 0
                    aderencia = 1.0
                    exemplos_fora = []

                s_datas = pd.to_datetime(
                    serie, errors="coerce", format="ISO8601"
                ).dt.date.dropna()
                dias_mes = sorted({int(d.day) for d in s_datas})

                aderencia_data_particao[col] = {
                    "total_validos": int(total_val),
                    "qtd_fora": qtd_fora,
                    "aderencia": round(aderencia, 6),
                    "exemplos_fora": exemplos_fora,
                    "prefixo_esperado": prefixo_esperado,
                    "dias_mes_distintos": dias_mes,
                }

                # Contagem por data_evento deduplicada por PK (para maturação)
                s_datas_dedup = pd.to_datetime(
                    df_dedup_pk[col], errors="coerce", format="ISO8601"
                ).dt.date.dropna()
                if len(s_datas_dedup) > 0:
                    vc_dt = s_datas_dedup.value_counts()
                    eventos_por_data[col] = {
                        d_ev.isoformat(): int(cnt) for d_ev, cnt in vc_dt.items()
                    }
                continue

            if pd.api.types.is_numeric_dtype(serie) and not pd.api.types.is_bool_dtype(
                serie
            ):
                eh_int = pd.api.types.is_integer_dtype(serie.dtype)
                tipos_colunas[col] = "numerico_int" if eh_int else "numerico_float"
                if not eh_id_estrito:
                    s_num = pd.to_numeric(serie, errors="coerce").dropna()
                    if len(s_num) > 0:
                        somas_numericas[col] = round(float(s_num.sum()), 4)
                        mins_numericos[col] = float(s_num.min())
                        maxs_numericos[col] = float(s_num.max())
                        # Guarda extremos (3 menores e 3 maiores valores únicos) para diagnóstico em streaming/cache
                        vals_uniq_ord = np.sort(s_num.unique())
                        extremos = list(vals_uniq_ord[:3]) + list(vals_uniq_ord[-3:])
                        amostras_numericas[col] = [
                            float(x) for x in dict.fromkeys(extremos)
                        ]
                continue

            if (
                pd.api.types.is_string_dtype(serie)
                or pd.api.types.is_object_dtype(serie)
                or isinstance(serie.dtype, pd.CategoricalDtype)
            ):
                tipos_colunas[col] = "string_cat"
                if not eh_id_estrito:
                    s_val = s_sem_nulo.astype(str)
                    n_preench = len(s_val)
                    if n_preench > 0:
                        n_uniq = int(s_val.nunique())
                        if n_uniq <= 25:
                            vc_cat = s_val.value_counts()
                            dominios_por_col[col] = {
                                str(k): int(v) for k, v in vc_cat.items()
                            }
                        lens = s_val.str.len()
                        tam_med = int(np.median(lens))
                        mask_dig_tam = s_val.str.isdigit() & (lens == tam_med)
                        # Vetorizado: string com todos os caracteres idênticos (ex.: "00000000000")
                        mask_rep = detectar_caracteres_repetidos_serie(s_val)
                        mask_nao_rep = ~mask_rep

                        taxa_dig = float(mask_dig_tam.mean())
                        taxa_nao_rep = float(mask_nao_rep.mean())
                        qtd_inv_dig = int((~mask_dig_tam).sum())
                        qtd_rep = int(mask_rep.sum())
                        exemplos_inv_dig = (
                            s_val[~mask_dig_tam].unique()[:4].tolist()
                            if qtd_inv_dig > 0
                            else []
                        )
                        exemplos_rep = (
                            s_val[mask_rep].unique()[:4].tolist()
                            if qtd_rep > 0
                            else []
                        )

                        info_reg_col: dict[str, Any] = {
                            "total_preenchidos": n_preench,
                            "tam_med": tam_med,
                            "padrao_inferido": f"^\\d{{{tam_med}}}$",
                            "taxa_dig": round(taxa_dig, 6),
                            "taxa_nao_rep": round(taxa_nao_rep, 6),
                            "qtd_inv_dig": qtd_inv_dig,
                            "qtd_rep": qtd_rep,
                            "exemplos_inv_dig": exemplos_inv_dig,
                            "exemplos_rep": exemplos_rep,
                        }

                        # Se tiver 11 ou 14 dígitos, pré-calcula aderência ao Módulo 11 (CPF/CNPJ)
                        if tam_med == 11 and taxa_dig >= 0.50:
                            s_11_validos_fmt = s_val[mask_dig_tam & mask_nao_rep]
                            if len(s_11_validos_fmt) > 0:
                                mask_cpf_ok = validar_cpf_serie(s_11_validos_fmt)
                                taxa_cpf = float(mask_cpf_ok.mean())
                                inv_cpf = s_11_validos_fmt[~mask_cpf_ok]
                                info_reg_col["taxa_cpf_mod11"] = round(taxa_cpf, 6)
                                info_reg_col["qtd_inv_cpf_mod11"] = int(len(inv_cpf))
                                info_reg_col["exemplos_inv_cpf_mod11"] = (
                                    inv_cpf.unique()[:4].tolist()
                                )
                        elif tam_med == 14 and taxa_dig >= 0.50:
                            s_14_validos_fmt = s_val[mask_dig_tam & mask_nao_rep]
                            if len(s_14_validos_fmt) > 0:
                                mask_cnpj_ok = validar_cnpj_serie(s_14_validos_fmt)
                                taxa_cnpj = float(mask_cnpj_ok.mean())
                                inv_cnpj = s_14_validos_fmt[~mask_cnpj_ok]
                                info_reg_col["taxa_cnpj_mod11"] = round(taxa_cnpj, 6)
                                info_reg_col["qtd_inv_cnpj_mod11"] = int(len(inv_cnpj))
                                info_reg_col["exemplos_inv_cnpj_mod11"] = (
                                    inv_cnpj.unique()[:4].tolist()
                                )

                        # Avalia padrões regex customizados do YAML (se informados)
                        if regras_regex_custom and col in regras_regex_custom:
                            pad_c = str(regras_regex_custom[col])
                            try:
                                m_c = s_val.str.match(pad_c, na=False)
                                inv_c = s_val[~m_c]
                                info_reg_col["padroes_custom"] = {
                                    pad_c: {
                                        "qtd_inv": int(len(inv_c)),
                                        "exemplos_inv": inv_c.unique()[:4].tolist(),
                                    }
                                }
                            except re.error:
                                pass

                        regex_str_cols[col] = info_reg_col
                continue

            tipos_colunas[col] = "outro"

        # Unicidade de Chaves Compostas (para auto-descoberta e para avaliação em CACHE_GOLD / Streaming)
        combinacoes_pk_avaliar: list[tuple[str, ...]] = []
        if chaves_primarias_hint:
            pks_pres = tuple(c for c in chaves_primarias_hint if c in colunas)
            if len(pks_pres) > 1:
                combinacoes_pk_avaliar.append(pks_pres)

        # Pares de colunas candidatas a ID para suportar auto-descoberta de chave composta (até 6 candidatas)
        cands_para_pares = cols_id_candidatas[:6]
        for par in combinations(cands_para_pares, 2):
            if par not in combinacoes_pk_avaliar:
                combinacoes_pk_avaliar.append(par)

        for tupla_cols in combinacoes_pk_avaliar:
            lista_c = list(tupla_cols)
            chave_comp = ",".join(lista_c)
            if qtd_linhas > 0:
                n_nulos_qualquer = int(df_part[lista_c].isna().any(axis=1).sum())
                df_sem_nulo_c = df_part.dropna(subset=lista_c)
                qtd_dup_c = int(df_sem_nulo_c.duplicated(subset=lista_c).sum())
                n_unicos_c = int(len(df_sem_nulo_c) - qtd_dup_c)
            else:
                n_nulos_qualquer = 0
                qtd_dup_c = 0
                n_unicos_c = 0
            unicidade_composta[chave_comp] = {
                "n_nulos": n_nulos_qualquer,
                "n_unicos": n_unicos_c,
                "qtd_dup": qtd_dup_c,
            }

        # Relações entre pares de colunas de data (Cross-Column Consistency)
        relacoes_colunas: dict[str, dict[str, Any]] = {}
        cols_dt_rel = [
            c
            for c in colunas_data_identificadas
            if not c.lower().startswith(("ts_", "dt_atualizacao"))
        ]
        if qtd_linhas > 0 and len(cols_dt_rel) >= 2:
            datas_convertidas: dict[str, pd.Series] = {}
            for c_dt in cols_dt_rel[:6]:
                datas_convertidas[c_dt] = pd.to_datetime(
                    df_part[c_dt], errors="coerce", format="ISO8601"
                ).dt.date

            for col_a, col_b in combinations(cols_dt_rel[:6], 2):
                sa = datas_convertidas[col_a]
                sb = datas_convertidas[col_b]
                mask_ambos = sa.notna() & sb.notna()
                total_pares = int(mask_ambos.sum())
                if total_pares == 0:
                    continue
                sa_v = sa[mask_ambos]
                sb_v = sb[mask_ambos]

                # Relação 1: col_a >= col_b
                viol_a_ge_b = sa_v < sb_v
                qtd_viol_ab = int(viol_a_ge_b.sum())
                ex_ab = [
                    f"{col_a}={a} < {col_b}={b}"
                    for a, b in zip(
                        sa_v[viol_a_ge_b].head(3), sb_v[viol_a_ge_b].head(3)
                    )
                ]
                relacoes_colunas[f"{col_a}>={col_b}"] = {
                    "col_maior": col_a,
                    "col_menor": col_b,
                    "total_pares": total_pares,
                    "qtd_violacoes": qtd_viol_ab,
                    "taxa_aderencia": round(
                        (total_pares - qtd_viol_ab) / total_pares, 6
                    ),
                    "exemplos_violacoes": ex_ab,
                }

                # Relação 2: col_b >= col_a
                viol_b_ge_a = sb_v < sa_v
                qtd_viol_ba = int(viol_b_ge_a.sum())
                ex_ba = [
                    f"{col_b}={b} < {col_a}={a}"
                    for a, b in zip(
                        sa_v[viol_b_ge_a].head(3), sb_v[viol_b_ge_a].head(3)
                    )
                ]
                relacoes_colunas[f"{col_b}>={col_a}"] = {
                    "col_maior": col_b,
                    "col_menor": col_a,
                    "total_pares": total_pares,
                    "qtd_violacoes": qtd_viol_ba,
                    "taxa_aderencia": round(
                        (total_pares - qtd_viol_ba) / total_pares, 6
                    ),
                    "exemplos_violacoes": ex_ba,
                }

        perfil_dq = {
            "colunas": colunas,
            "tipos_colunas": tipos_colunas,
            "dtypes_brutos": dtypes_brutos,
            "qtd_dup_linha": int(df_part.duplicated().sum()) if qtd_linhas > 0 else 0,
            "unicidade_cols": unicidade_cols,
            "unicidade_composta": unicidade_composta,
            "aderencia_data_particao": aderencia_data_particao,
            "regex_str_cols": regex_str_cols,
            "eventos_por_data": eventos_por_data,
            "amostras_numericas": amostras_numericas,
            "relacoes_colunas": relacoes_colunas,
        }

        return {
            "particao": nome_particao,
            "qtd_registros": qtd_linhas,
            "qtd_arquivos": int(mf.get("qtd_arquivos", 1)),
            "tamanho_bytes": int(mf.get("tamanho_bytes", 0)),
            "max_mtime_ns": int(mf.get("max_mtime_ns", 0)),
            "fingerprint_fisico": str(mf.get("fingerprint_fisico", "")),
            "hash_conteudo": hash_conteudo,
            "hashes_colunas_json": json.dumps(hashes_colunas, ensure_ascii=False),
            "preenchidos_json": json.dumps(preenchidos_por_col, ensure_ascii=False),
            "nulos_json": json.dumps(nulos_por_col, ensure_ascii=False),
            "somas_numericas_json": json.dumps(somas_numericas, ensure_ascii=False),
            "mins_numericos_json": json.dumps(mins_numericos, ensure_ascii=False),
            "maxs_numericos_json": json.dumps(maxs_numericos, ensure_ascii=False),
            "dominios_json": json.dumps(dominios_por_col, ensure_ascii=False),
            "perfil_dq_json": json.dumps(perfil_dq, ensure_ascii=False),
        }

    def carregar_fotos_particoes(self) -> pd.DataFrame:
        if self.arq_fotos_particoes.exists():
            return pd.read_parquet(self.arq_fotos_particoes)
        return pd.DataFrame()

    def carregar_fotos_eventos(self) -> pd.DataFrame:
        if self.arq_fotos_eventos.exists():
            return pd.read_parquet(self.arq_fotos_eventos)
        return pd.DataFrame()

    def carregar_regras_aprendidas(self) -> pd.DataFrame:
        if self.arq_regras_aprendidas.exists():
            return pd.read_parquet(self.arq_regras_aprendidas)
        return pd.DataFrame()

    def registrar_regras_aprendidas(
        self,
        tabela: str,
        dt_referencia: str,
        registros_regras: list[dict[str, Any]],
    ) -> pd.DataFrame:
        """Persiste em `_metadados_dq/` todas as regras auto-aprendidas (e overrides) da tabela."""
        if not registros_regras:
            return pd.DataFrame()

        df_novo = pd.DataFrame(registros_regras)
        self._salvar_metadado_particionado(
            "regras_aprendidas", tabela, dt_referencia, df_novo
        )

        df_hist = self.carregar_regras_aprendidas()
        if not df_hist.empty:
            mask_manter = ~(
                (df_hist["tabela"] == tabela)
                & (df_hist["dt_referencia"] == dt_referencia)
            )
            df_final = pd.concat([df_hist[mask_manter], df_novo], ignore_index=True)
        else:
            df_final = df_novo

        df_final.to_parquet(
            self.arq_regras_aprendidas,
            engine="pyarrow",
            compression="snappy",
            index=False,
        )
        return df_novo

    def registrar_foto_particoes(
        self,
        run_id: str,
        dt_referencia: str,
        ts_execucao: str,
        tabela: str,
        particoes_dfs: dict[str, pd.DataFrame] | None = None,
        mapa_camada_particoes: dict[str, str] | None = None,
        perfis_particoes: dict[str, dict[str, Any]] | None = None,
        mapa_modo_avaliacao: dict[str, str] | None = None,
        tipo_particao: str = "mensal",
    ) -> pd.DataFrame:
        """
        Registra uma fotografia de todas as partições da tabela em `_metadados_dq/`,
        anotando a `camada_qualidade` (`GOLD`, `SILVER`, `BRONZE`), `modo_avaliacao`
        e preservando o `baseline_gold_json` quando uma partição `GOLD` cai em quarentena
        por reprocessamento não autorizado.
        """
        mapa_camada = mapa_camada_particoes or {}
        mapa_modo = mapa_modo_avaliacao or {}
        df_hist = self.carregar_fotos_particoes()

        # Busca o último snapshot GOLD conhecido de cada partição para blindar o baseline
        ultimo_gold_por_part: dict[str, dict[str, Any]] = {}
        if not df_hist.empty:
            df_tab_hist = df_hist[df_hist["tabela"] == tabela].sort_values(
                "dt_referencia"
            )
            for _, row_h in df_tab_hist.iterrows():
                p_name = str(row_h["particao"])
                cam_h = str(row_h.get("camada_qualidade", "GOLD"))
                if cam_h == "GOLD":
                    ultimo_gold_por_part[p_name] = {
                        "dt_referencia": str(row_h["dt_referencia"]),
                        "qtd_registros": int(row_h["qtd_registros"]),
                        "fingerprint_fisico": str(row_h.get("fingerprint_fisico", "")),
                        "hash_conteudo": str(row_h.get("hash_conteudo", "")),
                        "hashes_colunas_json": str(
                            row_h.get("hashes_colunas_json", "{}")
                        ),
                        "preenchidos_json": str(row_h.get("preenchidos_json", "{}")),
                        "nulos_json": str(row_h.get("nulos_json", "{}")),
                        "somas_numericas_json": str(
                            row_h.get("somas_numericas_json", "{}")
                        ),
                        "dominios_json": str(row_h.get("dominios_json", "{}")),
                    }
                elif not ultimo_gold_por_part.get(p_name) and row_h.get(
                    "baseline_gold_json"
                ):
                    try:
                        bg = json.loads(str(row_h["baseline_gold_json"]))
                        if bg:
                            ultimo_gold_por_part[p_name] = bg
                    except Exception:
                        pass

        perfis_uso: dict[str, dict[str, Any]] = {}
        if perfis_particoes:
            perfis_uso.update(perfis_particoes)
        elif particoes_dfs:
            for nome_p, df_p in sorted(particoes_dfs.items()):
                perfis_uso[nome_p] = self.construir_perfil_particao(
                    nome_particao=nome_p,
                    df_part=df_p,
                    tipo_particao=tipo_particao,
                )

        registros = []
        for nome_particao, perfil in sorted(perfis_uso.items()):
            camada_atual = mapa_camada.get(nome_particao, "GOLD")
            modo_atual = mapa_modo.get(nome_particao, "SCAN_PARQUET")

            baseline_gold_str = ""
            if camada_atual != "GOLD" and nome_particao in ultimo_gold_por_part:
                baseline_gold_str = json.dumps(
                    ultimo_gold_por_part[nome_particao], ensure_ascii=False
                )

            registros.append(
                {
                    "run_id": run_id,
                    "dt_referencia": dt_referencia,
                    "ts_execucao": ts_execucao,
                    "tabela": tabela,
                    "particao": nome_particao,
                    "camada_qualidade": camada_atual,
                    "modo_avaliacao": modo_atual,
                    "qtd_registros": int(perfil["qtd_registros"]),
                    "qtd_arquivos": int(perfil.get("qtd_arquivos", 1)),
                    "tamanho_bytes": int(perfil.get("tamanho_bytes", 0)),
                    "max_mtime_ns": int(perfil.get("max_mtime_ns", 0)),
                    "fingerprint_fisico": str(perfil.get("fingerprint_fisico", "")),
                    "hash_conteudo": str(perfil.get("hash_conteudo", "")),
                    "hashes_colunas_json": str(perfil.get("hashes_colunas_json", "{}")),
                    "preenchidos_json": str(perfil.get("preenchidos_json", "{}")),
                    "nulos_json": str(perfil.get("nulos_json", "{}")),
                    "somas_numericas_json": str(
                        perfil.get("somas_numericas_json", "{}")
                    ),
                    "mins_numericos_json": str(perfil.get("mins_numericos_json", "{}")),
                    "maxs_numericos_json": str(perfil.get("maxs_numericos_json", "{}")),
                    "dominios_json": str(perfil.get("dominios_json", "{}")),
                    "perfil_dq_json": str(perfil.get("perfil_dq_json", "{}")),
                    "baseline_gold_json": baseline_gold_str,
                }
            )

        df_novo = pd.DataFrame(registros)
        self._salvar_metadado_particionado(
            "snapshots_particoes", tabela, dt_referencia, df_novo
        )

        if not df_hist.empty:
            mask_manter = ~(
                (df_hist["tabela"] == tabela)
                & (df_hist["dt_referencia"] == dt_referencia)
            )
            df_hist = df_hist[mask_manter]
            df_final = pd.concat([df_hist, df_novo], ignore_index=True)
        else:
            df_final = df_novo

        df_final.to_parquet(
            self.arq_fotos_particoes, engine="pyarrow", compression="snappy", index=False
        )
        return df_novo

    def registrar_foto_eventos_data(
        self,
        run_id: str,
        dt_referencia: str,
        ts_execucao: str,
        tabela: str,
        coluna_data: str,
        df_completo: pd.DataFrame,
        chaves_primarias: list[str] | None = None,
        janela_captura_dias: int = 25,
        perfis_cache_gold: dict[str, dict[str, Any]] | None = None,
    ) -> pd.DataFrame:
        """
        Tira uma fotografia da volumetria diária agrupada por `coluna_data`.
        Remove duplicatas de chave primária/linha antes da contagem e soma também
        as contagens pré-agregadas das partições em `perfis_cache_gold` / streaming.
        """
        dt_ref = dt.date.fromisoformat(dt_referencia)

        contagem_por_data: dict[dt.date, int] = {}

        if not df_completo.empty and coluna_data in df_completo.columns:
            pks_validas = [
                k for k in (chaves_primarias or []) if k in df_completo.columns
            ]
            if pks_validas:
                df_base = df_completo.dropna(subset=pks_validas).drop_duplicates(
                    subset=pks_validas
                )
            else:
                df_base = df_completo.drop_duplicates()

            serie_datas = pd.to_datetime(
                df_base[coluna_data], errors="coerce", format="ISO8601"
            ).dt.date
            serie_valida = serie_datas.dropna()
            for d_val, cnt in serie_valida.value_counts().items():
                contagem_por_data[d_val] = contagem_por_data.get(d_val, 0) + int(cnt)

        # Soma contagens das partições cujos eventos vieram de perfis pré-agregados (Cache GOLD / Streaming)
        if perfis_cache_gold:
            for perfil_c in perfis_cache_gold.values():
                try:
                    pdq = json.loads(str(perfil_c.get("perfil_dq_json", "{}")))
                    ev_col = pdq.get("eventos_por_data", {}).get(coluna_data, {})
                    for d_iso, cnt in ev_col.items():
                        d_obj = dt.date.fromisoformat(str(d_iso))
                        contagem_por_data[d_obj] = contagem_por_data.get(
                            d_obj, 0
                        ) + int(cnt)
                except Exception:
                    pass

        volumes_cauda_estavel: list[int] = []
        for lag in range(8, janela_captura_dias + 1):
            d_alvo = dt_ref - dt.timedelta(days=lag)
            qtd = int(contagem_por_data.get(d_alvo, 0))
            if qtd > 0:
                volumes_cauda_estavel.append(qtd)

        if not volumes_cauda_estavel:
            volumes_cauda_estavel = [
                int(v) for v in contagem_por_data.values() if int(v) > 0
            ]

        volume_maduro_ref = (
            float(np.median(volumes_cauda_estavel)) if volumes_cauda_estavel else 1.0
        )

        registros = []
        for lag in range(0, janela_captura_dias + 1):
            d_evento = dt_ref - dt.timedelta(days=lag)
            qtd = int(contagem_por_data.get(d_evento, 0))
            fator = round(qtd / volume_maduro_ref, 4) if volume_maduro_ref > 0 else 0.0
            registros.append(
                {
                    "run_id": run_id,
                    "dt_referencia": dt_referencia,
                    "ts_execucao": ts_execucao,
                    "tabela": tabela,
                    "coluna_data": coluna_data,
                    "data_evento": d_evento.isoformat(),
                    "lag_dias": int(lag),
                    "qtd_registros": qtd,
                    "volume_maduro_referencia": round(volume_maduro_ref, 2),
                    "fator_maturacao": fator,
                }
            )

        df_novo = pd.DataFrame(registros)
        self._salvar_metadado_particionado(
            "snapshots_eventos_data", tabela, dt_referencia, df_novo
        )

        df_hist = self.carregar_fotos_eventos()
        if not df_hist.empty:
            mask_manter = ~(
                (df_hist["tabela"] == tabela)
                & (df_hist["coluna_data"] == coluna_data)
                & (df_hist["dt_referencia"] == dt_referencia)
            )
            df_hist = df_hist[mask_manter]
            df_final = pd.concat([df_hist, df_novo], ignore_index=True)
        else:
            df_final = df_novo

        df_final.to_parquet(
            self.arq_fotos_eventos, engine="pyarrow", compression="snappy", index=False
        )
        return df_novo

    @staticmethod
    def _identificar_datas_evento_contaminadas_estatisticamente(
        df_treino_bruto: pd.DataFrame,
    ) -> set[str]:
        """
        Detecta datas de evento anômalas/contaminadas de forma 100% estatística (baseada na
        mediana e MAD do histórico de cada `lag_dias` e da cauda madura), evitando cortes fixos
        que descartariam eventos com atraso natural legítimo (ex.: lag=0 naturalmente igual a 0%).
        """
        datas_contaminadas: set[str] = set()
        if df_treino_bruto.empty:
            return datas_contaminadas

        # 1. Calcula mediana e sigma robusto (MAD) para cada lag_dias
        stats_por_lag: dict[int, tuple[float, float]] = {}
        for lag_val, grp_lag in df_treino_bruto.groupby("lag_dias"):
            fatores_lag = grp_lag["fator_maturacao"].astype(float).tolist()
            med_l, sig_l = mediana_e_sigma_mad(
                fatores_lag, sigma_min_rel=0.15, sigma_min_abs=0.05
            )
            stats_por_lag[int(lag_val)] = (med_l, sig_l)

        # 2. Mediana de referência da cauda madura (lag >= 4)
        df_maduros = df_treino_bruto[df_treino_bruto["lag_dias"] >= 4]
        if not df_maduros.empty:
            med_maduro = float(
                np.median(df_maduros["fator_maturacao"].astype(float).to_numpy())
            )
        else:
            med_maduro = 1.0

        # 3. Avalia cada registro contra a distribuição histórica de seu próprio lag
        for _, r_ev in df_treino_bruto.iterrows():
            lag_ev = int(r_ev["lag_dias"])
            fator_ev = float(r_ev["fator_maturacao"])
            dt_ev_str = str(r_ev["data_evento"])

            med_lag, sig_lag = stats_por_lag.get(lag_ev, (0.0, 0.10))

            # Se historicamente esse lag tem volume relevante (med_lag > 0.10), mas esta data
            # caiu severamente abaixo da mediana histórica daquele mesmo lag:
            if (
                med_lag > 0.10
                and fator_ev < max(0.02, med_lag - 3.0 * sig_lag)
                and fator_ev < med_lag * 0.45
            ):
                datas_contaminadas.add(dt_ev_str)
            # Para lags já maduros (lag >= 4 onde a mediana madura já é alta),
            # reprova quedas severas (< 55% da mediana madura):
            elif lag_ev >= 4 and med_maduro >= 0.70 and fator_ev < med_maduro * 0.55:
                datas_contaminadas.add(dt_ev_str)

        return datas_contaminadas

    def atualizar_e_obter_curva_aprendida(
        self,
        tabela: str,
        coluna_data: str,
        dt_referencia_atual: str,
        max_lag_busca: int = 10,
        persistir: bool = True,
    ) -> dict[str, Any]:
        """
        Aprende AUTOMATICAMENTE a partir das fotografias anteriores, filtrando APENAS
        datas de evento saudáveis (`GOLD`) por detecção estatística por lag para evitar que
        datas com falha de ingestão contaminem a curva esperada ou o `lag_estabilizacao_aprendido`.
        """
        df_fotos = self.carregar_fotos_eventos()
        if df_fotos.empty:
            return {
                "qtd_fotos_historico": 0,
                "lag_estabilizacao_aprendido": 0,
                "janela_avaliacao_aprendida": 5,
                "tolerancia_desvio_aprendida": 0.35,
                "curva_por_lag": {},
            }

        df_treino_bruto = df_fotos[
            (df_fotos["tabela"] == tabela)
            & (df_fotos["coluna_data"] == coluna_data)
            & (df_fotos["dt_referencia"] < dt_referencia_atual)
        ].copy()

        if df_treino_bruto.empty:
            return {
                "qtd_fotos_historico": 0,
                "lag_estabilizacao_aprendido": 0,
                "janela_avaliacao_aprendida": 5,
                "tolerancia_desvio_aprendida": 0.35,
                "curva_por_lag": {},
            }

        qtd_fotos_total = int(df_treino_bruto["dt_referencia"].nunique())

        # Filtra datas de evento contaminadas/anômalas via detecção estatística por lag
        datas_contaminadas = (
            self._identificar_datas_evento_contaminadas_estatisticamente(
                df_treino_bruto
            )
        )

        df_treino_gold = df_treino_bruto[
            ~df_treino_bruto["data_evento"].isin(datas_contaminadas)
        ].copy()
        df_treino = df_treino_gold if not df_treino_gold.empty else df_treino_bruto

        # Pareia a MESMA data_evento em fotos consecutivas (apenas eventos GOLD): lag -> lag + 1
        df_lag_atual = df_treino[
            ["data_evento", "lag_dias", "qtd_registros"]
        ].rename(columns={"qtd_registros": "qtd_lag"})

        df_lag_seguinte = df_treino[
            ["data_evento", "lag_dias", "qtd_registros"]
        ].copy()
        df_lag_seguinte["lag_dias"] = df_lag_seguinte["lag_dias"] - 1
        df_lag_seguinte = df_lag_seguinte.rename(
            columns={"qtd_registros": "qtd_lag_mais_1"}
        )

        df_transicoes = pd.merge(
            df_lag_atual,
            df_lag_seguinte,
            on=["data_evento", "lag_dias"],
            how="inner",
        )
        df_transicoes["delta_qtd"] = (
            df_transicoes["qtd_lag_mais_1"] - df_transicoes["qtd_lag"]
        )
        df_transicoes["taxa_crescimento"] = df_transicoes["delta_qtd"] / np.maximum(
            df_transicoes["qtd_lag"], 1
        )

        crescimento_por_lag: dict[int, tuple[float, float]] = {}
        if not df_transicoes.empty:
            for lag_t, grp_t in df_transicoes.groupby("lag_dias"):
                med_delta = float(np.median(grp_t["delta_qtd"].to_numpy()))
                med_taxa = float(np.median(grp_t["taxa_crescimento"].to_numpy()))
                crescimento_por_lag[int(lag_t)] = (med_delta, med_taxa)

        curva_por_lag: dict[int, dict[str, Any]] = {}
        stds_maduros: list[float] = []

        for lag, grupo in df_treino.groupby("lag_dias"):
            lag_int = int(lag)
            fatores = grupo["fator_maturacao"].astype(float).to_numpy()
            qtds = grupo["qtd_registros"].astype(float).to_numpy()
            n_fotos_lag = int(grupo["dt_referencia"].nunique())

            fator_medio = float(np.median(fatores))
            fator_std = float(np.std(fatores)) if len(fatores) > 1 else 0.05
            if lag_int >= 8:
                stds_maduros.append(fator_std)

            qtd_media = float(np.median(qtds))
            med_delta, med_taxa = crescimento_por_lag.get(lag_int, (0.0, 0.0))

            curva_por_lag[lag_int] = {
                "tabela": tabela,
                "coluna_data": coluna_data,
                "lag_dias": lag_int,
                "qtd_fotos_aprendizado": n_fotos_lag,
                "fator_maturacao_esperado": round(fator_medio, 4),
                "fator_maturacao_std": round(fator_std, 4),
                "qtd_registros_mediana_historica": round(qtd_media, 2),
                "delta_registros_proxima_foto": round(med_delta, 2),
                "taxa_crescimento_proxima_foto": round(med_taxa, 4),
            }

        lag_estabilizacao = 0
        for lag_cand in range(0, max_lag_busca):
            info = curva_por_lag.get(lag_cand)
            if info is None:
                continue
            delta_prox = float(info["delta_registros_proxima_foto"])
            taxa_prox = float(info["taxa_crescimento_proxima_foto"])
            fator_esp = float(info["fator_maturacao_esperado"])

            ainda_cresce_entre_fotos = (delta_prox >= 1.0) or (taxa_prox > 0.03)
            sem_transicao_mas_incompleto = (
                lag_cand not in crescimento_por_lag and fator_esp < 0.88
            )

            if ainda_cresce_entre_fotos or sem_transicao_mas_incompleto:
                lag_estabilizacao = lag_cand + 1
            else:
                break

        janela_aprendida = max(5, int(lag_estabilizacao) + 5)

        std_ref_maduro = float(np.median(stds_maduros)) if stds_maduros else 0.08
        tolerancia_aprendida = float(
            np.clip(round(max(3.0 * std_ref_maduro, 0.30), 2), 0.25, 0.38)
        )

        linhas_silver = []
        for lag_int, info in sorted(curva_por_lag.items()):
            info["lag_estabilizacao_aprendido"] = int(lag_estabilizacao)
            info["janela_avaliacao_aprendida"] = int(janela_aprendida)
            info["tolerancia_desvio_aprendida"] = float(tolerancia_aprendida)
            info["estagio"] = (
                "EM_MATURACAO" if lag_int < lag_estabilizacao else "MADURO"
            )
            linhas_silver.append(info)

        if persistir and linhas_silver:
            df_curva_nova = pd.DataFrame(linhas_silver)
            if self.arq_curva_aprendida.exists():
                df_curva_ant = pd.read_parquet(self.arq_curva_aprendida)
                mask = ~(
                    (df_curva_ant["tabela"] == tabela)
                    & (df_curva_ant["coluna_data"] == coluna_data)
                )
                df_curva_final = pd.concat(
                    [df_curva_ant[mask], df_curva_nova], ignore_index=True
                )
            else:
                df_curva_final = df_curva_nova

            df_curva_final.to_parquet(
                self.arq_curva_aprendida,
                engine="pyarrow",
                compression="snappy",
                index=False,
            )

        return {
            "qtd_fotos_historico": qtd_fotos_total,
            "lag_estabilizacao_aprendido": int(lag_estabilizacao),
            "janela_avaliacao_aprendida": int(janela_aprendida),
            "tolerancia_desvio_aprendida": float(tolerancia_aprendida),
            "curva_por_lag": curva_por_lag,
        }
