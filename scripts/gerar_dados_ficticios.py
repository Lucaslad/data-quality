"""
Script para geração de dados fictícios (formato Parquet particionado estilo Hive/S3/Athena).

Estrutura criada na pasta `dados/`:
- `dados/origem/`:
  1. `tabela_particao_mensal` (dados limpos, partições `anomes=202605` a `anomes=202610`)
  2. `tabela_particao_diaria` (dados limpos, partições `anomesdia=20261001` a `anomesdia=20261030`)
  3. `tabela_particao_mensal_anomalias` (com anomalias de Data Quality, partições `anomes=202605` a `anomes=202610`)
  4. `tabela_particao_diaria_anomalias` (com anomalias de Data Quality, partições `anomesdia=20261001` a `anomesdia=20261030`)
- `dados/bronze/`: Bucket S3 simulado para camada Bronze
- `dados/silver/`: Bucket S3 simulado para camada Silver
- `dados/gold/`: Bucket S3 simulado para camada Gold
"""

from __future__ import annotations

import datetime as dt
import hashlib
from pathlib import Path
import random
import shutil

import numpy as np
import pandas as pd

BASE_DIR = Path(__file__).resolve().parent.parent / "dados"
ORIGEM_DIR = BASE_DIR / "origem"
SAIDA_DIR = BASE_DIR / "saida"
BUCKETS_MEDALLION = ("bronze", "silver", "gold")

UFS_VALIDAS = ["SP", "RJ", "MG", "PR", "RS", "BA", "SC", "DF"]
SEGMENTOS_VALIDOS = ["VAREJO", "UNICLASS", "PERSONNALITE", "EMPRESAS"]
STATUS_PAGAMENTO_VALIDOS = ["PAGO", "EM_ABERTO", "ATRASADO"]

TIPOS_TRANSACAO_VALIDOS = [
    "PIX",
    "CARTAO_CREDITO",
    "CARTAO_DEBITO",
    "TED",
    "BOLETO",
]
CANAIS_VALIDOS = ["APP", "INTERNET_BANKING", "CAIXA_ELETRONICO", "AGENCIA"]
STATUS_TRANSACAO_VALIDOS = ["APROVADA", "RECUSADA", "ESTORNADA"]

PARTICOES_MENSAIS = ["202605", "202606", "202607", "202608", "202609", "202610"]
PARTICOES_DIARIAS = [f"202610{dia:02d}" for dia in range(1, 31)]


def _nome_arquivo_s3(semente: str) -> str:
    """Gera um nome de arquivo Parquet simulando a saída do AWS Glue / Spark no S3."""
    hash_curto = hashlib.md5(semente.encode("utf-8")).hexdigest()[:12]
    return f"part-00000-{hash_curto}-c000.snappy.parquet"


def _gerar_cpf_valido(rng: random.Random) -> str:
    """Gera uma string numérica de 11 dígitos não repetidos simulando um CPF."""
    while True:
        cpf = "".join(str(rng.randint(0, 9)) for _ in range(11))
        if len(set(cpf)) > 1:
            return cpf


def gerar_df_mensal_limpo(
    anomes: str,
    n_linhas: int,
    seed: int,
    dt_referencia_foto: dt.date = dt.date(2026, 10, 30),
) -> pd.DataFrame:
    """
    Gera a base mensal de clientes/faturas particionada pela safra de abertura (`anomes`).
    Inclui `dt_abertura_conta` e `dt_cadastro_chave_pix`, onde:
    - O cadastro da chave Pix pode ocorrer no mês de abertura ou em meses posteriores
      (permitindo que partições passadas sejam atualizadas em execuções futuras).
    - O evento de cadastro de chave Pix leva de 0 a 3 dias (`delay_reflexao`) para
      refletir na base em relação à `dt_referencia_foto`.
    """
    rng = random.Random(seed)
    np_rng = np.random.default_rng(seed)

    ano = int(anomes[:4])
    mes = int(anomes[4:6])

    ids_clientes = rng.sample(range(10001, 99999), n_linhas)
    registros = []

    for i, id_cliente in enumerate(ids_clientes, start=1):
        dia_abertura = ((i - 1) % 30) + 1
        dt_abertura = dt.date(ano, mes, dia_abertura)

        dia_fechamento = rng.choice([5, 10, 15, 20, 25])
        dt_fech = dt.date(ano, mes, dia_fechamento)
        ts_atualizacao = dt.datetime(
            ano,
            mes,
            dia_fechamento,
            rng.randint(0, 23),
            rng.randint(0, 59),
            rng.randint(0, 59),
        )
        qtd_tx = int(np_rng.integers(1, 80))
        valor = round(float(np_rng.uniform(50.0, 15000.0)), 2)
        score = int(np_rng.integers(300, 1001))

        # Simulação realista de dt_cadastro_chave_pix:
        # ~85% dos clientes cadastram chave Pix no próprio mês (no dia da abertura),
        # ~10% cadastram tardio em outubro/2026 (atualizando partições passadas),
        # ~5% nunca cadastraram chave Pix.
        r_pix = rng.random()
        dt_evento_pix: dt.date | None = None
        if r_pix < 0.85:
            dt_evento_pix = dt_abertura
        elif r_pix < 0.95:
            dia_outubro = rng.randint(1, 30)
            dt_cand = dt.date(2026, 10, dia_outubro)
            dt_evento_pix = dt_cand if dt_cand >= dt_abertura else dt_abertura

        # Atraso de até 3 dias para o cadastro da chave Pix refletir na base:
        # 25% refletem no mesmo dia (lag 0), 30% em 1 dia (lag 1),
        # 25% em 2 dias (lag 2), 20% em 3 dias (lag 3).
        delay_dias = rng.choices([0, 1, 2, 3], weights=[0.25, 0.30, 0.25, 0.20], k=1)[0]

        dt_cadastro_pix_visivel: str | None = None
        if dt_evento_pix is not None:
            dt_reflexao = dt_evento_pix + dt.timedelta(days=delay_dias)
            if dt_evento_pix <= dt_referencia_foto and dt_reflexao <= dt_referencia_foto:
                dt_cadastro_pix_visivel = dt_evento_pix.isoformat()

        cpf_val = _gerar_cpf_valido(rng)
        uf_val = rng.choice(UFS_VALIDAS)
        seg_val = rng.choice(SEGMENTOS_VALIDOS)
        status_pag_val = rng.choices(
            STATUS_PAGAMENTO_VALIDOS, weights=[0.75, 0.15, 0.10], k=1
        )[0]

        # Clientes que abrem conta após a data da fotografia ainda não existem na foto
        if dt_abertura > dt_referencia_foto:
            continue

        registros.append(
            {
                "id_fatura": f"FAT-{anomes}-{i:05d}",
                "id_cliente": int(id_cliente),
                "cpf": cpf_val,
                "uf": uf_val,
                "segmento": seg_val,
                "score_credito": score,
                "valor_fatura": valor,
                "qtd_transacoes": qtd_tx,
                "status_pagamento": status_pag_val,
                "dt_abertura_conta": dt_abertura.isoformat(),
                "dt_cadastro_chave_pix": dt_cadastro_pix_visivel,
                "dt_fechamento": dt_fech.isoformat(),
                "dt_atualizacao": ts_atualizacao,
            }
        )

    df = pd.DataFrame(registros)
    df["id_cliente"] = df["id_cliente"].astype("Int64")
    df["score_credito"] = df["score_credito"].astype("Int64")
    df["qtd_transacoes"] = df["qtd_transacoes"].astype("Int64")
    df["valor_fatura"] = df["valor_fatura"].astype("float64")
    return df


def injetar_anomalias_mensal(df: pd.DataFrame, anomes: str, seed: int) -> pd.DataFrame:
    """
    Injeta anomalias de Data Quality para testes:
    - 202605 a 202607: histórico limpo (baseline)
    - 202608: nulos e erros de domínio/formato
    - 202609: queda brusca de volumetria + valores fora do intervalo (negativos/outliers)
    - 202610: todas as anomalias combinadas (nulos, duplicatas, outliers, domínio inválido, data fora da partição, pico de volume)
    """
    df_anom = df.copy()
    rng = random.Random(seed)

    if anomes in ("202605", "202606", "202607"):
        return df_anom

    if anomes == "202608":
        # 1. Completude (Nulos)
        df_anom.loc[0:9, "valor_fatura"] = np.nan
        df_anom.loc[10:17, "cpf"] = None
        df_anom.loc[18:22, "status_pagamento"] = None
        # 2. Validade / Domínio e Formato
        df_anom.loc[25:29, "uf"] = "XX"
        df_anom.loc[30:34, "cpf"] = "12345"  # CPF com tamanho inválido
        df_anom.loc[35:39, "status_pagamento"] = "STATUS_INVALIDO"
        return df_anom

    if anomes == "202609":
        # 1. Queda severa de volumetria (apenas ~90 registros em vez de ~500)
        df_anom = df_anom.iloc[:90].copy().reset_index(drop=True)
        # 2. Consistência numérica (valores negativos e score fora de [300, 1000])
        df_anom.loc[0:4, "valor_fatura"] = -1250.75
        df_anom.loc[5:9, "score_credito"] = 1450
        df_anom.loc[10:14, "score_credito"] = -50
        df_anom.loc[15:18, "qtd_transacoes"] = -5
        return df_anom

    if anomes == "202610":
        # 1. Completude (Nulos em colunas críticas)
        df_anom.loc[0:14, "id_cliente"] = pd.NA
        df_anom.loc[15:29, "valor_fatura"] = np.nan
        df_anom.loc[30:39, "cpf"] = None
        df_anom.loc[40:49, "score_credito"] = pd.NA

        # 2. Consistência / Intervalos numéricos (negativos e outliers extremos)
        df_anom.loc[50:57, "valor_fatura"] = -999.90
        df_anom.loc[58:62, "valor_fatura"] = 99999999.99  # Outlier extremo
        df_anom.loc[63:69, "score_credito"] = 2500
        df_anom.loc[70:74, "score_credito"] = 0
        df_anom.loc[75:79, "qtd_transacoes"] = -10

        # 3. Domínio / Formato inválido
        df_anom.loc[80:87, "uf"] = "99"
        df_anom.loc[88:94, "segmento"] = "SEGMENTO_DESCONHECIDO"
        df_anom.loc[95:102, "status_pagamento"] = "CANCELADO_ERRO"
        df_anom.loc[103:110, "cpf"] = "00000000000"
        df_anom.loc[111:115, "cpf"] = "ABC12345678"

        # 4. Timeliness / Data fora da partição
        df_anom.loc[116:125, "dt_fechamento"] = "2025-01-10"
        df_anom.loc[126:130, "dt_fechamento"] = "2099-12-31"

        # 5. Anomalia de Maturação / Atraso anormal em dt_cadastro_chave_pix:
        # Remove a maioria dos cadastros dos dias 2026-10-25 e 2026-10-26 (que já deveriam
        # estar 100% maduros em 2026-10-30, pois lag > 3) e zera 2026-10-29 (lag=1 abaixo da curva).
        mask_maduros_afetados = df_anom["dt_cadastro_chave_pix"].isin(
            ["2026-10-25", "2026-10-26"]
        )
        indices_afetados = df_anom[mask_maduros_afetados].index
        if len(indices_afetados) > 2:
            df_anom.loc[indices_afetados[:-2], "dt_cadastro_chave_pix"] = None

        mask_lag1_parada = df_anom["dt_cadastro_chave_pix"] == "2026-10-29"
        df_anom.loc[mask_lag1_parada, "dt_cadastro_chave_pix"] = None

        # 6. Unicidade (Duplicatas exatas e de chave primária + pico de volumetria)
        duplicatas = df_anom.iloc[131:250].copy()
        df_anom = pd.concat([df_anom, duplicatas], ignore_index=True)

    return df_anom


def gerar_df_diario_limpo(anomesdia: str, n_linhas: int, seed: int) -> pd.DataFrame:
    rng = random.Random(seed)
    np_rng = np.random.default_rng(seed)

    ano = int(anomesdia[:4])
    mes = int(anomesdia[4:6])
    dia = int(anomesdia[6:8])
    dt_iso = dt.date(ano, mes, dia).isoformat()

    registros = []
    for i in range(1, n_linhas + 1):
        ts = dt.datetime(
            ano,
            mes,
            dia,
            rng.randint(0, 23),
            rng.randint(0, 59),
            rng.randint(0, 59),
        )
        valor = round(float(np_rng.lognormal(mean=4.5, sigma=1.1)), 2)
        valor = max(1.0, min(valor, 25000.0))

        registros.append(
            {
                "id_transacao": f"TX-{anomesdia}-{i:05d}",
                "id_cliente": int(rng.randint(10001, 50000)),
                "tipo_transacao": rng.choices(
                    TIPOS_TRANSACAO_VALIDOS,
                    weights=[0.45, 0.25, 0.15, 0.05, 0.10],
                    k=1,
                )[0],
                "canal": rng.choices(
                    CANAIS_VALIDOS, weights=[0.65, 0.20, 0.10, 0.05], k=1
                )[0],
                "valor_transacao": valor,
                "moeda": "BRL",
                "status_transacao": rng.choices(
                    STATUS_TRANSACAO_VALIDOS, weights=[0.90, 0.07, 0.03], k=1
                )[0],
                "uf_origem": rng.choice(UFS_VALIDAS),
                "dt_transacao": dt_iso,
                "ts_transacao": ts,
            }
        )

    df = pd.DataFrame(registros)
    df["id_cliente"] = df["id_cliente"].astype("Int64")
    df["valor_transacao"] = df["valor_transacao"].astype("float64")
    return df


def injetar_anomalias_diario(df: pd.DataFrame, anomesdia: str) -> pd.DataFrame:
    """
    Injeta anomalias nas partições diárias:
    - 20261001 a 20261024: histórico limpo (baseline de 24 dias)
    - 20261025: valores nulos (completude)
    - 20261026: duplicidade de chave primária (`id_transacao`)
    - 20261027: violação de domínio e valores negativos/zero
    - 20261028: queda brusca de volumetria (apenas 25 linhas)
    - 20261029: datas divergentes da partição (`dt_transacao` fora do `anomesdia`)
    - 20261030: pico de volumetria + combinação de todas as anomalias (nulos, duplicatas, negativos, outliers, domínio inválido)
    """
    df_anom = df.copy()

    if anomesdia <= "20261024":
        return df_anom

    if anomesdia == "20261025":
        df_anom.loc[0:14, "id_cliente"] = pd.NA
        df_anom.loc[15:29, "valor_transacao"] = np.nan
        df_anom.loc[30:39, "tipo_transacao"] = None
        df_anom.loc[40:49, "canal"] = None
        return df_anom

    if anomesdia == "20261026":
        duplicatas = df_anom.iloc[0:35].copy()
        df_anom = pd.concat([df_anom, duplicatas], ignore_index=True)
        return df_anom

    if anomesdia == "20261027":
        df_anom.loc[0:9, "valor_transacao"] = -350.00
        df_anom.loc[10:14, "valor_transacao"] = 0.00
        df_anom.loc[15:22, "tipo_transacao"] = "CRIPTO_NAO_SUPORTADO"
        df_anom.loc[23:29, "moeda"] = "XYZ"
        df_anom.loc[30:35, "status_transacao"] = "STATUS_DESCONHECIDO"
        df_anom.loc[36:40, "uf_origem"] = "ZZ"
        return df_anom

    if anomesdia == "20261028":
        # Queda severa de volumetria
        return df_anom.iloc[:25].copy().reset_index(drop=True)

    if anomesdia == "20261029":
        # Datas inconsistentes com a partição
        df_anom.loc[0:19, "dt_transacao"] = "2026-09-15"
        df_anom.loc[20:29, "dt_transacao"] = "2099-01-01"
        return df_anom

    if anomesdia == "20261030":
        # Combinação de anomalias + pico de volumetria
        df_anom.loc[0:14, "id_cliente"] = pd.NA
        df_anom.loc[15:29, "valor_transacao"] = np.nan
        df_anom.loc[30:39, "valor_transacao"] = -500.00
        df_anom.loc[40:44, "valor_transacao"] = 88888888.88
        df_anom.loc[45:54, "tipo_transacao"] = "INVALIDO"
        df_anom.loc[55:64, "moeda"] = "USD_ERRO"
        df_anom.loc[65:74, "uf_origem"] = "00"
        df_anom.loc[75:84, "dt_transacao"] = "2020-01-01"

        # Triplica parte dos registros gerando duplicatas e pico de volume (~900 linhas)
        extras_1 = df_anom.copy()
        extras_2 = df_anom.copy()
        df_anom = pd.concat([df_anom, extras_1, extras_2], ignore_index=True)
        return df_anom

    return df_anom


def salvar_particao(
    tabela_dir: Path, nome_particao: str, df: pd.DataFrame
) -> Path:
    part_dir = tabela_dir / nome_particao
    part_dir.mkdir(parents=True, exist_ok=True)

    # Remove arquivos parquet antigos na partição para evitar acúmulo em re-execuções
    for arq in part_dir.glob("*.parquet"):
        arq.unlink()

    caminho_arquivo = part_dir / _nome_arquivo_s3(f"{tabela_dir.name}/{nome_particao}")
    df.to_parquet(caminho_arquivo, engine="pyarrow", compression="snappy", index=False)
    return caminho_arquivo


def gerar_bases_para_data(
    dt_referencia_foto: dt.date = dt.date(2026, 10, 30),
    verbose: bool = True,
) -> None:
    """Gera as tabelas de origem simulando o estado da base na `dt_referencia_foto`."""
    rng_vol = random.Random(42)

    ORIGEM_DIR.mkdir(parents=True, exist_ok=True)
    SAIDA_DIR.mkdir(parents=True, exist_ok=True)
    for bucket in BUCKETS_MEDALLION:
        bucket_dir = SAIDA_DIR / bucket
        bucket_dir.mkdir(parents=True, exist_ok=True)
        gitkeep = bucket_dir / ".gitkeep"
        if not gitkeep.exists():
            gitkeep.touch()

    dir_mensal_limpa = ORIGEM_DIR / "tabela_particao_mensal"
    dir_mensal_anom = ORIGEM_DIR / "tabela_particao_mensal_anomalias"
    dir_diaria_limpa = ORIGEM_DIR / "tabela_particao_diaria"
    dir_diaria_anom = ORIGEM_DIR / "tabela_particao_diaria_anomalias"

    if verbose:
        print(
            f"Gerando tabelas mensais (limpa e com anomalias) para foto {dt_referencia_foto}..."
        )
    for idx, anomes in enumerate(PARTICOES_MENSAIS):
        n_linhas = rng_vol.randint(485, 515)
        df_limpo = gerar_df_mensal_limpo(
            anomes,
            n_linhas=n_linhas,
            seed=100 + idx,
            dt_referencia_foto=dt_referencia_foto,
        )
        salvar_particao(dir_mensal_limpa, f"anomes={anomes}", df_limpo)

        df_anom = injetar_anomalias_mensal(df_limpo, anomes=anomes, seed=200 + idx)
        salvar_particao(dir_mensal_anom, f"anomes={anomes}", df_anom)

    if verbose:
        print("Gerando tabelas diárias (limpa e com anomalias)...")
    for idx, anomesdia in enumerate(PARTICOES_DIARIAS):
        n_linhas = rng_vol.randint(285, 315)
        dt_part = dt.date(
            int(anomesdia[:4]), int(anomesdia[4:6]), int(anomesdia[6:8])
        )
        if dt_part > dt_referencia_foto:
            continue
        df_limpo = gerar_df_diario_limpo(anomesdia, n_linhas=n_linhas, seed=500 + idx)
        salvar_particao(dir_diaria_limpa, f"anomesdia={anomesdia}", df_limpo)

        df_anom = injetar_anomalias_diario(df_limpo, anomesdia=anomesdia)
        salvar_particao(dir_diaria_anom, f"anomesdia={anomesdia}", df_anom)


def main() -> None:
    gerar_bases_para_data(dt.date(2026, 10, 30), verbose=True)
    print("Concluído com sucesso!")


if __name__ == "__main__":
    main()
