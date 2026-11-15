"""
Utilitários estatísticos, matemáticos e validadores semânticos centralizados do motor de Data Quality.

Reúne:
1. Estatísticas robustas baseadas em Mediana e MAD (Median Absolute Deviation).
2. Escalonamento dinâmico de passo de arredondamento por ordem de grandeza (`log10`).
3. Métricas de *Distribution Drift* categórico: PSI (*Population Stability Index*) e
   Divergência de Jensen-Shannon (JSD).
4. Validação matemática vetorizada de dígitos verificadores (Módulo 11 para CPF e CNPJ).
"""

from __future__ import annotations

from collections.abc import Sequence

import numpy as np
import pandas as pd


def mediana_e_sigma_mad(
    valores: Sequence[float | int] | np.ndarray,
    sigma_min_rel: float = 0.0,
    sigma_min_abs: float = 0.0,
) -> tuple[float, float]:
    """
    Calcula a mediana e o desvio padrão robusto baseado no MAD (Median Absolute Deviation).
    Permite aplicar pisos mínimos relativos à mediana (`sigma_min_rel`) e absolutos (`sigma_min_abs`)
    para evitar divisão por zero quando `MAD == 0`.
    """
    arr = np.asarray(valores, dtype=float)
    if arr.size == 0:
        return 0.0, 0.0
    med = float(np.median(arr))
    mad = float(np.median(np.abs(arr - med)))
    sigma_rob = mad * 1.4826
    if sigma_min_rel > 0.0 or sigma_min_abs > 0.0:
        sigma_rob = max(sigma_rob, abs(med) * sigma_min_rel, sigma_min_abs)
    return med, sigma_rob


def calcular_estatisticas_robustas_volume(
    valores: Sequence[float | int] | np.ndarray,
) -> tuple[float, float]:
    """Retorna `(mediana, sigma_robusto_mad)` para séries de volumetria de partição."""
    if len(valores) == 0:
        return 0.0, 1.0
    return mediana_e_sigma_mad(valores, sigma_min_rel=0.03, sigma_min_abs=1.0)


def calcular_passo_arredondamento_dinamico(valor_ref: float) -> tuple[float, int]:
    """
    Calcula o passo de arredondamento e o número de casas decimais dinamicamente
    conforme a ordem de grandeza (`log10`) do valor de referência:
    - Para valores `>= 1000.0`, mantém passo `1000.0` e `2` casas decimais.
    - Para valores `< 1000.0` (incluindo decimais em `[0.0, 1.0]` como taxas, alíquotas e probabilidades),
      escala o passo em potências de 10 (`10 ** (ordem - 1)`).
    """
    val_abs = abs(float(valor_ref))
    if val_abs == 0.0:
        return 0.01, 2
    if val_abs >= 1000.0:
        return 1000.0, 2

    ordem = int(np.floor(np.log10(val_abs)))
    passo = float(10.0 ** (ordem - 1))
    casas = max(2, int(-ordem + 2))
    return passo, casas


def calcular_psi(
    dist_esperada: dict[str, float],
    dist_observada: dict[str, float],
    epsilon: float = 0.001,
) -> float:
    """
    Calcula o PSI (*Population Stability Index*) entre a distribuição esperada (baseline GOLD)
    e a distribuição observada na partição atual.
    Valores de referência clássicos:
      - PSI < 0.10: distribuição estável
      - 0.10 <= PSI < 0.25: mudança leve
      - 0.25 <= PSI < 0.50: mudança moderada (WARNING)
      - PSI >= 0.50: mudança severa de mix (ANOMALIA)
    """
    if not dist_esperada or not dist_observada:
        return 0.0

    categorias = sorted(set(dist_esperada.keys()) | set(dist_observada.keys()))
    soma_esp = sum(max(0.0, float(dist_esperada.get(c, 0.0))) for c in categorias)
    soma_obs = sum(max(0.0, float(dist_observada.get(c, 0.0))) for c in categorias)
    if soma_esp <= 0.0 or soma_obs <= 0.0:
        return 0.0

    psi_total = 0.0
    for cat in categorias:
        p_esp = max(float(dist_esperada.get(cat, 0.0)) / soma_esp, epsilon)
        p_obs = max(float(dist_observada.get(cat, 0.0)) / soma_obs, epsilon)
        psi_total += (p_obs - p_esp) * float(np.log(p_obs / p_esp))

    return round(max(0.0, float(psi_total)), 6)


def calcular_divergencia_js(
    dist_esperada: dict[str, float],
    dist_observada: dict[str, float],
    epsilon: float = 1e-6,
) -> float:
    """Calcula a Divergência de Jensen-Shannon (simétrica e limitada em [0, ln(2)]) entre duas distribuições."""
    if not dist_esperada or not dist_observada:
        return 0.0

    categorias = sorted(set(dist_esperada.keys()) | set(dist_observada.keys()))
    soma_p = sum(max(0.0, float(dist_esperada.get(c, 0.0))) for c in categorias)
    soma_q = sum(max(0.0, float(dist_observada.get(c, 0.0))) for c in categorias)
    if soma_p <= 0.0 or soma_q <= 0.0:
        return 0.0

    p = np.array([max(float(dist_esperada.get(c, 0.0)) / soma_p, epsilon) for c in categorias])
    q = np.array([max(float(dist_observada.get(c, 0.0)) / soma_q, epsilon) for c in categorias])
    p = p / p.sum()
    q = q / q.sum()
    m = 0.5 * (p + q)

    kl_pm = float(np.sum(p * np.log(p / m)))
    kl_qm = float(np.sum(q * np.log(q / m)))
    return round(max(0.0, 0.5 * (kl_pm + kl_qm)), 6)


def gerar_dv_cpf(nove_digitos: str) -> str:
    """Calcula os 2 dígitos verificadores (Módulo 11) para os 9 primeiros dígitos de um CPF."""
    soma1 = sum(int(d) * peso for d, peso in zip(nove_digitos[:9], range(10, 1, -1)))
    r1 = (soma1 * 10) % 11
    d1 = 0 if r1 == 10 else r1

    dez_digitos = nove_digitos[:9] + str(d1)
    soma2 = sum(int(d) * peso for d, peso in zip(dez_digitos, range(11, 1, -1)))
    r2 = (soma2 * 10) % 11
    d2 = 0 if r2 == 10 else r2
    return f"{d1}{d2}"


def gerar_dv_cnpj(doze_digitos: str) -> str:
    """Calcula os 2 dígitos verificadores (Módulo 11) para os 12 primeiros dígitos de um CNPJ."""
    pesos1 = [5, 4, 3, 2, 9, 8, 7, 6, 5, 4, 3, 2]
    pesos2 = [6, 5, 4, 3, 2, 9, 8, 7, 6, 5, 4, 3, 2]

    soma1 = sum(int(d) * p for d, p in zip(doze_digitos[:12], pesos1))
    r1 = soma1 % 11
    d1 = 0 if r1 < 2 else 11 - r1

    treze_digitos = doze_digitos[:12] + str(d1)
    soma2 = sum(int(d) * p for d, p in zip(treze_digitos, pesos2))
    r2 = soma2 % 11
    d2 = 0 if r2 < 2 else 11 - r2
    return f"{d1}{d2}"


def detectar_caracteres_repetidos_serie(serie_str: pd.Series) -> pd.Series:
    """
    Detecta de forma 100% vetorizada strings vazias, de 1 caractere ou formadas por um único
    caractere repetido (ex.: '00000000000', '11111111111', 'AAAA').
    Utiliza a identidade algébrica `s[1:] == s[:-1]`, sem backreferences regex (totalmente
    compatível com motores C/PyArrow RE2 no Pandas 3+).
    """
    if serie_str.empty:
        return pd.Series(dtype=bool, index=serie_str.index)
    s = serie_str.astype(str)
    lens = s.str.len()
    return (lens <= 1) | (s.str.slice(1) == s.str.slice(0, -1))


def validar_cpf_str(val: str) -> bool:
    """Valida um CPF individual (11 dígitos numéricos não repetidos + Módulo 11)."""
    s = str(val).strip()
    if len(s) != 11 or not s.isdigit() or len(set(s)) <= 1:
        return False
    return s[9:11] == gerar_dv_cpf(s[:9])


def validar_cnpj_str(val: str) -> bool:
    """Valida um CNPJ individual (14 dígitos numéricos não repetidos + Módulo 11)."""
    s = str(val).strip()
    if len(s) != 14 or not s.isdigit() or len(set(s)) <= 1:
        return False
    return s[12:14] == gerar_dv_cnpj(s[:12])


def validar_cpf_serie(serie_str: pd.Series) -> pd.Series:
    """
    Valida de forma vetorizada em NumPy/Pandas se uma Series de strings contém CPFs
    válidos pelo algoritmo de Módulo 11.
    Retorna Series booleana com mesmo índice de `serie_str`.
    """
    if serie_str.empty:
        return pd.Series(dtype=bool, index=serie_str.index)

    s_clean = serie_str.astype(str).str.strip()
    mask_formato = s_clean.str.match(r"^\d{11}$", na=False) & (
        ~detectar_caracteres_repetidos_serie(s_clean)
    )
    resultado = pd.Series(False, index=serie_str.index, dtype=bool)
    if not mask_formato.any():
        return resultado

    validos_fmt = s_clean[mask_formato]
    # Converte os 11 caracteres numéricos em matriz (N, 11) de inteiros via view ASCII
    arr_bytes = validos_fmt.to_numpy(dtype="S11").view(np.uint8).reshape(-1, 11) - ord("0")

    pesos1 = np.arange(10, 1, -1, dtype=np.int32)
    soma1 = arr_bytes[:, :9].dot(pesos1)
    r1 = (soma1 * 10) % 11
    d1 = np.where(r1 == 10, 0, r1)

    pesos2 = np.arange(11, 1, -1, dtype=np.int32)
    soma2 = arr_bytes[:, :10].dot(pesos2)
    r2 = (soma2 * 10) % 11
    d2 = np.where(r2 == 10, 0, r2)

    ok_dv = (arr_bytes[:, 9] == d1) & (arr_bytes[:, 10] == d2)
    resultado.loc[validos_fmt.index] = ok_dv
    return resultado


def validar_cnpj_serie(serie_str: pd.Series) -> pd.Series:
    """
    Valida de forma vetorizada em NumPy/Pandas se uma Series de strings contém CNPJs
    válidos pelo algoritmo de Módulo 11.
    Retorna Series booleana com mesmo índice de `serie_str`.
    """
    if serie_str.empty:
        return pd.Series(dtype=bool, index=serie_str.index)

    s_clean = serie_str.astype(str).str.strip()
    mask_formato = s_clean.str.match(r"^\d{14}$", na=False) & (
        ~detectar_caracteres_repetidos_serie(s_clean)
    )
    resultado = pd.Series(False, index=serie_str.index, dtype=bool)
    if not mask_formato.any():
        return resultado

    validos_fmt = s_clean[mask_formato]
    arr_bytes = validos_fmt.to_numpy(dtype="S14").view(np.uint8).reshape(-1, 14) - ord("0")

    pesos1 = np.array([5, 4, 3, 2, 9, 8, 7, 6, 5, 4, 3, 2], dtype=np.int32)
    soma1 = arr_bytes[:, :12].dot(pesos1)
    r1 = soma1 % 11
    d1 = np.where(r1 < 2, 0, 11 - r1)

    pesos2 = np.array([6, 5, 4, 3, 2, 9, 8, 7, 6, 5, 4, 3, 2], dtype=np.int32)
    soma2 = arr_bytes[:, :13].dot(pesos2)
    r2 = soma2 % 11
    d2 = np.where(r2 < 2, 0, 11 - r2)

    ok_dv = (arr_bytes[:, 12] == d1) & (arr_bytes[:, 13] == d2)
    resultado.loc[validos_fmt.index] = ok_dv
    return resultado
