# Relatório Executivo de Data Quality (2026-10-30)
- **Run ID:** `dq-20261030-cc143756`
- **Total de Verificações Executadas:** `1534`

## 1. Resumo por Tabela e Nível de Check

| Tabela | Nível | Total Checks | OK | Warning | Anomalia | Score (%) | Status |
|---|---|---:|---:|---:|---:|---:|---|
| `tabela_particao_diaria` | `COLUNA` | 510 | 510 | 0 | 0 | 100.0% | **SAUDAVEL** |
| `tabela_particao_diaria` | `FOTOGRAFIA_DIFF` | 30 | 30 | 0 | 0 | 100.0% | **SAUDAVEL** |
| `tabela_particao_diaria` | `PARTICAO` | 60 | 60 | 0 | 0 | 100.0% | **SAUDAVEL** |
| `tabela_particao_diaria_anomalias` | `COLUNA` | 510 | 491 | 0 | 19 | 96.3% | **CRITICO** |
| `tabela_particao_diaria_anomalias` | `FOTOGRAFIA_DIFF` | 30 | 30 | 0 | 0 | 100.0% | **SAUDAVEL** |
| `tabela_particao_diaria_anomalias` | `PARTICAO` | 60 | 56 | 0 | 4 | 93.3% | **CRITICO** |
| `tabela_particao_mensal` | `COLUNA` | 126 | 126 | 0 | 0 | 100.0% | **SAUDAVEL** |
| `tabela_particao_mensal` | `FOTOGRAFIA_DIFF` | 6 | 6 | 0 | 0 | 100.0% | **SAUDAVEL** |
| `tabela_particao_mensal` | `MATURACAO_EVENTO` | 17 | 17 | 0 | 0 | 100.0% | **SAUDAVEL** |
| `tabela_particao_mensal` | `PARTICAO` | 18 | 18 | 0 | 0 | 100.0% | **SAUDAVEL** |
| `tabela_particao_mensal_anomalias` | `COLUNA` | 126 | 104 | 0 | 22 | 82.5% | **CRITICO** |
| `tabela_particao_mensal_anomalias` | `FOTOGRAFIA_DIFF` | 6 | 6 | 0 | 0 | 100.0% | **SAUDAVEL** |
| `tabela_particao_mensal_anomalias` | `MATURACAO_EVENTO` | 17 | 14 | 0 | 3 | 82.3% | **CRITICO** |
| `tabela_particao_mensal_anomalias` | `PARTICAO` | 18 | 16 | 0 | 2 | 88.9% | **CRITICO** |

## 2. Análise de Maturação / Atraso de Datas (`dt_cadastro_chave_pix`)

| Tabela | Coluna[Data Evento] | Check (Lag) | Observado | Esperado (Curva Aprendida) | Status | Detalhes |
|---|---|---|---:|---|---|---|
| `tabela_particao_mensal` | `dt_cadastro_chave_pix` | `diagnostico_atraso_aprendido` | `3d` | `Auto-aprendido via snapshots` | **OK** | Motor aprendeu sozinho (em 4 fotos) que `dt_cadastro_chave_pix` leva 3 dia(s) para estabilizar (lag 0d=22%, lag 1d=56%, lag 2d=87%, lag 3d=109%) |
| `tabela_particao_mensal` | `dt_cadastro_chave_pix[2026-10-30]` | `maturacao_lag_0d` | `6` | `~5 casos (curva lag=0d: 22%)` | **OK** | Data 2026-10-30 (lag=0d < atraso_aprendido=3d): 6 casos vs ref madura de 22/dia (27%). Queda normal p/ lag=0d segundo aprendizado (22% / ~5 casos) |
| `tabela_particao_mensal` | `dt_cadastro_chave_pix[2026-10-29]` | `maturacao_lag_1d` | `12` | `~12 casos (curva lag=1d: 56%)` | **OK** | Data 2026-10-29 (lag=1d < atraso_aprendido=3d): 12 casos vs ref madura de 22/dia (53%). Queda normal p/ lag=1d segundo aprendizado (56% / ~12 casos) |
| `tabela_particao_mensal` | `dt_cadastro_chave_pix[2026-10-28]` | `maturacao_lag_2d` | `21` | `~20 casos (curva lag=2d: 87%)` | **OK** | Data 2026-10-28 (lag=2d < atraso_aprendido=3d): 21 casos vs ref madura de 22/dia (93%). Queda normal p/ lag=2d segundo aprendizado (87% / ~20 casos) |
| `tabela_particao_mensal` | `dt_cadastro_chave_pix[2026-10-27]` | `maturacao_lag_3d` | `26` | `>= 15 casos (ref madura=22)` | **OK** | Data madura 2026-10-27 (lag=3d >= atraso_aprendido=3d) consolidada: 26 casos (116% da ref madura de 22/dia) |
| `tabela_particao_mensal` | `dt_cadastro_chave_pix[2026-10-26]` | `maturacao_lag_4d` | `28` | `>= 15 casos (ref madura=22)` | **OK** | Data madura 2026-10-26 (lag=4d >= atraso_aprendido=3d) consolidada: 28 casos (124% da ref madura de 22/dia) |
| `tabela_particao_mensal` | `dt_cadastro_chave_pix[2026-10-25]` | `maturacao_lag_5d` | `26` | `>= 15 casos (ref madura=22)` | **OK** | Data madura 2026-10-25 (lag=5d >= atraso_aprendido=3d) consolidada: 26 casos (116% da ref madura de 22/dia) |
| `tabela_particao_mensal` | `dt_cadastro_chave_pix[2026-10-24]` | `maturacao_lag_6d` | `22` | `>= 15 casos (ref madura=22)` | **OK** | Data madura 2026-10-24 (lag=6d >= atraso_aprendido=3d) consolidada: 22 casos (98% da ref madura de 22/dia) |
| `tabela_particao_mensal` | `dt_cadastro_chave_pix[2026-10-23]` | `maturacao_lag_7d` | `23` | `>= 15 casos (ref madura=22)` | **OK** | Data madura 2026-10-23 (lag=7d >= atraso_aprendido=3d) consolidada: 23 casos (102% da ref madura de 22/dia) |
| `tabela_particao_mensal` | `dt_cadastro_chave_pix[2026-10-22]` | `maturacao_lag_8d` | `21` | `>= 15 casos (ref madura=22)` | **OK** | Data madura 2026-10-22 (lag=8d >= atraso_aprendido=3d) consolidada: 21 casos (93% da ref madura de 22/dia) |
| `tabela_particao_mensal` | `dt_abertura_conta` | `diagnostico_atraso_aprendido` | `0d` | `Auto-aprendido via snapshots` | **OK** | Motor aprendeu sozinho (em 4 fotos) que `dt_abertura_conta` leva 0 dia(s) para estabilizar (lag 0d=94%) |
| `tabela_particao_mensal` | `dt_abertura_conta[2026-10-30]` | `maturacao_lag_0d` | `16` | `>= 10 casos (ref madura=16)` | **OK** | Data madura 2026-10-30 (lag=0d >= atraso_aprendido=0d) consolidada: 16 casos (100% da ref madura de 16/dia) |
| `tabela_particao_mensal` | `dt_abertura_conta[2026-10-29]` | `maturacao_lag_1d` | `16` | `>= 10 casos (ref madura=16)` | **OK** | Data madura 2026-10-29 (lag=1d >= atraso_aprendido=0d) consolidada: 16 casos (100% da ref madura de 16/dia) |
| `tabela_particao_mensal` | `dt_abertura_conta[2026-10-28]` | `maturacao_lag_2d` | `16` | `>= 10 casos (ref madura=16)` | **OK** | Data madura 2026-10-28 (lag=2d >= atraso_aprendido=0d) consolidada: 16 casos (100% da ref madura de 16/dia) |
| `tabela_particao_mensal` | `dt_abertura_conta[2026-10-27]` | `maturacao_lag_3d` | `16` | `>= 10 casos (ref madura=16)` | **OK** | Data madura 2026-10-27 (lag=3d >= atraso_aprendido=0d) consolidada: 16 casos (100% da ref madura de 16/dia) |
| `tabela_particao_mensal` | `dt_abertura_conta[2026-10-26]` | `maturacao_lag_4d` | `16` | `>= 10 casos (ref madura=16)` | **OK** | Data madura 2026-10-26 (lag=4d >= atraso_aprendido=0d) consolidada: 16 casos (100% da ref madura de 16/dia) |
| `tabela_particao_mensal` | `dt_abertura_conta[2026-10-25]` | `maturacao_lag_5d` | `16` | `>= 10 casos (ref madura=16)` | **OK** | Data madura 2026-10-25 (lag=5d >= atraso_aprendido=0d) consolidada: 16 casos (100% da ref madura de 16/dia) |
| `tabela_particao_mensal_anomalias` | `dt_cadastro_chave_pix` | `diagnostico_atraso_aprendido` | `4d` | `Auto-aprendido via snapshots` | **OK** | Motor aprendeu sozinho (em 4 fotos) que `dt_cadastro_chave_pix` leva 4 dia(s) para estabilizar (lag 0d=13%, lag 1d=37%, lag 2d=63%, lag 3d=75%, lag 4d=96%) |
| `tabela_particao_mensal_anomalias` | `dt_cadastro_chave_pix[2026-10-30]` | `maturacao_lag_0d` | `7` | `~3 casos (curva lag=0d: 13%)` | **OK** | Data 2026-10-30 (lag=0d < atraso_aprendido=4d): 7 casos vs ref madura de 26/dia (27%). Queda normal p/ lag=0d segundo aprendizado (13% / ~3 casos) |
| `tabela_particao_mensal_anomalias` | `dt_cadastro_chave_pix[2026-10-29]` | `maturacao_lag_1d` | `2` | `~9 casos (curva lag=1d: 37%)` | **ANOMALIA** | Queda anormal em data recente 2026-10-29 (lag=1d < atraso_aprendido=4d): 2 casos (8%), ABAIXO até mesmo da curva aprendida para lag=1d (esperado ~37% / mín 4 casos) |
| `tabela_particao_mensal_anomalias` | `dt_cadastro_chave_pix[2026-10-28]` | `maturacao_lag_2d` | `25` | `~16 casos (curva lag=2d: 63%)` | **OK** | Data 2026-10-28 (lag=2d < atraso_aprendido=4d): 25 casos vs ref madura de 26/dia (98%). Queda normal p/ lag=2d segundo aprendizado (63% / ~16 casos) |
| `tabela_particao_mensal_anomalias` | `dt_cadastro_chave_pix[2026-10-27]` | `maturacao_lag_3d` | `28` | `~19 casos (curva lag=3d: 75%)` | **OK** | Data 2026-10-27 (lag=3d < atraso_aprendido=4d): 28 casos vs ref madura de 26/dia (110%). Queda normal p/ lag=3d segundo aprendizado (75% / ~19 casos) |
| `tabela_particao_mensal_anomalias` | `dt_cadastro_chave_pix[2026-10-26]` | `maturacao_lag_4d` | `10` | `>= 16 casos (ref madura=26)` | **ANOMALIA** | QUEDA REAL em data já madura (2026-10-26, lag=4d >= atraso_aprendido=4d): apenas 10 casos (39% da ref madura de 26/dia; mínimo esperado=16) |
| `tabela_particao_mensal_anomalias` | `dt_cadastro_chave_pix[2026-10-25]` | `maturacao_lag_5d` | `12` | `>= 16 casos (ref madura=26)` | **ANOMALIA** | QUEDA REAL em data já madura (2026-10-25, lag=5d >= atraso_aprendido=4d): apenas 12 casos (47% da ref madura de 26/dia; mínimo esperado=16) |
| `tabela_particao_mensal_anomalias` | `dt_cadastro_chave_pix[2026-10-24]` | `maturacao_lag_6d` | `26` | `>= 16 casos (ref madura=26)` | **OK** | Data madura 2026-10-24 (lag=6d >= atraso_aprendido=4d) consolidada: 26 casos (102% da ref madura de 26/dia) |
| `tabela_particao_mensal_anomalias` | `dt_cadastro_chave_pix[2026-10-23]` | `maturacao_lag_7d` | `27` | `>= 16 casos (ref madura=26)` | **OK** | Data madura 2026-10-23 (lag=7d >= atraso_aprendido=4d) consolidada: 27 casos (106% da ref madura de 26/dia) |
| `tabela_particao_mensal_anomalias` | `dt_cadastro_chave_pix[2026-10-22]` | `maturacao_lag_8d` | `23` | `>= 16 casos (ref madura=26)` | **OK** | Data madura 2026-10-22 (lag=8d >= atraso_aprendido=4d) consolidada: 23 casos (90% da ref madura de 26/dia) |
| `tabela_particao_mensal_anomalias` | `dt_abertura_conta` | `diagnostico_atraso_aprendido` | `0d` | `Auto-aprendido via snapshots` | **OK** | Motor aprendeu sozinho (em 4 fotos) que `dt_abertura_conta` leva 0 dia(s) para estabilizar (lag 0d=95%) |
| `tabela_particao_mensal_anomalias` | `dt_abertura_conta[2026-10-30]` | `maturacao_lag_0d` | `20` | `>= 12 casos (ref madura=20)` | **OK** | Data madura 2026-10-30 (lag=0d >= atraso_aprendido=0d) consolidada: 20 casos (100% da ref madura de 20/dia) |
| `tabela_particao_mensal_anomalias` | `dt_abertura_conta[2026-10-29]` | `maturacao_lag_1d` | `20` | `>= 12 casos (ref madura=20)` | **OK** | Data madura 2026-10-29 (lag=1d >= atraso_aprendido=0d) consolidada: 20 casos (100% da ref madura de 20/dia) |
| `tabela_particao_mensal_anomalias` | `dt_abertura_conta[2026-10-28]` | `maturacao_lag_2d` | `20` | `>= 13 casos (ref madura=20)` | **OK** | Data madura 2026-10-28 (lag=2d >= atraso_aprendido=0d) consolidada: 20 casos (100% da ref madura de 20/dia) |
| `tabela_particao_mensal_anomalias` | `dt_abertura_conta[2026-10-27]` | `maturacao_lag_3d` | `20` | `>= 13 casos (ref madura=20)` | **OK** | Data madura 2026-10-27 (lag=3d >= atraso_aprendido=0d) consolidada: 20 casos (100% da ref madura de 20/dia) |
| `tabela_particao_mensal_anomalias` | `dt_abertura_conta[2026-10-26]` | `maturacao_lag_4d` | `20` | `>= 12 casos (ref madura=20)` | **OK** | Data madura 2026-10-26 (lag=4d >= atraso_aprendido=0d) consolidada: 20 casos (100% da ref madura de 20/dia) |
| `tabela_particao_mensal_anomalias` | `dt_abertura_conta[2026-10-25]` | `maturacao_lag_5d` | `20` | `>= 12 casos (ref madura=20)` | **OK** | Data madura 2026-10-25 (lag=5d >= atraso_aprendido=0d) consolidada: 20 casos (100% da ref madura de 20/dia) |

## 3. Detalhamento de Anomalias Detectadas

| Tabela | Partição | Nível | Check | Coluna | Observado | Esperado | Detalhes |
|---|---|---|---|---|---|---|---|
| `tabela_particao_mensal_anomalias` | `anomes=202609` | `PARTICAO` | `volumetria_historica` | `*` | `90` | `[348, 645] (mediana=496)` | QUEDA BRUSCA de volumetria na partição: 90 linhas (-81.9% vs mediana histórica de 496, z=23.8) |
| `tabela_particao_mensal_anomalias` | `anomes=202610` | `PARTICAO` | `consistencia_data_particao` | `dt_fechamento` | `15` | `0 registros fora de 2026-10` | 15 registros (2.5%) com `dt_fechamento` fora da partição `anomes=202610` (esperado prefixo `2026-10`, exemplos encontrados: ['2025-01-10', '2099-12-31']) |
| `tabela_particao_mensal_anomalias` | `TODAS_PARTICOES` | `MATURACAO_EVENTO` | `maturacao_lag_1d` | `dt_cadastro_chave_pix[2026-10-29]` | `2` | `~9 casos (curva lag=1d: 37%)` | Queda anormal em data recente 2026-10-29 (lag=1d < atraso_aprendido=4d): 2 casos (8%), ABAIXO até mesmo da curva aprendida para lag=1d (esperado ~37% / mín 4 casos) |
| `tabela_particao_mensal_anomalias` | `TODAS_PARTICOES` | `MATURACAO_EVENTO` | `maturacao_lag_4d` | `dt_cadastro_chave_pix[2026-10-26]` | `10` | `>= 16 casos (ref madura=26)` | QUEDA REAL em data já madura (2026-10-26, lag=4d >= atraso_aprendido=4d): apenas 10 casos (39% da ref madura de 26/dia; mínimo esperado=16) |
| `tabela_particao_mensal_anomalias` | `TODAS_PARTICOES` | `MATURACAO_EVENTO` | `maturacao_lag_5d` | `dt_cadastro_chave_pix[2026-10-25]` | `12` | `>= 16 casos (ref madura=26)` | QUEDA REAL em data já madura (2026-10-25, lag=5d >= atraso_aprendido=4d): apenas 12 casos (47% da ref madura de 26/dia; mínimo esperado=16) |
| `tabela_particao_mensal_anomalias` | `anomes=202608` | `COLUNA` | `completude` | `cpf` | `0.0157` | `<= 0.0000` | Coluna `cpf` possui 8/508 nulos (1.57%), acima do limite permitido de 0.00% |
| `tabela_particao_mensal_anomalias` | `anomes=202608` | `COLUNA` | `completude` | `valor_fatura` | `0.0197` | `<= 0.0000` | Coluna `valor_fatura` possui 10/508 nulos (1.97%), acima do limite permitido de 0.00% |
| `tabela_particao_mensal_anomalias` | `anomes=202608` | `COLUNA` | `completude` | `status_pagamento` | `0.0098` | `<= 0.0000` | Coluna `status_pagamento` possui 5/508 nulos (0.98%), acima do limite permitido de 0.00% |
| `tabela_particao_mensal_anomalias` | `anomes=202608` | `COLUNA` | `dominio` | `uf` | `5` | `In ['BA', 'DF', 'MG', 'PR', 'RJ', 'RS', 'SC', 'SP']` | 5 valores fora do domínio permitido em `uf`. Valores inválidos encontrados: ['XX'] |
| `tabela_particao_mensal_anomalias` | `anomes=202608` | `COLUNA` | `dominio` | `status_pagamento` | `5` | `In ['ATRASADO', 'EM_ABERTO', 'PAGO']` | 5 valores fora do domínio permitido em `status_pagamento`. Valores inválidos encontrados: ['STATUS_INVALIDO'] |
| `tabela_particao_mensal_anomalias` | `anomes=202608` | `COLUNA` | `formato_regex` | `cpf` | `5` | `Regex ^\d{11}$` | 5 valores com formato inválido em `cpf` (padrão `^\d{11}$`). Exemplos: ['12345'] |
| `tabela_particao_mensal_anomalias` | `anomes=202609` | `COLUNA` | `intervalo_numerico` | `valor_fatura` | `5` | `[0.01, 100000.0]` | 5 registros em `valor_fatura` fora do intervalo [0.01, 100000.0]. Exemplos: [-1250.75] |
| `tabela_particao_mensal_anomalias` | `anomes=202609` | `COLUNA` | `intervalo_numerico` | `score_credito` | `10` | `[300, 1000]` | 10 registros em `score_credito` fora do intervalo [300, 1000]. Exemplos: [1450, -50] |
| `tabela_particao_mensal_anomalias` | `anomes=202609` | `COLUNA` | `intervalo_numerico` | `qtd_transacoes` | `4` | `[0, 1000]` | 4 registros em `qtd_transacoes` fora do intervalo [0, 1000]. Exemplos: [-5] |
| `tabela_particao_mensal_anomalias` | `anomes=202610` | `COLUNA` | `completude` | `id_cliente` | `0.0245` | `<= 0.0000` | Coluna `id_cliente` possui 15/611 nulos (2.45%), acima do limite permitido de 0.00% |
| `tabela_particao_mensal_anomalias` | `anomes=202610` | `COLUNA` | `completude` | `cpf` | `0.0164` | `<= 0.0000` | Coluna `cpf` possui 10/611 nulos (1.64%), acima do limite permitido de 0.00% |
| `tabela_particao_mensal_anomalias` | `anomes=202610` | `COLUNA` | `completude` | `score_credito` | `0.0164` | `<= 0.0000` | Coluna `score_credito` possui 10/611 nulos (1.64%), acima do limite permitido de 0.00% |
| `tabela_particao_mensal_anomalias` | `anomes=202610` | `COLUNA` | `completude` | `valor_fatura` | `0.0245` | `<= 0.0000` | Coluna `valor_fatura` possui 15/611 nulos (2.45%), acima do limite permitido de 0.00% |
| `tabela_particao_mensal_anomalias` | `anomes=202610` | `COLUNA` | `unicidade_chave_primaria` | `id_fatura` | `119` | `0 duplicatas` | 119 registros duplicados na chave primária (id_fatura) |
| `tabela_particao_mensal_anomalias` | `anomes=202610` | `COLUNA` | `unicidade_linha_completa` | `*` | `119` | `0 duplicatas` | 119 linhas inteiramente duplicadas na partição |
| `tabela_particao_mensal_anomalias` | `anomes=202610` | `COLUNA` | `dominio` | `uf` | `8` | `In ['BA', 'DF', 'MG', 'PR', 'RJ', 'RS', 'SC', 'SP']` | 8 valores fora do domínio permitido em `uf`. Valores inválidos encontrados: ['99'] |
| `tabela_particao_mensal_anomalias` | `anomes=202610` | `COLUNA` | `dominio` | `segmento` | `7` | `In ['EMPRESAS', 'PERSONNALITE', 'UNICLASS', 'VAREJO']` | 7 valores fora do domínio permitido em `segmento`. Valores inválidos encontrados: ['SEGMENTO_DESCONHECIDO'] |
| `tabela_particao_mensal_anomalias` | `anomes=202610` | `COLUNA` | `dominio` | `status_pagamento` | `8` | `In ['ATRASADO', 'EM_ABERTO', 'PAGO']` | 8 valores fora do domínio permitido em `status_pagamento`. Valores inválidos encontrados: ['CANCELADO_ERRO'] |
| `tabela_particao_mensal_anomalias` | `anomes=202610` | `COLUNA` | `intervalo_numerico` | `valor_fatura` | `13` | `[0.01, 100000.0]` | 13 registros em `valor_fatura` fora do intervalo [0.01, 100000.0]. Exemplos: [-999.9, 99999999.99] |
| `tabela_particao_mensal_anomalias` | `anomes=202610` | `COLUNA` | `intervalo_numerico` | `score_credito` | `12` | `[300, 1000]` | 12 registros em `score_credito` fora do intervalo [300, 1000]. Exemplos: [2500, 0] |
| `tabela_particao_mensal_anomalias` | `anomes=202610` | `COLUNA` | `intervalo_numerico` | `qtd_transacoes` | `5` | `[0, 1000]` | 5 registros em `qtd_transacoes` fora do intervalo [0, 1000]. Exemplos: [-10] |
| `tabela_particao_mensal_anomalias` | `anomes=202610` | `COLUNA` | `formato_regex` | `cpf` | `13` | `Regex ^\d{11}$` | 13 valores com formato inválido em `cpf` (padrão `^\d{11}$`). Exemplos: ['00000000000', 'ABC12345678'] |
| `tabela_particao_diaria_anomalias` | `anomesdia=20261028` | `PARTICAO` | `volumetria_historica` | `*` | `25` | `[211, 392] (mediana=302)` | QUEDA BRUSCA de volumetria na partição: 25 linhas (-91.7% vs mediana histórica de 302, z=19.6) |
| `tabela_particao_diaria_anomalias` | `anomesdia=20261030` | `PARTICAO` | `volumetria_historica` | `*` | `879` | `[211, 392] (mediana=302)` | PICO ANÔMALO de volumetria na partição: 879 linhas (+191.5% vs mediana histórica de 302, z=41.0) |
| `tabela_particao_diaria_anomalias` | `anomesdia=20261029` | `PARTICAO` | `consistencia_data_particao` | `dt_transacao` | `30` | `0 registros fora de 2026-10-29` | 30 registros (9.9%) com `dt_transacao` fora da partição `anomesdia=20261029` (esperado prefixo `2026-10-29`, exemplos encontrados: ['2026-09-15', '2099-01-01']) |
| `tabela_particao_diaria_anomalias` | `anomesdia=20261030` | `PARTICAO` | `consistencia_data_particao` | `dt_transacao` | `30` | `0 registros fora de 2026-10-30` | 30 registros (3.4%) com `dt_transacao` fora da partição `anomesdia=20261030` (esperado prefixo `2026-10-30`, exemplos encontrados: ['2020-01-01']) |
| `tabela_particao_diaria_anomalias` | `anomesdia=20261025` | `COLUNA` | `completude` | `id_cliente` | `0.0497` | `<= 0.0000` | Coluna `id_cliente` possui 15/302 nulos (4.97%), acima do limite permitido de 0.00% |
| `tabela_particao_diaria_anomalias` | `anomesdia=20261025` | `COLUNA` | `completude` | `tipo_transacao` | `0.0331` | `<= 0.0000` | Coluna `tipo_transacao` possui 10/302 nulos (3.31%), acima do limite permitido de 0.00% |
| `tabela_particao_diaria_anomalias` | `anomesdia=20261025` | `COLUNA` | `completude` | `canal` | `0.0331` | `<= 0.0000` | Coluna `canal` possui 10/302 nulos (3.31%), acima do limite permitido de 0.00% |
| `tabela_particao_diaria_anomalias` | `anomesdia=20261025` | `COLUNA` | `completude` | `valor_transacao` | `0.0497` | `<= 0.0000` | Coluna `valor_transacao` possui 15/302 nulos (4.97%), acima do limite permitido de 0.00% |
| `tabela_particao_diaria_anomalias` | `anomesdia=20261026` | `COLUNA` | `unicidade_chave_primaria` | `id_transacao` | `35` | `0 duplicatas` | 35 registros duplicados na chave primária (id_transacao) |
| `tabela_particao_diaria_anomalias` | `anomesdia=20261026` | `COLUNA` | `unicidade_linha_completa` | `*` | `35` | `0 duplicatas` | 35 linhas inteiramente duplicadas na partição |
| `tabela_particao_diaria_anomalias` | `anomesdia=20261027` | `COLUNA` | `dominio` | `tipo_transacao` | `8` | `In ['BOLETO', 'CARTAO_CREDITO', 'CARTAO_DEBITO', 'PIX', 'TED']` | 8 valores fora do domínio permitido em `tipo_transacao`. Valores inválidos encontrados: ['CRIPTO_NAO_SUPORTADO'] |
| `tabela_particao_diaria_anomalias` | `anomesdia=20261027` | `COLUNA` | `dominio` | `moeda` | `7` | `In ['BRL']` | 7 valores fora do domínio permitido em `moeda`. Valores inválidos encontrados: ['XYZ'] |
| `tabela_particao_diaria_anomalias` | `anomesdia=20261027` | `COLUNA` | `dominio` | `status_transacao` | `6` | `In ['APROVADA', 'ESTORNADA', 'RECUSADA']` | 6 valores fora do domínio permitido em `status_transacao`. Valores inválidos encontrados: ['STATUS_DESCONHECIDO'] |
| `tabela_particao_diaria_anomalias` | `anomesdia=20261027` | `COLUNA` | `dominio` | `uf_origem` | `5` | `In ['BA', 'DF', 'MG', 'PR', 'RJ', 'RS', 'SC', 'SP']` | 5 valores fora do domínio permitido em `uf_origem`. Valores inválidos encontrados: ['ZZ'] |
| `tabela_particao_diaria_anomalias` | `anomesdia=20261027` | `COLUNA` | `intervalo_numerico` | `valor_transacao` | `15` | `[0.01, 100000.0]` | 15 registros em `valor_transacao` fora do intervalo [0.01, 100000.0]. Exemplos: [-350.0, 0.0] |
| `tabela_particao_diaria_anomalias` | `anomesdia=20261030` | `COLUNA` | `completude` | `id_cliente` | `0.0512` | `<= 0.0000` | Coluna `id_cliente` possui 45/879 nulos (5.12%), acima do limite permitido de 0.00% |
| `tabela_particao_diaria_anomalias` | `anomesdia=20261030` | `COLUNA` | `completude` | `valor_transacao` | `0.0512` | `<= 0.0000` | Coluna `valor_transacao` possui 45/879 nulos (5.12%), acima do limite permitido de 0.00% |
| `tabela_particao_diaria_anomalias` | `anomesdia=20261030` | `COLUNA` | `unicidade_chave_primaria` | `id_transacao` | `586` | `0 duplicatas` | 586 registros duplicados na chave primária (id_transacao) |
| `tabela_particao_diaria_anomalias` | `anomesdia=20261030` | `COLUNA` | `unicidade_linha_completa` | `*` | `586` | `0 duplicatas` | 586 linhas inteiramente duplicadas na partição |
| `tabela_particao_diaria_anomalias` | `anomesdia=20261030` | `COLUNA` | `dominio` | `tipo_transacao` | `30` | `In ['BOLETO', 'CARTAO_CREDITO', 'CARTAO_DEBITO', 'PIX', 'TED']` | 30 valores fora do domínio permitido em `tipo_transacao`. Valores inválidos encontrados: ['INVALIDO'] |
| `tabela_particao_diaria_anomalias` | `anomesdia=20261030` | `COLUNA` | `dominio` | `moeda` | `30` | `In ['BRL']` | 30 valores fora do domínio permitido em `moeda`. Valores inválidos encontrados: ['USD_ERRO'] |
| `tabela_particao_diaria_anomalias` | `anomesdia=20261030` | `COLUNA` | `dominio` | `uf_origem` | `30` | `In ['BA', 'DF', 'MG', 'PR', 'RJ', 'RS', 'SC', 'SP']` | 30 valores fora do domínio permitido em `uf_origem`. Valores inválidos encontrados: ['00'] |
| `tabela_particao_diaria_anomalias` | `anomesdia=20261030` | `COLUNA` | `intervalo_numerico` | `valor_transacao` | `45` | `[0.01, 100000.0]` | 45 registros em `valor_transacao` fora do intervalo [0.01, 100000.0]. Exemplos: [-500.0, 88888888.88] |
