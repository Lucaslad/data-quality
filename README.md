# data-quality
Motor de Data Quality projetado para ser executado no **AWS Glue**.

## Diretrizes do Projeto

- **Fase Atual (Execução Local):** Inicialmente, todo o desenvolvimento, lógicas e regras de validação serão executados e testados localmente.
- **Fase Futura (Migração AWS):** Após a validação completa das lógicas e garantias de funcionamento local, o código será migrado e adaptado para rodar no AWS Glue.

## Estrutura de Pastas de Dados (`dados/`)

Para simular localmente o ambiente de Data Lake na AWS (S3 / Glue Data Catalog / Athena), os arquivos estão organizados dentro do diretório `dados/`, divididos entre **origem** e **saída**:

```text
dados/
├── origem/
│   ├── tabela_particao_diaria/
│   │   └── anomesdia=YYYYMMDD/
│   ├── tabela_particao_diaria_anomalias/
│   │   └── anomesdia=YYYYMMDD/
│   ├── tabela_particao_mensal/
│   │   └── anomes=YYYYMM/
│   └── tabela_particao_mensal_anomalias/
│       └── anomes=YYYYMM/
└── saida/
    ├── bronze/
    ├── silver/
    └── gold/
```

### 1. Dados de Origem (`dados/origem/`)
Armazena as tabelas de entrada (em formato **Parquet** com compressão **Snappy** e particionamento padrão Hive) que serão inspecionadas e validadas pelo motor de Data Quality. Os dados fictícios são gerados pelo script `scripts/gerar_dados_ficticios.py`:

- **`tabela_particao_mensal`**: Base limpa de faturas de clientes, particionada mensalmente (`anomes=202605` a `anomes=202610`).
- **`tabela_particao_mensal_anomalias`**: Mesma estrutura da tabela mensal, porém com anomalias de qualidade injetadas nas partições `202608` a `202610` (valores nulos, erros de domínio/formato, queda e pico de volumetria, outliers/valores negativos, datas fora da partição e duplicatas).
- **`tabela_particao_diaria`**: Base limpa de transações financeiras, particionada diariamente (`anomesdia=20261001` a `anomesdia=20261030`).
- **`tabela_particao_diaria_anomalias`**: Mesma estrutura da tabela diária, porém com anomalias de qualidade injetadas nas partições `20261025` a `20261030`.

### 2. Dados de Saída (`dados/saida/`)
Simula os buckets/camadas de destino no S3 (arquitetura **Medallion**) onde o motor de Data Quality grava os artefatos gerados durante e após a execução das validações:

- **`bronze/`**: Fotografias brutas (*snapshots*) tiradas a cada execução:
  - `snapshots_particoes.parquet`: volumetria e contagem de preenchidos/nulos de todas as partições (atuais e passadas) em cada foto.
  - `snapshots_eventos_data.parquet`: distribuição de registros por data de evento interna (ex.: `dt_cadastro_chave_pix`) e seu `lag_dias` em relação à data da fotografia.
- **`silver/`**: Dados de qualidade padronizados e aprendizado histórico:
  - `curva_maturacao_aprendida.parquet`: curva histórica de atraso/maturação aprendida por `lag_dias` a partir das fotografias anteriores.
  - `resultados_checks_detalhados.parquet`: histórico detalhado com o resultado individual de todas as regras aplicadas.
- **`gold/`**: Camada analítica consolidada:
  - `resumo_qualidade_tabelas.parquet`: score de qualidade (`0%` a `100%`) e totais por tabela e nível de check.
  - `alertas_anomalias.parquet`: tabela filtrada apenas com as anomalias e desvios ativos.
  - `relatorio_ultima_execucao.md`: relatório executivo legível em Markdown.

## Arquitetura do Motor e Configuração YAML

O motor é configurado pelo arquivo `config/dq_config.yaml`, onde é possível habilitar/desabilitar cada base, coluna e regra de validação em 4 níveis:

1. **Partição (`PARTICAO`)**: Volumetria histórica robusta (Mediana + MAD) e consistência entre colunas de data e a safra da partição (`anomes` / `anomesdia`).
2. **Evolução entre Fotografias (`FOTOGRAFIA_DIFF`)**: Compara a fotografia atual de todas as partições com a fotografia anterior para auditar atualizações em partições passadas (ex.: novos cadastros em `dt_cadastro_chave_pix`) e alertar regressões ou perdas de registros.
3. **Maturação / Atraso de Datas (`MATURACAO_EVENTO`)**: Aprende com as fotografias históricas o atraso natural de reflexão de eventos (ex.: `dt_cadastro_chave_pix` que demora até 3 dias para refletir na base), diferenciando quedas normais nos últimos dias (`lag < 3`) de quedas anormais ou falhas em datas que já deveriam estar maduras (`lag >= 3`).
4. **Colunas (`COLUNA`)**: Completude (limite de nulos), unicidade (chave primária e linha completa), domínio categórico, intervalo numérico (`[min, max]`) e formato via Regex.

### Como Executar

```powershell
# Executar simulando fotografias históricas (D-4 a D-1) para aprendizado da curva de atraso e avaliando D:
.\.venv\Scripts\python.exe scripts\executar_dq.py --simular-historico-fotos

# Executar apenas na data de referência padrão definida no YAML:
.\.venv\Scripts\python.exe scripts\executar_dq.py
```


