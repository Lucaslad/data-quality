# data-quality
Motor de Data Quality com **Auto-Aprendizado de Regras** e **Roteamento Medallion por Partição**, projetado para ser executado no **AWS Glue**.

## Diretrizes do Projeto

- **Fase Atual (Execução Local):** Inicialmente, todo o desenvolvimento, lógicas de aprendizado e roteamento de partições são executados e testados localmente.
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
    ├── gold/
    │   └── <tabela>/<particao>/*.snappy.parquet
    ├── silver/
    │   └── <tabela>/<particao>/*.snappy.parquet
    ├── bronze/
    │   └── <tabela>/<particao>/*.snappy.parquet
    └── _metadados_dq/
        ├── snapshots_particoes.parquet
        ├── snapshots_eventos_data.parquet
        ├── curva_maturacao_aprendida.parquet
        ├── regras_aprendidas.parquet
        ├── classificacao_particoes.parquet
        ├── resultados_checks_detalhados.parquet
        ├── resumo_qualidade_tabelas.parquet
        ├── alertas_anomalias.parquet
        ├── catalogo_particoes_registradas.parquet
        ├── relatorio_ultima_execucao.md
        └── particionado/
            └── <artefato>/tabela=<nome>/dt_referencia=<YYYY-MM-DD>/part-0.snappy.parquet
```

### 1. Dados de Origem (`dados/origem/`)
Armazena as tabelas de entrada (em formato **Parquet** com compressão **Snappy** e particionamento padrão Hive) que serão inspecionadas, validadas e roteadas pelo motor de Data Quality. Os dados fictícios são gerados pelo script `scripts/gerar_dados_ficticios.py`:

- **`tabela_particao_mensal`**: Base limpa de faturas de clientes, particionada mensalmente (`anomes=202605` a `anomes=202610`).
- **`tabela_particao_mensal_anomalias`**: Mesma estrutura da tabela mensal, porém com anomalias de qualidade injetadas nas partições `202608` a `202610`.
- **`tabela_particao_diaria`**: Base limpa de transações financeiras, particionada diariamente (`anomesdia=20261001` a `anomesdia=20261030`).
- **`tabela_particao_diaria_anomalias`**: Mesma estrutura da tabela diária, porém com anomalias de qualidade injetadas nas partições `20261025` a `20261030`.

### 2. Dados de Saída (`dados/saida/`) — Roteamento Exclusivo por Partição
Cada partição avaliada em `dados/origem/` é copiada **exclusivamente** para a pasta correspondente à sua classificação de qualidade:

- **`gold/<tabela>/<particao>/`**: Partições **100% saudáveis** (`0 anomalias` e `0 warnings`), prontas para consumo analítico e de negócio.
- **`silver/<tabela>/<particao>/`**: Partições **em atenção / falha leve**, cuja estrutura principal (chave primária sem nulos ou duplicatas, schema obrigatório, volumetria histórica, evolução entre fotos e curva de maturação) está íntegra, mas que possuem *warnings* ou desvios restritos a colunas não-chave (ex.: `anomes=202608` na mensal; `anomesdia=20261025`, `20261027` e `20261029` na diária).
- **`bronze/<tabela>/<particao>/`**: Partições em **quarentena / falha crítica**, reprovadas por quebra estrutural (chave primária duplicada ou nula, coluna obrigatória ausente/incompatível, queda brusca ou pico anômalo de volumetria, perda de registros entre fotografias, falha na curva de maturação ou excesso de anomalias simultâneas — ex.: `anomes=202609` e `202610` na mensal; `anomesdia=20261026`, `20261028` e `20261030` na diária).
- **`_metadados_dq/`**: Artefatos internos de controle, aprendizado e observabilidade do motor (salvos tanto consolidados quanto particionados em `_metadados_dq/particionado/` para evitar *race conditions* em jobs concorrentes no AWS Glue):
  - `snapshots_particoes.parquet` e `snapshots_eventos_data.parquet`: fotografias históricas para aprendizado.
  - `regras_aprendidas.parquet` e `curva_maturacao_aprendida.parquet`: todas as regras, schemas, faixas, domínios, dígitos verificadores, relações entre colunas e curvas de atraso aprendidos automaticamente pelo motor.
  - `classificacao_particoes.parquet` e `catalogo_particoes_registradas.parquet`: registro auditável do destino (`GOLD`, `SILVER` ou `BRONZE`), motivo de classificação e manifesto de partições registradas no Glue Data Catalog / Apache Iceberg.
  - `resultados_checks_detalhados.parquet`, `resumo_qualidade_tabelas.parquet`, `alertas_anomalias.parquet` e `relatorio_ultima_execucao.md`.

### Como Executar

```powershell
# Executar simulando fotografias históricas (D-4 a D-1) para aprendizado e avaliando/roteando as partições em D:
.\.venv\Scripts\python.exe scripts\executar_dq.py --simular-historico-fotos

# Executar apenas na data de referência padrão definida no YAML (leitura incremental via Cache GOLD):
.\.venv\Scripts\python.exe scripts\executar_dq.py

# Executar em modo Streaming (libera DataFrames da memória RAM imediatamente após extrair o perfil):
.\.venv\Scripts\python.exe scripts\executar_dq.py --streaming

# Executar a suíte de testes automatizados:
.\.venv\Scripts\python.exe -m unittest discover -s tests -v
```
