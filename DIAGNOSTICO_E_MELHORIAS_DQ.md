# Diagnóstico Técnico e Plano de Melhorias — Motor de Data Quality

Data da auditoria: `2026-11-15`  
Escopo: Análise completa do código-fonte (`src/`, `scripts/`, `tests/`, `config/dq_config.yaml`) e dos artefatos gerados na última execução (`dados/saida/_metadados_dq/relatorio_ultima_execucao.md`).

---

## 1. Visão Geral: O Motor Está Funcionando Como Deveria?

No fluxo principal de execução local, **a arquitetura central está sólida e cumprindo o roteamento Medallion esperado**:
- Todas as **72 partições** avaliadas nas 4 tabelas (`tabela_particao_mensal`, `tabela_particao_mensal_anomalias`, `tabela_particao_diaria` e `tabela_particao_diaria_anomalias`) foram classificadas e roteadas exatamente para as camadas `GOLD`, `SILVER` e `BRONZE` previstas no `README.md`.
- O motor já conta com excelentes mecanismos de engenharia e estatística:
  - Auto-descoberta de chaves primárias simples e compostas.
  - Contrato de Schema (*Schema Drift*) diferenciando colunas ausentes/incompatíveis (`ANOMALIA` $\rightarrow$ `BRONZE`) de colunas extras (`WARNING` $\rightarrow$ `SILVER`).
  - Volumetria histórica robusta baseada em Mediana e MAD (*Median Absolute Deviation*), com detecção automática de sazonalidade semanal (Dia Útil vs. Fim de Semana) e duplo estágio (`WARNING` / `ANOMALIA`).
  - Curva de maturação de eventos aprendida automaticamente por *lag* (`dt_cadastro_chave_pix` identificando estabilização em 3 dias).
  - Validação vetorizada de dígito verificador Módulo 11 (CPF e CNPJ), *Distribution Drift* categórico via PSI/JSD e consistência relacional entre colunas (*Cross-Column*).

Contudo, uma inspeção minuciosa linha a linha revelou **6 bugs e inconsistências técnicas** que comprometem a exatidão das métricas reportadas no modo *Streaming* / `CACHE_GOLD`, permitem vazamento temporal (*Data Leakage*) em cenários específicos e impedem a execução real em URIs `s3://` no AWS Glue.

---

## 2. Problemas e Inconsistências Encontrados (Bugs Atuais)

### 2.1. Contagem Incorreta de Violações em `intervalo_numerico` e `formato_regex` no Modo Streaming / Cache GOLD
- **Arquivos afetados:**
  - `src/snapshot_manager.py` (linhas 348–352 e 373–403)
  - `src/checks_colunas.py` (linhas 387–400 e 446–491)
- **Causa raiz:**
  1. Em `SnapshotManager.construir_perfil_particao`, o perfil estatístico salva apenas até **6 valores únicos extremos** (`vals_uniq_ord[:3]` e `vals_uniq_ord[-3:]`) no campo `amostras_numericas`.
  2. Quando o check de `intervalo_numerico` roda via perfil pré-agregado (`_executar_checks_colunas_via_cache`, utilizado tanto no `CACHE_GOLD` quanto no modo `--streaming`), ele calcula `qtd_fora_num = len(exemplos_fora)` sobre essa amostra de no máximo 6 valores únicos, em vez de contar o total real de linhas fora do intervalo.
  3. Além disso, `_executar_checks_colunas_via_cache` **ignora o parâmetro `tolerancia_warning_pct`** nos checks `intervalo_numerico` e `formato_regex` (que existe e funciona na execução em memória nas linhas 949 e 1013 de `src/checks_colunas.py`).
  4. Em `formato_regex` via cache (`src/checks_colunas.py`, linhas 450 e 476), a soma `qtd_inv_dig + qtd_rep` pode contar em duplicidade registros que falham simultaneamente no tamanho/dígitos e na regra de caracteres repetidos (ex.: strings de 1 caractere ou `"AAAA"`).
- **Evidência no relatório (`relatorio_ultima_execucao.md`):**
  - Na partição `anomes=202609` de `tabela_particao_mensal_anomalias`, foram injetados **10 registros** fora do intervalo em `score_credito` (`5` com `1450` e `5` com `-50`), **5 registros** em `valor_fatura` (`-1250.75`) e **4 registros** em `qtd_transacoes` (`-5`), mas o relatório aponta `Valor Observado = 2`, `1` e `1` (linhas 263–265).
  - Na partição `anomesdia=20261030` de `tabela_particao_diaria_anomalias`, há **45 registros** fora do intervalo em `valor_transacao`, mas o relatório aponta `Valor Observado = 2` (linha 307).

---

### 2.2. Vazamento Temporal (*Data Leakage*) no Aprendizado de `evolucao_fotografias` e da Partição Recém-Chegada
- **Arquivos afetados:**
  - `src/auto_learner.py` (linhas 433–439, 501–504, 790–809 e 1471–1488)
  - `src/checks_particao.py` (linhas 704–712)
- **Causa raiz:**
  1. **Auto-aprovação involuntária de mutação retroativa em colunas imutáveis:** Em `src/auto_learner.py` (linhas 790–804), para descobrir quais colunas possuem "evolução positiva" (`cols_evolucao_aprendidas`), o motor compara a última fotografia histórica `GOLD` (`D-1`) **com o perfil da própria execução atual (`D`)**. Se uma coluna historicamente imutável sofrer um preenchimento retroativo indevido na data `D` (`delta_col > delta_linhas`), o `auto_learner` passa a classificá-la imediatamente como coluna de `evolucao_positiva`. Em seguida, `checks_particao.py` (linha 705) vê a coluna em `cols_evolucao` e aprova a alteração como `OK`, tornando o erro `preenchimento retroativo em coluna imutável` (linha 711) **inalcançável em modo `"auto"`**.
  2. **Inclusão da partição recém-chegada no conjunto de treino `GOLD`:** Em `_filtrar_particoes_gold_para_treino` (`src/auto_learner.py`, linha 433), se a última partição (`ultima_particao`) não tiver uma falha estrutural grosseira capturada pelo pré-filtro, ela é incluída em `nomes_particoes_gold`. Como o pré-filtro não verifica `consistencia_entre_colunas`, `formato_regex`, `digito_verificador` nem *Distribution Drift* (PSI):
     - Uma única violação de `dt_cadastro_chave_pix < dt_abertura_conta` na partição recém-chegada derruba `min(taxas_r) == 1.0` (linha 1475) e **impede o motor de aprender a regra relacional**.
     - Um aumento de nulos em coluna opcional na partição recém-chegada infla `max_gold = max(taxas_nulos_gold)` (linha 1046) usado para julgar a própria partição.
  3. **Extração de `todas_colunas` e `tipos_colunas` restrita à primeira partição:** Em `src/auto_learner.py` (linhas 203 e 501), a lista de colunas e tipos usada nas etapas de aprendizado é lida apenas de `nomes_todas_particoes[0]`, em vez de consolidar o schema modal de todas as partições `GOLD`.

---

### 2.3. Falso Positivo de `REPROCESSAMENTO_DETECTADO` e Sobreposição do Status `QUARENTENA`
- **Arquivos afetados:**
  - `src/engine.py` (linhas 320–350, 806–811 e 1161–1172)
- **Causa raiz:**
  - Em `_inspecionar_e_carregar_particoes_tabela` (`src/engine.py`, linhas 324–327), a verificação:
    ```python
    elif mudou_fisicamente_intraday or (
        mudou_fisicamente_vs_ant and nome_part < ultima_particao
    ):
        modo = "REPROCESSAMENTO_DETECTADO"
    ```
    é executada **antes** de verificar se a partição estava em `QUARENTENA` (`camada_ref != "GOLD"`) e sem distinguir se a tabela possui evolução positiva legítima no passado (`permitir_atualizacao_particoes_passadas = True`) ou se a partição pertence à janela ativa (`particoes_janela_recente`).
- **Evidência no relatório (`relatorio_ultima_execucao.md`):**
  - Na `tabela_particao_mensal` (onde o cadastro tardio de chave Pix atualiza safras anteriores legitimamente), **5 das 6 partições (`202605` a `202609`) aparecem como `REPROCESSAMENTO_DETECTADO`** (linhas 13 e 20–24), disparando `WARNING` de reprocessamento histórico no console mesmo com 100% dos checks `OK`.
  - Na `tabela_particao_mensal_anomalias`, a partição `anomes=202608` (que estava em quarentena `SILVER`) teve seu modo sobrescrito para `REPROCESSAMENTO_DETECTADO` (linha 29).

---

### 2.4. Rebaixamento Indevido da `ultima_particao` por Checks Globais (`freshness_particao` e Cold Start de Maturação)
- **Arquivos afetados:**
  - `src/checks_particao.py` (linhas 354–369)
  - `src/checks_maturacao.py` (linhas 183–198)
  - `src/engine.py` (linhas 47–54, 499–519 e 565–579)
- **Causa raiz:**
  1. **`freshness_particao` penalizando partição histórica saudável:** O check de *Freshness/Lacunas* avalia a tabela como um todo, mas grava seu `ResultadoCheck` com `particao=ultima_particao` (`src/checks_particao.py`, linha 360) e faz parte de `CHECKS_ESTRUTURAIS_CRITICOS`. Se a partição do dia `D` atrasar e ainda não tiver chegado na origem, `ultima_particao` será `D-1` (uma partição saudável que já estava em `GOLD`). O motor então **reprova `D-1`, remove os arquivos de `D-1` da pasta `gold/` e os move para `bronze/`**, quebrando o consumo analítico de dados históricos íntegros.
  2. **`diagnostico_atraso_aprendido` em Cold Start impedindo promoção para `GOLD`:** Nas duas primeiras execuções do motor (`qtd_fotos_hist < min_fotos`), o check meramente informativo `diagnostico_atraso_aprendido` emite `status="WARNING"` com `particao="TODAS_PARTICOES"` (`src/checks_maturacao.py`, linha 195). Em `src/engine.py` (linha 518), qualquer check `TODAS_PARTICOES` sem data específica é atribuído à `ultima_particao`. Como a camada `GOLD` exige `n_warn == 0`, **a última partição de uma tabela 100% limpa é rebaixada para `SILVER` durante o Cold Start**.

---

### 2.5. Contaminação da Curva de Maturação por Partições `BRONZE` e Falta de Validação de Parse em Colunas de Data
- **Arquivos afetados:**
  - `src/checks_maturacao.py` (linhas 64–69 e 147–164)
  - `src/snapshot_manager.py` (linhas 83–94 e 295–334)
- **Causa raiz:**
  1. **Persistência de eventos de partição `BRONZE` no histórico de maturação:** Em `src/checks_maturacao.py` (linha 68), o filtro `if not (nome_p < ultima_particao_nome and nome_p in contaminadas_set)` mantém a `ultima_particao_nome` para que ela possa ser avaliada no dia `D`. Porém, logo em seguida (linha 147), `registrar_foto_eventos_data` grava as contagens dessa partição contaminada em `snapshots_eventos_data.parquet`. Nas execuções `D-4` a `D-1` de `tabela_particao_mensal_anomalias`, a partição `anomes=202610` (que é `BRONZE`) contaminou o histórico de eventos e distorceu a curva aprendida (`lag 1d=42%` na tabela com anomalias vs. `lag 1d=56%` na tabela limpa, conforme linhas 222 e 239 do `relatorio_ultima_execucao.md`).
  2. **Strings inválidas em colunas de data ignoradas silenciosamente:** Em `eh_coluna_data_ou_ts` (`src/snapshot_manager.py`, linha 86), qualquer coluna cujo nome comece com `dt_`, `data_`, `ts_` ou `timestamp_` recebe `tipos_colunas[col] = "data_ts"` incondicionalmente. Ao converter os valores com `pd.to_datetime(serie, errors="coerce", format="ISO8601")`, strings corrompidas (ex.: `"DATA_INVALIDA"` ou `"9999-99-99"`) viram `NaT` silenciosamente: não são contadas como nulos em `nulos_json` (que checa apenas `df_part[col].isna()`) e, se a coluna não for de safra (como `dt_cadastro_chave_pix`), nenhum check acusa os valores corrompidos.
  3. **Relações *Cross-Column* numéricas ou com operadores `>` / `==` ausentes no perfil:** `SnapshotManager.construir_perfil_particao` (linhas 480–542) pré-calcula apenas pares de colunas de data com operador `>=`. Regras manuais no YAML entre colunas numéricas (ex.: `valor_pago <= valor_fatura`) ou com operadores `>` / `==` funcionam em memória, mas são ignoradas em modo `--streaming` ou `CACHE_GOLD`.

---

### 2.6. Incompatibilidade Real com URIs `s3://` no `DataQualityEngine`, `SnapshotManager` e `GlueCatalogIntegrator`
- **Arquivos afetados:**
  - `src/engine.py` (linhas 95–100, 303, 528–532, 596–598 e 981–1110)
  - `src/snapshot_manager.py` (linhas 106–131, 576–589, 616, 747, 858 e 1069–1086)
  - `src/catalog_integration.py` (linhas 163–182)
  - `src/storage_backend.py` (linha 369)
- **Causa raiz:**
  1. Em `src/engine.py` (linhas 95–100), `self.dir_origem = self.base_dir / cfg_geral.get("diretorio_origem")` concatena `self.base_dir` usando `pathlib.Path`. Se `diretorio_origem` for `"s3://meu-bucket/origem"`, o `Path` transforma a URI em um caminho local inválido (`C:\...\s3:\meu-bucket\origem`) e `eh_uri_s3` retorna `False`.
  2. Em `SnapshotManager.__init__`, `Path(diretorio_saida)` corrompe `s3://` para `s3:/` e executa `d.mkdir(parents=True, exist_ok=True)` criando pastas locais no disco.
  3. Todos os métodos de leitura e escrita de metadados em `SnapshotManager`, `DataQualityEngine._persistir_metadados_e_relatorios` e `GlueCatalogIntegrator._persistir_manifesto` chamam `Path.exists()`, `pd.read_parquet(Path)` e `df.to_parquet(Path)` diretamente, ignorando `self.storage.existe()`, `self.storage.ler_parquet()` e `self.storage.escrever_parquet()`.
  4. Em `S3StorageBackend.remover_particao` (`src/storage_backend.py`, linha 369), `self.s3.delete_objects` envia todas as chaves de uma só vez (a API do S3 impõe limite máximo de 1.000 objetos por chamada).

---

## 3. Plano de Melhorias Recomendado

### Prioridade 1 — Precisão dos Checks e Blindagem do Auto-Aprendizado
1. **Paridade 100% entre Execução em Memória, Streaming e Cache GOLD (`src/snapshot_manager.py` e `src/checks_colunas.py`):**
   - Armazenar a série/frequência numérica compacta ou avaliar contagens exatas de violação no perfil para que `intervalo_numerico` reporte o número exato de linhas violadas (e não o tamanho de `amostras_numericas`).
   - Aplicar `tolerancia_warning_pct` de forma idêntica em `_executar_checks_colunas_via_cache` para `intervalo_numerico` e `formato_regex`.
   - Eliminar a dupla contagem entre `qtd_inv_dig` e `qtd_rep` no check de `formato_regex` via cache (`qtd_inv_dig_ou_rep = int((~mask_dig_tam | mask_rep).sum())`).
   - Suportar regras *Cross-Column* numéricas e operadores `>` / `==` no perfil estatístico e em `cache_gold_suporta_regras_atuais`.
2. **Eliminação de *Data Leakage* no `auto_learner.py`:**
   - Aprender `colunas_evolucao_positiva` exclusivamente a partir de transições entre fotografias históricas `GOLD` (`dt_referencia < dt_referencia_atual`), removendo a comparação direta com a partição em avaliação (`D`) durante a fase de treino.
   - Ao aprender regras intrínsecas (`consistencia_entre_colunas`, `formato_regex`, `digito_verificador`, `completude`, `dominio`, `intervalo_numerico`), priorizar as partições históricas `GOLD` anteriores à `ultima_particao` quando houver histórico suficiente ($\ge 2$ partições `GOLD`), evitando que uma anomalia inédita na safra recém-chegada contamine ou desative a regra.
   - Consolidar `todas_colunas` e `tipos_colunas` a partir da união/moda das partições `GOLD` em vez de olhar apenas `nomes_todas_particoes[0]`.
3. **Correção de Atribuição de Checks Globais e Maturação (`src/checks_particao.py`, `src/checks_maturacao.py`, `src/engine.py`):**
   - Associar `freshness_particao` à partição esperada (`part_esp_ref`) ou ao nível de tabela sem rebaixar a partição histórica `D-1` já aprovada em `GOLD`.
   - Ignorar `diagnostico_atraso_aprendido` (check informativo de diagnóstico da curva) no cômputo de `n_warn` para rebaixamento de partição durante o *Cold Start*.
   - Em `executar_checks_maturacao`, ao persistir `snapshots_eventos_data.parquet` para execuções futuras, excluir partições classificadas com falha crítica estrutural (`BRONZE`) ou marcar o snapshot de eventos como contaminado para não distorcer a curva aprendida.
4. **Validação de Integridade de Datas (`data_ts`):**
   - Contabilizar no perfil (`aderencia_data_particao` / `regex_str_cols`) valores não-nulos em colunas `data_ts` que falham no parse ISO8601 (`serie.notna() & datas_convertidas.isna()`), reportando-os como anomalia de formato/validade.
5. **Refinamento do `modo_avaliacao` (`src/engine.py`):**
   - Preservar `QUARENTENA` quando a partição não era `GOLD` na foto anterior.
   - Distinguir `ATUALIZACAO_SAFRA_PASSADA` (quando a mudança ocorre apenas em `colunas_evolucao_positiva` permitidas) de `REPROCESSAMENTO_DETECTADO` (quando há regravação física ou mutação em partição histórica fechada).

### Prioridade 2 — Compatibilidade Nativa com S3 / AWS Glue
6. **Suporte Ponta a Ponta a URIs `s3://`:**
   - Criar helper de junção de caminhos (`join_uri_or_path`) que preserve `s3://bucket/prefix/...` sem converter para `WindowsPath`/`PosixPath` quando `eh_uri_s3` for verdadeiro.
   - Substituir todas as chamadas diretas a `Path.exists()`, `Path.mkdir()`, `pd.read_parquet()` e `df.to_parquet()` em `SnapshotManager`, `DataQualityEngine` e `GlueCatalogIntegrator` pelos métodos de `StorageBackend`.
   - Paginar `S3StorageBackend.remover_particao` em lotes de até 1.000 objetos no `delete_objects`.

### Prioridade 3 — Observabilidade e Manutenibilidade
7. **Score da Janela Ativa no Resumo Executivo:**
   - Adicionar ao `resumo_qualidade_tabelas.parquet` e ao relatório Markdown o **Score da Janela Recente / Última Partição** ao lado do Score Global Histórico, evitando que dezenas de partições antigas em `CACHE_GOLD` diluam visualmente falhas críticas nas partições da semana atual.
8. **Herança de Configuração (`defaults`) e Validação Profunda no `config_loader.py`:**
   - Permitir um bloco `defaults_tabela` no `dq_config.yaml` para eliminar a repetição de ~60 linhas idênticas por tabela, e validar também as subchaves de cada check (`volumetria_historica`, `roteamento_e_score`, `maturacao_datas`, etc.).
