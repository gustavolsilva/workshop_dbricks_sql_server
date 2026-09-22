# Databricks notebook source
# DBTITLE 1,Passo a passo do workshop
# MAGIC %md
# MAGIC # Passo a Passo do Workshop: Azure SQL até a Camada Gold
# MAGIC **Engenharia de Dados Aplicada | BRQ Digital Solutions**
# MAGIC
# MAGIC ## Visão geral
# MAGIC Este notebook organiza a narrativa do workshop com base no que já foi implementado nos notebooks operacionais. A jornada parte do Azure SQL Database, passa pela camada Bronze e evolui até Silver e Gold no Unity Catalog, usando classes Python para manter o código legível, reutilizável e pronto para orquestração. Agora, a demonstração também já inclui a operação fim a fim por meio do Job [jb_ingestion_sql_server_to_databricks](#job-910129648162452).
# MAGIC
# MAGIC ## Ambiente utilizado
# MAGIC * **Servidor Azure SQL:** `svlabgus.database.windows.net`
# MAGIC * **Database:** `db_adventureworks_lt`
# MAGIC * **Key Vault / Secret Scope:** `kvault-adventureworks`
# MAGIC * **Secrets utilizados:** `sql-user` e `sql-password`
# MAGIC * **Catálogo do workshop:** `db_lab_brq`
# MAGIC * **Job operacional:** [jb_ingestion_sql_server_to_databricks](#job-910129648162452)
# MAGIC * **Formato do Job:** multi-task, fila habilitada, `PERFORMANCE_OPTIMIZED` e notificações por e-mail
# MAGIC
# MAGIC ## Sequência prática recomendada
# MAGIC
# MAGIC ### Etapa 1: Setup e validação de acesso
# MAGIC Antes do código, explique três conceitos:
# MAGIC * O Azure Key Vault armazena as credenciais do banco
# MAGIC * O Secret Scope permite recuperar essas credenciais no Databricks com segurança
# MAGIC * O Unity Catalog é a camada lógica onde as tabelas serão publicadas
# MAGIC
# MAGIC Também vale contextualizar que a mesma base técnica será usada tanto para a execução manual dos notebooks quanto para a execução orquestrada via Job.
# MAGIC
# MAGIC ### Etapa 2: Ingestão Bronze
# MAGIC No notebook [02_Extract_Camada_Bronze](#notebook-3724764295311696), a classe `BronzeIngestor`:
# MAGIC * lê via JDBC as tabelas `SalesLT.Customer`, `SalesLT.Address`, `SalesLT.Product`, `SalesLT.SalesOrderHeader` e `SalesLT.SalesOrderDetail`
# MAGIC * usa retry para lidar com indisponibilidade transitória do Azure SQL
# MAGIC * cria o schema `db_lab_brq.bronze` se necessário
# MAGIC * grava as tabelas `db_lab_brq.bronze.adventureworks_customers`, `db_lab_brq.bronze.adventureworks_addresses`, `db_lab_brq.bronze.adventureworks_products`, `db_lab_brq.bronze.adventureworks_salesorder_headers` e `db_lab_brq.bronze.adventureworks_salesorder_details`
# MAGIC * adiciona a coluna `_ingestion_time` em cada carga
# MAGIC
# MAGIC ### Etapa 3: Transformação Silver
# MAGIC No notebook [03_Transform_Camada_Silver](#notebook-1657912297639093), a classe `SilverTransformer`:
# MAGIC * lê as tabelas bronze publicadas no catálogo
# MAGIC * aplica regras específicas por domínio, como deduplicação, trim, padronização e filtros de qualidade
# MAGIC * trata clientes, endereços, produtos, cabeçalhos de pedido e itens de pedido
# MAGIC * grava as tabelas `db_lab_brq.silver.adventureworks_customers`, `db_lab_brq.silver.adventureworks_addresses`, `db_lab_brq.silver.adventureworks_products`, `db_lab_brq.silver.adventureworks_salesorder_headers` e `db_lab_brq.silver.adventureworks_salesorder_details`
# MAGIC
# MAGIC ### Etapa 4: Agregação Gold
# MAGIC No notebook [04_Gold_Aggregation](#notebook-1657912297639094), a classe `GoldAggregator`:
# MAGIC * lê as tabelas silver necessárias para cada análise
# MAGIC * calcula contatos válidos por empresa, produtos por cor, vendas por cliente e vendas por dia
# MAGIC * grava as tabelas `db_lab_brq.gold.customer_summary_by_company`, `db_lab_brq.gold.product_summary_by_color`, `db_lab_brq.gold.sales_summary_by_customer` e `db_lab_brq.gold.sales_summary_by_day`
# MAGIC
# MAGIC ## Passo a passo operacional do Job
# MAGIC Depois de explicar os notebooks individualmente, apresentar a orquestração do Job ajuda a consolidar a visão operacional.
# MAGIC
# MAGIC ### Ordem das tarefas
# MAGIC 1. `Ingestao_Tables_Azure_SQL_Server_Bronze` executa [02_Extract_Camada_Bronze](#notebook-3724764295311696)
# MAGIC 2. `Transformacao_Tables_Silver` executa [03_Transform_Camada_Silver](#notebook-1657912297639093)
# MAGIC 3. `Agregacao_Tables_Gold` executa [04_Gold_Aggregation](#notebook-1657912297639094)
# MAGIC
# MAGIC A ordem é importante porque cada etapa depende da publicação bem-sucedida da camada anterior.
# MAGIC
# MAGIC ### O que validar após cada etapa
# MAGIC **Após Bronze:**
# MAGIC * confirmar que as cinco tabelas foram recriadas ou atualizadas no schema `db_lab_brq.bronze`
# MAGIC * verificar presença da coluna `_ingestion_time`
# MAGIC * validar se houve leitura das tabelas esperadas do `SalesLT`
# MAGIC
# MAGIC **Após Silver:**
# MAGIC * confirmar que as tabelas `db_lab_brq.silver.*` foram publicadas
# MAGIC * revisar se as regras de limpeza, deduplicação e padronização foram aplicadas sem erro
# MAGIC * verificar se as estruturas fazem sentido para consumo analítico posterior
# MAGIC
# MAGIC **Após Gold:**
# MAGIC * confirmar que as tabelas agregadas `db_lab_brq.gold.*` foram materializadas
# MAGIC * validar se os produtos analíticos esperados aparecem na camada final
# MAGIC * revisar se a run concluiu sem interromper o encadeamento entre as tarefas
# MAGIC
# MAGIC ## Como interpretar falha transitória de JDBC
# MAGIC Durante a demo, pode ocorrer uma falha transitória de JDBC na etapa Bronze. Um ponto importante para explicar é que a leitura JDBC em Spark é lazy. Isso significa que o erro nem sempre aparece no momento em que o DataFrame é definido; ele pode surgir apenas quando a ação é materializada, por exemplo durante o `saveAsTable`.
# MAGIC
# MAGIC Na prática, isso quer dizer:
# MAGIC * a mensagem de erro pode aparecer na escrita, mesmo que a origem do problema esteja na conexão com o Azure SQL
# MAGIC * falhas intermitentes de rede, disponibilidade momentânea do banco ou handshake JDBC podem ser percebidas apenas na ação final
# MAGIC * um retry pode resolver o problema sem necessidade de mudar a lógica do notebook, quando a causa é transitória
# MAGIC
# MAGIC Ao apresentar esse ponto, vale reforçar que nem toda falha no `saveAsTable` indica problema de modelagem ou de Delta. Em alguns casos, a execução apenas materializou uma leitura JDBC que ainda não havia sido efetivamente consumida.
# MAGIC
# MAGIC ## Instruções de demo para abrir e acompanhar runs
# MAGIC Para tornar a apresentação mais prática, siga este roteiro:
# MAGIC 1. Abrir o Job [jb_ingestion_sql_server_to_databricks](#job-910129648162452)
# MAGIC 2. Mostrar a DAG com as três tarefas encadeadas
# MAGIC 3. Abrir uma run recente e navegar pelas tarefas Bronze, Silver e Gold
# MAGIC 4. Entrar no detalhe da tarefa Bronze para mostrar o notebook executado e os logs
# MAGIC 5. Se houver falha transitória, explicar a natureza lazy da leitura JDBC e demonstrar a opção de retry
# MAGIC 6. Após uma run bem-sucedida, voltar ao histórico e destacar visibilidade, rastreabilidade e notificações por e-mail
# MAGIC
# MAGIC ## Mensagens que valem reforçar no workshop
# MAGIC * Cada notebook representa uma responsabilidade clara dentro da arquitetura Medallion
# MAGIC * O uso de classes facilita manutenção, testes e reutilização em Jobs
# MAGIC * O Unity Catalog simplifica governança e remove dependência de caminhos físicos expostos no código
# MAGIC * A separação Bronze, Silver e Gold ajuda a explicar para a audiência como dados evoluem de bruto para analítico
# MAGIC * O Job mostra como a solução sai do modo laboratório e entra em um modelo operacional observável
# MAGIC
# MAGIC ## Extensão já implementada para enriquecer a demonstração
# MAGIC A versão atual do workshop já inclui a expansão do pipeline para múltiplas tabelas do AdventureWorks e o encadeamento operacional por Job.
# MAGIC
# MAGIC ### Tabelas adicionais já implementadas
# MAGIC * `SalesLT.Address`
# MAGIC * `SalesLT.Product`
# MAGIC * `SalesLT.SalesOrderHeader`
# MAGIC * `SalesLT.SalesOrderDetail`
# MAGIC
# MAGIC ### Cenários Silver já implementados
# MAGIC * padronização e qualidade de endereços
# MAGIC * tratamento de catálogo de produtos
# MAGIC * deduplicação e padronização de pedidos
# MAGIC * validação de chaves para itens de pedido
# MAGIC
# MAGIC ### Produtos Gold já implementados
# MAGIC * contatos válidos por empresa
# MAGIC * produtos por cor com preço médio
# MAGIC * vendas por cliente
# MAGIC * faturamento por dia
# MAGIC
# MAGIC ## Fechamento recomendado do workshop
# MAGIC Encerrar mostrando que o mesmo desenho já foi evoluído para um Job com múltiplas tarefas, em que Bronze, Silver e Gold rodam de forma encadeada. Isso permite concluir a narrativa com uma ponte direta entre desenvolvimento, operação, retentativa e monitoramento do pipeline completo no Databricks.
# MAGIC
# MAGIC ## Literatura e referências oficiais da Databricks
# MAGIC Use esta seção ao final do workshop ou como material pré-leitura para os participantes.
# MAGIC
# MAGIC * **Conexão com Azure SQL e JDBC:** [Connect to data sources from Azure Databricks](https://learn.microsoft.com/en-us/azure/databricks/scenarios/databricks-connect-to-data-sources/) e [JDBC connection](https://learn.microsoft.com/en-us/azure/databricks/connect/jdbc-connection/)
# MAGIC * **Delta Lake:** [What is Delta Lake in Azure Databricks?](https://learn.microsoft.com/en-us/azure/databricks/delta/index/)
# MAGIC * **Arquitetura Medallion:** [What is the medallion lakehouse architecture?](https://learn.microsoft.com/en-us/azure/databricks/lakehouse/medallion/)
# MAGIC * **Unity Catalog:** [What is Azure Databricks?](https://learn.microsoft.com/en-us/azure/databricks/introduction/index/) e [Database objects in Azure Databricks](https://learn.microsoft.com/en-us/azure/databricks/database-objects/index/)
# MAGIC * **Databricks Secrets:** [Secret management](https://learn.microsoft.com/en-us/azure/databricks/security/secrets/index/)
# MAGIC * **Azure Key Vault-backed secret scope:** [Create an Azure Key Vault-backed secret scope](https://learn.microsoft.com/en-us/azure/databricks/security/secrets/index/)
# MAGIC * **Arquiteturas de referência:** [Databricks reference architectures](https://learn.microsoft.com/en-us/azure/databricks/lakehouse-architecture/reference/)
# MAGIC
# MAGIC ## Sugestão de narrativa para apresentar as referências
# MAGIC * Começar com Medallion e Delta Lake para explicar o porquê da separação Bronze, Silver e Gold
# MAGIC * Em seguida, apresentar Secrets, Azure Key Vault e conexão JDBC como base de segurança e conectividade
# MAGIC * Depois, reforçar Unity Catalog como camada de governança e publicação das tabelas
# MAGIC * Finalizar com Jobs e arquiteturas de referência para mostrar como esse laboratório evolui para uma solução corporativa