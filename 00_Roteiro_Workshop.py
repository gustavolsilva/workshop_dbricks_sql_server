# Databricks notebook source
# DBTITLE 1,Roteiro do Workshop
# MAGIC %md
# MAGIC # Workshop: Azure SQL para Medallion no Databricks
# MAGIC **Roteiro Executivo e Técnico | BRQ Digital Solutions**
# MAGIC
# MAGIC ## Objetivo do Workshop
# MAGIC Este workshop demonstra uma jornada ponta a ponta saindo de um Azure SQL Database e chegando a uma arquitetura Medallion no Databricks com Unity Catalog. A proposta é mostrar para desenvolvedores como aplicar organização em camadas, segurança com Azure Key Vault, orientação a objetos em PySpark, publicação de tabelas governadas no catálogo e orquestração operacional com Job.
# MAGIC
# MAGIC ## O que já foi implementado
# MAGIC * **Origem relacional:** Azure SQL Database `db_adventureworks_lt` no servidor `svlabgus.database.windows.net`.
# MAGIC * **Segurança:** Azure Key Vault `kvault-adventureworks` com os secrets `sql-user` e `sql-password`.
# MAGIC * **Integração no Databricks:** Secret Scope `kvault-adventureworks` apontando para o Key Vault.
# MAGIC * **Catálogo utilizado no workshop:** `db_lab_brq`.
# MAGIC * **Camadas publicadas:** `db_lab_brq.bronze`, `db_lab_brq.silver` e `db_lab_brq.gold`.
# MAGIC * **Pipeline atual:**
# MAGIC   * [02_Extract_Camada_Bronze](#notebook-3724764295311696) cria `db_lab_brq.bronze.adventureworks_customers`, `db_lab_brq.bronze.adventureworks_addresses`, `db_lab_brq.bronze.adventureworks_products`, `db_lab_brq.bronze.adventureworks_salesorder_headers` e `db_lab_brq.bronze.adventureworks_salesorder_details`
# MAGIC   * [03_Transform_Camada_Silver](#notebook-1657912297639093) cria `db_lab_brq.silver.adventureworks_customers`, `db_lab_brq.silver.adventureworks_addresses`, `db_lab_brq.silver.adventureworks_products`, `db_lab_brq.silver.adventureworks_salesorder_headers` e `db_lab_brq.silver.adventureworks_salesorder_details`
# MAGIC   * [04_Gold_Aggregation](#notebook-1657912297639094) cria `db_lab_brq.gold.customer_summary_by_company`, `db_lab_brq.gold.product_summary_by_color`, `db_lab_brq.gold.sales_summary_by_customer` e `db_lab_brq.gold.sales_summary_by_day`
# MAGIC * **Job já criado:** [jb_ingestion_sql_server_to_databricks](#job-910129648162452), no formato multi-task, com fila habilitada, `PERFORMANCE_OPTIMIZED` e notificações por e-mail.
# MAGIC * **Encadeamento operacional atual:** `Ingestao_Tables_Azure_SQL_Server_Bronze` -> `Transformacao_Tables_Silver` -> `Agregacao_Tables_Gold`.
# MAGIC
# MAGIC ## Arquitetura apresentada
# MAGIC 1. O notebook solicita credenciais via `dbutils.secrets.get(...)` usando o Secret Scope.
# MAGIC 2. O Secret Scope resolve a autenticação contra o Azure Key Vault sem expor credenciais no código.
# MAGIC 3. O Spark lê os dados do Azure SQL Database por JDBC.
# MAGIC 4. Os dados são persistidos no Unity Catalog com nomes lógicos no formato `catalog.schema.table`.
# MAGIC 5. Cada camada aplica uma responsabilidade específica:
# MAGIC    * **Bronze:** ingestão bruta com `_ingestion_time`
# MAGIC    * **Silver:** limpeza, deduplicação e padronização
# MAGIC    * **Gold:** agregações orientadas ao consumo analítico
# MAGIC 6. A orquestração do fluxo foi consolidada em um Job para garantir sequência, retentativa operacional e visibilidade de execução ponta a ponta.
# MAGIC
# MAGIC ## Orquestração executiva já implementada
# MAGIC O workshop agora permite mostrar não apenas a construção das camadas, mas também como o processo é executado de forma coordenada em produção. O Job [jb_ingestion_sql_server_to_databricks](#job-910129648162452) materializa essa visão ao encadear ingestão, transformação e agregação em três tarefas dependentes, respeitando a ordem natural do pipeline.
# MAGIC
# MAGIC Do ponto de vista executivo, isso traz benefícios claros:
# MAGIC * previsibilidade operacional, porque cada etapa só inicia quando a anterior termina com sucesso
# MAGIC * rastreabilidade, porque cada run mostra onde começou, onde terminou e em qual tarefa ocorreu eventual falha
# MAGIC * governança, porque o fluxo deixa de depender de execuções manuais isoladas
# MAGIC * capacidade de retentativa, porque uma falha transitória pode ser reprocessada sem refazer toda a narrativa do workshop manualmente
# MAGIC * prontidão para produção, porque o mesmo desenho usado na demonstração já se aproxima do modelo de operação real
# MAGIC
# MAGIC ## Agenda sugerida do Workshop
# MAGIC 1. **Contexto e arquitetura (15 min)**
# MAGIC    * O que é a arquitetura Medallion
# MAGIC    * Por que usar Unity Catalog e tabelas governadas
# MAGIC 2. **Segurança e conectividade (20 min)**
# MAGIC    * Azure Key Vault
# MAGIC    * Secret Scope no Databricks
# MAGIC    * Leitura JDBC com Azure SQL
# MAGIC 3. **Hands-on Bronze (25 min)**
# MAGIC    * Classe `BronzeIngestor`
# MAGIC    * Criação do schema bronze
# MAGIC    * Ingestão multi-tabela de clientes, endereços, produtos, pedidos e itens de pedido
# MAGIC 4. **Hands-on Silver (20 min)**
# MAGIC    * Classe `SilverTransformer`
# MAGIC    * Deduplicação, limpeza e padronização por domínio de dados
# MAGIC    * Persistência nas tabelas silver para clientes, endereços, produtos, cabeçalho e detalhe de pedidos
# MAGIC 5. **Hands-on Gold (15 min)**
# MAGIC    * Classe `GoldAggregator`
# MAGIC    * Agregações de contatos por empresa, produtos por cor, vendas por cliente e vendas por dia
# MAGIC    * Persistência nas tabelas gold para consumo analítico
# MAGIC 6. **Encerramento e próximos passos (15 min)**
# MAGIC    * Orquestração com Jobs
# MAGIC    * Expansão para novas tabelas e novos produtos analíticos
# MAGIC    * Monitoramento, retentativa e evolução para operação contínua
# MAGIC
# MAGIC ## Sequência operacional ponta a ponta
# MAGIC Ao apresentar o fluxo completo, vale explicar a sequência abaixo como uma cadeia única de entrega:
# MAGIC 1. O Job inicia a tarefa `Ingestao_Tables_Azure_SQL_Server_Bronze` e executa [02_Extract_Camada_Bronze](#notebook-3724764295311696).
# MAGIC 2. As tabelas `SalesLT` são lidas via JDBC e gravadas no schema `db_lab_brq.bronze` com `_ingestion_time`.
# MAGIC 3. Em caso de sucesso, o Job libera a tarefa `Transformacao_Tables_Silver`, que executa [03_Transform_Camada_Silver](#notebook-1657912297639093).
# MAGIC 4. As tabelas bronze são tratadas, deduplicadas e publicadas em `db_lab_brq.silver`.
# MAGIC 5. Em seguida, o Job executa `Agregacao_Tables_Gold`, chamando [04_Gold_Aggregation](#notebook-1657912297639094).
# MAGIC 6. As tabelas silver são agregadas e publicadas em `db_lab_brq.gold`, fechando o ciclo analítico.
# MAGIC 7. Ao final da run, o usuário pode inspecionar a execução por tarefa, revisar duração, confirmar sucesso e usar as notificações por e-mail como mecanismo adicional de acompanhamento.
# MAGIC
# MAGIC ## Mensagem principal do Workshop
# MAGIC O foco não é apenas mover dados, mas mostrar boas práticas de engenharia de software em dados: encapsulamento com classes, separação de responsabilidades por camada, segurança sem credenciais em texto claro, publicação de ativos reutilizáveis no Unity Catalog e orquestração como parte integrante da solução.
# MAGIC
# MAGIC ## Evolução recomendada para a demo
# MAGIC O workshop já foi expandido para múltiplas tabelas do AdventureWorks, o que permite mostrar um fluxo mais realista de ponta a ponta.
# MAGIC
# MAGIC * **Tabelas Bronze já implementadas:** `SalesLT.Customer`, `SalesLT.Address`, `SalesLT.Product`, `SalesLT.SalesOrderHeader`, `SalesLT.SalesOrderDetail`
# MAGIC * **Transformações Silver já implementadas:** limpeza de clientes, endereços e produtos, além de padronização e deduplicação de cabeçalhos e itens de pedido
# MAGIC * **Agregações Gold já implementadas:** contatos por empresa, produtos por cor, vendas por cliente e faturamento por dia
# MAGIC * **Evolução já materializada:** Bronze, Silver e Gold foram encadeados no Job [jb_ingestion_sql_server_to_databricks](#job-910129648162452), demonstrando governança, observabilidade e reprocessamento controlado
# MAGIC
# MAGIC ## Como monitorar e retentar a execução
# MAGIC Durante a apresentação, vale mostrar que a operação não termina quando o código é escrito. O acompanhamento do Job é parte da solução.
# MAGIC
# MAGIC * Abrir o Job [jb_ingestion_sql_server_to_databricks](#job-910129648162452) para visualizar a DAG com as três tarefas.
# MAGIC * Entrar em uma run e acompanhar o status de cada etapa: Bronze, Silver e Gold.
# MAGIC * Em caso de falha, identificar se o problema ocorreu na leitura, na transformação ou na agregação.
# MAGIC * Quando a falha for transitória, como indisponibilidade momentânea de JDBC ou de rede, usar retry para reexecutar a tarefa ou a run.
# MAGIC * Usar o histórico de runs e as notificações por e-mail para demonstrar observabilidade operacional.
# MAGIC
# MAGIC ## Ponte entre workshop e operação produtiva
# MAGIC Este material deixa claro para a audiência que o laboratório não é apenas conceitual. O mesmo fluxo apresentado no workshop já contém elementos fundamentais de uma operação produtiva:
# MAGIC * ingestão segura com credenciais protegidas
# MAGIC * persistência governada no Unity Catalog
# MAGIC * separação em camadas com responsabilidades bem definidas
# MAGIC * encadeamento em Job multi-task
# MAGIC * fila habilitada para organizar execuções concorrentes
# MAGIC * visibilidade operacional por run, tarefa e tempo de execução
# MAGIC
# MAGIC A principal ponte para produção passa agora por ampliar o escopo do mesmo desenho: adicionar agenda, novos domínios, cargas incrementais, validações adicionais de qualidade e alertas operacionais mais específicos.
# MAGIC
# MAGIC ## Resultado esperado na apresentação
# MAGIC Ao final, o participante deve entender como sair de uma base transacional no Azure SQL e entregar tabelas prontas para consumo analítico no Databricks, com uma fundação que já evolui naturalmente para automação, monitoramento e novas cargas incrementais.
# MAGIC
# MAGIC ## Literatura e referências oficiais da Databricks
# MAGIC Para apoiar o workshop com material oficial, use as referências abaixo como leitura complementar e base conceitual.
# MAGIC
# MAGIC * **Arquitetura Medallion:** [What is the medallion lakehouse architecture?](https://learn.microsoft.com/en-us/azure/databricks/lakehouse/medallion/)
# MAGIC * **Delta Lake:** [What is Delta Lake in Azure Databricks?](https://learn.microsoft.com/en-us/azure/databricks/delta/index/)
# MAGIC * **Unity Catalog:** [What is Azure Databricks?](https://learn.microsoft.com/en-us/azure/databricks/introduction/index/) e [Database objects in Azure Databricks](https://learn.microsoft.com/en-us/azure/databricks/database-objects/index/)
# MAGIC * **Databricks Secrets e secret scopes:** [Secret management](https://learn.microsoft.com/en-us/azure/databricks/security/secrets/index/)
# MAGIC * **Azure Key Vault-backed secret scope:** [Create an Azure Key Vault-backed secret scope](https://learn.microsoft.com/en-us/azure/databricks/security/secrets/index/)
# MAGIC * **Conexão com Azure SQL e JDBC:** [Connect to data sources from Azure Databricks](https://learn.microsoft.com/en-us/azure/databricks/scenarios/databricks-connect-to-data-sources/) e [JDBC connection](https://learn.microsoft.com/en-us/azure/databricks/connect/jdbc-connection/)
# MAGIC * **Arquiteturas de referência da plataforma:** [Databricks reference architectures](https://learn.microsoft.com/en-us/azure/databricks/lakehouse-architecture/reference/)
# MAGIC
# MAGIC ## Sugestão de uso dessas referências no workshop
# MAGIC * Antes da demo: usar a documentação de arquitetura Medallion e Delta Lake para nivelar conceitos
# MAGIC * Durante a parte de setup: usar a documentação de Secrets e Azure Key Vault-backed secret scope
# MAGIC * Durante a parte de ingestão: usar a documentação de conexão com fontes externas por JDBC
# MAGIC * No encerramento: apontar Unity Catalog, Jobs e arquiteturas de referência como próximos passos de governança e evolução da solução