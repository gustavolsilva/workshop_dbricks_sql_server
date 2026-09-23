# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,Passo a passo do workshop
# MAGIC %md
# MAGIC # Passo a Passo do Workshop: Azure SQL até a Camada Gold
# MAGIC **Engenharia de Dados Aplicada | BRQ Digital Solutions**
# MAGIC
# MAGIC ## Visão geral
# MAGIC Este notebook organiza a narrativa do workshop de forma simples e progressiva. A ideia é mostrar, passo a passo, como os dados saem de um banco Azure SQL, passam pelas camadas Bronze, Silver e Gold no Databricks e chegam a uma estrutura pronta para análise. Além dos notebooks técnicos, a demonstração também já inclui a execução fim a fim com o Job [jb_ingestion_sql_server_to_databricks](#job-910129648162452).
# MAGIC
# MAGIC ## Requisitos do Lab
# MAGIC Antes de iniciar a demonstração, vale confirmar estes pré-requisitos:
# MAGIC
# MAGIC * Estar em uma cloud provider, no nosso exemplo a Azure
# MAGIC * Ter um servidor SQL Server disponível
# MAGIC * Criar uma base de exemplo, no nosso caso `AdventureWorksLT`
# MAGIC * Criar um Azure Key Vault para armazenar credenciais de acesso
# MAGIC * Criar uma instância do Databricks
# MAGIC * Configurar no Secret Scope do Databricks a integração com o Azure Key Vault
# MAGIC
# MAGIC ## Ambiente utilizado
# MAGIC * **Servidor Azure SQL:** `seuservidor.database.windows.net`
# MAGIC * **Database:** `AdventureWorksLT` no exemplo funcional, com `db_adventureworks_lt` como nome técnico usado na conexão do laboratório
# MAGIC * **Key Vault / Secret Scope:** `kvault-adventureworks`
# MAGIC * **Secrets utilizados:** `sql-user` e `sql-password`
# MAGIC * **Catálogo do workshop:** `db_lab_brq`
# MAGIC * **Job operacional:** [jb_ingestion_sql_server_to_databricks](#job-910129648162452)
# MAGIC * **Formato do Job:** multi-task, fila habilitada, `PERFORMANCE_OPTIMIZED` e notificações por e-mail
# MAGIC
# MAGIC ## Sequência prática recomendada
# MAGIC
# MAGIC ### Etapa 1: Setup e validação de acesso
# MAGIC Antes do código, vale explicar três conceitos de forma simples:
# MAGIC * O Azure Key Vault guarda as credenciais do banco com segurança
# MAGIC * O Secret Scope permite que o Databricks use essas credenciais sem expor usuário e senha no notebook
# MAGIC * O Unity Catalog é o local onde as tabelas finais ficam organizadas
# MAGIC
# MAGIC Também é importante comentar que a mesma base técnica serve tanto para rodar os notebooks manualmente quanto para rodar tudo de forma automatizada via Job.
# MAGIC
# MAGIC ### Etapa 2: Ingestão Bronze
# MAGIC No notebook [02_Extract_Camada_Bronze](#notebook-3724764295311696), a classe `BronzeIngestor`:
# MAGIC * lê via JDBC as tabelas `SalesLT.Customer`, `SalesLT.Address`, `SalesLT.Product`, `SalesLT.SalesOrderHeader` e `SalesLT.SalesOrderDetail`
# MAGIC * usa retry para lidar com indisponibilidade transitória do Azure SQL
# MAGIC * cria o schema `db_lab_brq.bronze` se necessário
# MAGIC * grava as tabelas `db_lab_brq.bronze.adventureworks_customers`, `db_lab_brq.bronze.adventureworks_addresses`, `db_lab_brq.bronze.adventureworks_products`, `db_lab_brq.bronze.adventureworks_salesorder_headers` e `db_lab_brq.bronze.adventureworks_salesorder_details`
# MAGIC * adiciona a coluna `_ingestion_time` em cada carga
# MAGIC
# MAGIC Em uma linguagem mais acessível, esta é a etapa em que os dados saem da origem e entram no Databricks quase como chegaram, com o mínimo de tratamento.
# MAGIC
# MAGIC ### Etapa 3: Transformação Silver
# MAGIC No notebook [03_Transform_Camada_Silver](#notebook-1657912297639093), a classe `SilverTransformer`:
# MAGIC * lê as tabelas bronze publicadas no catálogo
# MAGIC * aplica regras específicas por domínio, como deduplicação, trim, padronização e filtros de qualidade
# MAGIC * trata clientes, endereços, produtos, cabeçalhos de pedido e itens de pedido
# MAGIC * grava as tabelas `db_lab_brq.silver.adventureworks_customers`, `db_lab_brq.silver.adventureworks_addresses`, `db_lab_brq.silver.adventureworks_products`, `db_lab_brq.silver.adventureworks_salesorder_headers` e `db_lab_brq.silver.adventureworks_salesorder_details`
# MAGIC
# MAGIC Aqui, a mensagem principal é que os dados começam a ficar mais organizados, limpos e confiáveis para uso posterior.
# MAGIC
# MAGIC ### Etapa 4: Agregação Gold
# MAGIC No notebook [04_Gold_Aggregation](#notebook-1657912297639094), a classe `GoldAggregator`:
# MAGIC * lê as tabelas silver necessárias para cada análise
# MAGIC * calcula contatos válidos por empresa, produtos por cor, vendas por cliente e vendas por dia
# MAGIC * grava as tabelas `db_lab_brq.gold.customer_summary_by_company`, `db_lab_brq.gold.product_summary_by_color`, `db_lab_brq.gold.sales_summary_by_customer` e `db_lab_brq.gold.sales_summary_by_day`
# MAGIC
# MAGIC Nesta etapa, os dados já estão prontos para consumo analítico. É a camada que mais conversa com relatórios, dashboards e tomada de decisão.
# MAGIC
# MAGIC ## Passo a passo operacional do Job
# MAGIC Depois de explicar os notebooks individualmente, apresentar a orquestração do Job ajuda a mostrar como tudo funciona em conjunto.
# MAGIC
# MAGIC ### Ordem das tarefas
# MAGIC 1. `Ingestao_Tables_Azure_SQL_Server_Bronze` executa [02_Extract_Camada_Bronze](#notebook-3724764295311696)
# MAGIC 2. `Transformacao_Tables_Silver` executa [03_Transform_Camada_Silver](#notebook-1657912297639093)
# MAGIC 3. `Agregacao_Tables_Gold` executa [04_Gold_Aggregation](#notebook-1657912297639094)
# MAGIC
# MAGIC A ordem é importante porque cada etapa depende do resultado da anterior.
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
# MAGIC Durante a demo, pode ocorrer uma falha transitória de JDBC na etapa Bronze. Um ponto importante para explicar é que a leitura JDBC em Spark é lazy. Em termos simples, isso significa que o erro pode não aparecer no momento em que o DataFrame é criado, mas apenas quando o sistema realmente tenta usar os dados, por exemplo durante o `saveAsTable`.
# MAGIC
# MAGIC Na prática, isso quer dizer:
# MAGIC * a mensagem de erro pode aparecer na escrita, mesmo que a origem do problema esteja na conexão com o Azure SQL
# MAGIC * falhas intermitentes de rede, disponibilidade momentânea do banco ou handshake JDBC podem ser percebidas apenas na ação final
# MAGIC * um retry pode resolver o problema sem necessidade de mudar a lógica do notebook, quando a causa é transitória
# MAGIC
# MAGIC Ao apresentar esse ponto, vale reforçar que nem toda falha no `saveAsTable` indica problema de modelagem ou de Delta. Em muitos casos, a execução apenas materializou uma leitura JDBC que ainda não tinha sido consumida de fato.
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
# MAGIC * A separação Bronze, Silver e Gold ajuda a explicar para a audiência como os dados evoluem até ficarem prontos para análise
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
# MAGIC ## Onde aprofundar o conteúdo
# MAGIC Para quem quiser ir além da apresentação, vale deixar caminhos simples e confiáveis para consulta.
# MAGIC
# MAGIC * **Documentação oficial Databricks:** melhor fonte para conceitos, setup, governança e boas práticas
# MAGIC * **Microsoft Learn / Azure Databricks:** útil para integração com Azure, Key Vault e Azure SQL
# MAGIC * **Databricks Academy (Partner):** recomendada para trilhas de capacitação, cursos e aprofundamento prático na plataforma. O acesso depende do credenciamento como parceiro.
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
# MAGIC * **Databricks Academy (Partner):** portal indicado para trilhas e treinamentos complementares voltados a parceiros. Requer acesso de parceiro.
# MAGIC
# MAGIC ## Sugestão de narrativa para apresentar as referências
# MAGIC * Começar com Medallion e Delta Lake para explicar o porquê da separação Bronze, Silver e Gold
# MAGIC * Em seguida, apresentar Secrets, Azure Key Vault e conexão JDBC como base de segurança e conectividade
# MAGIC * Depois, reforçar Unity Catalog como camada de governança e publicação das tabelas
# MAGIC * Ao final: indicar a documentação oficial e a Databricks Academy (Partner) para quem quiser se aprofundar