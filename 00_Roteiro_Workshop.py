# Databricks notebook source
# DBTITLE 1,Roteiro do Workshop
# MAGIC %md
# MAGIC # Workshop: Azure SQL para Medallion no Databricks
# MAGIC **Roteiro Executivo e Técnico | BRQ Digital Solutions**
# MAGIC
# MAGIC ## Objetivo do Workshop
# MAGIC Este workshop mostra, de forma simples, como sair de um banco SQL Server na Azure e chegar a uma arquitetura Medallion no Databricks com Unity Catalog. A ideia é apresentar uma jornada ponta a ponta, com segurança, organização em camadas e automação, mas em uma linguagem acessível também para quem não é especialista em engenharia de dados.
# MAGIC
# MAGIC ## Requisitos do Lab
# MAGIC Antes de executar o laboratório, considerar os pré-requisitos abaixo.
# MAGIC
# MAGIC * Estar em uma cloud provider, no nosso exemplo a Azure
# MAGIC * Ter um servidor SQL Server disponível
# MAGIC * Criar uma database de exemplo, no nosso caso `AdventureWorksLT`
# MAGIC * Criar um Azure Key Vault para armazenar o nome do database e a senha de acesso
# MAGIC * Criar uma instância do Databricks
# MAGIC * Configurar no Secret Scope do Databricks a integração com o Azure Key Vault
# MAGIC
# MAGIC ## Overview da Plataforma Databricks
# MAGIC Para contextualizar o laboratório, vale apresentar rapidamente a plataforma Databricks e o papel de cada componente dentro da solução.
# MAGIC
# MAGIC * **Workspace:** é o espaço de trabalho onde ficam notebooks, jobs, arquivos, consultas e dashboards. É o lugar onde as pessoas constroem, organizam e acompanham a solução.
# MAGIC * **Compute:** é o recurso de processamento que executa os notebooks e jobs. Em termos simples, é a “máquina” que faz o trabalho pesado.
# MAGIC * **Databricks Runtime:** é o ambiente pronto para uso com Spark e bibliotecas integradas. Ele simplifica a execução de tarefas de dados, análise e engenharia.
# MAGIC * **Notebooks:** são os documentos interativos onde escrevemos código, documentação e comentários. No workshop, eles mostram cada etapa do processo.
# MAGIC * **Jobs:** são usados para automatizar a execução. Eles ajudam a rodar tarefas na ordem correta, monitorar o andamento e repetir a execução quando necessário.
# MAGIC * **Unity Catalog:** é a camada de organização e governança dos dados. Ele ajuda a manter tabelas, schemas e catálogos bem estruturados e seguros.
# MAGIC * **Delta Lake:** é a tecnologia usada nas tabelas analíticas para garantir mais confiabilidade, consistência e controle das mudanças nos dados.
# MAGIC * **Secret Scope:** é o recurso que permite usar credenciais com segurança, sem colocar usuário ou senha diretamente no código.
# MAGIC * **Integração com Azure Key Vault:** permite guardar segredos fora do notebook e acessá-los de forma segura no Databricks.
# MAGIC * **Lakehouse / Arquitetura Medallion:** é a forma de organizar os dados em camadas Bronze, Silver e Gold, facilitando a evolução do dado bruto até o dado pronto para análise.
# MAGIC
# MAGIC Uma forma simples de explicar a plataforma durante o workshop é dizer que o Databricks reúne, em um único ambiente, desenvolvimento, processamento, governança e operação. No contexto deste laboratório, ele conecta a origem SQL Server, protege credenciais com Key Vault e Secret Scope, processa os dados com Spark e publica tabelas prontas para consumo analítico.
# MAGIC
# MAGIC ## A implementação que está pronta é:
# MAGIC * **Origem relacional:** Azure SQL Database `db_adventureworks_lt` no servidor `seuservidor.database.windows.net`.
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
# MAGIC 1. O notebook busca as credenciais com segurança usando o Secret Scope.
# MAGIC 2. O Secret Scope se conecta ao Azure Key Vault sem expor usuário e senha no código.
# MAGIC 3. O Spark lê os dados do Azure SQL Database via JDBC.
# MAGIC 4. Os dados são publicados no Unity Catalog com nomes organizados no formato `catalog.schema.table`.
# MAGIC 5. Cada camada tem um papel claro:
# MAGIC    * **Bronze:** recebe os dados brutos vindos da origem
# MAGIC    * **Silver:** organiza, limpa e padroniza os dados
# MAGIC    * **Gold:** entrega dados prontos para consumo analítico
# MAGIC 6. Todo esse fluxo pode ser executado de forma sequencial por um Job, trazendo mais controle e previsibilidade.
# MAGIC
# MAGIC ## Orquestração executiva já implementada
# MAGIC O workshop agora mostra não apenas como construir as camadas, mas também como colocar esse processo para rodar de forma organizada. O Job [jb_ingestion_sql_server_to_databricks](#job-910129648162452) encadeia ingestão, transformação e agregação em três tarefas dependentes, respeitando a ordem natural do pipeline.
# MAGIC
# MAGIC Em uma linguagem mais simples, isso significa:
# MAGIC * cada etapa roda na hora certa
# MAGIC * fica fácil saber onde uma execução começou, terminou ou falhou
# MAGIC * o processo deixa de depender apenas de execução manual
# MAGIC * uma falha transitória pode ser reprocessada com mais facilidade
# MAGIC * a demonstração já se aproxima de um cenário real de operação
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
# MAGIC A principal mensagem do workshop é que não estamos apenas movendo dados. Estamos mostrando como montar uma solução organizada, segura e preparada para crescer, usando boas práticas desde a origem até a camada analítica.
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
# MAGIC Ao final, a expectativa é que mesmo uma pessoa com pouca familiaridade técnica consiga entender a lógica do processo: os dados saem de uma base transacional, passam por etapas de organização e qualidade e chegam a tabelas prontas para análise dentro do Databricks.
# MAGIC
# MAGIC ## Onde aprofundar o conteúdo
# MAGIC Para quem quiser ir além da apresentação, vale deixar caminhos claros de estudo e consulta.
# MAGIC
# MAGIC * **Documentação oficial Databricks:** melhor ponto de consulta para conceitos, arquitetura, setup e boas práticas
# MAGIC * **Microsoft Learn / Azure Databricks:** útil para temas ligados ao ecossistema Azure, integração com Key Vault e conectividade com Azure SQL
# MAGIC * **Databricks Academy (Partner):** recomendada para quem quiser fazer trilhas de capacitação, cursos e aprofundamento prático na plataforma
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
# MAGIC * **Databricks Academy (Partner):** portal indicado para trilhas e treinamentos complementares voltados a parceiros
# MAGIC
# MAGIC ## Sugestão de uso dessas referências no workshop
# MAGIC * Antes da demo: usar a documentação de arquitetura Medallion e Delta Lake para nivelar conceitos
# MAGIC * Durante a parte de setup: usar a documentação de Secrets e Azure Key Vault-backed secret scope
# MAGIC * Durante a parte de ingestão: usar a documentação de conexão com fontes externas por JDBC
# MAGIC * Ao final: indicar a documentação oficial e a Databricks Academy (Partner) para quem quiser se aprofundar
# MAGIC
# MAGIC ## Reset do ambiente para o workshop
# MAGIC Para reexecutar a demonstração do zero amanhã, use primeiro o drop das tabelas analíticas e depois avance até a camada bruta. Assim, a ordem respeita a dependência natural entre Gold, Silver e Bronze.
# MAGIC
# MAGIC Execute o bloco abaixo em uma célula `%sql` quando quiser limpar o ambiente:
# MAGIC
# MAGIC ```sql
# MAGIC DROP TABLE IF EXISTS db_lab_brq.gold.sales_summary_by_day;
# MAGIC DROP TABLE IF EXISTS db_lab_brq.gold.sales_summary_by_customer;
# MAGIC DROP TABLE IF EXISTS db_lab_brq.gold.product_summary_by_color;
# MAGIC DROP TABLE IF EXISTS db_lab_brq.gold.customer_summary_by_company;
# MAGIC
# MAGIC DROP TABLE IF EXISTS db_lab_brq.silver.adventureworks_salesorder_details;
# MAGIC DROP TABLE IF EXISTS db_lab_brq.silver.adventureworks_salesorder_headers;
# MAGIC DROP TABLE IF EXISTS db_lab_brq.silver.adventureworks_products;
# MAGIC DROP TABLE IF EXISTS db_lab_brq.silver.adventureworks_addresses;
# MAGIC DROP TABLE IF EXISTS db_lab_brq.silver.adventureworks_customers;
# MAGIC
# MAGIC DROP TABLE IF EXISTS db_lab_brq.bronze.adventureworks_salesorder_details;
# MAGIC DROP TABLE IF EXISTS db_lab_brq.bronze.adventureworks_salesorder_headers;
# MAGIC DROP TABLE IF EXISTS db_lab_brq.bronze.adventureworks_products;
# MAGIC DROP TABLE IF EXISTS db_lab_brq.bronze.adventureworks_addresses;
# MAGIC DROP TABLE IF EXISTS db_lab_brq.bronze.adventureworks_customers;
# MAGIC ```
# MAGIC
# MAGIC Observações para a apresentação:
# MAGIC * o Job [jb_ingestion_sql_server_to_databricks](#job-910129648162452) pode ser mantido; não é necessário recriá-lo
# MAGIC * o reset remove apenas as tabelas publicadas no catálogo, preservando notebooks, Job, secrets e configuração do ambiente
# MAGIC * após o drop, basta executar novamente Bronze, Silver e Gold para reconstruir toda a trilha do workshop