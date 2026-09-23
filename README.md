# Workshop: Azure SQL Server para arquitetura Medallion no Databricks

Pipeline de engenharia de dados que demonstra a ingestão de dados do **Azure SQL Database/SQL Server** para o **Azure Databricks**, usando **PySpark**, **JDBC**, **Delta Lake**, **Unity Catalog** e a arquitetura **Medallion** — Bronze, Silver e Gold.

> **Status:** material de workshop/laboratório. O código foi estruturado para demonstrar o fluxo ponta a ponta e não deve ser considerado, sem ajustes adicionais, uma implementação pronta para produção.

## Visão geral

O projeto implementa o seguinte fluxo:

```text
Azure SQL Database / SQL Server
            |
            | JDBC + credenciais no Azure Key Vault
            v
    Bronze — ingestão próxima da origem
            |
            v
    Silver — limpeza, deduplicação e padronização
            |
            v
    Gold — agregações para consumo analítico
```

A origem utilizada no laboratório é o banco **AdventureWorksLT**, com dados do schema `SalesLT`. As tabelas são persistidas no Unity Catalog no formato:

```text
<catalog>.<schema>.<table>
```

A configuração demonstrada utiliza o catálogo `db_lab_brq` e os schemas `bronze`, `silver` e `gold`.

## Objetivos

- Demonstrar a conexão do Databricks com Azure SQL/SQL Server via JDBC.
- Evitar credenciais diretamente no código por meio de Azure Key Vault e Databricks Secret Scope.
- Implementar uma arquitetura Medallion com responsabilidades separadas por camada.
- Persistir dados em tabelas Delta governadas pelo Unity Catalog.
- Aplicar regras básicas de qualidade, limpeza e deduplicação.
- Criar produtos analíticos agregados na camada Gold.
- Demonstrar como as etapas podem ser encadeadas em um Job multi-task do Databricks.

## Estrutura do repositório

| Arquivo | Responsabilidade |
|---|---|
| [`00_Roteiro_Workshop.py`](./00_Roteiro_Workshop.py) | Roteiro executivo e técnico da apresentação, incluindo arquitetura, agenda, operação e reset do ambiente. |
| [`01_Step_by_step.py`](./01_Step_by_step.py) | Guia operacional detalhado do workshop, com validações esperadas após cada camada. |
| [`02_Extract_Camada_Bronze.py`](./02_Extract_Camada_Bronze.py) | Leitura via JDBC e gravação das tabelas de origem na camada Bronze. |
| [`03_Transform_Camada_Silver.py`](./03_Transform_Camada_Silver.py) | Limpeza, padronização, filtros de qualidade e deduplicação das tabelas Bronze. |
| [`04_Gold_Aggregation.py`](./04_Gold_Aggregation.py) | Construção das tabelas agregadas para consumo analítico. |

Os arquivos `.py` usam a convenção de notebooks do Databricks (`# Databricks notebook source`, `# COMMAND ----------` e células `%md`). Portanto, devem ser importados ou executados em um workspace Databricks, e não tratados apenas como scripts Python convencionais.

## Tecnologias utilizadas

- Python
- PySpark
- Apache Spark
- Azure Databricks
- Azure SQL Database/SQL Server
- JDBC
- Delta Lake
- Unity Catalog
- Azure Key Vault
- Databricks Secret Scope

## Pré-requisitos

Antes de executar o workshop, é necessário possuir:

1. Uma assinatura Azure com permissões para utilizar Azure Databricks, Azure SQL e Azure Key Vault.
2. Uma instância de Azure Databricks com compute compatível com Spark/PySpark.
3. Um banco `AdventureWorksLT` ou equivalente contendo as tabelas de origem esperadas.
4. Conectividade de rede entre o compute do Databricks e o servidor SQL.
5. Um Azure Key Vault contendo as credenciais do banco.
6. Um Secret Scope no Databricks integrado ao Azure Key Vault.
7. Um Unity Catalog habilitado e um catálogo com permissões de criação de schemas e tabelas.
8. O driver JDBC apropriado para SQL Server, normalmente disponível no runtime Databricks utilizado.

## Configuração esperada

A implementação atualmente utiliza os seguintes valores no exemplo:

| Item | Valor utilizado no laboratório |
|---|---|
| Servidor SQL | `svlabgus.database.windows.net` |
| Banco de dados | `db_adventureworks_lt` |
| Secret Scope | `kvault-adventureworks` |
| Secret do usuário | `sql-user` |
| Secret da senha | `sql-password` |
| Catálogo | `db_lab_brq` |

Esses valores são específicos do ambiente do workshop. Para reutilizar o projeto em outro ambiente, altere-os de forma parametrizada e evite gravar informações específicas da infraestrutura diretamente no código.

### Secrets necessários

O `Secret Scope` deve disponibilizar, no mínimo:

- `sql-user`: usuário do SQL Server.
- `sql-password`: senha do SQL Server.

O código possui chaves alternativas de compatibilidade (`sql_user`, `user`, `sql_password` e `password`), mas a configuração recomendada é utilizar `sql-user` e `sql-password`.

> **Segurança:** nunca armazene usuário, senha, tokens ou connection strings com credenciais no notebook, no README ou no controle de versão. O servidor utilizado no exemplo não é uma credencial, mas continua sendo uma configuração específica de infraestrutura que deve ser externalizada em ambientes reais.

## Tabelas de origem

A camada Bronze lê as seguintes tabelas do schema `SalesLT`:

| Origem | Tabela Bronze |
|---|---|
| `SalesLT.Customer` | `db_lab_brq.bronze.adventureworks_customers` |
| `SalesLT.Address` | `db_lab_brq.bronze.adventureworks_addresses` |
| `SalesLT.Product` | `db_lab_brq.bronze.adventureworks_products` |
| `SalesLT.SalesOrderHeader` | `db_lab_brq.bronze.adventureworks_salesorder_headers` |
| `SalesLT.SalesOrderDetail` | `db_lab_brq.bronze.adventureworks_salesorder_details` |

## Camadas do pipeline

### Bronze

Implementada em `02_Extract_Camada_Bronze.py` pela classe `BronzeIngestor`.

Responsabilidades:

- Ler tabelas do SQL Server via JDBC.
- Recuperar credenciais usando `dbutils.secrets`.
- Criar o schema Bronze caso ele não exista.
- Gravar os dados em tabelas Delta.
- Adicionar a coluna `_ingestion_time`.
- Executar `OPTIMIZE` após a gravação.
- Tentar novamente leituras que falhem por indisponibilidade transitória identificada pelo código.

A carga atual utiliza `mode("overwrite")`. Isso é adequado para a demonstração, mas não representa uma estratégia incremental ou histórica.

### Silver

Implementada em `03_Transform_Camada_Silver.py` pela classe `SilverTransformer`.

Regras aplicadas incluem:

- Deduplicação por chave de negócio/técnica.
- Remoção de registros sem campos essenciais.
- `trim` de atributos textuais.
- Padronização de e-mails para minúsculas.
- Padronização de números de produto para maiúsculas.
- Tratamento das tabelas de clientes, endereços, produtos, cabeçalhos e itens de pedido.

Tabelas publicadas:

```text
db_lab_brq.silver.adventureworks_customers
db_lab_brq.silver.adventureworks_addresses
db_lab_brq.silver.adventureworks_products
db_lab_brq.silver.adventureworks_salesorder_headers
db_lab_brq.silver.adventureworks_salesorder_details
```

Assim como na Bronze, a persistência atual utiliza `overwrite`. O próprio código indica que uma implementação produtiva poderia utilizar `MERGE` para cenários de upsert e dimensões lentamente mutáveis.

### Gold

Implementada em `04_Gold_Aggregation.ipynb` pela classe `GoldAggregator`.

Produtos analíticos criados:

| Tabela Gold | Conteúdo |
|---|---|
| `customer_summary_by_company` | Quantidade de contatos/clientes válidos por empresa. |
| `product_summary_by_color` | Quantidade de produtos e preço médio de lista por cor. |
| `sales_summary_by_customer` | Quantidade de pedidos e receita total por empresa/cliente associado. |
| `sales_summary_by_day` | Quantidade de pedidos e receita total por dia. |

Tabelas publicadas:

```text
db_lab_brq.gold.customer_summary_by_company
db_lab_brq.gold.product_summary_by_color
db_lab_brq.gold.sales_summary_by_customer
db_lab_brq.gold.sales_summary_by_day
```

## Ordem de execução

Execute as etapas nesta ordem, porque cada camada depende da anterior:

1. `02_Extract_Camada_Bronze.py`
2. `03_Transform_Camada_Silver.py`
3. `04_Gold_Aggregation.ipynb`

O arquivo `01_Step_by_step.py` contém a narrativa operacional e as validações sugeridas após cada etapa. O arquivo `00_Roteiro_Workshop.py` apresenta a visão executiva, a arquitetura e a agenda recomendada.

## Execução no Databricks

### 1. Importar os arquivos

Importe os notebooks para o workspace Databricks ou conecte o repositório ao Databricks Repos.

### 2. Validar a configuração

Antes da execução, confirme:

- O compute está ativo.
- O cluster possui acesso ao Azure SQL.
- O Secret Scope está acessível.
- Os secrets esperados existem.
- O usuário ou service principal possui permissão no Unity Catalog.
- O catálogo `db_lab_brq` pode criar os schemas `bronze`, `silver` e `gold`.

### 3. Executar Bronze

Execute `02_Extract_Camada_Bronze.py` e valide a criação das cinco tabelas no schema `db_lab_brq.bronze`.

### 4. Executar Silver

Execute `03_Transform_Camada_Silver.py` e valide a publicação das cinco tabelas no schema `db_lab_brq.silver`.

### 5. Executar Gold

Execute `04_Gold_Aggregation.py` e valide a criação das quatro tabelas agregadas no schema `db_lab_brq.gold`.

## Orquestração recomendada

Para execução automatizada, crie um Job multi-task com dependências sequenciais:

```text
Ingestao_Tables_Azure_SQL_Server_Bronze
                |
                v
Transformacao_Tables_Silver
                |
                v
Agregacao_Tables_Gold
```

A dependência entre tarefas é necessária porque a Silver lê as tabelas Bronze e a Gold lê as tabelas Silver.

O roteiro do workshop também considera um Job chamado `jb_ingestion_sql_server_to_databricks`, com fila habilitada, modo de performance otimizado e notificações por e-mail. Esses parâmetros dependem do ambiente Databricks e devem ser revisados antes de serem reutilizados.

## Validações pós-execução

### Bronze

```sql
SHOW TABLES IN db_lab_brq.bronze;

SELECT *
FROM db_lab_brq.bronze.adventureworks_customers
LIMIT 10;

DESCRIBE TABLE db_lab_brq.bronze.adventureworks_customers;
```

Confirme principalmente a existência de `_ingestion_time` e a quantidade esperada de tabelas.

### Silver

```sql
SHOW TABLES IN db_lab_brq.silver;

SELECT COUNT(*) AS total_registros
FROM db_lab_brq.silver.adventureworks_customers;
```

Verifique se as regras de limpeza e deduplicação foram aplicadas sem eliminar registros válidos indevidamente.

### Gold

```sql
SHOW TABLES IN db_lab_brq.gold;

SELECT *
FROM db_lab_brq.gold.sales_summary_by_day
ORDER BY OrderDateKey DESC;
```

Valide se as métricas de pedidos e receita estão coerentes com os dados da Silver.

## Reset do ambiente

Para reconstruir as tabelas do workshop do zero, remova as tabelas na ordem inversa de dependência:

```sql
DROP TABLE IF EXISTS db_lab_brq.gold.sales_summary_by_day;
DROP TABLE IF EXISTS db_lab_brq.gold.sales_summary_by_customer;
DROP TABLE IF EXISTS db_lab_brq.gold.product_summary_by_color;
DROP TABLE IF EXISTS db_lab_brq.gold.customer_summary_by_company;

DROP TABLE IF EXISTS db_lab_brq.silver.adventureworks_salesorder_details;
DROP TABLE IF EXISTS db_lab_brq.silver.adventureworks_salesorder_headers;
DROP TABLE IF EXISTS db_lab_brq.silver.adventureworks_products;
DROP TABLE IF EXISTS db_lab_brq.silver.adventureworks_addresses;
DROP TABLE IF EXISTS db_lab_brq.silver.adventureworks_customers;

DROP TABLE IF EXISTS db_lab_brq.bronze.adventureworks_salesorder_details;
DROP TABLE IF EXISTS db_lab_brq.bronze.adventureworks_salesorder_headers;
DROP TABLE IF EXISTS db_lab_brq.bronze.adventureworks_products;
DROP TABLE IF EXISTS db_lab_brq.bronze.adventureworks_addresses;
DROP TABLE IF EXISTS db_lab_brq.bronze.adventureworks_customers;
```

Esse procedimento remove apenas as tabelas materializadas no catálogo. Ele não remove notebooks, Jobs, Secret Scopes, Azure Key Vault ou configurações do workspace.

## Limitações atuais e evolução para produção

O projeto é adequado para demonstração, mas há diferenças importantes entre o estado atual e uma implementação operacional robusta:

- **Carga completa:** Bronze e Silver utilizam `overwrite`; para produção, avalie cargas incrementais, watermark, CDC ou Change Tracking.
- **Upsert e histórico:** substitua, quando aplicável, o `overwrite` por `MERGE` Delta e modele SCD conforme o requisito do domínio.
- **Retry:** o retry atual identifica uma mensagem específica de indisponibilidade. Uma solução produtiva deveria classificar exceções, aplicar backoff exponencial e registrar métricas.
- **Idempotência:** valide explicitamente o comportamento em reexecuções e concorrência.
- **Qualidade de dados:** adicione contratos de esquema, métricas de qualidade, quarentena e alertas.
- **Observabilidade:** registre duração, volume lido/escrito, contagem de rejeitados e contexto da execução.
- **Configuração:** externalize servidor, banco, catálogo, schema e nomes de secrets por parâmetros de Job ou configuração de ambiente.
- **Governança:** aplique permissões mínimas no Unity Catalog, segregação de ambientes e controle de acesso às tabelas.
- **Performance:** avalie particionamento, clustering, `OPTIMIZE`, `ZORDER` quando aplicável e volume real dos dados.
- **Conectividade:** em ambientes corporativos, considere Private Link, VNet injection, firewall, DNS privado e regras de rede.
- **Testes:** inclua testes unitários para transformações e testes de integração para JDBC, Delta e permissões.

Esses pontos não invalidam o workshop; apenas delimitam corretamente o que está sendo demonstrado: um pipeline funcional e didático de laboratório, não uma plataforma completa de produção.

## Referências oficiais

- [Arquitetura Medallion no Azure Databricks](https://learn.microsoft.com/azure/databricks/lakehouse/medallion)
- [Delta Lake no Azure Databricks](https://learn.microsoft.com/azure/databricks/delta)
- [Unity Catalog no Azure Databricks](https://learn.microsoft.com/azure/databricks/data-governance/unity-catalog)
- [Conexão com fontes de dados usando JDBC](https://learn.microsoft.com/azure/databricks/connect/jdbc)
- [Gerenciamento de secrets no Databricks](https://learn.microsoft.com/azure/databricks/security/secrets)
- [Secret Scope integrado ao Azure Key Vault](https://learn.microsoft.com/azure/databricks/security/secrets/secret-scopes)
- [Arquiteturas de referência do Azure Databricks](https://learn.microsoft.com/azure/databricks/lakehouse-architecture/reference)

## Licença

Este repositório não declara uma licença. Na ausência de uma licença explícita, os direitos autorais permanecem com o autor e a reutilização, distribuição ou modificação deve ser autorizada pelo proprietário do repositório.
