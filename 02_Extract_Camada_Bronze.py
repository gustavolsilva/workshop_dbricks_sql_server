# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,Import Libs
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp

# COMMAND ----------

# DBTITLE 1,Class
class BronzeIngestor:
    """
    Author: Gustavo Lourenço
    Description: Classe para ingestão de dados no formato bronze de tabelas do SQL Server
    """
    def __init__(
        self,
        spark: SparkSession,
        secret_scope: str,
        catalog: str,
        user_secret_key: str = "sql-user",
        password_secret_key: str = "sql-password",
    ):
        self.spark = spark
        self.secret_scope = secret_scope
        self.catalog = catalog
        self.user_secret_key = user_secret_key
        self.password_secret_key = password_secret_key
        self.dbutils = self._get_dbutils()
        self.urlserver = "svlabgus.database.windows.net"
        
    def _get_dbutils(self):
        """
        Description: Método para obter o dbutils do spark utilizando a função get_ipython().user_ns.get("dbutils")
        Input: None
        Output: dbutils
        """
        try:
            import IPython
            dbutils = IPython.get_ipython().user_ns.get("dbutils")
            if dbutils is not None:
                return dbutils
        except Exception:
            pass

        from pyspark.dbutils import DBUtils
        return DBUtils(self.spark)

    def _get_secret(self, primary_key: str, *fallback_keys: str) -> str:
        """
        Description: Método para obter o secret do dbutils
        Input: primary_key, *fallback_keys
        Output: secret  
        """
        keys_to_try = [primary_key, *fallback_keys]
        last_error = None

        for key in keys_to_try:
            try:
                return self.dbutils.secrets.get(scope=self.secret_scope, key=key)
            except Exception as exc:
                last_error = exc

        tried_keys = ", ".join(keys_to_try)
        raise ValueError(
            f"Nenhum secret foi encontrado no scope '{self.secret_scope}'. Chaves testadas: {tried_keys}"
        ) from last_error

    def execute(self, db_server: str, db_name: str, table_origem: str, table_destino: str):
        """
        Description: Método para realizar a ingestão de dados no formato bronze do SQL Server
        Input: db_server, db_name, table_origem, table_destino
        Output: None
        """
        jdbc_url = f"jdbc:sqlserver://{db_server}:1433;database={db_name}"
        user = self._get_secret(self.user_secret_key, "sql_user", "user")
        password = self._get_secret(self.password_secret_key, "sql_password", "password")
        
        import time

        last_error = None
        for attempt in range(1, 7):
            try:
                df = self.spark.read.format("jdbc")\
                    .option("url", jdbc_url)\
                    .option("user", user)\
                    .option("password", password)\
                    .option("dbtable", table_origem)\
                    .load()
                break
            except Exception as exc:
                last_error = exc
                error_message = str(exc)
                is_transient_pause = "not currently available" in error_message.lower()

                if not is_transient_pause or attempt == 6:
                    raise

                print(f"Banco indisponível no momento. Tentativa {attempt}/6. Aguardando 20 segundos para novo teste...")
                time.sleep(20)
            
        if last_error is not None and 'df' not in locals():
            raise last_error
            
        self.spark.sql(f"CREATE SCHEMA IF NOT EXISTS {self.catalog}.bronze")
        full_dest = f"{self.catalog}.bronze.{table_destino}"
        df.withColumn("_ingestion_time", current_timestamp())\
          .write.format("delta").mode("overwrite").option("mergeSchema", "true").saveAsTable(full_dest)
        
        self.spark.sql(f"OPTIMIZE {full_dest}")
        print(f"Tabela bronze carregada com sucesso: {full_dest}")

    def execute_many(self, db_server: str, db_name: str, table_mappings: list[tuple[str, str]]):
        """
        Description: Executa a ingestão de múltiplas tabelas na camada bronze
        Input: db_server, db_name, table_mappings
        Output: None
        """
        for table_origem, table_destino in table_mappings:
            print(f"Iniciando ingestão de {table_origem} para {self.catalog}.bronze.{table_destino}")
            self.execute(db_server, db_name, table_origem, table_destino)

if __name__ == "__main__":
    spark = SparkSession.builder.appName("Ingestao_Bronze").getOrCreate()
    ingestor = BronzeIngestor(
        spark,
        "kvault-adventureworks",
        "db_lab_brq",
        user_secret_key="sql-user",
        password_secret_key="sql-password",
    )

    tabelas_bronze = [
        ("SalesLT.Customer", "adventureworks_customers"),
        ("SalesLT.Address", "adventureworks_addresses"),
        ("SalesLT.Product", "adventureworks_products"),
        ("SalesLT.SalesOrderHeader", "adventureworks_salesorder_headers"),
        ("SalesLT.SalesOrderDetail", "adventureworks_salesorder_details"),
    ]

    ingestor.execute_many(ingestor.urlserver, "db_adventureworks_lt", tabelas_bronze)