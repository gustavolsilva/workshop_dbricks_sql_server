# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,Import Libs
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, lower, upper

# COMMAND ----------

# DBTITLE 1,Classe Silver
class SilverTransformer:
    """
    Last Modification: 2026-09-22
    Author: Gustavo Lourenço
    Description: Classe para transformar dados da tabela bronze em tabela silver
    """
    def __init__(self, spark: SparkSession, catalog: str):
        self.spark = spark
        self.catalog = catalog

    def _transform_customers(self, df_bronze):
        """
        Description: Transforma a tabela bronze de customers em tabela silver 
        Input: df_bronze: dataframe da tabela bronze
        Output: dataframe da tabela silver 
        """
        return df_bronze.dropDuplicates(["CustomerID"]) \
            .filter(col("EmailAddress").isNotNull()) \
            .withColumn("EmailAddress", lower(trim(col("EmailAddress")))) \
            .withColumn("CompanyName", trim(col("CompanyName")))

    def _transform_addresses(self, df_bronze):
        """
        Description: Transforma a tabela bronze de addresses em tabela silver 
        Input: df_bronze: dataframe da tabela bronze
        Output: dataframe da tabela silver
        """
        return df_bronze.dropDuplicates(["AddressID"]) \
            .filter(col("AddressLine1").isNotNull()) \
            .withColumn("AddressLine1", trim(col("AddressLine1"))) \
            .withColumn("AddressLine2", trim(col("AddressLine2"))) \
            .withColumn("City", trim(col("City"))) \
            .withColumn("StateProvince", trim(col("StateProvince"))) \
            .withColumn("CountryRegion", trim(col("CountryRegion"))) \
            .withColumn("PostalCode", trim(col("PostalCode")))

    def _transform_products(self, df_bronze):
        """
        Description: Transforma a tabela bronze de products em tabela silver 
        Input: df_bronze: dataframe da tabela bronze
        Output: dataframe da tabela silver
        """
        return df_bronze.dropDuplicates(["ProductID"]) \
            .filter(col("Name").isNotNull()) \
            .withColumn("Name", trim(col("Name"))) \
            .withColumn("ProductNumber", upper(trim(col("ProductNumber")))) \
            .withColumn("Color", trim(col("Color")))

    def _transform_salesorder_headers(self, df_bronze):
        """
        Description: Transforma a tabela bronze de salesorder_headers em tabela silver 
        Input: df_bronze: dataframe da tabela bronze
        Output: dataframe da tabela silver
        """
        return df_bronze.dropDuplicates(["SalesOrderID"]) \
            .filter(col("CustomerID").isNotNull()) \
            .withColumn("PurchaseOrderNumber", trim(col("PurchaseOrderNumber"))) \
            .withColumn("AccountNumber", trim(col("AccountNumber"))) \
            .withColumn("ShipMethod", trim(col("ShipMethod")))

    def _transform_salesorder_details(self, df_bronze):
        """
        Description: Transforma a tabela bronze de salesorder_details em tabela silver 
        Input: df_bronze: dataframe da tabela bronze
        Output: dataframe da tabela silver  
        """
        return df_bronze.dropDuplicates(["SalesOrderID", "SalesOrderDetailID"]) \
            .filter(col("ProductID").isNotNull())

    def execute(self, table_name: str):
        """
        Description: Executa a transformação da tabela bronze em tabela silver
        Input: table_name: nome da tabela bronze da qual será extraído os dados
        Output: None
        """
        tabela_bronze = f"{self.catalog}.bronze.{table_name}"
        tabela_silver = f"{self.catalog}.silver.{table_name}"
        
        df_bronze = self.spark.read.table(tabela_bronze)

        transformations = {
            "adventureworks_customers": self._transform_customers,
            "adventureworks_addresses": self._transform_addresses,
            "adventureworks_products": self._transform_products,
            "adventureworks_salesorder_headers": self._transform_salesorder_headers,
            "adventureworks_salesorder_details": self._transform_salesorder_details,
        }

        if table_name not in transformations:
            raise ValueError(f"Transformação silver não mapeada para a tabela: {table_name}")

        df_silver = transformations[table_name](df_bronze)
            
        # Obs para Devs: Em produção rigorosa, usaríamos a instrução MERGE (Upsert) 
        # do Delta Lake em vez de overwrite para tratar Slowly Changing Dimensions (SCD).
        self.spark.sql(f"CREATE SCHEMA IF NOT EXISTS {self.catalog}.silver")
        df_silver.write.format("delta").mode("overwrite").option("mergeSchema", "true").saveAsTable(tabela_silver)
        self.spark.sql(f"OPTIMIZE {tabela_silver}")
        print(f"Tabela silver carregada com sucesso: {tabela_silver}")

    def execute_many(self, table_names: list[str]):
        """
        Description: Executa a transformação de múltiplas tabelas bronze para silver
        Input: table_names
        Output: None
        """
        for table_name in table_names:
            print(f"Iniciando transformação silver de {table_name}")
            self.execute(table_name)

if __name__ == "__main__":
    spark = SparkSession.builder.appName("Transformacao_Silver").getOrCreate()
    transformer = SilverTransformer(spark, "db_lab_brq")

    tabelas_silver = [
        "adventureworks_customers",
        "adventureworks_addresses",
        "adventureworks_products",
        "adventureworks_salesorder_headers",
        "adventureworks_salesorder_details",
    ]

    transformer.execute_many(tabelas_silver)