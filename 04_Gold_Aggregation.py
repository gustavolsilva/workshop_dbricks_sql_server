# Databricks notebook source
# DBTITLE 1,Import Libs
from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, col, count, countDistinct, sum as spark_sum, to_date

# COMMAND ----------

# DBTITLE 1,Classe Gold
class GoldAggregator:
    """
    Last Modification: 2026-09-22
    Author: Gustavo Lourenço
    Description: Classe para realizar agregações de dados na camada Gold.
    """
    def __init__(self, spark: SparkSession, catalog: str):
        self.spark = spark
        self.catalog = catalog

    def _save_gold(self, df_gold, table_name: str):
        tabela_gold = f"{self.catalog}.gold.{table_name}"
        self.spark.sql(f"CREATE SCHEMA IF NOT EXISTS {self.catalog}.gold")
        df_gold.write.format("delta").mode("overwrite").saveAsTable(tabela_gold)
        self.spark.sql(f"OPTIMIZE {tabela_gold}")
        print(f"Tabela gold carregada com sucesso: {tabela_gold}")

    def build_customer_summary_by_company(self):
        df_customers = self.spark.read.table(f"{self.catalog}.silver.adventureworks_customers")

        df_gold = df_customers.groupBy("CompanyName") \
            .agg(count("CustomerID").alias("Total_Valid_Contacts")) \
            .orderBy(col("Total_Valid_Contacts").desc())

        self._save_gold(df_gold, "customer_summary_by_company")

    def build_product_summary_by_color(self):
        df_products = self.spark.read.table(f"{self.catalog}.silver.adventureworks_products")

        df_gold = df_products.filter(col("Color").isNotNull()) \
            .groupBy("Color") \
            .agg(
                count("ProductID").alias("Total_Products"),
                avg("ListPrice").alias("Average_List_Price")
            ) \
            .orderBy(col("Total_Products").desc(), col("Average_List_Price").desc())

        self._save_gold(df_gold, "product_summary_by_color")

    def build_sales_summary_by_customer(self):
        df_orders = self.spark.read.table(f"{self.catalog}.silver.adventureworks_salesorder_headers")
        df_customers = self.spark.read.table(f"{self.catalog}.silver.adventureworks_customers")

        df_gold = df_orders.join(
            df_customers.select("CustomerID", "CompanyName"),
            on="CustomerID",
            how="left"
        ).groupBy("CompanyName") \
         .agg(
             countDistinct("SalesOrderID").alias("Total_Orders"),
             spark_sum("TotalDue").alias("Total_Revenue")
         ) \
         .orderBy(col("Total_Revenue").desc())

        self._save_gold(df_gold, "sales_summary_by_customer")

    def build_sales_summary_by_day(self):
        df_orders = self.spark.read.table(f"{self.catalog}.silver.adventureworks_salesorder_headers")

        df_gold = df_orders.withColumn("OrderDateKey", to_date(col("OrderDate"))) \
            .groupBy("OrderDateKey") \
            .agg(
                countDistinct("SalesOrderID").alias("Total_Orders"),
                spark_sum("TotalDue").alias("Total_Revenue")
            ) \
            .orderBy(col("OrderDateKey").desc())

        self._save_gold(df_gold, "sales_summary_by_day")

    def execute(self):
        """
        Description: Executa as agregações de dados na camada Gold.
        Input: None
        Output: None
        """
        self.build_customer_summary_by_company()
        self.build_product_summary_by_color()
        self.build_sales_summary_by_customer()
        self.build_sales_summary_by_day()

if __name__ == "__main__":
    spark = SparkSession.builder.appName("Agregacao_Gold").getOrCreate()
    aggregator = GoldAggregator(spark, "db_lab_brq")
    aggregator.execute()