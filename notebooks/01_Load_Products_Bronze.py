# Databricks notebook source
from pyspark.sql import functions as F

# COMMAND ----------

df_products = spark.read.option("header", "True").option("inferschema", "True").csv("/Workspace/Users/mayurgujar500@gmail.com/RetailX/Source/Products.csv")

#display(df_products)

# COMMAND ----------

print(f"Total Records: {df_products.count()}")

# COMMAND ----------


duplicates = (df_products.groupBy("ProductID").agg(F.count("*").alias("cnt")).
              filter(F.col("cnt")> 1))
display(duplicates)


# COMMAND ----------

display(df_products.filter(F.col("ProductID").isNull()))

# COMMAND ----------

df_products.printSchema()

# COMMAND ----------

df_audit = df_products.withColumn("CreatedDate", F.current_timestamp()).withColumn("PipelineName", F.lit("01_Load_Products_Bronze")).withColumn("SourceSystem", F.lit("RetailX"))

display(df_audit)

# COMMAND ----------

df_audit.write.mode("overwrite").format("delta").saveAsTable("bronze.products")

# COMMAND ----------

# MAGIC %%sql
# MAGIC
# MAGIC SELECT * FROM bronze.products;
# MAGIC DESCRIBE DETAIL bronze.products;