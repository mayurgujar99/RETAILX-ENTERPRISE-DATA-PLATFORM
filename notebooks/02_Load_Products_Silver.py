# Databricks notebook source
from pyspark.sql import functions as F

# COMMAND ----------

df_products_bronze = spark.table("bronze.products")

#display(df_products_bronze)

# COMMAND ----------

print(df_products_bronze.count())

df_products_bronze.printSchema()

# COMMAND ----------

df_removed_null = df_products_bronze.filter(F.col("ProductID").isNotNull())

display(df_removed_null)

# COMMAND ----------

df_product_counts = (df_removed_null.groupBy("ProductID").agg(F.count("*").alias("cnt")))

# COMMAND ----------

df_product_join_counts = df_removed_null.join(df_product_counts, "ProductID", "left")

#display(df_product_join_count)

# COMMAND ----------

df_valid_products = df_product_join_counts.filter(F.col("cnt")== 1).drop("cnt")
df_invalid_rule1 =df_product_join_counts.filter(F.col("cnt")> 1).drop("cnt").withColumn("RejectedReason", F.lit("DUPLICATE PRODUCTID")).withColumn("RejectedRule", F.lit("BUSINESS_KEY_VALIDATION"))

# COMMAND ----------

df_valid_product_price = df_valid_products.filter(F.col("ListPrice") >= F.col("StandardCost"))

df_invalid_rule2 = df_valid_products.filter(F.col("ListPrice") < F.col("StandardCost")).withColumn("RejectedReason", F.lit("Selling price is less than standard cost")).withColumn("RejectedRule", F.lit("PRICE_VALIDATION"))



display(df_invalid_product_price)

# COMMAND ----------

df_valid_cost = df_valid_product_price.filter(F.col("StandardCost") > 0)

df_invalid_rule3 = df_valid_product_price.filter(F.col("StandardCost") <= 0).withColumn("RejectedReason", F.lit("standard cost is 0 or less")).withColumn("RejectedRule", F.lit("COST_VALIDATION"))

# COMMAND ----------

df_trimmed = df_valid_cost.select([
    F.trim(F.col(c_name)).alias(c_name) if c_type == "string" 
    else F.col(c_name)
    for c_name, c_type in df_valid_cost.dtypes
])

# COMMAND ----------

df_valid_prod_name = df_trimmed.filter((F.col("ProductName").isNotNull()) & (F.col("ProductName") != ""))

df_invalid_rule4 = df_trimmed.filter((F.col("ProductName").isNull()) | (F.col("ProductName") == "")).withColumn("RejectedReason", F.lit("PRODUCT NAME IS NULL OR BLANK")).withColumn("RejectedRule", F.lit("PRODUCT_NAME_VALIDATION"))


# COMMAND ----------

df_valid_category = df_valid_prod_name.filter((F.col("Category").isNotNull()) & (F.col("Category") != ""))

df_invalid_rule5 = df_valid_prod_name.filter((F.col("Category").isNull()) | (F.col("Category") == "")).withColumn("RejectedReason", F.lit("CATEGORY NAME IS NULL OR BLANK")).withColumn("RejectedRule", F.lit("CATEGORY_NAME_VALIDATION"))

# COMMAND ----------

df_valid_date = df_valid_category.filter((F.col("CreatedDate") <= F.current_timestamp()) & (F.col("CreatedDate").isNotNull()))

df_invalid_rule6 = df_valid_category.filter((F.col("CreatedDate") > F.current_timestamp()) | (F.col("CreatedDate").isNull())).withColumn("RejectedReason", F.lit("DATE IS FUTURE DATE OR NULL")).withColumn("RejectedRule", F.lit("DATE_VALIDATION"))

# COMMAND ----------

df_valid_source_code = df_valid_date.filter((F.col("SourceSystem") == "RetailX") )

df_invalid_rule7 = df_valid_date.filter((F.col("SourceSystem") != "RetailX") | (F.col("SourceSystem").isNull()) ).withColumn("RejectedReason", F.lit("INVALID SOURCE SYSTEM CODE")).withColumn("RejectedRule", F.lit("SOURCE_CODE_VALIDATION"))

# COMMAND ----------

df_valid_source_code.write.format("delta").mode("overwrite").saveAsTable("silver.products")


# COMMAND ----------

df_rejected_products = (
    df_invalid_rule1
        .unionByName(df_invalid_rule2)
        .unionByName(df_invalid_rule3)
        .unionByName(df_invalid_rule4)
        .unionByName(df_invalid_rule5)
        .unionByName(df_invalid_rule6)
        .unionByName(df_invalid_rule7)
        
)

# COMMAND ----------

df_rejected_products.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("silver.products_reject")

# COMMAND ----------

total_records = df_products_bronze.count()

valid_records = df_valid_source_code.count()

rejected_records = df_rejected_products.count()

print(f"Total Records    : {total_records}")
print(f"Valid Records    : {valid_records}")
print(f"Rejected Records : {rejected_records}")