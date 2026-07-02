from src.framework.spark_session import get_spark_session


def main():
    """
    Entry point for the Bronze Product Load Pipeline
    """

    # 1. Create Spark Session
    spark = get_spark_session()
    spark.sparkContext.setLogLevel("ERROR")

    # 2. Read Configuration

    # 3. Read Source File
    df_prod = (spark.read.option("header", "True")
               .option("inferSchema", "true")
               .csv("data/source/Products.csv"))

    #df_prod.show()

    #spark.stop()
    # 4. Validate Source

    # 5. Add Audit Columns

    # 6. Write Bronze Layer
    df_prod.write.mode("overwrite").parquet("data/bronze/products")

    #df_prod_pq = spark.read.parquet("data/bronze/products")
    #df_prod_pq.show()
    # 7. Log Completion


if __name__ == "__main__":
    main()