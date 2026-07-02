from pyspark.sql import SparkSession

def get_spark_session():
    """
    Creates and returns a SparkSession.
    """
    spark = ( 
               SparkSession.builder.appName("RetailX Enterprise Data Platform")
               .master("local[*]")
               .getOrCreate()  
            )
    return spark