import pyspark
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("CheckSpark").getOrCreate()

df_pyspark = spark.read.option("header", "true").csv("test.csv")
print(type(df_pyspark))