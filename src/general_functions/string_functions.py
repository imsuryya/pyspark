"""PySpark String Functions"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, concat, length, trim, lower, upper, substring, lit

spark = SparkSession.builder.appName("StringFunctions").getOrCreate()

data = [("  Alice  ",), ("  Bob  ",), ("Charlie",)]
df = spark.createDataFrame(data, ["name"])

df.withColumn("trimmed", trim(col("name"))).show()
df.withColumn("len", length(trim(col("name")))).show()
df.withColumn("first_3", substring(trim(col("name")), 1, 3)).show()
df.withColumn("greeting", concat(lit("Hello, "), trim(col("name")))).show()

spark.stop()
