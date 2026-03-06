"""PySpark DataFrame Basics"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit

spark = SparkSession.builder.appName("DataFrameBasics").getOrCreate()

data = [("Alice", 25), ("Bob", 30), ("Charlie", 35)]
df = spark.createDataFrame(data, ["name", "age"])

df.show()
df.select("name").show()
df.filter(col("age") > 28).show()
df.withColumn("country", lit("USA")).show()

spark.stop()
