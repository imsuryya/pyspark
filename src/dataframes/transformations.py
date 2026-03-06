"""PySpark DataFrame Transformations"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, upper, when

spark = SparkSession.builder.appName("Transformations").getOrCreate()

data = [("Alice", 25, "F"), ("Bob", 30, "M"), ("Charlie", 35, "M")]
df = spark.createDataFrame(data, ["name", "age", "gender"])

df.withColumnRenamed("name", "full_name").show()
df.drop("gender").show()
df.withColumn("age_group", 
    when(col("age") < 30, "Young")
    .otherwise("Adult")
).show()
df.withColumn("name_upper", upper(col("name"))).show()

spark.stop()
