"""PySpark Date Functions"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_date, current_timestamp, datediff, date_add, year, month

spark = SparkSession.builder.appName("DateFunctions").getOrCreate()

data = [("2024-01-15",), ("2024-06-20",), ("2024-12-25",)]
df = spark.createDataFrame(data, ["date_str"])

df.withColumn("today", current_date()).show()
df.withColumn("year", year(col("date_str"))).show()
df.withColumn("plus_7_days", date_add(col("date_str"), 7)).show()
df.withColumn("days_from_now", datediff(current_date(), col("date_str"))).show()

spark.stop()
