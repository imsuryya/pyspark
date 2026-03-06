"""PySpark SQL Basics"""
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("SparkSQLBasics").getOrCreate()

data = [("Alice", 25, "HR"), ("Bob", 30, "IT"), ("Charlie", 35, "IT")]
df = spark.createDataFrame(data, ["name", "age", "dept"])

df.createOrReplaceTempView("employees")

spark.sql("SELECT * FROM employees").show()
spark.sql("SELECT * FROM employees WHERE age > 28").show()
spark.sql("SELECT dept, COUNT(*) as count FROM employees GROUP BY dept").show()

spark.stop()
