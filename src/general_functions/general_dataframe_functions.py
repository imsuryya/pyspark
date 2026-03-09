"""PySpark General DataFrame Functions"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, when, coalesce, expr
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

spark = SparkSession.builder.appName("GeneralDataFrameFunctions").getOrCreate()

schema = StructType([
    StructField("name", StringType()),
    StructField("age", IntegerType()),
    StructField("salary", DoubleType()),
    StructField("department", StringType())
])

data = [
    ("Alice", 30, 60000.0, "Engineering"),
    ("Bob", 25, 45000.0, "Marketing"),
    ("Charlie", 35, 80000.0, "Engineering"),
    ("Diana", 28, 52000.0, None),
    ("Eve", 32, 70000.0, "Marketing")
]

df = spark.createDataFrame(data, schema)

print("=== show() ===")
df.show()

print("=== printSchema() ===")
df.printSchema()

print("=== describe() ===")
df.describe().show()

print("=== select() ===")
df.select("name", "salary").show()

print("=== filter() ===")
df.filter(col("age") > 28).show()

print("=== withColumn() ===")
df.withColumn("bonus", col("salary") * 0.1).show()

print("=== withColumnRenamed() ===")
df.withColumnRenamed("name", "employee_name").show()

print("=== drop() ===")
df.drop("department").show()

print("=== dropDuplicates() ===")
df.dropDuplicates(["department"]).show()

print("=== orderBy() ===")
df.orderBy(col("salary").desc()).show()

print("=== limit() ===")
df.limit(3).show()

print("=== when() / otherwise() ===")
df.withColumn("level",
    when(col("salary") >= 70000, "Senior")
    .when(col("salary") >= 50000, "Mid")
    .otherwise("Junior")
).show()

print("=== coalesce() for null handling ===")
df.withColumn("department", coalesce(col("department"), lit("Unknown"))).show()

print("=== distinct() ===")
df.select("department").distinct().show()

print("=== column count and row count ===")
print("Columns:", len(df.columns))
print("Rows:", df.count())

print("=== collect() ===")
rows = df.collect()
for row in rows:
    print(row["name"], row["salary"])

print("=== alias() ===")
df.select(col("name").alias("employee"), col("salary").alias("pay")).show()

print("=== expr() ===")
df.withColumn("annual_bonus", expr("salary * 0.15")).show()

spark.stop()
