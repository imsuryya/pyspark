"""PySpark Aggregate Functions"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, sum, avg, count, min, max,
    countDistinct, sumDistinct,
    collect_list, collect_set,
    first, last,
    variance, stddev,
    mean, grouping
)

spark = SparkSession.builder.appName("AggregateFunctions").getOrCreate()

data = [
    ("Alice", "Engineering", 60000),
    ("Bob", "Marketing", 45000),
    ("Charlie", "Engineering", 80000),
    ("Diana", "Marketing", 52000),
    ("Eve", "Engineering", 70000),
    ("Frank", "HR", 48000),
    ("Grace", "HR", 55000),
    ("Hank", "Marketing", 47000)
]
df = spark.createDataFrame(data, ["name", "department", "salary"])

print("=== count() ===")
df.select(count("*").alias("total_rows")).show()

print("=== sum() ===")
df.select(sum("salary").alias("total_salary")).show()

print("=== avg() / mean() ===")
df.select(
    avg("salary").alias("avg_salary"),
    mean("salary").alias("mean_salary")
).show()

print("=== min() and max() ===")
df.select(
    min("salary").alias("min_salary"),
    max("salary").alias("max_salary")
).show()

print("=== countDistinct() ===")
df.select(countDistinct("department").alias("unique_departments")).show()

print("=== variance() and stddev() ===")
df.select(
    variance("salary").alias("salary_variance"),
    stddev("salary").alias("salary_stddev")
).show()

print("=== groupBy() with aggregations ===")
df.groupBy("department").agg(
    count("*").alias("employee_count"),
    sum("salary").alias("total_salary"),
    avg("salary").alias("avg_salary"),
    min("salary").alias("min_salary"),
    max("salary").alias("max_salary")
).show()

print("=== collect_list() ===")
df.groupBy("department").agg(
    collect_list("name").alias("employees")
).show(truncate=False)

print("=== collect_set() ===")
df.groupBy("department").agg(
    collect_set("name").alias("unique_employees")
).show(truncate=False)

print("=== first() and last() ===")
df.groupBy("department").agg(
    first("name").alias("first_employee"),
    last("name").alias("last_employee")
).show()

print("=== multiple groupBy aggregations ===")
df.groupBy("department") \
    .agg(avg("salary").alias("avg_salary")) \
    .orderBy(col("avg_salary").desc()) \
    .show()

print("=== pivot() ===")
pivot_data = [
    ("Alice", "Q1", 5000),
    ("Alice", "Q2", 6000),
    ("Bob", "Q1", 4500),
    ("Bob", "Q2", 4800),
    ("Alice", "Q3", 7000),
    ("Bob", "Q3", 5200)
]
pivot_df = spark.createDataFrame(pivot_data, ["name", "quarter", "sales"])
pivot_df.groupBy("name").pivot("quarter").sum("sales").show()

spark.stop()
