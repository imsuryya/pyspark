"""PySpark Window Functions"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, sum, avg, count, min, max,
    row_number, rank, dense_rank, percent_rank, ntile,
    lag, lead,
    first, last,
    cume_dist
)
from pyspark.sql.window import Window

spark = SparkSession.builder.appName("WindowFunctions").getOrCreate()

data = [
    ("Alice", "Engineering", 80000),
    ("Bob", "Engineering", 75000),
    ("Charlie", "Engineering", 90000),
    ("Diana", "Marketing", 60000),
    ("Eve", "Marketing", 65000),
    ("Frank", "Marketing", 55000),
    ("Grace", "HR", 50000),
    ("Hank", "HR", 52000),
    ("Ivy", "HR", 48000),
    ("Jack", "Engineering", 85000)
]
df = spark.createDataFrame(data, ["name", "department", "salary"])

# --- Ranking Functions ---
dept_window = Window.partitionBy("department").orderBy(col("salary").desc())

print("=== row_number() ===")
df.withColumn("row_num", row_number().over(dept_window)).show()

print("=== rank() ===")
df.withColumn("rank", rank().over(dept_window)).show()

print("=== dense_rank() ===")
df.withColumn("dense_rank", dense_rank().over(dept_window)).show()

print("=== percent_rank() ===")
df.withColumn("pct_rank", percent_rank().over(dept_window)).show()

print("=== ntile() (split into N buckets) ===")
df.withColumn("quartile", ntile(4).over(dept_window)).show()

# --- Analytic Functions ---
print("=== lag() / lead() ===")
df.select(
    "name", "department", "salary",
    lag("salary", 1).over(dept_window).alias("prev_salary"),
    lead("salary", 1).over(dept_window).alias("next_salary")
).show()

print("=== lag() for salary difference ===")
df.select(
    "name", "department", "salary",
    (col("salary") - lag("salary", 1).over(dept_window)).alias("diff_from_prev")
).show()

print("=== first() / last() within window ===")
df.select(
    "name", "department", "salary",
    first("name").over(dept_window).alias("highest_paid"),
    last("name").over(dept_window).alias("lowest_paid")
).show()

print("=== cume_dist() ===")
df.withColumn("cumulative_dist", cume_dist().over(dept_window)).show()

# --- Aggregate Window Functions ---
print("=== Running total with sum() ===")
df.select(
    "name", "department", "salary",
    sum("salary").over(dept_window).alias("running_total")
).show()

print("=== Cumulative average with avg() ===")
rows_window = Window.partitionBy("department").orderBy("salary").rowsBetween(Window.unboundedPreceding, Window.currentRow)
df.select(
    "name", "department", "salary",
    avg("salary").over(rows_window).alias("cumulative_avg")
).show()

print("=== Department-level aggregates alongside rows ===")
unordered_window = Window.partitionBy("department")
df.select(
    "name", "department", "salary",
    sum("salary").over(unordered_window).alias("dept_total"),
    avg("salary").over(unordered_window).alias("dept_avg"),
    min("salary").over(unordered_window).alias("dept_min"),
    max("salary").over(unordered_window).alias("dept_max"),
    count("*").over(unordered_window).alias("dept_count")
).show()

print("=== Percentage of department total ===")
df.select(
    "name", "department", "salary",
    (col("salary") / sum("salary").over(unordered_window) * 100).alias("pct_of_dept")
).show()

# --- Window Frame Specifications ---
print("=== Moving average (3-row window) ===")
moving_window = Window.partitionBy("department").orderBy("salary") \
    .rowsBetween(-1, 1)

df.select(
    "name", "department", "salary",
    avg("salary").over(moving_window).alias("moving_avg_3")
).show()

print("=== rangeBetween() ===")
range_window = Window.partitionBy("department").orderBy("salary") \
    .rangeBetween(-5000, 5000)

df.select(
    "name", "department", "salary",
    count("*").over(range_window).alias("similar_salary_count")
).show()

# --- Top N per Group ---
print("=== Top 2 earners per department ===")
df.withColumn("rn", row_number().over(dept_window)) \
    .filter(col("rn") <= 2) \
    .drop("rn") \
    .show()

spark.stop()
