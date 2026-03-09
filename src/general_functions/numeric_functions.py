"""PySpark Numeric Functions"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, abs, ceil, floor, round, sqrt, pow, log, exp, greatest, least, rand

spark = SparkSession.builder.appName("NumericFunctions").getOrCreate()

data = [
    (1, -15.7, 3.14159, 100.0),
    (2, 22.3, 2.71828, 49.0),
    (3, -8.9, 1.41421, 64.0),
    (4, 45.6, 0.57721, 81.0),
    (5, -33.1, 9.80665, 25.0)
]
df = spark.createDataFrame(data, ["id", "value", "decimal_val", "square_num"])

print("=== abs() ===")
df.select("id", "value", abs(col("value")).alias("absolute")).show()

print("=== round() ===")
df.select("id", "decimal_val", round(col("decimal_val"), 2).alias("rounded")).show()

print("=== ceil() ===")
df.select("id", "value", ceil(col("value")).alias("ceiling")).show()

print("=== floor() ===")
df.select("id", "value", floor(col("value")).alias("floored")).show()

print("=== sqrt() ===")
df.select("id", "square_num", sqrt(col("square_num")).alias("square_root")).show()

print("=== pow() ===")
df.select("id", "square_num", pow(col("square_num"), 2).alias("squared")).show()

print("=== log() ===")
df.select("id", "square_num", log(col("square_num")).alias("natural_log")).show()

print("=== exp() ===")
df.select("id", "decimal_val", exp(col("decimal_val")).alias("exponential")).show()

print("=== greatest() and least() ===")
df.select(
    "id",
    "value",
    "decimal_val",
    greatest(col("value"), col("decimal_val")).alias("max_of_two"),
    least(col("value"), col("decimal_val")).alias("min_of_two")
).show()

print("=== arithmetic operations ===")
df.select(
    "id",
    (col("value") + col("decimal_val")).alias("addition"),
    (col("value") - col("decimal_val")).alias("subtraction"),
    (col("value") * col("decimal_val")).alias("multiplication"),
    (col("value") / col("decimal_val")).alias("division"),
    (col("value") % 10).alias("modulo")
).show()

print("=== rand() ===")
df.select("id", rand().alias("random_value")).show()

spark.stop()
