"""PySpark String Functions"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lit, concat, concat_ws, length, trim, ltrim, rtrim,
    lower, upper, initcap, substring, split, regexp_replace,
    regexp_extract, lpad, rpad, reverse, instr, locate,
    translate, repeat, overlay
)

spark = SparkSession.builder.appName("StringFunctions").getOrCreate()

data = [
    ("  Alice Johnson  ", "alice@email.com", "123-456-7890"),
    ("  Bob Smith  ", "bob@email.com", "987-654-3210"),
    ("  Charlie Brown  ", "charlie@email.com", "555-123-4567")
]
df = spark.createDataFrame(data, ["name", "email", "phone"])

print("=== trim() / ltrim() / rtrim() ===")
df.select(
    trim(col("name")).alias("trimmed"),
    ltrim(col("name")).alias("left_trimmed"),
    rtrim(col("name")).alias("right_trimmed")
).show()

print("=== upper() / lower() / initcap() ===")
df.select(
    upper(trim(col("name"))).alias("upper"),
    lower(trim(col("name"))).alias("lower"),
    initcap(trim(col("name"))).alias("initcap")
).show()

print("=== length() ===")
df.select("name", length(trim(col("name"))).alias("name_length")).show()

print("=== substring() ===")
df.select(trim(col("name")).alias("name"), substring(trim(col("name")), 1, 5).alias("first_5")).show()

print("=== concat() / concat_ws() ===")
df.select(
    concat(lit("Hello, "), trim(col("name"))).alias("greeting"),
    concat_ws(" | ", trim(col("name")), col("email")).alias("combined")
).show(truncate=False)

print("=== split() ===")
df.select("phone", split(col("phone"), "-").alias("phone_parts")).show()

print("=== regexp_replace() ===")
df.select("phone", regexp_replace(col("phone"), "-", "").alias("digits_only")).show()

print("=== regexp_extract() ===")
df.select("email", regexp_extract(col("email"), r"^(\w+)@", 1).alias("username")).show()

print("=== lpad() / rpad() ===")
df.select(
    trim(col("name")).alias("name"),
    lpad(trim(col("name")), 20, "*").alias("left_padded"),
    rpad(trim(col("name")), 20, "*").alias("right_padded")
).show()

print("=== reverse() ===")
df.select(trim(col("name")).alias("name"), reverse(trim(col("name"))).alias("reversed")).show()

print("=== instr() / locate() ===")
df.select(
    trim(col("name")).alias("name"),
    instr(trim(col("name")), "o").alias("instr_o"),
    locate("o", trim(col("name"))).alias("locate_o")
).show()

print("=== translate() ===")
df.select("phone", translate(col("phone"), "-", ".").alias("dotted_phone")).show()

print("=== repeat() ===")
df.select(trim(col("name")).alias("name"), repeat(trim(col("name")), 2).alias("doubled")).show(truncate=False)

spark.stop()
