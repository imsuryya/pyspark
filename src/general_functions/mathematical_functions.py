"""PySpark Mathematical Functions"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, sin, cos, tan, asin, acos, atan, atan2,
    degrees, radians, hypot,
    log, log10, log2, log1p,
    factorial, cbrt, signum,
    bin, hex, unhex,
    conv, pmod,
    bround
)
import math

spark = SparkSession.builder.appName("MathematicalFunctions").getOrCreate()

data = [
    (1, 0.0, 1.0, 8.0),
    (2, math.pi / 6, 2.0, 27.0),
    (3, math.pi / 4, 3.0, 64.0),
    (4, math.pi / 3, 4.0, 125.0),
    (5, math.pi / 2, 5.0, -1.0)
]
df = spark.createDataFrame(data, ["id", "angle_rad", "value", "cube_num"])

print("=== sin() / cos() / tan() ===")
df.select(
    "id",
    "angle_rad",
    sin(col("angle_rad")).alias("sine"),
    cos(col("angle_rad")).alias("cosine"),
    tan(col("angle_rad")).alias("tangent")
).show()

print("=== asin() / acos() / atan() ===")
trig_data = [(1, 0.5), (2, 0.0), (3, 1.0), (4, -0.5)]
trig_df = spark.createDataFrame(trig_data, ["id", "val"])
trig_df.select(
    "id",
    "val",
    asin(col("val")).alias("arc_sine"),
    acos(col("val")).alias("arc_cosine"),
    atan(col("val")).alias("arc_tangent")
).show()

print("=== atan2() ===")
coord_data = [(1, 1.0, 1.0), (2, 0.0, 1.0), (3, 1.0, 0.0)]
coord_df = spark.createDataFrame(coord_data, ["id", "y", "x"])
coord_df.select("id", atan2(col("y"), col("x")).alias("angle")).show()

print("=== degrees() / radians() ===")
df.select(
    "id",
    "angle_rad",
    degrees(col("angle_rad")).alias("in_degrees"),
    radians(degrees(col("angle_rad"))).alias("back_to_rad")
).show()

print("=== hypot() (hypotenuse) ===")
coord_df.select("id", "x", "y", hypot(col("x"), col("y")).alias("hypotenuse")).show()

print("=== log() / log10() / log2() / log1p() ===")
df.select(
    "id",
    "value",
    log(col("value")).alias("ln"),
    log10(col("value")).alias("log10"),
    log2(col("value")).alias("log2"),
    log1p(col("value")).alias("ln_1_plus_x")
).show()

print("=== cbrt() (cube root) ===")
df.select("id", "cube_num", cbrt(col("cube_num")).alias("cube_root")).show()

print("=== factorial() ===")
df.select("id", "value", factorial(col("value").cast("long")).alias("factorial")).show()

print("=== signum() (sign of number) ===")
sign_data = [(1, -10.0), (2, 0.0), (3, 25.0)]
sign_df = spark.createDataFrame(sign_data, ["id", "val"])
sign_df.select("id", "val", signum(col("val")).alias("sign")).show()

print("=== bround() (banker's rounding) ===")
round_data = [(1, 2.5), (2, 3.5), (3, 4.5), (4, 5.5)]
round_df = spark.createDataFrame(round_data, ["id", "val"])
round_df.select("id", "val", bround(col("val"), 0).alias("bankers_rounded")).show()

print("=== bin() / hex() ===")
num_data = [(1, 10), (2, 255), (3, 42)]
num_df = spark.createDataFrame(num_data, ["id", "num"])
num_df.select(
    "id",
    "num",
    bin(col("num")).alias("binary"),
    hex(col("num")).alias("hexadecimal")
).show()

print("=== conv() (base conversion) ===")
num_df.select(
    "id",
    "num",
    conv(col("num"), 10, 2).alias("decimal_to_binary"),
    conv(col("num"), 10, 16).alias("decimal_to_hex")
).show()

print("=== pmod() (positive modulo) ===")
mod_data = [(1, 10, 3), (2, -10, 3), (3, 10, -3)]
mod_df = spark.createDataFrame(mod_data, ["id", "a", "b"])
mod_df.select("id", "a", "b", pmod(col("a"), col("b")).alias("positive_mod")).show()

spark.stop()
