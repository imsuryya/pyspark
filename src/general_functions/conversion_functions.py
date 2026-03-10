"""PySpark Conversion Functions"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, cast, lit,
    to_date, to_timestamp, to_utc_timestamp, from_utc_timestamp,
    from_json, to_json, schema_of_json,
    from_csv, schema_of_csv,
    encode, decode,
    base64, unbase64,
    hex, unhex,
    bin,
    array, create_map,
    explode, explode_outer
)
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType,
    DoubleType, ArrayType, MapType, BooleanType
)

spark = SparkSession.builder.appName("ConversionFunctions").getOrCreate()

# --- Type Casting ---
print("=== cast() - Type Casting ===")
data = [("1", "100.50", "true", "2024-01-15"), ("2", "200.75", "false", "2024-06-20")]
df = spark.createDataFrame(data, ["id_str", "amount_str", "flag_str", "date_str"])

df.select(
    col("id_str").cast(IntegerType()).alias("id_int"),
    col("amount_str").cast(DoubleType()).alias("amount_double"),
    col("flag_str").cast(BooleanType()).alias("flag_bool"),
    col("id_str").cast("long").alias("id_long")
).show()

# --- String to Date/Timestamp ---
print("=== to_date() / to_timestamp() ===")
date_data = [("2024-01-15",), ("01/20/2024",), ("15-Mar-2024",)]
date_df = spark.createDataFrame(date_data, ["date_str"])

date_df.select(
    "date_str",
    to_date(col("date_str"), "yyyy-MM-dd").alias("iso_date"),
    to_date(col("date_str"), "MM/dd/yyyy").alias("us_date"),
    to_date(col("date_str"), "dd-MMM-yyyy").alias("custom_date")
).show()

print("=== to_timestamp() with format ===")
ts_data = [("2024-01-15 10:30:00",), ("2024-06-20 14:45:30",)]
ts_df = spark.createDataFrame(ts_data, ["ts_str"])
ts_df.select(
    "ts_str",
    to_timestamp(col("ts_str"), "yyyy-MM-dd HH:mm:ss").alias("timestamp")
).show(truncate=False)

# --- Timezone Conversions ---
print("=== to_utc_timestamp() / from_utc_timestamp() ===")
ts_df.select(
    "ts_str",
    to_utc_timestamp(to_timestamp(col("ts_str")), "America/New_York").alias("utc_time"),
    from_utc_timestamp(to_timestamp(col("ts_str")), "Asia/Kolkata").alias("kolkata_time")
).show(truncate=False)

# --- JSON Conversions ---
print("=== from_json() / to_json() ===")
json_data = [('{"name": "Alice", "age": 30}',), ('{"name": "Bob", "age": 25}',)]
json_df = spark.createDataFrame(json_data, ["json_str"])

json_schema = StructType([
    StructField("name", StringType()),
    StructField("age", IntegerType())
])

json_df.select(
    from_json(col("json_str"), json_schema).alias("parsed")
).select("parsed.name", "parsed.age").show()

print("=== to_json() from struct ===")
struct_data = [("Alice", 30), ("Bob", 25)]
struct_df = spark.createDataFrame(struct_data, ["name", "age"])
struct_df.select(to_json(create_map(lit("name"), col("name"), lit("age"), col("age"))).alias("json_output")).show(truncate=False)

# --- CSV Conversions ---
print("=== from_csv() ===")
csv_data = [("Alice,30,Engineering",), ("Bob,25,Marketing",)]
csv_df = spark.createDataFrame(csv_data, ["csv_str"])

csv_schema = StructType([
    StructField("name", StringType()),
    StructField("age", IntegerType()),
    StructField("dept", StringType())
])

csv_df.select(from_csv(col("csv_str"), csv_schema).alias("parsed")) \
    .select("parsed.name", "parsed.age", "parsed.dept").show()

# --- Encoding ---
print("=== base64() / unbase64() ===")
enc_data = [("Hello PySpark",), ("Data Engineering",)]
enc_df = spark.createDataFrame(enc_data, ["text"])

enc_df.select(
    "text",
    base64(encode(col("text"), "UTF-8")).alias("base64_encoded")
).show(truncate=False)

enc_df.select(
    "text",
    decode(unbase64(base64(encode(col("text"), "UTF-8"))), "UTF-8").alias("decoded")
).show(truncate=False)

# --- Hex Encoding ---
print("=== hex() / unhex() ===")
hex_data = [(65,), (255,), (1024,)]
hex_df = spark.createDataFrame(hex_data, ["num"])
hex_df.select("num", hex(col("num")).alias("hex_val"), bin(col("num")).alias("bin_val")).show()

# --- Array / Map Conversions ---
print("=== array() / create_map() ===")
arr_data = [("Alice", 80, 90, 85), ("Bob", 70, 75, 80)]
arr_df = spark.createDataFrame(arr_data, ["name", "math", "science", "english"])

arr_df.select(
    "name",
    array(col("math"), col("science"), col("english")).alias("scores"),
    create_map(
        lit("math"), col("math"),
        lit("science"), col("science"),
        lit("english"), col("english")
    ).alias("scores_map")
).show(truncate=False)

# --- Explode (Collection to Rows) ---
print("=== explode() / explode_outer() ===")
list_data = [("Alice", ["Python", "Java"]), ("Bob", ["Scala"]), ("Charlie", None)]
list_df = spark.createDataFrame(list_data, ["name", "skills"])

list_df.select("name", explode_outer(col("skills")).alias("skill")).show()

spark.stop()
