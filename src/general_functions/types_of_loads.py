"""
PySpark - Types of Loads (Full Load, Incremental Load, CDC, SCD, Upsert)
========================================================================
Demonstrates different data loading strategies used in ETL/ELT pipelines.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lit, current_timestamp, when, max as spark_max,
    hash as spark_hash, md5, concat_ws, coalesce
)
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType,
    DoubleType, TimestampType, DateType
)

spark = SparkSession.builder \
    .appName("Types of Loads") \
    .master("local[*]") \
    .getOrCreate()

print("=" * 70)
print("1. FULL LOAD (COMPLETE REFRESH)")
print("=" * 70)
print("""
Full Load replaces the entire target dataset with a fresh copy of the source.
Use when:
  - Dataset is small enough to reload completely
  - Source system does not support change tracking
  - You need a guaranteed exact copy of source
""")

source_data = [
    (1, "Alice", "Engineering", 75000.0, "2022-01-15"),
    (2, "Bob", "Sales", 65000.0, "2022-03-20"),
    (3, "Charlie", "Engineering", 80000.0, "2021-08-10"),
    (4, "Diana", "Marketing", 70000.0, "2023-02-01"),
    (5, "Eve", "Finance", 72000.0, "2023-06-15"),
]

source_schema = StructType([
    StructField("id", IntegerType(), False),
    StructField("name", StringType(), True),
    StructField("department", StringType(), True),
    StructField("salary", DoubleType(), True),
    StructField("hire_date", StringType(), True),
])

df_source = spark.createDataFrame(source_data, source_schema)

df_full_load = df_source \
    .withColumn("load_timestamp", current_timestamp()) \
    .withColumn("load_type", lit("FULL"))

print("Full Load Result:")
df_full_load.show(truncate=False)

"""
df_full_load.write \
    .mode("overwrite") \
    .parquet("path/to/target/full_load/")
"""

print("=" * 70)
print("2. INCREMENTAL LOAD (APPEND / DELTA LOAD)")
print("=" * 70)
print("""
Incremental Load only loads new or changed records since the last load.
Use when:
  - Dataset is too large for full refresh
  - Source has a reliable timestamp or sequence column
  - You want to minimize processing time and cost
""")

existing_data = [
    (1, "Alice", "Engineering", 75000.0, "2024-01-01 10:00:00"),
    (2, "Bob", "Sales", 65000.0, "2024-01-01 10:00:00"),
    (3, "Charlie", "Engineering", 80000.0, "2024-01-01 10:00:00"),
]

existing_schema = StructType([
    StructField("id", IntegerType(), False),
    StructField("name", StringType(), True),
    StructField("department", StringType(), True),
    StructField("salary", DoubleType(), True),
    StructField("last_modified", StringType(), True),
])

df_existing = spark.createDataFrame(existing_data, existing_schema)

new_source_data = [
    (1, "Alice", "Engineering", 75000.0, "2024-01-01 10:00:00"),
    (2, "Bob", "Sales", 68000.0, "2024-01-15 14:30:00"),
    (3, "Charlie", "Engineering", 80000.0, "2024-01-01 10:00:00"),
    (4, "Diana", "Marketing", 70000.0, "2024-01-10 09:00:00"),
    (5, "Eve", "Finance", 72000.0, "2024-01-20 11:00:00"),
]

df_new_source = spark.createDataFrame(new_source_data, existing_schema)

last_load_timestamp = df_existing.agg(spark_max("last_modified")).collect()[0][0]
print(f"Last load timestamp: {last_load_timestamp}")

df_incremental = df_new_source.filter(col("last_modified") > lit(last_load_timestamp))

df_incremental_final = df_incremental \
    .withColumn("load_timestamp", current_timestamp()) \
    .withColumn("load_type", lit("INCREMENTAL"))

print("New/Changed Records (Incremental):")
df_incremental_final.show(truncate=False)

"""
df_incremental_final.write \
    .mode("append") \
    .parquet("path/to/target/incremental_load/")
"""

print("=" * 70)
print("3. UPSERT LOAD (MERGE / INSERT + UPDATE)")
print("=" * 70)
print("""
Upsert inserts new records and updates existing ones based on a key.
Use when:
  - Source contains both new and updated records
  - You need to maintain current state in the target
  - Supported natively with Delta Lake MERGE INTO
""")

target_data = [
    (1, "Alice", "Engineering", 75000.0, "active"),
    (2, "Bob", "Sales", 65000.0, "active"),
    (3, "Charlie", "Engineering", 80000.0, "active"),
]

target_schema = StructType([
    StructField("id", IntegerType(), False),
    StructField("name", StringType(), True),
    StructField("department", StringType(), True),
    StructField("salary", DoubleType(), True),
    StructField("status", StringType(), True),
])

df_target = spark.createDataFrame(target_data, target_schema)

updates_data = [
    (2, "Bob", "Sales", 68000.0, "active"),
    (4, "Diana", "Marketing", 70000.0, "active"),
    (5, "Eve", "Finance", 72000.0, "active"),
]

df_updates = spark.createDataFrame(updates_data, target_schema)

df_updates_matched = df_target.alias("t") \
    .join(df_updates.alias("u"), "id", "inner") \
    .select(
        col("t.id"),
        col("u.name"),
        col("u.department"),
        col("u.salary"),
        col("u.status"),
    )

df_target_unchanged = df_target.alias("t") \
    .join(df_updates.alias("u"), "id", "left_anti")

df_new_records = df_updates.alias("u") \
    .join(df_target.alias("t"), "id", "left_anti")

df_upserted = df_target_unchanged \
    .unionByName(df_updates_matched) \
    .unionByName(df_new_records) \
    .withColumn("load_timestamp", current_timestamp())

print("Upserted Result:")
df_upserted.orderBy("id").show(truncate=False)

"""
Delta Lake MERGE syntax (SQL):

MERGE INTO target_table AS t
USING source_table AS s
ON t.id = s.id
WHEN MATCHED THEN
    UPDATE SET t.name = s.name, t.salary = s.salary, t.department = s.department
WHEN NOT MATCHED THEN
    INSERT (id, name, department, salary, status)
    VALUES (s.id, s.name, s.department, s.salary, s.status)
"""

print("=" * 70)
print("4. CHANGE DATA CAPTURE (CDC) LOAD")
print("=" * 70)
print("""
CDC tracks INSERT, UPDATE, DELETE operations from the source system.
Use when:
  - Source provides a change log or CDC events
  - You need to replay exact changes in order
  - Real-time or near-real-time sync is required
""")

cdc_events = [
    (1, "Alice", "Engineering", 75000.0, "I", "2024-01-01 10:00:00"),
    (2, "Bob", "Sales", 65000.0, "I", "2024-01-01 10:00:00"),
    (3, "Charlie", "Engineering", 80000.0, "I", "2024-01-01 10:00:00"),
    (2, "Bob", "Sales", 68000.0, "U", "2024-01-15 14:30:00"),
    (4, "Diana", "Marketing", 70000.0, "I", "2024-01-10 09:00:00"),
    (3, "Charlie", "Engineering", 80000.0, "D", "2024-01-20 11:00:00"),
]

cdc_schema = StructType([
    StructField("id", IntegerType(), False),
    StructField("name", StringType(), True),
    StructField("department", StringType(), True),
    StructField("salary", DoubleType(), True),
    StructField("operation", StringType(), True),
    StructField("event_timestamp", StringType(), True),
])

df_cdc = spark.createDataFrame(cdc_events, cdc_schema)

print("CDC Events Log:")
df_cdc.show(truncate=False)

from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, desc

window_latest = Window.partitionBy("id").orderBy(desc("event_timestamp"))

df_latest_cdc = df_cdc \
    .withColumn("rn", row_number().over(window_latest)) \
    .filter(col("rn") == 1) \
    .drop("rn")

df_cdc_result = df_latest_cdc.filter(col("operation") != "D")

print("Current State After Applying CDC:")
df_cdc_result.orderBy("id").show(truncate=False)

print("=" * 70)
print("5. HASH-BASED INCREMENTAL LOAD")
print("=" * 70)
print("""
Uses hash comparison to detect changed records without timestamps.
Use when:
  - Source has no reliable change tracking column
  - You need to detect changes in any column
  - Works with any source system
""")

old_data = [
    (1, "Alice", "Engineering", 75000.0),
    (2, "Bob", "Sales", 65000.0),
    (3, "Charlie", "Engineering", 80000.0),
]

new_data = [
    (1, "Alice", "Engineering", 75000.0),
    (2, "Bob", "Sales", 68000.0),
    (3, "Charlie", "Data Science", 85000.0),
    (4, "Diana", "Marketing", 70000.0),
]

hash_schema = StructType([
    StructField("id", IntegerType(), False),
    StructField("name", StringType(), True),
    StructField("department", StringType(), True),
    StructField("salary", DoubleType(), True),
])

df_old = spark.createDataFrame(old_data, hash_schema)
df_new = spark.createDataFrame(new_data, hash_schema)

df_old_hashed = df_old.withColumn(
    "row_hash", md5(concat_ws("||", col("name"), col("department"), col("salary").cast("string")))
)

df_new_hashed = df_new.withColumn(
    "row_hash", md5(concat_ws("||", col("name"), col("department"), col("salary").cast("string")))
)

df_changed = df_new_hashed.alias("new") \
    .join(df_old_hashed.alias("old"), "id", "left") \
    .filter(
        col("old.row_hash").isNull() |
        (col("new.row_hash") != col("old.row_hash"))
    ) \
    .select("new.*")

print("Changed/New Records (Hash-based Detection):")
df_changed.show(truncate=False)

print("=" * 70)
print("6. PARTITION-BASED LOAD")
print("=" * 70)
print("""
Loads or overwrites specific partitions while keeping others intact.
Use when:
  - Data is naturally partitioned (by date, region, etc.)
  - You want to reload only affected partitions
  - Efficient for time-series and event data
""")

partition_data = [
    (1, "Alice", "Engineering", 75000.0, "2024-01"),
    (2, "Bob", "Sales", 65000.0, "2024-01"),
    (3, "Charlie", "Engineering", 80000.0, "2024-02"),
    (4, "Diana", "Marketing", 70000.0, "2024-02"),
    (5, "Eve", "Finance", 72000.0, "2024-03"),
]

partition_schema = StructType([
    StructField("id", IntegerType(), False),
    StructField("name", StringType(), True),
    StructField("department", StringType(), True),
    StructField("salary", DoubleType(), True),
    StructField("load_month", StringType(), True),
])

df_partitioned = spark.createDataFrame(partition_data, partition_schema)

print("Partitioned Data:")
df_partitioned.show(truncate=False)

"""
df_partitioned.write \
    .mode("overwrite") \
    .partitionBy("load_month") \
    .parquet("path/to/target/partitioned_data/")

spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
df_new_month = df_partitioned.filter(col("load_month") == "2024-03")
df_new_month.write \
    .mode("overwrite") \
    .partitionBy("load_month") \
    .parquet("path/to/target/partitioned_data/")
"""

print("=" * 70)
print("7. SNAPSHOT LOAD")
print("=" * 70)
print("""
Stores full snapshots at regular intervals for historical comparison.
Use when:
  - Full audit trail of data changes is required
  - Source does not provide CDC
  - You need point-in-time queries
""")

snapshot_data = [
    (1, "Alice", "Engineering", 75000.0, "2024-01-01"),
    (2, "Bob", "Sales", 65000.0, "2024-01-01"),
    (1, "Alice", "Engineering", 78000.0, "2024-02-01"),
    (2, "Bob", "Sales", 68000.0, "2024-02-01"),
    (3, "Charlie", "Engineering", 80000.0, "2024-02-01"),
]

snapshot_schema = StructType([
    StructField("id", IntegerType(), False),
    StructField("name", StringType(), True),
    StructField("department", StringType(), True),
    StructField("salary", DoubleType(), True),
    StructField("snapshot_date", StringType(), True),
])

df_snapshots = spark.createDataFrame(snapshot_data, snapshot_schema)

print("Snapshot History:")
df_snapshots.show(truncate=False)

latest_snapshot = df_snapshots.filter(col("snapshot_date") == "2024-02-01")
print("Latest Snapshot (2024-02-01):")
latest_snapshot.show(truncate=False)

"""
df_snapshots.write \
    .mode("append") \
    .partitionBy("snapshot_date") \
    .parquet("path/to/target/snapshots/")
"""

print("=" * 70)
print("SUMMARY: Types of Loads")
print("=" * 70)
print("""
+----------------------------+---------------------------+-----------------------------+
| Load Type                  | When to Use               | Write Mode                  |
+----------------------------+---------------------------+-----------------------------+
| Full Load                  | Small datasets, no CDC    | overwrite                   |
| Incremental Load           | Large data, has timestamp  | append                      |
| Upsert (Merge)             | Insert + Update needed    | Delta MERGE / manual join   |
| CDC Load                   | Source provides change log | Apply in sequence           |
| Hash-based Incremental     | No timestamp available    | Compare hashes              |
| Partition-based Load       | Partitioned datasets      | dynamic partition overwrite |
| Snapshot Load              | Audit/history required    | append with snapshot_date   |
+----------------------------+---------------------------+-----------------------------+
""")

spark.stop()
