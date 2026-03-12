"""
PySpark - Slowly Changing Dimensions (SCD), Write Methods & Table Operations
=============================================================================
This module covers data warehousing concepts including SCD types,
various write methods, and table operations in PySpark.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lit, current_timestamp, when, coalesce,
    max as spark_max, row_number, to_date
)
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType, BooleanType

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("SCD_WriteMethods_Tables") \
    .master("local[*]") \
    .config("spark.sql.warehouse.dir", "spark-warehouse") \
    .enableHiveSupport() \
    .getOrCreate()

# ============================================================================
# SLOWLY CHANGING DIMENSIONS (SCD) - OVERVIEW
# ============================================================================
"""
SLOWLY CHANGING DIMENSIONS (SCD)
================================
SCD is a data warehousing concept that tracks changes in dimension data over time.
Dimensions are descriptive attributes (e.g., customer info, product details).

Common SCD Types:
-----------------
| Type   | Description                                    | History Preserved |
|--------|------------------------------------------------|-------------------|
| Type 0 | Fixed/Original - No changes allowed            | No               |
| Type 1 | Overwrite - Update existing record             | No               |
| Type 2 | Add new row - Full history tracking            | Yes (Full)       |
| Type 3 | Add new column - Limited history               | Yes (Limited)    |
| Type 4 | Mini-dimension - Separate history table        | Yes              |
| Type 6 | Hybrid (1+2+3) - Combined approach             | Yes (Hybrid)     |
"""

# Sample dimension data
customer_schema = StructType([
    StructField("customer_id", IntegerType(), False),
    StructField("name", StringType(), True),
    StructField("city", StringType(), True),
    StructField("email", StringType(), True),
    StructField("tier", StringType(), True)
])

# Existing dimension data
existing_customers = [
    (1, "Alice Johnson", "New York", "alice@email.com", "Gold"),
    (2, "Bob Smith", "Los Angeles", "bob@email.com", "Silver"),
    (3, "Charlie Brown", "Chicago", "charlie@email.com", "Bronze"),
]

# New/Updated customer data (incoming changes)
updated_customers = [
    (1, "Alice Johnson", "Boston", "alice.new@email.com", "Platinum"),  # Changed city, email, tier
    (2, "Bob Smith", "Los Angeles", "bob@email.com", "Gold"),           # Changed tier only
    (4, "Diana Prince", "Seattle", "diana@email.com", "Silver"),        # New customer
]

df_existing = spark.createDataFrame(existing_customers, customer_schema)
df_updates = spark.createDataFrame(updated_customers, customer_schema)

print("=" * 70)
print("EXISTING CUSTOMER DIMENSION")
print("=" * 70)
df_existing.show(truncate=False)

print("=" * 70)
print("INCOMING UPDATES")
print("=" * 70)
df_updates.show(truncate=False)

# ============================================================================
# SCD TYPE 0 - FIXED/ORIGINAL (NO CHANGES)
# ============================================================================
print("=" * 70)
print("SCD TYPE 0 - FIXED/ORIGINAL (No Changes Allowed)")
print("=" * 70)
print("""
SCD Type 0: Original values are never modified.
- Use case: Audit data, original registration date, SSN
- Strategy: Simply ignore updates to existing records
""")

# Only insert new records, never update existing ones
scd0_result = df_existing.union(
    df_updates.join(df_existing, "customer_id", "left_anti")
)
print("SCD Type 0 Result (only new records added):")
scd0_result.show(truncate=False)

# ============================================================================
# SCD TYPE 1 - OVERWRITE (NO HISTORY)
# ============================================================================
print("=" * 70)
print("SCD TYPE 1 - OVERWRITE (No History Preserved)")
print("=" * 70)
print("""
SCD Type 1: Simply overwrite old values with new values.
- Use case: Correcting data errors, non-critical attributes
- Pros: Simple, no storage overhead
- Cons: History is lost
""")

# Get unchanged records (not in updates)
unchanged = df_existing.join(df_updates, "customer_id", "left_anti")
# Combine with all updates (new + modified)
scd1_result = unchanged.union(df_updates)

print("SCD Type 1 Result (existing records overwritten):")
scd1_result.orderBy("customer_id").show(truncate=False)

# ============================================================================
# SCD TYPE 2 - ADD NEW ROW (FULL HISTORY)
# ============================================================================
print("=" * 70)
print("SCD TYPE 2 - ADD NEW ROW (Full History Tracking)")
print("=" * 70)
print("""
SCD Type 2: Add a new row for each change, maintaining full history.
- Use case: Customer address changes, pricing history
- Key columns: surrogate_key, effective_date, end_date, is_current
- Pros: Complete history preserved
- Cons: Table grows with each change
""")

# SCD Type 2 Schema with history tracking columns
scd2_schema = StructType([
    StructField("surrogate_key", IntegerType(), False),
    StructField("customer_id", IntegerType(), False),
    StructField("name", StringType(), True),
    StructField("city", StringType(), True),
    StructField("email", StringType(), True),
    StructField("tier", StringType(), True),
    StructField("effective_date", StringType(), True),
    StructField("end_date", StringType(), True),
    StructField("is_current", BooleanType(), True)
])

# Existing SCD2 dimension (with history columns)
existing_scd2 = [
    (1, 1, "Alice Johnson", "New York", "alice@email.com", "Gold", "2023-01-01", "9999-12-31", True),
    (2, 2, "Bob Smith", "Los Angeles", "bob@email.com", "Silver", "2023-01-01", "9999-12-31", True),
    (3, 3, "Charlie Brown", "Chicago", "charlie@email.com", "Bronze", "2023-01-01", "9999-12-31", True),
]

df_scd2_existing = spark.createDataFrame(existing_scd2, scd2_schema)

# Process SCD2 updates
# Step 1: Identify changed records
joined = df_scd2_existing.filter(col("is_current") == True).alias("existing").join(
    df_updates.alias("updates"),
    col("existing.customer_id") == col("updates.customer_id"),
    "inner"
)

# Step 2: Find records that actually changed
changed_records = joined.filter(
    (col("existing.city") != col("updates.city")) |
    (col("existing.email") != col("updates.email")) |
    (col("existing.tier") != col("updates.tier"))
).select("existing.*")

# Step 3: Close old records (set end_date and is_current = False)
closed_records = changed_records.withColumn("end_date", lit("2024-01-15")).withColumn("is_current", lit(False))

# Step 4: Create new current records
max_surrogate = df_scd2_existing.agg(spark_max("surrogate_key")).collect()[0][0]

new_records = df_updates.join(
    changed_records.select("customer_id"),
    "customer_id",
    "inner"
).select(
    lit(0).alias("surrogate_key"),  # Will be assigned properly
    col("customer_id"),
    col("name"),
    col("city"),
    col("email"),
    col("tier"),
    lit("2024-01-15").alias("effective_date"),
    lit("9999-12-31").alias("end_date"),
    lit(True).alias("is_current")
)

# Add row numbers for surrogate keys
window = Window.orderBy("customer_id")
new_records = new_records.withColumn(
    "surrogate_key", 
    row_number().over(window) + max_surrogate
)

# Step 5: Get unchanged records
unchanged_records = df_scd2_existing.join(
    changed_records.select("customer_id"),
    "customer_id",
    "left_anti"
)

# Step 6: Insert new customers (not in existing)
new_customers = df_updates.join(df_scd2_existing.select("customer_id").distinct(), "customer_id", "left_anti")
max_surrogate_new = max_surrogate + new_records.count()
new_customer_records = new_customers.select(
    lit(0).alias("surrogate_key"),
    col("customer_id"),
    col("name"),
    col("city"),
    col("email"),
    col("tier"),
    lit("2024-01-15").alias("effective_date"),
    lit("9999-12-31").alias("end_date"),
    lit(True).alias("is_current")
).withColumn("surrogate_key", row_number().over(window) + max_surrogate_new)

# Final SCD2 Result
scd2_result = unchanged_records.union(closed_records).union(new_records).union(new_customer_records)

print("SCD Type 2 Result (full history preserved):")
scd2_result.orderBy("customer_id", "effective_date").show(truncate=False)

# ============================================================================
# SCD TYPE 3 - ADD NEW COLUMN (LIMITED HISTORY)
# ============================================================================
print("=" * 70)
print("SCD TYPE 3 - ADD NEW COLUMN (Limited History)")
print("=" * 70)
print("""
SCD Type 3: Add columns to track previous values.
- Use case: When only one previous value needs to be tracked
- Key columns: current_value, previous_value
- Pros: Simple queries, single row per record
- Cons: Limited history (typically only one prior value)
""")

# SCD Type 3: Track current and previous city
scd3_schema = StructType([
    StructField("customer_id", IntegerType(), False),
    StructField("name", StringType(), True),
    StructField("current_city", StringType(), True),
    StructField("previous_city", StringType(), True),
    StructField("email", StringType(), True),
    StructField("tier", StringType(), True)
])

existing_scd3 = [
    (1, "Alice Johnson", "New York", None, "alice@email.com", "Gold"),
    (2, "Bob Smith", "Los Angeles", None, "bob@email.com", "Silver"),
    (3, "Charlie Brown", "Chicago", None, "charlie@email.com", "Bronze"),
]

df_scd3_existing = spark.createDataFrame(existing_scd3, scd3_schema)

# Apply SCD3 logic for city changes
scd3_result = df_scd3_existing.alias("existing").join(
    df_updates.alias("updates"),
    col("existing.customer_id") == col("updates.customer_id"),
    "left"
).select(
    col("existing.customer_id"),
    coalesce(col("updates.name"), col("existing.name")).alias("name"),
    coalesce(col("updates.city"), col("existing.current_city")).alias("current_city"),
    when(col("updates.city") != col("existing.current_city"), col("existing.current_city"))
        .otherwise(col("existing.previous_city")).alias("previous_city"),
    coalesce(col("updates.email"), col("existing.email")).alias("email"),
    coalesce(col("updates.tier"), col("existing.tier")).alias("tier")
)

# Add new customers
new_customers_scd3 = df_updates.join(df_scd3_existing.select("customer_id"), "customer_id", "left_anti")
new_customers_scd3 = new_customers_scd3.select(
    col("customer_id"),
    col("name"),
    col("city").alias("current_city"),
    lit(None).alias("previous_city"),
    col("email"),
    col("tier")
)

scd3_final = scd3_result.union(new_customers_scd3)

print("SCD Type 3 Result (previous city tracked):")
scd3_final.orderBy("customer_id").show(truncate=False)

# ============================================================================
# WRITE METHODS - COMPREHENSIVE OVERVIEW
# ============================================================================
print("=" * 70)
print("WRITE METHODS IN PYSPARK")
print("=" * 70)
print("""
WRITE MODES:
============
| Mode              | Description                                        |
|-------------------|---------------------------------------------------|
| "error"           | Throw error if path exists (default)              |
| "errorifexists"   | Same as "error"                                   |
| "append"          | Append to existing data                           |
| "overwrite"       | Overwrite existing data                           |
| "ignore"          | Silently ignore if data exists                    |

SYNTAX OPTIONS:
===============
# Method 1: Using DataFrameWriter methods
df.write.mode("overwrite").format("parquet").save("path/to/output")

# Method 2: Using format-specific methods
df.write.mode("overwrite").parquet("path/to/output")
df.write.mode("overwrite").csv("path/to/output")
df.write.mode("overwrite").json("path/to/output")
df.write.mode("overwrite").orc("path/to/output")

# Method 3: Save as Table
df.write.mode("overwrite").saveAsTable("database.table_name")

# Method 4: Insert Into (append mode only)
df.write.insertInto("database.table_name")
""")

# ============================================================================
# WRITE METHOD EXAMPLES
# ============================================================================
print("=" * 70)
print("WRITE METHOD EXAMPLES")
print("=" * 70)

sample_data = [
    (1, "Product A", 100.0),
    (2, "Product B", 200.0),
    (3, "Product C", 150.0)
]
df_sample = spark.createDataFrame(sample_data, ["id", "name", "price"])

# Example 1: Write with Partitioning
print("""
1. WRITE WITH PARTITIONING:
---------------------------
df.write \\
    .mode("overwrite") \\
    .partitionBy("department", "year") \\
    .parquet("path/to/partitioned_data")

Benefits:
- Faster queries when filtering on partition columns
- Better data organization
- Partition pruning for performance
""")

# Example 2: Write with Bucketing
print("""
2. WRITE WITH BUCKETING:
------------------------
df.write \\
    .mode("overwrite") \\
    .bucketBy(4, "customer_id") \\
    .sortBy("order_date") \\
    .saveAsTable("orders_bucketed")

Benefits:
- Optimizes joins on bucket columns
- Reduces shuffle operations
- Must be saved as table (not file path)
""")

# Example 3: Write with Compression
print("""
3. WRITE WITH COMPRESSION:
--------------------------
# Parquet with snappy compression (default)
df.write.mode("overwrite") \\
    .option("compression", "snappy") \\
    .parquet("path/to/output")

# Available compression codecs:
# Parquet: none, snappy, gzip, lzo, brotli, lz4, zstd
# ORC: none, snappy, zlib, lzo
# CSV/JSON: none, bzip2, gzip, lz4, snappy, deflate
""")

# Example 4: Write with Coalesce/Repartition
print("""
4. CONTROL OUTPUT FILE COUNT:
-----------------------------
# Reduce number of output files (no shuffle)
df.coalesce(1).write.mode("overwrite").parquet("path/to/single_file")

# Control partitions with shuffle (rebalances data)
df.repartition(10).write.mode("overwrite").parquet("path/to/multi_files")

# Repartition by column
df.repartition("department").write.mode("overwrite").parquet("path/to/by_dept")
""")

# Example 5: Write Options for Different Formats
print("""
5. FORMAT-SPECIFIC OPTIONS:
---------------------------

CSV Options:
df.write.mode("overwrite") \\
    .option("header", "true") \\
    .option("delimiter", ",") \\
    .option("quote", '"') \\
    .option("escape", "\\\\") \\
    .option("nullValue", "NULL") \\
    .option("dateFormat", "yyyy-MM-dd") \\
    .option("timestampFormat", "yyyy-MM-dd HH:mm:ss") \\
    .csv("path/to/output.csv")

JSON Options:
df.write.mode("overwrite") \\
    .option("dateFormat", "yyyy-MM-dd") \\
    .option("timestampFormat", "yyyy-MM-dd'T'HH:mm:ss") \\
    .option("compression", "gzip") \\
    .json("path/to/output.json")

Parquet Options:
df.write.mode("overwrite") \\
    .option("compression", "snappy") \\
    .option("parquet.bloom.filter.enabled", "true") \\
    .parquet("path/to/output.parquet")
""")

# ============================================================================
# TABLE OPERATIONS
# ============================================================================
print("=" * 70)
print("TABLE OPERATIONS IN PYSPARK")
print("=" * 70)

# Create sample table for demonstrations
df_sample.write.mode("overwrite").saveAsTable("sample_products")

print("""
TABLE CREATION METHODS:
=======================

1. CREATE TABLE FROM DATAFRAME:
-------------------------------
df.write.mode("overwrite").saveAsTable("my_table")

2. CREATE TABLE WITH SPECIFIC FORMAT:
-------------------------------------
df.write \\
    .mode("overwrite") \\
    .format("parquet") \\
    .saveAsTable("my_parquet_table")

3. CREATE EXTERNAL TABLE:
-------------------------
df.write \\
    .mode("overwrite") \\
    .option("path", "/path/to/external/location") \\
    .saveAsTable("external_table")

4. CREATE TABLE USING SQL:
--------------------------
spark.sql('''
    CREATE TABLE IF NOT EXISTS my_table (
        id INT,
        name STRING,
        price DOUBLE
    )
    USING parquet
    PARTITIONED BY (year INT, month INT)
''')
""")

# Table Operations Examples
print("""
TABLE READ & QUERY OPERATIONS:
==============================

# Read table as DataFrame
df = spark.table("my_table")

# Query with SQL
result = spark.sql("SELECT * FROM my_table WHERE price > 100")

# List all tables
spark.sql("SHOW TABLES").show()

# Describe table schema
spark.sql("DESCRIBE my_table").show()

# Show detailed table info
spark.sql("DESCRIBE EXTENDED my_table").show(truncate=False)

# Get table properties
spark.sql("SHOW TBLPROPERTIES my_table").show()
""")

# List tables
print("Available Tables:")
spark.sql("SHOW TABLES").show()

print("""
TABLE MAINTENANCE OPERATIONS:
=============================

# Rename table
spark.sql("ALTER TABLE old_name RENAME TO new_name")

# Add column
spark.sql("ALTER TABLE my_table ADD COLUMNS (new_col STRING)")

# Drop column (requires specific formats like Delta Lake)
# spark.sql("ALTER TABLE my_table DROP COLUMN old_col")

# Change column type (limited support)
spark.sql("ALTER TABLE my_table CHANGE COLUMN name name STRING COMMENT 'Product name'")

# Add partition
spark.sql("ALTER TABLE my_table ADD PARTITION (year=2024, month=1)")

# Drop partition
spark.sql("ALTER TABLE my_table DROP PARTITION (year=2024, month=1)")

# Set table properties
spark.sql("ALTER TABLE my_table SET TBLPROPERTIES ('description' = 'Product catalog')")

# Refresh table metadata
spark.sql("REFRESH TABLE my_table")

# Repair table (discover partitions)
spark.sql("MSCK REPAIR TABLE my_table")
""")

print("""
TABLE DROP & TRUNCATE:
======================

# Drop table (removes data and metadata)
spark.sql("DROP TABLE IF EXISTS my_table")

# Drop only if table exists
spark.sql("DROP TABLE IF EXISTS my_table PURGE")

# Truncate table (removes data, keeps structure)
spark.sql("TRUNCATE TABLE my_table")

# Drop external table (metadata only, keeps files)
# - External tables: Only metadata removed
# - Managed tables: Both metadata and data removed
""")

# ============================================================================
# INSERT OPERATIONS
# ============================================================================
print("=" * 70)
print("INSERT OPERATIONS")
print("=" * 70)
print("""
INSERT METHODS:
===============

1. INSERT INTO (Append):
------------------------
df.write.insertInto("my_table")

# Or with SQL
spark.sql("INSERT INTO my_table VALUES (1, 'New Product', 99.99)")

2. INSERT OVERWRITE:
--------------------
df.write.mode("overwrite").insertInto("my_table")

# Or with SQL
spark.sql("INSERT OVERWRITE TABLE my_table SELECT * FROM source_table")

3. INSERT OVERWRITE PARTITION:
------------------------------
spark.sql('''
    INSERT OVERWRITE TABLE my_table PARTITION (year=2024, month=1)
    SELECT id, name, price FROM source_table
''')

4. DYNAMIC PARTITION INSERT:
----------------------------
spark.sql('''
    INSERT OVERWRITE TABLE my_table PARTITION (year, month)
    SELECT id, name, price, year, month FROM source_table
''')
""")

# ============================================================================
# CTAS & CVAS OPERATIONS
# ============================================================================
print("=" * 70)
print("CTAS & CVAS OPERATIONS")
print("=" * 70)
print("""
CREATE TABLE AS SELECT (CTAS):
==============================
spark.sql('''
    CREATE TABLE new_table AS
    SELECT * FROM existing_table
    WHERE price > 100
''')

# With partitioning
spark.sql('''
    CREATE TABLE partitioned_table
    USING parquet
    PARTITIONED BY (department)
    AS SELECT * FROM source_table
''')

CREATE VIEW AS SELECT:
======================
# Create view
spark.sql('''
    CREATE VIEW expensive_products AS
    SELECT * FROM products WHERE price > 1000
''')

# Create or replace view
spark.sql('''
    CREATE OR REPLACE VIEW product_summary AS
    SELECT department, COUNT(*) as count, AVG(price) as avg_price
    FROM products
    GROUP BY department
''')

# Create temporary view
df.createOrReplaceTempView("temp_view")

# Create global temporary view (accessible across sessions)
df.createOrReplaceGlobalTempView("global_temp_view")
# Query: SELECT * FROM global_temp.global_temp_view
""")

# ============================================================================
# MERGE OPERATIONS (UPSERT)
# ============================================================================
print("=" * 70)
print("MERGE OPERATIONS (UPSERT)")
print("=" * 70)
print("""
MERGE / UPSERT PATTERN:
=======================
Note: Native MERGE is available in Delta Lake, Iceberg, Hudi.
For standard Spark, use this pattern:

# Step 1: Identify matching records
matched = existing_df.join(updates_df, "key_column", "inner")

# Step 2: Get records to update (from updates)
to_update = updates_df.join(existing_df.select("key_column"), "key_column", "inner")

# Step 3: Get records to insert (new in updates)
to_insert = updates_df.join(existing_df.select("key_column"), "key_column", "left_anti")

# Step 4: Get unchanged records
unchanged = existing_df.join(updates_df.select("key_column"), "key_column", "left_anti")

# Step 5: Combine results
result = unchanged.union(to_update).union(to_insert)


DELTA LAKE MERGE SYNTAX:
========================
from delta.tables import DeltaTable

delta_table = DeltaTable.forPath(spark, "/path/to/delta/table")

delta_table.alias("target").merge(
    updates_df.alias("source"),
    "target.id = source.id"
).whenMatchedUpdate(
    set = {
        "name": "source.name",
        "price": "source.price"
    }
).whenNotMatchedInsert(
    values = {
        "id": "source.id",
        "name": "source.name",
        "price": "source.price"
    }
).execute()
""")

# ============================================================================
# SUMMARY TABLE
# ============================================================================
print("=" * 70)
print("QUICK REFERENCE SUMMARY")
print("=" * 70)
print("""
+-------------------+----------------------------------+------------------------+
|     Operation     |           Code Example           |         Notes          |
+-------------------+----------------------------------+------------------------+
| Write to file     | df.write.parquet("path")         | Creates directory      |
| Write to table    | df.write.saveAsTable("tbl")      | Creates managed table  |
| Append to table   | df.write.insertInto("tbl")       | Must exist             |
| Overwrite table   | df.write.mode("ow").save()       | Replaces all data      |
| Create view       | df.createTempView("v")           | Session-scoped         |
| Read table        | spark.table("tbl")               | Returns DataFrame      |
| Query SQL         | spark.sql("SELECT...")           | Returns DataFrame      |
| Drop table        | spark.sql("DROP TABLE t")        | Removes permanently    |
+-------------------+----------------------------------+------------------------+

SCD TYPE DECISION GUIDE:
========================
+--------+------------------+---------------------------+----------------------+
|  Type  |   When to Use    |       Implementation      |     Complexity       |
+--------+------------------+---------------------------+----------------------+
|   0    | Immutable data   | Reject updates            | Very Low             |
|   1    | Latest only      | UPDATE statement          | Low                  |
|   2    | Full history     | New row + versioning      | High                 |
|   3    | Limited history  | Previous value column     | Medium               |
|   6    | Complex needs    | Combine Type 1+2+3        | Very High            |
+--------+------------------+---------------------------+----------------------+
""")

# Cleanup
spark.sql("DROP TABLE IF EXISTS sample_products")

print("\n" + "=" * 70)
print("END OF SCD, WRITE METHODS & TABLE OPERATIONS GUIDE")
print("=" * 70)
