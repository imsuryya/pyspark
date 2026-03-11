"""
PySpark - Writing Different File Formats with Schema
=====================================================
This module demonstrates writing data to various file formats in PySpark.
"""

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType,
    BooleanType, DateType, TimestampType, ArrayType, MapType
)
from pyspark.sql.functions import col, to_date, current_timestamp

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("Writing File Formats with Schema") \
    .master("local[*]") \
    .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
    .getOrCreate()

# ============================================================================
# CREATE SAMPLE DATA
# ============================================================================
print("=" * 70)
print("CREATING SAMPLE DATA")
print("=" * 70)

# Simple data
simple_data = [
    (1, "Alice", "Engineering", 75000.0, "2022-01-15", True),
    (2, "Bob", "Sales", 65000.0, "2022-03-20", True),
    (3, "Charlie", "Engineering", 80000.0, "2021-08-10", False),
    (4, "Diana", "Marketing", 70000.0, "2023-02-01", True),
    (5, "Eve", "Engineering", 85000.0, "2020-06-15", True),
]

simple_schema = StructType([
    StructField("id", IntegerType(), False),
    StructField("name", StringType(), True),
    StructField("department", StringType(), True),
    StructField("salary", DoubleType(), True),
    StructField("hire_date", StringType(), True),
    StructField("is_active", BooleanType(), True),
])

df = spark.createDataFrame(simple_data, simple_schema)
df = df.withColumn("hire_date", to_date(col("hire_date"), "yyyy-MM-dd"))
print("Sample DataFrame:")
df.show()
df.printSchema()

# Complex nested data
nested_data = [
    (1, "Alice", {"street": "123 Main St", "city": "NYC", "zip": "10001"}, 
     ["Python", "Java", "Scala"], {"level": "senior", "team": "data"}),
    (2, "Bob", {"street": "456 Oak Ave", "city": "LA", "zip": "90001"}, 
     ["Python", "SQL"], {"level": "mid", "team": "analytics"}),
]

nested_schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("address", StructType([
        StructField("street", StringType(), True),
        StructField("city", StringType(), True),
        StructField("zip", StringType(), True),
    ]), True),
    StructField("skills", ArrayType(StringType()), True),
    StructField("metadata", MapType(StringType(), StringType()), True),
])

df_nested = spark.createDataFrame(nested_data, nested_schema)
print("Nested DataFrame:")
df_nested.show(truncate=False)

# ============================================================================
# WRITE MODES
# ============================================================================
print("=" * 70)
print("WRITE MODES")
print("=" * 70)

print("""
Available Write Modes:
- "error" or "errorifexists": Throw exception if data already exists (default)
- "append": Append to existing data
- "overwrite": Overwrite existing data
- "ignore": Silently ignore if data already exists

Usage:
df.write.mode("overwrite").format("parquet").save("path/to/output")
""")

# ============================================================================
# 1. WRITING CSV FILES
# ============================================================================
print("=" * 70)
print("1. WRITING CSV FILES")
print("=" * 70)

# Basic CSV write
"""
df.write \
    .format("csv") \
    .mode("overwrite") \
    .option("header", "true") \
    .save("output/csv/employees")
"""

# CSV with full options
"""
df.write \
    .format("csv") \
    .mode("overwrite") \
    .option("header", "true") \              # write header row
    .option("sep", ",") \                    # delimiter
    .option("quote", '"') \                  # quote character
    .option("escape", "\\") \                # escape character
    .option("nullValue", "NULL") \           # representation of null
    .option("emptyValue", "") \              # representation of empty string
    .option("dateFormat", "yyyy-MM-dd") \    # date format
    .option("timestampFormat", "yyyy-MM-dd HH:mm:ss") \
    .option("compression", "gzip") \         # none, gzip, snappy, lz4, bzip2
    .option("quoteAll", "false") \           # quote all fields
    .option("encoding", "UTF-8") \
    .option("lineSep", "\n") \               # line separator
    .save("output/csv/employees_full")

# Write to single file (use with caution on large data)
df.coalesce(1).write \
    .format("csv") \
    .mode("overwrite") \
    .option("header", "true") \
    .save("output/csv/employees_single")
"""

print("CSV Writer Options:")
print("""
Key Options:
- header: include header row (default: false)
- sep: field delimiter (default: ,)
- quote: quote character (default: ")
- nullValue: string for null values
- dateFormat: format for date columns
- timestampFormat: format for timestamp columns
- compression: gzip, snappy, lz4, bzip2, none
- quoteAll: quote all values (default: false)
""")

# ============================================================================
# 2. WRITING JSON FILES
# ============================================================================
print("=" * 70)
print("2. WRITING JSON FILES")
print("=" * 70)

# Basic JSON write (JSON Lines format)
"""
df.write \
    .format("json") \
    .mode("overwrite") \
    .save("output/json/employees")
"""

# JSON with options
"""
df.write \
    .format("json") \
    .mode("overwrite") \
    .option("dateFormat", "yyyy-MM-dd") \
    .option("timestampFormat", "yyyy-MM-dd'T'HH:mm:ss.SSSZ") \
    .option("compression", "gzip") \         # none, gzip, snappy, lz4, bzip2
    .option("ignoreNullFields", "true") \    # don't write null fields
    .option("encoding", "UTF-8") \
    .save("output/json/employees_options")

# Write nested data as JSON
df_nested.write \
    .format("json") \
    .mode("overwrite") \
    .option("ignoreNullFields", "false") \
    .save("output/json/employees_nested")
"""

print("JSON Writer Options:")
print("""
Key Options:
- dateFormat: format for date columns
- timestampFormat: format for timestamp columns
- compression: gzip, snappy, lz4, bzip2, none
- ignoreNullFields: skip null values in output (default: false)

Note: PySpark writes JSON Lines format (one JSON per line) by default.
""")

# ============================================================================
# 3. WRITING PARQUET FILES
# ============================================================================
print("=" * 70)
print("3. WRITING PARQUET FILES")
print("=" * 70)

# Basic Parquet write (recommended for big data)
"""
df.write \
    .format("parquet") \
    .mode("overwrite") \
    .save("output/parquet/employees")
"""

# Parquet with options
"""
df.write \
    .format("parquet") \
    .mode("overwrite") \
    .option("compression", "snappy") \       # snappy, gzip, lz4, zstd, none
    .option("parquet.block.size", "134217728") \  # 128MB block size
    .save("output/parquet/employees_compressed")

# Write nested data - Parquet handles complex types well
df_nested.write \
    .format("parquet") \
    .mode("overwrite") \
    .save("output/parquet/employees_nested")
"""

print("Parquet Writer Options:")
print("""
Key Options:
- compression: snappy (default), gzip, lz4, zstd, none
- parquet.block.size: row group size in bytes

Benefits of Parquet:
- Column-oriented storage (efficient for analytics)
- Built-in compression
- Schema is stored with data
- Predicate pushdown support
- Handles nested/complex types well
""")

# ============================================================================
# 4. WRITING ORC FILES
# ============================================================================
print("=" * 70)
print("4. WRITING ORC FILES")
print("=" * 70)

"""
df.write \
    .format("orc") \
    .mode("overwrite") \
    .option("compression", "snappy") \       # snappy, zlib, lzo, none
    .option("orc.bloom.filter.columns", "name,department") \
    .option("orc.stripe.size", "67108864") \  # 64MB stripe size
    .save("output/orc/employees")
"""

print("ORC Writer Options:")
print("""
Key Options:
- compression: snappy, zlib, lzo, none
- orc.bloom.filter.columns: columns for bloom filter
- orc.stripe.size: stripe size in bytes

Note: ORC is optimized for Hive and similar to Parquet in benefits.
""")

# ============================================================================
# 5. WRITING AVRO FILES
# ============================================================================
print("=" * 70)
print("5. WRITING AVRO FILES")
print("=" * 70)

"""
# Requires spark-avro package

df.write \
    .format("avro") \
    .mode("overwrite") \
    .option("compression", "snappy") \       # uncompressed, snappy, deflate
    .option("avroSchema", avro_schema_json) \  # optional custom schema
    .save("output/avro/employees")
"""

print("Avro Writer Options:")
print("""
Requirements:
- Add spark-avro package: --packages org.apache.spark:spark-avro_2.12:3.x.x

Key Options:
- compression: uncompressed, snappy, deflate
- avroSchema: custom Avro schema in JSON format
""")

# ============================================================================
# 6. WRITING XML FILES
# ============================================================================
print("=" * 70)
print("6. WRITING XML FILES")
print("=" * 70)

"""
# Requires spark-xml package

df.write \
    .format("xml") \
    .mode("overwrite") \
    .option("rootTag", "employees") \        # root element
    .option("rowTag", "employee") \          # row element
    .option("declaration", "version='1.0' encoding='UTF-8'") \
    .option("attributePrefix", "_") \
    .option("valueTag", "_VALUE") \
    .option("nullValue", "") \
    .save("output/xml/employees")
"""

print("XML Writer Options:")
print("""
Requirements:
- Add spark-xml package: --packages com.databricks:spark-xml_2.12:0.17.0

Key Options:
- rootTag: root XML element (default: ROWS)
- rowTag: element name for each row (default: ROW)
- attributePrefix: prefix for attribute columns (default: _)
""")

# ============================================================================
# 7. WRITING TEXT FILES
# ============================================================================
print("=" * 70)
print("7. WRITING TEXT FILES")
print("=" * 70)

"""
# DataFrame must have single string column
df.select("name").write \
    .format("text") \
    .mode("overwrite") \
    .option("compression", "gzip") \
    .option("lineSep", "\n") \
    .save("output/text/names")

# Convert to delimited string first
from pyspark.sql.functions import concat_ws
df.select(concat_ws(",", *df.columns).alias("value")).write \
    .text("output/text/employees_delimited")
"""

print("Text Writer Options:")
print("""
Key Options:
- compression: gzip, snappy, lz4, bzip2, none
- lineSep: line separator (default: \\n)

Note: DataFrame must have exactly one string column named 'value'.
""")

# ============================================================================
# 8. WRITING DELTA LAKE
# ============================================================================
print("=" * 70)
print("8. WRITING DELTA LAKE")
print("=" * 70)

"""
# Requires delta-spark package and configuration

df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \     # allow schema changes
    .option("mergeSchema", "true") \         # merge new columns
    .save("output/delta/employees")

# Append with merge schema
df.write \
    .format("delta") \
    .mode("append") \
    .option("mergeSchema", "true") \
    .save("output/delta/employees")

# Write as Delta table
df.write \
    .format("delta") \
    .saveAsTable("employees_delta")
"""

print("Delta Lake Writer Options:")
print("""
Key Options:
- overwriteSchema: allow overwriting schema (default: false)
- mergeSchema: merge new columns with existing schema
- replaceWhere: conditionally overwrite partitions

Benefits:
- ACID transactions
- Time travel
- Schema enforcement and evolution
- Unified batch and streaming
""")

# ============================================================================
# 9. WRITING TO JDBC (DATABASES)
# ============================================================================
print("=" * 70)
print("9. WRITING TO JDBC (DATABASES)")
print("=" * 70)

"""
df.write \
    .format("jdbc") \
    .mode("overwrite") \                     # overwrite, append, ignore, error
    .option("url", "jdbc:postgresql://localhost:5432/mydb") \
    .option("driver", "org.postgresql.Driver") \
    .option("dbtable", "employees") \
    .option("user", "username") \
    .option("password", "password") \
    .option("batchsize", "10000") \          # rows per batch
    .option("numPartitions", 10) \           # parallel connections
    .option("truncate", "true") \            # truncate instead of drop
    .option("createTableOptions", "ENGINE=InnoDB") \
    .option("createTableColumnTypes", "name VARCHAR(100), department VARCHAR(50)") \
    .save()

# Different JDBC URLs
# PostgreSQL: jdbc:postgresql://host:5432/database
# MySQL:      jdbc:mysql://host:3306/database
# SQL Server: jdbc:sqlserver://host:1433;databaseName=database
# Oracle:     jdbc:oracle:thin:@host:1521:database
"""

print("JDBC Writer Options:")
print("""
Key Options:
- url: JDBC connection URL
- driver: JDBC driver class
- dbtable: target table name
- user, password: credentials
- batchsize: rows per batch insert
- numPartitions: parallel write connections
- truncate: truncate table before overwrite (vs drop and recreate)
- createTableOptions: DDL options for table creation
- createTableColumnTypes: custom column type mapping
""")

# ============================================================================
# 10. PARTITIONED WRITING
# ============================================================================
print("=" * 70)
print("10. PARTITIONED WRITING")
print("=" * 70)

# Write with partitioning
"""
df.write \
    .format("parquet") \
    .mode("overwrite") \
    .partitionBy("department") \             # partition by column(s)
    .save("output/parquet/employees_partitioned")

# Multiple partition columns (creates nested directories)
df.write \
    .format("parquet") \
    .mode("overwrite") \
    .partitionBy("department", "is_active") \
    .save("output/parquet/employees_multi_partitioned")

# Output structure:
# output/parquet/employees_partitioned/
#   department=Engineering/
#     part-00000-xxx.parquet
#   department=Sales/
#     part-00000-xxx.parquet
#   department=Marketing/
#     part-00000-xxx.parquet
"""

print("Partitioning Options:")
print("""
Usage:
.partitionBy("col1", "col2", ...)

Benefits:
- Enables partition pruning (only read relevant data)
- Improves query performance
- Better for incremental updates

Best Practices:
- Use low-cardinality columns
- Consider query patterns
- Avoid over-partitioning (too many small files)
""")

# ============================================================================
# 11. BUCKETING
# ============================================================================
print("=" * 70)
print("11. BUCKETING (Save as Table)")
print("=" * 70)

"""
# Bucketing requires saving as a table
df.write \
    .format("parquet") \
    .mode("overwrite") \
    .bucketBy(4, "department") \             # number of buckets, column(s)
    .sortBy("salary") \                      # sort within buckets
    .saveAsTable("employees_bucketed")

# Partitioning + Bucketing
df.write \
    .format("parquet") \
    .mode("overwrite") \
    .partitionBy("is_active") \
    .bucketBy(4, "department") \
    .sortBy("salary") \
    .saveAsTable("employees_partitioned_bucketed")
"""

print("Bucketing Options:")
print("""
Usage:
.bucketBy(numBuckets, "col1", "col2", ...)
.sortBy("sortCol1", "sortCol2", ...)

Benefits:
- Optimizes joins on bucketed columns
- Avoids shuffle for bucket joins
- Pre-sorts data within buckets

Note: Bucketing only works with saveAsTable, not save()
""")

# ============================================================================
# 12. CONTROLLING OUTPUT FILES
# ============================================================================
print("=" * 70)
print("12. CONTROLLING OUTPUT FILES")
print("=" * 70)

"""
# Control number of output files with repartition/coalesce
# Repartition - shuffle data, can increase or decrease partitions
df.repartition(10).write \
    .format("parquet") \
    .mode("overwrite") \
    .save("output/repartitioned")

# Coalesce - reduce partitions without full shuffle (more efficient)
df.coalesce(1).write \
    .format("parquet") \
    .mode("overwrite") \
    .save("output/single_file")

# Repartition by column (optimize for partition writing)
df.repartition("department").write \
    .format("parquet") \
    .mode("overwrite") \
    .partitionBy("department") \
    .save("output/optimized_partitioned")

# maxRecordsPerFile option (Spark 2.4+)
df.write \
    .format("parquet") \
    .mode("overwrite") \
    .option("maxRecordsPerFile", 100000) \   # max rows per file
    .save("output/controlled_file_size")
"""

print("File Control Options:")
print("""
Methods:
- repartition(n): shuffle to n partitions
- repartition(col): partition by column values
- coalesce(n): reduce to n partitions without shuffle

Options:
- maxRecordsPerFile: limit rows per output file

Tips:
- Use coalesce(1) sparingly (creates single large file)
- Match partition count to data size (aim for ~128MB files)
- Repartition before partitionBy for even distribution
""")

# ============================================================================
# 13. WRITING TO CLOUD STORAGE
# ============================================================================
print("=" * 70)
print("13. WRITING TO CLOUD STORAGE")
print("=" * 70)

"""
# AWS S3
df.write \
    .format("parquet") \
    .mode("overwrite") \
    .save("s3a://bucket-name/path/to/output/")

# Azure Blob Storage
df.write \
    .format("parquet") \
    .mode("overwrite") \
    .save("wasbs://container@storage.blob.core.windows.net/path/")

# Azure Data Lake Storage Gen2
df.write \
    .format("parquet") \
    .mode("overwrite") \
    .save("abfss://container@storage.dfs.core.windows.net/path/")

# Google Cloud Storage
df.write \
    .format("parquet") \
    .mode("overwrite") \
    .save("gs://bucket-name/path/to/output/")
"""

print("Cloud Storage URLs:")
print("""
AWS S3:     s3a://bucket-name/path/
Azure Blob: wasbs://container@account.blob.core.windows.net/path/
Azure ADLS: abfss://container@account.dfs.core.windows.net/path/
GCS:        gs://bucket-name/path/
""")

# ============================================================================
# 14. COMPLETE EXAMPLE: WRITE PIPELINE
# ============================================================================
print("=" * 70)
print("14. COMPLETE EXAMPLE: WRITE PIPELINE")
print("=" * 70)

print("""
Example: Complete Write Pipeline

# Read data
df = spark.read \\
    .format("csv") \\
    .schema(input_schema) \\
    .option("header", "true") \\
    .load("input/raw_data.csv")

# Transform data
df_transformed = df \\
    .withColumn("processed_date", current_timestamp()) \\
    .withColumn("year", year(col("date"))) \\
    .withColumn("month", month(col("date")))

# Write to different formats

# 1. Raw archive (preserve original)
df.write \\
    .format("parquet") \\
    .mode("append") \\
    .save("archive/raw/")

# 2. Processed data (partitioned for analytics)
df_transformed \\
    .repartition("year", "month") \\
    .write \\
    .format("parquet") \\
    .mode("overwrite") \\
    .partitionBy("year", "month") \\
    .option("compression", "snappy") \\
    .save("processed/data/")

# 3. Export for external systems (CSV)
df_transformed \\
    .coalesce(1) \\
    .write \\
    .format("csv") \\
    .mode("overwrite") \\
    .option("header", "true") \\
    .save("export/csv/")

# 4. Save as managed table
df_transformed.write \\
    .format("parquet") \\
    .mode("overwrite") \\
    .partitionBy("year", "month") \\
    .saveAsTable("processed_data")
""")

# ============================================================================
# Demonstration with actual write
# ============================================================================
import tempfile
import os

temp_dir = tempfile.mkdtemp()
print(f"\nDemonstration - Writing to temporary directory: {temp_dir}")

# Write CSV
csv_path = os.path.join(temp_dir, "csv_output")
df.write.format("csv").mode("overwrite").option("header", "true").save(csv_path)
print(f"CSV written to: {csv_path}")

# Write JSON
json_path = os.path.join(temp_dir, "json_output")
df.write.format("json").mode("overwrite").save(json_path)
print(f"JSON written to: {json_path}")

# Write Parquet
parquet_path = os.path.join(temp_dir, "parquet_output")
df.write.format("parquet").mode("overwrite").save(parquet_path)
print(f"Parquet written to: {parquet_path}")

# Write Parquet with partitioning
partitioned_path = os.path.join(temp_dir, "partitioned_output")
df.write.format("parquet").mode("overwrite").partitionBy("department").save(partitioned_path)
print(f"Partitioned Parquet written to: {partitioned_path}")

# List output files
print("\nOutput directory structure:")
for root, dirs, files in os.walk(temp_dir):
    level = root.replace(temp_dir, '').count(os.sep)
    indent = ' ' * 2 * level
    print(f'{indent}{os.path.basename(root)}/')
    subindent = ' ' * 2 * (level + 1)
    for file in files[:3]:  # Show first 3 files
        print(f'{subindent}{file}')
    if len(files) > 3:
        print(f'{subindent}... and {len(files) - 3} more files')

# ============================================================================
# Cleanup
# ============================================================================
import shutil
shutil.rmtree(temp_dir, ignore_errors=True)
spark.stop()
