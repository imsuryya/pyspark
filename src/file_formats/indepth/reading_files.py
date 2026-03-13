"""
PySpark - Reading Different File Formats with Schema
=====================================================
This module demonstrates reading various file formats in PySpark with explicit schemas.
"""

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType,
    FloatType, LongType, BooleanType, DateType, TimestampType,
    ArrayType, MapType, DecimalType
)

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("Reading File Formats with Schema") \
    .master("local[*]") \
    .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
    .getOrCreate()

# ============================================================================
# DEFINING SCHEMAS
# ============================================================================
print("=" * 70)
print("DEFINING SCHEMAS - Multiple Approaches")
print("=" * 70)

# Method 1: Using StructType and StructField (Recommended)
schema_struct = StructType([
    StructField("id", IntegerType(), nullable=False),
    StructField("name", StringType(), nullable=True),
    StructField("age", IntegerType(), nullable=True),
    StructField("salary", DoubleType(), nullable=True),
    StructField("is_active", BooleanType(), nullable=True),
    StructField("join_date", DateType(), nullable=True),
])

# Method 2: Using DDL String (Simpler syntax)
schema_ddl = "id INT NOT NULL, name STRING, age INT, salary DOUBLE, is_active BOOLEAN, join_date DATE"

# Method 3: Nested schema with complex types
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

print("Schema (StructType):")
print(schema_struct)
print("\nSchema (DDL):")
print(schema_ddl)

# ============================================================================
# 1. READING CSV FILES
# ============================================================================
print("\n" + "=" * 70)
print("1. READING CSV FILES")
print("=" * 70)

csv_schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("department", StringType(), True),
    StructField("salary", DoubleType(), True),
    StructField("hire_date", DateType(), True),
])

# Basic CSV read with schema
"""
df_csv = spark.read \
    .format("csv") \
    .schema(csv_schema) \
    .option("header", "true") \
    .option("inferSchema", "false") \
    .load("path/to/employees.csv")
"""

# CSV with various options
"""
df_csv_options = spark.read \
    .format("csv") \
    .schema(csv_schema) \
    .option("header", "true") \
    .option("sep", ",") \                    # delimiter
    .option("quote", '"') \                  # quote character
    .option("escape", "\\") \                # escape character
    .option("nullValue", "NULL") \           # null representation
    .option("nanValue", "NaN") \             # NaN representation
    .option("dateFormat", "yyyy-MM-dd") \    # date format
    .option("timestampFormat", "yyyy-MM-dd HH:mm:ss") \
    .option("mode", "PERMISSIVE") \          # PERMISSIVE, DROPMALFORMED, FAILFAST
    .option("columnNameOfCorruptRecord", "_corrupt_record") \
    .option("multiLine", "true") \           # for multi-line values
    .option("encoding", "UTF-8") \
    .option("ignoreLeadingWhiteSpace", "true") \
    .option("ignoreTrailingWhiteSpace", "true") \
    .load("path/to/data.csv")
"""

# Reading multiple CSV files
"""
df_multi_csv = spark.read.schema(csv_schema).csv(
    ["path/to/file1.csv", "path/to/file2.csv"],
    header=True
)

# Using glob pattern
df_glob = spark.read.schema(csv_schema).csv("path/to/data/*.csv", header=True)
"""

print("CSV Reader Options:")
print("""
Key Options:
- header: true/false - first line contains column names
- inferSchema: true/false - infer data types (disable when using schema)
- sep/delimiter: field delimiter (default: ,)
- quote: quote character for strings (default: ")
- escape: escape character (default: \\)
- nullValue: string representation of null
- dateFormat: format for parsing dates
- timestampFormat: format for parsing timestamps
- mode: error handling (PERMISSIVE, DROPMALFORMED, FAILFAST)
- multiLine: handle multi-line values
- encoding: file encoding (default: UTF-8)
""")

# ============================================================================
# 2. READING JSON FILES
# ============================================================================
print("=" * 70)
print("2. READING JSON FILES")
print("=" * 70)

json_schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("email", StringType(), True),
    StructField("address", StructType([
        StructField("street", StringType(), True),
        StructField("city", StringType(), True),
        StructField("country", StringType(), True),
    ]), True),
    StructField("orders", ArrayType(StructType([
        StructField("order_id", StringType(), True),
        StructField("amount", DoubleType(), True),
    ])), True),
])

# Basic JSON read
"""
df_json = spark.read \
    .format("json") \
    .schema(json_schema) \
    .load("path/to/users.json")
"""

# JSON with options
"""
df_json_options = spark.read \
    .format("json") \
    .schema(json_schema) \
    .option("multiLine", "true") \           # for pretty-printed JSON
    .option("allowComments", "true") \       # allow // and /* */ comments
    .option("allowUnquotedFieldNames", "true") \
    .option("allowSingleQuotes", "true") \   # allow 'value' instead of "value"
    .option("allowNumericLeadingZeros", "true") \
    .option("allowBackslashEscapingAnyCharacter", "true") \
    .option("dateFormat", "yyyy-MM-dd") \
    .option("timestampFormat", "yyyy-MM-dd HH:mm:ss") \
    .option("mode", "PERMISSIVE") \
    .option("columnNameOfCorruptRecord", "_corrupt_record") \
    .option("primitivesAsString", "false") \ # read primitives as strings
    .load("path/to/data.json")
"""

# Reading JSON lines (one JSON object per line)
"""
df_jsonl = spark.read.schema(json_schema).json("path/to/data.jsonl")
"""

print("JSON Reader Options:")
print("""
Key Options:
- multiLine: true for pretty-printed JSON (default: false for JSON lines)
- allowComments: allow JavaScript-style comments
- allowUnquotedFieldNames: allow field names without quotes
- allowSingleQuotes: allow single quotes for strings
- dateFormat: format for parsing dates
- primitivesAsString: treat all primitives as strings
- mode: error handling (PERMISSIVE, DROPMALFORMED, FAILFAST)
""")

# ============================================================================
# 3. READING PARQUET FILES
# ============================================================================
print("=" * 70)
print("3. READING PARQUET FILES")
print("=" * 70)

# Parquet has embedded schema, but you can override it
"""
# Read with embedded schema (recommended)
df_parquet = spark.read.parquet("path/to/data.parquet")

# Override schema if needed
df_parquet_schema = spark.read \
    .schema(some_schema) \
    .parquet("path/to/data.parquet")

# Read Parquet with options
df_parquet_options = spark.read \
    .format("parquet") \
    .option("mergeSchema", "true") \         # merge schemas from all files
    .option("recursiveFileLookup", "true") \ # read subdirectories
    .load("path/to/data/")

# Read partitioned Parquet
df_partitioned = spark.read \
    .option("basePath", "path/to/data/") \   # preserve partition columns
    .parquet("path/to/data/year=2024/month=01/")
"""

print("Parquet Reader Options:")
print("""
Key Options:
- mergeSchema: merge schemas from multiple Parquet files
- recursiveFileLookup: read files from subdirectories recursively
- basePath: base path for partitioned data (preserves partition columns)

Note: Parquet stores schema internally, so explicit schema is usually not needed.
""")

# ============================================================================
# 4. READING ORC FILES
# ============================================================================
print("=" * 70)
print("4. READING ORC FILES")
print("=" * 70)

"""
# Basic ORC read (schema is embedded)
df_orc = spark.read.orc("path/to/data.orc")

# With explicit schema
df_orc_schema = spark.read \
    .format("orc") \
    .schema(some_schema) \
    .option("mergeSchema", "true") \
    .load("path/to/data.orc")
"""

print("ORC Reader Options:")
print("""
Key Options:
- mergeSchema: merge schemas from multiple ORC files

Note: ORC also stores schema internally like Parquet.
""")

# ============================================================================
# 5. READING AVRO FILES
# ============================================================================
print("=" * 70)
print("5. READING AVRO FILES")
print("=" * 70)

"""
# Avro support requires spark-avro package
# spark-submit --packages org.apache.spark:spark-avro_2.12:3.x.x

# Basic Avro read
df_avro = spark.read.format("avro").load("path/to/data.avro")

# With explicit schema (Avro schema format)
avro_schema = '''
{
    "type": "record",
    "name": "User",
    "fields": [
        {"name": "id", "type": "int"},
        {"name": "name", "type": "string"},
        {"name": "email", "type": ["null", "string"]}
    ]
}
'''

df_avro_schema = spark.read \
    .format("avro") \
    .option("avroSchema", avro_schema) \
    .load("path/to/data.avro")
"""

print("Avro Reader Options:")
print("""
Requirements:
- Add spark-avro package: --packages org.apache.spark:spark-avro_2.12:3.x.x

Key Options:
- avroSchema: explicit Avro schema in JSON format
- ignoreExtension: read files without .avro extension
""")

# ============================================================================
# 6. READING XML FILES
# ============================================================================
print("=" * 70)
print("6. READING XML FILES")
print("=" * 70)

"""
# XML support requires spark-xml package
# spark-submit --packages com.databricks:spark-xml_2.12:0.17.0

xml_schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("price", DoubleType(), True),
])

df_xml = spark.read \
    .format("xml") \
    .schema(xml_schema) \
    .option("rowTag", "product") \           # XML element to treat as row
    .option("rootTag", "products") \         # root XML element
    .option("attributePrefix", "_") \        # prefix for attributes
    .option("valueTag", "_VALUE") \          # tag for element values
    .option("nullValue", "") \
    .load("path/to/data.xml")
"""

print("XML Reader Options:")
print("""
Requirements:
- Add spark-xml package: --packages com.databricks:spark-xml_2.12:0.17.0

Key Options:
- rowTag: XML element to treat as a row (required)
- rootTag: root XML element
- attributePrefix: prefix for XML attributes (default: _)
- valueTag: tag for simple element values (default: _VALUE)
""")

# ============================================================================
# 7. READING TEXT FILES
# ============================================================================
print("=" * 70)
print("7. READING TEXT FILES")
print("=" * 70)

"""
# Read as single column (value)
df_text = spark.read.text("path/to/data.txt")
df_text.printSchema()  # root |-- value: string

# Read with options
df_text_options = spark.read \
    .format("text") \
    .option("wholetext", "true") \           # read entire file as single row
    .option("lineSep", "\n") \               # line separator
    .load("path/to/data.txt")

# Read whole text file as single string
df_whole = spark.read \
    .option("wholetext", "true") \
    .text("path/to/data.txt")
"""

print("Text Reader Options:")
print("""
Key Options:
- wholetext: read entire file as single row (default: false)
- lineSep: line separator (default: \\n)

Note: Text files are read with single 'value' column containing each line.
""")

# ============================================================================
# 8. READING DELTA LAKE FILES
# ============================================================================
print("=" * 70)
print("8. READING DELTA LAKE FILES")
print("=" * 70)

"""
# Delta Lake requires delta-spark package
# spark-submit --packages io.delta:delta-spark_2.12:3.x.x

# Initialize Spark with Delta support
spark = SparkSession.builder \
    .appName("Delta Lake") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# Read Delta table
df_delta = spark.read.format("delta").load("path/to/delta_table")

# Read specific version (time travel)
df_delta_version = spark.read \
    .format("delta") \
    .option("versionAsOf", 5) \              # specific version
    .load("path/to/delta_table")

# Read as of timestamp
df_delta_timestamp = spark.read \
    .format("delta") \
    .option("timestampAsOf", "2024-01-15 10:30:00") \
    .load("path/to/delta_table")
"""

print("Delta Lake Reader Options:")
print("""
Requirements:
- Add delta-spark package and configure Spark session

Key Options:
- versionAsOf: read specific version (time travel)
- timestampAsOf: read as of specific timestamp
""")

# ============================================================================
# 9. READING FROM JDBC (DATABASES)
# ============================================================================
print("=" * 70)
print("9. READING FROM JDBC (DATABASES)")
print("=" * 70)

jdbc_schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("department", StringType(), True),
    StructField("salary", DecimalType(10, 2), True),
])

"""
df_jdbc = spark.read \
    .format("jdbc") \
    .schema(jdbc_schema) \
    .option("url", "jdbc:postgresql://localhost:5432/mydb") \
    .option("driver", "org.postgresql.Driver") \
    .option("dbtable", "employees") \
    .option("user", "username") \
    .option("password", "password") \
    .option("fetchsize", "10000") \          # rows to fetch per round trip
    .option("numPartitions", 10) \           # number of partitions
    .option("partitionColumn", "id") \       # column for partitioning
    .option("lowerBound", "1") \             # min value for partitioning
    .option("upperBound", "1000000") \       # max value for partitioning
    .load()

# Using SQL query instead of table
df_query = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://localhost:5432/mydb") \
    .option("query", "SELECT id, name, salary FROM employees WHERE department = 'IT'") \
    .option("user", "username") \
    .option("password", "password") \
    .load()
"""

print("JDBC Reader Options:")
print("""
Key Options:
- url: JDBC connection URL
- driver: JDBC driver class name
- dbtable: table name or subquery
- query: SQL query (alternative to dbtable)
- user, password: credentials
- fetchsize: rows per fetch
- numPartitions: number of parallel partitions
- partitionColumn: column for partitioning (with lowerBound, upperBound)
""")

# ============================================================================
# 10. SCHEMA EVOLUTION AND HANDLING
# ============================================================================
print("=" * 70)
print("10. SCHEMA EVOLUTION AND HANDLING")
print("=" * 70)

# Handling schema evolution with mergeSchema
"""
df_evolved = spark.read \
    .format("parquet") \
    .option("mergeSchema", "true") \
    .load("path/to/partitioned/data/")
"""

# Reading with corrupt record handling
"""
schema_with_corrupt = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("_corrupt_record", StringType(), True),  # captures malformed rows
])

df_permissive = spark.read \
    .format("json") \
    .schema(schema_with_corrupt) \
    .option("mode", "PERMISSIVE") \
    .option("columnNameOfCorruptRecord", "_corrupt_record") \
    .load("path/to/data.json")

# Filter corrupt records
corrupt_records = df_permissive.filter(df_permissive._corrupt_record.isNotNull())
valid_records = df_permissive.filter(df_permissive._corrupt_record.isNull())
"""

print("Schema Evolution Tips:")
print("""
1. Use mergeSchema=true for Parquet/ORC when schema changes over time
2. Use PERMISSIVE mode with _corrupt_record column for error handling
3. Define explicit schemas for CSV/JSON to ensure type consistency
4. Use basePath option for partitioned data to preserve partition columns
""")

# ============================================================================
# 11. READING FROM CLOUD STORAGE
# ============================================================================
print("=" * 70)
print("11. READING FROM CLOUD STORAGE")
print("=" * 70)

"""
# AWS S3
df_s3 = spark.read \
    .schema(some_schema) \
    .parquet("s3a://bucket-name/path/to/data/")

# Configure S3 credentials
spark.conf.set("spark.hadoop.fs.s3a.access.key", "YOUR_ACCESS_KEY")
spark.conf.set("spark.hadoop.fs.s3a.secret.key", "YOUR_SECRET_KEY")

# Azure Blob Storage
df_azure = spark.read \
    .schema(some_schema) \
    .parquet("wasbs://container@storage.blob.core.windows.net/path/")

# Azure Data Lake Storage Gen2
df_adls = spark.read \
    .schema(some_schema) \
    .parquet("abfss://container@storage.dfs.core.windows.net/path/")

# Google Cloud Storage
df_gcs = spark.read \
    .schema(some_schema) \
    .parquet("gs://bucket-name/path/to/data/")
"""

print("Cloud Storage URLs:")
print("""
AWS S3:     s3a://bucket-name/path/
Azure Blob: wasbs://container@account.blob.core.windows.net/path/
Azure ADLS: abfss://container@account.dfs.core.windows.net/path/
GCS:        gs://bucket-name/path/
""")

# ============================================================================
# Example: Creating sample data for demonstration
# ============================================================================
print("=" * 70)
print("DEMONSTRATION: Creating and Reading Sample Data")
print("=" * 70)

# Create sample data
sample_data = [
    (1, "Alice", "Engineering", 75000.0, "2022-01-15"),
    (2, "Bob", "Sales", 65000.0, "2022-03-20"),
    (3, "Charlie", "Engineering", 80000.0, "2021-08-10"),
    (4, "Diana", "Marketing", 70000.0, "2023-02-01"),
]

sample_schema = StructType([
    StructField("id", IntegerType(), False),
    StructField("name", StringType(), True),
    StructField("department", StringType(), True),
    StructField("salary", DoubleType(), True),
    StructField("hire_date", StringType(), True),
])

df_sample = spark.createDataFrame(sample_data, sample_schema)
print("Sample DataFrame:")
df_sample.show()
df_sample.printSchema()

# ============================================================================
# Cleanup
# ============================================================================
spark.stop()
