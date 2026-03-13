"""
PySpark - Libraries and Modules Reference
==========================================
Comprehensive guide to all major PySpark libraries, modules, and their key classes/functions.
"""

from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("PySpark Libraries Reference") \
    .master("local[*]") \
    .getOrCreate()

print("=" * 70)
print("1. pyspark.sql - Core DataFrame & SQL Module")
print("=" * 70)

from pyspark.sql import SparkSession, DataFrame, Row, Column
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.window import Window

print("""
Key Classes:
  SparkSession       - Entry point for DataFrame and SQL functionality
  DataFrame          - Distributed collection of data organized into columns
  Row                - A row in a DataFrame
  Column             - A column expression in a DataFrame
  GroupedData        - Returned by DataFrame.groupBy() for aggregations
  DataFrameReader    - Interface for loading DataFrames (spark.read)
  DataFrameWriter    - Interface for saving DataFrames (df.write)
  Window             - Window specification for window functions
""")

data = [(1, "Alice", 75000.0), (2, "Bob", 65000.0), (3, "Charlie", 80000.0)]
df = spark.createDataFrame(data, ["id", "name", "salary"])

print("SparkSession:")
print(f"  App Name: {spark.sparkContext.appName}")
print(f"  Version: {spark.version}")

print("\nDataFrame operations:")
df.show()
df.printSchema()
print(f"  Count: {df.count()}")
print(f"  Columns: {df.columns}")
print(f"  dtypes: {df.dtypes}")

print("\nRow usage:")
row = Row(id=1, name="Alice", salary=75000.0)
print(f"  Row: {row}")
print(f"  Access by name: {row.name}")
print(f"  Access by index: {row[1]}")

print("=" * 70)
print("2. pyspark.sql.functions - Built-in Functions")
print("=" * 70)

from pyspark.sql.functions import (
    col, lit, when, coalesce,
    upper, lower, trim, ltrim, rtrim, lpad, rpad,
    substring, concat, concat_ws, split, regexp_replace, regexp_extract,
    length, instr, locate, reverse, repeat,
    abs as spark_abs, ceil, floor, round as spark_round, sqrt, pow as spark_pow,
    log, log2, log10, exp,
    sum as spark_sum, avg, count, max as spark_max, min as spark_min,
    first, last, collect_list, collect_set, countDistinct,
    stddev, variance, skewness, kurtosis,
    current_date, current_timestamp, date_format,
    year, month, dayofmonth, dayofweek, dayofyear,
    hour, minute, second,
    datediff, months_between, add_months, date_add, date_sub,
    to_date, to_timestamp, from_unixtime, unix_timestamp,
    array, array_contains, array_distinct, array_sort, size, explode, posexplode,
    create_map, map_keys, map_values, map_from_arrays,
    struct, col,
    isnull, isnan, nanvl,
    asc, desc,
    monotonically_increasing_id, spark_partition_id,
    input_file_name,
    hash as spark_hash, md5, sha1, sha2, crc32,
    base64, unbase64,
    encode, decode,
    row_number, rank, dense_rank, ntile, percent_rank, cume_dist,
    lag, lead,
    broadcast,
    greatest, least, coalesce as func_coalesce,
)

print("""
Categories of Built-in Functions:

STRING FUNCTIONS:
  upper, lower, trim, ltrim, rtrim, lpad, rpad
  substring, concat, concat_ws, split
  regexp_replace, regexp_extract
  length, instr, locate, reverse, repeat

MATH FUNCTIONS:
  abs, ceil, floor, round, sqrt, pow
  log, log2, log10, exp

AGGREGATE FUNCTIONS:
  sum, avg, count, max, min
  first, last, collect_list, collect_set, countDistinct
  stddev, variance, skewness, kurtosis

DATE/TIME FUNCTIONS:
  current_date, current_timestamp, date_format
  year, month, dayofmonth, dayofweek, dayofyear
  hour, minute, second
  datediff, months_between, add_months, date_add, date_sub
  to_date, to_timestamp, from_unixtime, unix_timestamp

COLLECTION FUNCTIONS:
  array, array_contains, array_distinct, array_sort, size
  explode, posexplode
  create_map, map_keys, map_values, map_from_arrays

WINDOW FUNCTIONS:
  row_number, rank, dense_rank, ntile
  percent_rank, cume_dist, lag, lead

CONDITIONAL FUNCTIONS:
  when, coalesce, isnull, isnan, greatest, least

HASH FUNCTIONS:
  hash, md5, sha1, sha2, crc32, base64, unbase64
""")

print("String Functions Demo:")
df_str = spark.createDataFrame([("  Hello World  ",), ("pyspark functions",)], ["text"])
df_str.select(
    trim(col("text")).alias("trimmed"),
    upper(col("text")).alias("upper"),
    length(trim(col("text"))).alias("len"),
    reverse(trim(col("text"))).alias("reversed"),
).show(truncate=False)

print("Math Functions Demo:")
df_math = spark.createDataFrame([(3.7,), (-2.3,), (16.0,)], ["value"])
df_math.select(
    col("value"),
    spark_round(col("value"), 0).alias("rounded"),
    ceil(col("value")).alias("ceil"),
    floor(col("value")).alias("floor"),
    spark_abs(col("value")).alias("abs"),
    sqrt(spark_abs(col("value"))).alias("sqrt"),
).show()

print("Date Functions Demo:")
from pyspark.sql.functions import current_date, date_format, dayofweek
df_dates = spark.createDataFrame([("2024-06-15",), ("2024-12-25",)], ["date_str"])
df_dates.select(
    col("date_str"),
    to_date(col("date_str")).alias("date"),
    year(to_date(col("date_str"))).alias("year"),
    month(to_date(col("date_str"))).alias("month"),
    dayofmonth(to_date(col("date_str"))).alias("day"),
    date_format(to_date(col("date_str")), "EEEE").alias("day_name"),
).show(truncate=False)

print("=" * 70)
print("3. pyspark.sql.types - Data Types")
print("=" * 70)

from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, LongType, ShortType, ByteType,
    FloatType, DoubleType, DecimalType,
    BooleanType,
    DateType, TimestampType,
    ArrayType, MapType,
    BinaryType, NullType,
)

print("""
NUMERIC TYPES:
  ByteType()         - 1-byte signed integer (-128 to 127)
  ShortType()        - 2-byte signed integer (-32768 to 32767)
  IntegerType()      - 4-byte signed integer
  LongType()         - 8-byte signed integer
  FloatType()        - 4-byte floating point
  DoubleType()       - 8-byte floating point
  DecimalType(p, s)  - Fixed precision decimal (precision, scale)

STRING & BINARY TYPES:
  StringType()       - Character string
  BinaryType()       - Byte sequence

BOOLEAN TYPE:
  BooleanType()      - True/False

DATE/TIME TYPES:
  DateType()         - Date without time (yyyy-MM-dd)
  TimestampType()    - Date with time (yyyy-MM-dd HH:mm:ss)

COMPLEX TYPES:
  ArrayType(elementType, containsNull)
  MapType(keyType, valueType, valueContainsNull)
  StructType([StructField(...), ...])
  StructField(name, dataType, nullable)

NULL TYPE:
  NullType()         - Represents null/None
""")

complex_schema = StructType([
    StructField("id", IntegerType(), nullable=False),
    StructField("name", StringType(), nullable=True),
    StructField("scores", ArrayType(DoubleType()), nullable=True),
    StructField("metadata", MapType(StringType(), StringType()), nullable=True),
    StructField("address", StructType([
        StructField("city", StringType(), True),
        StructField("zip", StringType(), True),
    ]), nullable=True),
])

print("Complex Schema:")
print(complex_schema.json())
print(f"\nField names: {complex_schema.fieldNames()}")
print(f"Number of fields: {len(complex_schema.fields)}")

print("=" * 70)
print("4. pyspark.sql.window - Window Functions")
print("=" * 70)

from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, rank, dense_rank, lag, lead, sum as w_sum

print("""
Window Specification:
  Window.partitionBy(cols)    - Partition data into groups
  Window.orderBy(cols)        - Order within partitions
  Window.rowsBetween(s, e)    - Row-based frame
  Window.rangeBetween(s, e)   - Range-based frame

Frame Boundaries:
  Window.unboundedPreceding   - Start of partition
  Window.unboundedFollowing   - End of partition
  Window.currentRow           - Current row
""")

emp_data = [
    ("Engineering", "Alice", 75000),
    ("Engineering", "Charlie", 80000),
    ("Engineering", "Frank", 72000),
    ("Sales", "Bob", 65000),
    ("Sales", "Grace", 68000),
    ("Marketing", "Diana", 70000),
    ("Marketing", "Henry", 67000),
]

df_emp = spark.createDataFrame(emp_data, ["department", "name", "salary"])

window_dept = Window.partitionBy("department").orderBy(desc("salary"))
window_running = Window.partitionBy("department").orderBy("salary") \
    .rowsBetween(Window.unboundedPreceding, Window.currentRow)

df_window = df_emp.select(
    col("department"),
    col("name"),
    col("salary"),
    row_number().over(window_dept).alias("row_num"),
    rank().over(window_dept).alias("rank"),
    dense_rank().over(window_dept).alias("dense_rank"),
    w_sum("salary").over(window_running).alias("running_total"),
    lag("salary", 1).over(window_dept).alias("prev_salary"),
    lead("salary", 1).over(window_dept).alias("next_salary"),
)

print("Window Functions Demo:")
df_window.show(truncate=False)

print("=" * 70)
print("5. pyspark.sql.streaming - Structured Streaming")
print("=" * 70)

print("""
Key Classes:
  DataStreamReader      - spark.readStream (reads streaming sources)
  DataStreamWriter      - df.writeStream (writes streaming sinks)
  StreamingQuery        - Represents a running streaming query

Source Types:
  kafka                 - Apache Kafka
  socket                - TCP socket (testing only)
  rate                  - Auto-generated rate source (testing)
  file formats          - csv, json, parquet, orc, text

Sink/Output Modes:
  append                - Only new rows are written
  complete              - Entire result table is written
  update                - Only updated rows are written

Trigger Types:
  processingTime        - Fixed interval micro-batches
  once                  - Single micro-batch then stop
  continuous            - Low-latency continuous processing
  availableNow          - Process all available data then stop

Example Usage:
""")

"""
df_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "topic_name") \
    .option("startingOffsets", "latest") \
    .load()

query = df_stream.writeStream \
    .format("parquet") \
    .option("path", "path/to/output/") \
    .option("checkpointLocation", "path/to/checkpoint/") \
    .outputMode("append") \
    .trigger(processingTime="10 seconds") \
    .start()

query.awaitTermination()
"""

print("=" * 70)
print("6. pyspark.RDD - Resilient Distributed Datasets")
print("=" * 70)

print("""
Key Classes:
  SparkContext          - Entry point for RDD functionality (spark.sparkContext)
  RDD                   - Base distributed dataset

RDD Creation:
  sc.parallelize()      - Create RDD from Python collection
  sc.textFile()         - Create RDD from text file
  sc.wholeTextFiles()   - Read entire files as (filename, content) pairs

RDD Transformations (Lazy):
  map, flatMap, filter, distinct, sample
  union, intersection, subtract, zip
  groupByKey, reduceByKey, sortByKey, join
  mapPartitions, mapPartitionsWithIndex
  coalesce, repartition
  cache, persist

RDD Actions (Trigger Execution):
  collect, take, first, count
  reduce, fold, aggregate
  foreach, saveAsTextFile
  countByKey, countByValue
  takeSample, takeOrdered
""")

sc = spark.sparkContext
rdd = sc.parallelize([1, 2, 3, 4, 5, 6, 7, 8, 9, 10])

print(f"RDD partitions: {rdd.getNumPartitions()}")
print(f"RDD count: {rdd.count()}")
print(f"RDD sum: {rdd.sum()}")
print(f"RDD mean: {rdd.mean()}")
print(f"RDD max: {rdd.max()}")
print(f"RDD min: {rdd.min()}")

rdd_transformed = rdd \
    .filter(lambda x: x % 2 == 0) \
    .map(lambda x: x ** 2)

print(f"Squared evens: {rdd_transformed.collect()}")

rdd_pairs = sc.parallelize([("a", 1), ("b", 2), ("a", 3), ("b", 4)])
rdd_reduced = rdd_pairs.reduceByKey(lambda a, b: a + b)
print(f"Reduced by key: {rdd_reduced.collect()}")

print("=" * 70)
print("7. pyspark.ml - Machine Learning (DataFrame-based)")
print("=" * 70)

print("""
FEATURE ENGINEERING:
  pyspark.ml.feature
    VectorAssembler      - Combine columns into feature vector
    StandardScaler       - Standardize features (mean=0, std=1)
    MinMaxScaler         - Scale features to [0, 1]
    StringIndexer        - Encode string column as numeric index
    OneHotEncoder        - One-hot encode categorical features
    Bucketizer           - Bin continuous features into buckets
    PCA                  - Principal Component Analysis
    Tokenizer            - Split text into words
    HashingTF            - Term frequency via hashing
    IDF                  - Inverse Document Frequency
    Word2Vec             - Learn word embeddings
    Imputer              - Handle missing values
    PolynomialExpansion  - Generate polynomial features

CLASSIFICATION:
  pyspark.ml.classification
    LogisticRegression
    DecisionTreeClassifier
    RandomForestClassifier
    GBTClassifier
    NaiveBayes
    MultilayerPerceptronClassifier
    LinearSVC

REGRESSION:
  pyspark.ml.regression
    LinearRegression
    DecisionTreeRegressor
    RandomForestRegressor
    GBTRegressor
    GeneralizedLinearRegression
    IsotonicRegression

CLUSTERING:
  pyspark.ml.clustering
    KMeans
    BisectingKMeans
    GaussianMixture
    LDA (Latent Dirichlet Allocation)

RECOMMENDATION:
  pyspark.ml.recommendation
    ALS (Alternating Least Squares)

PIPELINE:
  pyspark.ml.Pipeline         - Chain multiple stages
  pyspark.ml.PipelineModel    - Fitted pipeline

EVALUATION:
  pyspark.ml.evaluation
    BinaryClassificationEvaluator
    MulticlassClassificationEvaluator
    RegressionEvaluator
    ClusteringEvaluator

TUNING:
  pyspark.ml.tuning
    CrossValidator
    TrainValidationSplit
    ParamGridBuilder
""")

from pyspark.ml.feature import VectorAssembler, StringIndexer
from pyspark.ml.classification import LogisticRegression
from pyspark.ml import Pipeline

ml_data = [
    (0, 1.0, 0.5, 0),
    (1, 2.0, 1.5, 0),
    (2, 3.0, 2.5, 1),
    (3, 4.0, 3.5, 1),
    (4, 5.0, 4.5, 1),
]

df_ml = spark.createDataFrame(ml_data, ["id", "feature1", "feature2", "label"])

assembler = VectorAssembler(inputCols=["feature1", "feature2"], outputCol="features")
lr = LogisticRegression(featuresCol="features", labelCol="label", maxIter=10)

pipeline = Pipeline(stages=[assembler, lr])
model = pipeline.fit(df_ml)

predictions = model.transform(df_ml)
print("ML Pipeline Demo (Logistic Regression):")
predictions.select("id", "features", "label", "prediction", "probability").show(truncate=False)

print("=" * 70)
print("8. pyspark.ml.stat & pyspark.sql Statistical Functions")
print("=" * 70)

print("""
pyspark.ml.stat:
  Correlation          - Compute correlation matrix
  ChiSquareTest        - Chi-squared test for independence
  Summarizer           - Multi-column summary statistics
  KolmogorovSmirnovTest - Goodness-of-fit test

DataFrame Statistical Methods:
  df.describe()        - Summary stats (count, mean, stddev, min, max)
  df.summary()         - Extended summary with percentiles
  df.stat.corr()       - Pearson correlation between two columns
  df.stat.cov()        - Covariance between two columns
  df.stat.crosstab()   - Cross-tabulation (contingency table)
  df.stat.freqItems()  - Frequent items using heavy hitters
  df.stat.approxQuantile()  - Approximate quantiles
  df.stat.sampleBy()   - Stratified sampling
""")

stat_data = [(1, 10.0, 100.0), (2, 20.0, 200.0), (3, 30.0, 280.0),
             (4, 40.0, 410.0), (5, 50.0, 490.0)]
df_stat = spark.createDataFrame(stat_data, ["id", "x", "y"])

print("describe():")
df_stat.describe().show()

print(f"Correlation (x, y): {df_stat.stat.corr('x', 'y'):.4f}")
print(f"Covariance (x, y): {df_stat.stat.cov('x', 'y'):.4f}")

print("=" * 70)
print("9. pyspark.sql.catalog - Metadata & Catalog API")
print("=" * 70)

print("""
Key Methods:
  spark.catalog.listDatabases()    - List all databases
  spark.catalog.listTables()       - List tables in current database
  spark.catalog.listColumns(table) - List columns of a table
  spark.catalog.listFunctions()    - List registered functions
  spark.catalog.tableExists(name)  - Check if table exists
  spark.catalog.currentDatabase()  - Get current database name
  spark.catalog.setCurrentDatabase(name) - Switch database
  spark.catalog.createTable()      - Create a managed table
  spark.catalog.dropTempView(name) - Drop a temp view
  spark.catalog.isCached(name)     - Check if table is cached
  spark.catalog.cacheTable(name)   - Cache a table
  spark.catalog.uncacheTable(name) - Uncache a table
  spark.catalog.refreshTable(name) - Refresh table metadata
""")

df.createOrReplaceTempView("employees")
print(f"Current database: {spark.catalog.currentDatabase()}")
print(f"Table exists: {spark.catalog.tableExists('employees')}")

print("=" * 70)
print("10. pyspark.broadcast & pyspark.Accumulator - Shared Variables")
print("=" * 70)

print("""
Broadcast Variables:
  sc.broadcast(value)         - Create broadcast variable
  broadcast_var.value         - Access the value on workers
  broadcast_var.unpersist()   - Remove from memory
  broadcast_var.destroy()     - Destroy the variable

  Used for efficiently sharing large read-only data across workers.

Accumulators:
  sc.accumulator(init)        - Create numeric accumulator
  accumulator.add(value)      - Add value (workers only)
  accumulator.value           - Read value (driver only)

  Used for aggregating values across workers (counters, sums, etc.)
""")

lookup = {"US": "United States", "UK": "United Kingdom", "IN": "India"}
broadcast_lookup = sc.broadcast(lookup)

rdd_codes = sc.parallelize(["US", "UK", "IN", "US", "IN"])
rdd_names = rdd_codes.map(lambda code: broadcast_lookup.value.get(code, "Unknown"))
print(f"Broadcast lookup: {rdd_names.collect()}")

error_count = sc.accumulator(0)

def count_errors(line):
    global error_count
    if "ERROR" in line:
        error_count.add(1)
    return line

rdd_logs = sc.parallelize(["INFO: ok", "ERROR: fail", "INFO: ok", "ERROR: crash"])
rdd_logs.foreach(count_errors)
print(f"Error count: {error_count.value}")

broadcast_lookup.unpersist()

print("=" * 70)
print("11. pyspark.sql.readwriter - Data Source I/O")
print("=" * 70)

print("""
DataFrameReader (spark.read):
  .format(source)        - Specify data source format
  .schema(schema)        - Specify schema
  .option(key, value)    - Set a single option
  .options(**kwargs)      - Set multiple options
  .load(path)            - Load data from path

  Shorthand methods:
  .csv(), .json(), .parquet(), .orc(), .text(), .table(), .jdbc()

DataFrameWriter (df.write):
  .format(source)        - Specify data source format
  .mode(saveMode)        - overwrite, append, ignore, error/errorifexists
  .option(key, value)    - Set a single option
  .partitionBy(cols)     - Partition output by columns
  .bucketBy(n, col)      - Bucket output into n buckets
  .sortBy(cols)          - Sort within each bucket
  .save(path)            - Save to path
  .saveAsTable(name)     - Save as managed table
  .insertInto(name)      - Insert into existing table

  Shorthand methods:
  .csv(), .json(), .parquet(), .orc(), .text(), .jdbc()
""")

print("=" * 70)
print("12. pyspark.sql.conf - Spark Configuration")
print("=" * 70)

print("""
Key Configuration Methods:
  spark.conf.set(key, value)     - Set a configuration property
  spark.conf.get(key)            - Get a configuration property
  spark.conf.get(key, default)   - Get with default value
  spark.conf.unset(key)          - Remove a configuration property
  spark.conf.isModifiable(key)   - Check if property is modifiable

Common Configuration Properties:
  spark.sql.shuffle.partitions       - Number of shuffle partitions (default: 200)
  spark.sql.adaptive.enabled         - Enable Adaptive Query Execution
  spark.sql.autoBroadcastJoinThreshold - Max bytes for broadcast join
  spark.sql.sources.partitionOverwriteMode - static/dynamic
  spark.sql.legacy.timeParserPolicy  - LEGACY/CORRECTED/EXCEPTION
  spark.sql.parquet.mergeSchema      - Merge Parquet schemas
  spark.default.parallelism          - Default number of partitions
  spark.executor.memory              - Memory per executor
  spark.driver.memory                - Driver memory
  spark.executor.cores               - Cores per executor
  spark.sql.warehouse.dir            - Warehouse directory
""")

print(f"Shuffle partitions: {spark.conf.get('spark.sql.shuffle.partitions')}")
print(f"AQE enabled: {spark.conf.get('spark.sql.adaptive.enabled', 'not set')}")

print("=" * 70)
print("13. pyspark.pandas (pandas API on Spark)")
print("=" * 70)

print("""
Available since PySpark 3.2+
Import: import pyspark.pandas as ps

Key APIs (mirror pandas):
  ps.DataFrame()           - Create DataFrame
  ps.read_csv()            - Read CSV file
  ps.read_parquet()        - Read Parquet file
  ps.read_json()           - Read JSON file
  ps.from_pandas()         - Convert pandas DataFrame to PySpark pandas
  df.to_pandas()           - Convert to pandas DataFrame
  df.to_spark()            - Convert to PySpark DataFrame

DataFrame Operations:
  df.head(), df.tail()     - View first/last rows
  df.describe()            - Summary statistics
  df.groupby().agg()       - Group and aggregate
  df.merge()               - Merge DataFrames
  df.sort_values()         - Sort by values
  df.apply()               - Apply function
  df.plot                  - Plotting (matplotlib backend)
""")

import pyspark.pandas as ps

ps_df = ps.DataFrame({
    "name": ["Alice", "Bob", "Charlie"],
    "age": [25, 30, 35],
    "salary": [75000.0, 65000.0, 80000.0],
})

print("PySpark pandas DataFrame:")
print(ps_df)
print(f"\nMean salary: {ps_df['salary'].mean()}")
print(f"Max age: {ps_df['age'].max()}")

print("=" * 70)
print("14. THIRD-PARTY LIBRARIES & CONNECTORS")
print("=" * 70)

print("""
DELTA LAKE:
  Package:  io.delta:delta-spark_2.12:3.x.x
  Import:   from delta.tables import DeltaTable
  Features: ACID transactions, time travel, schema evolution, MERGE

APACHE ICEBERG:
  Package:  org.apache.iceberg:iceberg-spark-runtime-3.x_2.12:x.x.x
  Features: Table format, schema evolution, partition evolution, time travel

APACHE HUDI:
  Package:  org.apache.hudi:hudi-spark3.x-bundle_2.12:x.x.x
  Features: Upserts, incremental pulls, record-level changes

SPARK-XML:
  Package:  com.databricks:spark-xml_2.12:0.17.0
  Features: Read/write XML files

SPARK-AVRO:
  Package:  org.apache.spark:spark-avro_2.12:3.x.x
  Features: Read/write Avro files

SPARK-EXCEL:
  Package:  com.crealytics:spark-excel_2.12:x.x.x
  Features: Read/write Excel files (.xlsx, .xls)

JDBC DRIVERS:
  PostgreSQL:  org.postgresql:postgresql:42.x.x
  MySQL:       mysql:mysql-connector-java:8.x.x
  SQL Server:  com.microsoft.sqlserver:mssql-jdbc:12.x.x
  Oracle:      com.oracle.database.jdbc:ojdbc11:23.x.x

KAFKA:
  Package:  org.apache.spark:spark-sql-kafka-0-10_2.12:3.x.x
  Features: Read/write Kafka streams

ELASTICSEARCH:
  Package:  org.elasticsearch:elasticsearch-spark-30_2.12:8.x.x
  Features: Read/write Elasticsearch indices

MONGODB:
  Package:  org.mongodb.spark:mongo-spark-connector_2.12:10.x.x
  Features: Read/write MongoDB collections

CASSANDRA:
  Package:  com.datastax.spark:spark-cassandra-connector_2.12:3.x.x
  Features: Read/write Cassandra tables

REDIS:
  Package:  com.redislabs:spark-redis_2.12:3.x.x
  Features: Read/write Redis data structures

GRAPHFRAMES:
  Package:  graphframes:graphframes:0.8.x-spark3.x-s_2.12
  Features: Graph processing on DataFrames (PageRank, BFS, connected components)
""")

print("=" * 70)
print("15. UTILITY MODULES")
print("=" * 70)

print("""
pyspark.SparkFiles:
  SparkFiles.get(filename)        - Get absolute path of a file added via addFile
  SparkFiles.getRootDirectory()   - Get root directory of files

pyspark.StorageLevel:
  MEMORY_ONLY                     - Cache in memory only (deserialized)
  MEMORY_ONLY_SER                 - Cache in memory (serialized)
  MEMORY_AND_DISK                 - Spill to disk if memory full
  MEMORY_AND_DISK_SER             - Serialized, spill to disk
  DISK_ONLY                       - Cache on disk only
  OFF_HEAP                        - Off-heap memory

pyspark.TaskContext:
  TaskContext.get()                - Access task context
  .stageId()                      - Current stage ID
  .partitionId()                  - Current partition ID
  .attemptNumber()                - Task attempt number
  .taskAttemptId()                - Unique task attempt ID

pyspark.SparkConf:
  SparkConf()                     - Create configuration object
  .setAppName(name)               - Set application name
  .setMaster(master)              - Set master URL
  .set(key, value)                - Set config property
  .getAll()                       - Get all properties
""")

from pyspark import StorageLevel

print("Storage Levels:")
print(f"  MEMORY_ONLY:         {StorageLevel.MEMORY_ONLY}")
print(f"  MEMORY_AND_DISK:     {StorageLevel.MEMORY_AND_DISK}")
print(f"  DISK_ONLY:           {StorageLevel.DISK_ONLY}")

print("=" * 70)
print("SUMMARY: PySpark Library Hierarchy")
print("=" * 70)
print("""
pyspark
├── SparkContext, SparkConf, SparkFiles, StorageLevel
├── RDD, Broadcast, Accumulator
├── sql
│   ├── SparkSession, DataFrame, Row, Column
│   ├── functions (F)         - 200+ built-in functions
│   ├── types (T)             - Data type definitions
│   ├── window                - Window specifications
│   ├── streaming             - Structured Streaming
│   ├── catalog               - Metadata API
│   ├── readwriter            - DataFrameReader/Writer
│   └── conf                  - Runtime configuration
├── ml
│   ├── feature               - Feature transformers
│   ├── classification        - Classification algorithms
│   ├── regression            - Regression algorithms
│   ├── clustering            - Clustering algorithms
│   ├── recommendation        - Recommendation (ALS)
│   ├── evaluation            - Model evaluators
│   ├── tuning                - Hyperparameter tuning
│   ├── stat                  - Statistical tests
│   └── Pipeline              - ML pipelines
├── pandas (ps)               - pandas API on Spark
└── Third-party connectors
    ├── Delta Lake, Iceberg, Hudi
    ├── Kafka, Elasticsearch
    ├── MongoDB, Cassandra, Redis
    └── JDBC drivers
""")

spark.stop()
