"""
PySpark Explode Array and Map Functions
========================================
This module demonstrates explode functions for arrays and maps in PySpark.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    explode, explode_outer, posexplode, posexplode_outer,
    inline, inline_outer, col, create_map, lit, map_keys, map_values,
    map_from_entries, map_entries, map_concat, map_filter,
    element_at, transform_keys, transform_values
)
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, ArrayType, MapType
)

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("Explode Functions") \
    .master("local[*]") \
    .getOrCreate()

# ============================================================================
# PART 1: EXPLODE ARRAY FUNCTIONS
# ============================================================================
print("=" * 70)
print("PART 1: EXPLODE ARRAY FUNCTIONS")
print("=" * 70)

# Sample data with arrays
array_data = [
    (1, "Alice", ["Python", "Java", "Scala"]),
    (2, "Bob", ["Python", "C++"]),
    (3, "Charlie", ["Rust"]),
    (4, "Diana", []),           # empty array
    (5, "Eve", None),           # null array
]
df_array = spark.createDataFrame(array_data, ["id", "name", "languages"])
print("Original DataFrame with Arrays:")
df_array.show(truncate=False)

# ============================================================================
# 1.1 explode() - Explode array into multiple rows
# ============================================================================
print("-" * 60)
print("1.1 explode() - Creates a row for each element")
print("Note: Rows with empty/null arrays are dropped!")
print("-" * 60)

df_array.select("id", "name", explode("languages").alias("language")).show()

# ============================================================================
# 1.2 explode_outer() - Explode with NULL preservation
# ============================================================================
print("-" * 60)
print("1.2 explode_outer() - Preserves rows with empty/null arrays")
print("-" * 60)

df_array.select("id", "name", explode_outer("languages").alias("language")).show()

# ============================================================================
# 1.3 posexplode() - Explode with position index
# ============================================================================
print("-" * 60)
print("1.3 posexplode() - Includes position index")
print("-" * 60)

df_array.select("id", "name", posexplode("languages").alias("pos", "language")).show()

# ============================================================================
# 1.4 posexplode_outer() - Explode with position, preserving nulls
# ============================================================================
print("-" * 60)
print("1.4 posexplode_outer() - Position index + null preservation")
print("-" * 60)

df_array.select("id", "name", posexplode_outer("languages").alias("pos", "language")).show()

# ============================================================================
# 1.5 inline() - Explode array of structs
# ============================================================================
print("-" * 60)
print("1.5 inline() - Explode array of structs into columns")
print("-" * 60)

struct_data = [
    (1, "Alice", [("Math", 90), ("Science", 85)]),
    (2, "Bob", [("Math", 78), ("English", 82)]),
    (3, "Charlie", []),
    (4, "Diana", None),
]
df_struct = spark.createDataFrame(struct_data, ["id", "name", "subjects"])
print("Original DataFrame with Array of Structs:")
df_struct.show(truncate=False)

# inline - drops empty/null arrays
df_struct.select("id", "name", inline("subjects")).show()

# inline_outer - preserves empty/null arrays
print("inline_outer() - preserves empty/null arrays:")
df_struct.select("id", "name", inline_outer("subjects")).show()

# ============================================================================
# PART 2: MAP FUNCTIONS
# ============================================================================
print("=" * 70)
print("PART 2: MAP FUNCTIONS")
print("=" * 70)

# Sample data with maps
map_data = [
    (1, "Alice", {"Python": 90, "Java": 85, "Scala": 88}),
    (2, "Bob", {"Python": 95, "C++": 78}),
    (3, "Charlie", {"Rust": 92}),
    (4, "Diana", {}),           # empty map
    (5, "Eve", None),           # null map
]
df_map = spark.createDataFrame(map_data, ["id", "name", "skill_scores"])
print("Original DataFrame with Maps:")
df_map.show(truncate=False)

# ============================================================================
# 2.1 explode() on Maps - Explode into key-value rows
# ============================================================================
print("-" * 60)
print("2.1 explode() on Maps - Creates rows with key-value pairs")
print("-" * 60)

df_map.select("id", "name", explode("skill_scores").alias("skill", "score")).show()

# ============================================================================
# 2.2 explode_outer() on Maps - Preserve nulls
# ============================================================================
print("-" * 60)
print("2.2 explode_outer() on Maps")
print("-" * 60)

df_map.select("id", "name", explode_outer("skill_scores").alias("skill", "score")).show()

# ============================================================================
# 2.3 posexplode() on Maps - With position
# ============================================================================
print("-" * 60)
print("2.3 posexplode() on Maps - With position index")
print("-" * 60)

df_map.select("id", "name", posexplode("skill_scores").alias("pos", "skill", "score")).show()

# ============================================================================
# 2.4 Map Creation Functions
# ============================================================================
print("-" * 60)
print("2.4 Map Creation Functions")
print("-" * 60)

# create_map() - Create map from columns
simple_data = [("Alice", "Python", 90), ("Bob", "Java", 85)]
df_simple = spark.createDataFrame(simple_data, ["name", "skill", "score"])
df_simple.select("name", create_map("skill", "score").alias("skill_map")).show(truncate=False)

# Create map with multiple key-value pairs
df_simple.select("name", 
                 create_map(lit("skill"), col("skill"), 
                           lit("score"), col("score")).alias("info_map")).show(truncate=False)

# map_from_entries() - Create map from array of key-value pairs
entry_data = [
    (1, [("a", 1), ("b", 2), ("c", 3)]),
    (2, [("x", 10), ("y", 20)]),
]
df_entries = spark.createDataFrame(entry_data, ["id", "entries"])
df_entries.select("id", map_from_entries("entries").alias("map")).show(truncate=False)

# ============================================================================
# 2.5 Map Access Functions
# ============================================================================
print("-" * 60)
print("2.5 Map Access Functions")
print("-" * 60)

# map_keys() - Get all keys
# map_values() - Get all values
df_map.select("name", "skill_scores",
              map_keys("skill_scores").alias("skills"),
              map_values("skill_scores").alias("scores")).show(truncate=False)

# element_at() - Get value by key
df_map.select("name", 
              element_at("skill_scores", "Python").alias("python_score")).show()

# map_entries() - Convert map to array of structs
df_map.select("name", "skill_scores",
              map_entries("skill_scores").alias("entries")).show(truncate=False)

# ============================================================================
# 2.6 Map Transformation Functions
# ============================================================================
print("-" * 60)
print("2.6 Map Transformation Functions")
print("-" * 60)

# map_concat() - Concatenate multiple maps
concat_data = [
    (1, {"a": 1, "b": 2}, {"c": 3, "d": 4}),
    (2, {"x": 10}, {"y": 20, "z": 30}),
]
df_concat = spark.createDataFrame(concat_data, ["id", "map1", "map2"])
df_concat.select("id", map_concat("map1", "map2").alias("combined")).show(truncate=False)

# map_filter() - Filter map entries
df_map.select("name", "skill_scores",
              map_filter("skill_scores", lambda k, v: v > 85).alias("high_scores")).show(truncate=False)

# transform_keys() - Transform map keys
df_map.select("name", "skill_scores",
              transform_keys("skill_scores", lambda k, v: concat(k, lit("_skill"))).alias("transformed_keys")).show(truncate=False)

# transform_values() - Transform map values
df_map.select("name", "skill_scores",
              transform_values("skill_scores", lambda k, v: v + 10).alias("scores_plus_10")).show(truncate=False)

# ============================================================================
# PART 3: PRACTICAL EXAMPLES
# ============================================================================
print("=" * 70)
print("PART 3: PRACTICAL EXAMPLES")
print("=" * 70)

# ============================================================================
# 3.1 Flatten nested JSON-like structure
# ============================================================================
print("-" * 60)
print("3.1 Flatten nested JSON-like structure")
print("-" * 60)

orders_data = [
    (1, "Alice", [
        {"product": "Laptop", "quantity": 1, "price": 999.99},
        {"product": "Mouse", "quantity": 2, "price": 29.99}
    ]),
    (2, "Bob", [
        {"product": "Keyboard", "quantity": 1, "price": 79.99}
    ]),
]

schema = StructType([
    StructField("order_id", IntegerType(), True),
    StructField("customer", StringType(), True),
    StructField("items", ArrayType(StructType([
        StructField("product", StringType(), True),
        StructField("quantity", IntegerType(), True),
        StructField("price", FloatType(), True),
    ])), True)
])

from pyspark.sql.types import FloatType
df_orders = spark.createDataFrame(orders_data, schema)
print("Original Orders:")
df_orders.show(truncate=False)

# Flatten to individual order lines
print("Flattened Order Lines:")
df_orders.select(
    "order_id", 
    "customer", 
    explode("items").alias("item")
).select(
    "order_id",
    "customer",
    col("item.product"),
    col("item.quantity"),
    col("item.price")
).show()

# ============================================================================
# 3.2 Aggregate back after explode (collect_list, collect_set)
# ============================================================================
print("-" * 60)
print("3.2 Aggregate back after explode")
print("-" * 60)

from pyspark.sql.functions import collect_list, collect_set, sum as spark_sum

# Explode, transform, then collect back
df_exploded = df_array.select("id", "name", explode("languages").alias("language"))
df_exploded.groupBy("id", "name").agg(
    collect_list("language").alias("languages_list"),
    collect_set("language").alias("languages_set")
).show(truncate=False)

# ============================================================================
# 3.3 Pivot using exploded data
# ============================================================================
print("-" * 60)
print("3.3 Pivot using exploded data")
print("-" * 60)

pivot_data = [
    (1, "Alice", {"Math": 90, "Science": 85, "English": 88}),
    (2, "Bob", {"Math": 78, "Science": 92, "English": 80}),
    (3, "Charlie", {"Math": 85, "English": 88}),  # Missing Science
]
df_pivot = spark.createDataFrame(pivot_data, ["id", "name", "scores"])

# Explode map and pivot
df_pivot.select("id", "name", explode("scores").alias("subject", "score")) \
    .groupBy("id", "name").pivot("subject").agg({"score": "first"}).show()

# ============================================================================
# 3.4 Multiple array columns - exploding independently
# ============================================================================
print("-" * 60)
print("3.4 Multiple array columns - Cartesian product")
print("-" * 60)

multi_array_data = [
    (1, ["A", "B"], [1, 2]),
    (2, ["X"], [10, 20, 30]),
]
df_multi = spark.createDataFrame(multi_array_data, ["id", "letters", "numbers"])
print("Original:")
df_multi.show(truncate=False)

# Exploding multiple arrays creates cartesian product
print("Cartesian product (explode both):")
df_multi.select("id", explode("letters").alias("letter")) \
    .join(df_multi.select("id", explode("numbers").alias("number")), "id") \
    .show()

# Using arrays_zip to pair elements
from pyspark.sql.functions import arrays_zip
print("Paired elements (using arrays_zip):")
df_multi.select("id", explode(arrays_zip("letters", "numbers")).alias("pair")) \
    .select("id", col("pair.letters").alias("letter"), col("pair.numbers").alias("number")) \
    .show()

# ============================================================================
# Cleanup
# ============================================================================
spark.stop()
