"""
PySpark Array Functions
========================
This module demonstrates various array functions available in PySpark.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    array, array_contains, array_distinct, array_except, array_intersect,
    array_join, array_max, array_min, array_position, array_remove,
    array_repeat, array_sort, array_union, arrays_overlap, arrays_zip,
    concat, element_at, flatten, reverse, shuffle, size, slice, sort_array,
    col, lit, struct
)

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("Array Functions") \
    .master("local[*]") \
    .getOrCreate()

# ============================================================================
# Sample Data
# ============================================================================
data = [
    (1, "Alice", ["Python", "Java", "Scala"], [90, 85, 88]),
    (2, "Bob", ["Python", "C++", "Go"], [95, 78, 82]),
    (3, "Charlie", ["Java", "Scala", "Rust"], [88, 92, 85]),
    (4, "Diana", ["Python", "Java", "Python"], [91, 87, 93]),  # duplicate in array
    (5, "Eve", [], []),  # empty arrays
]

df = spark.createDataFrame(data, ["id", "name", "languages", "scores"])
print("Original DataFrame:")
df.show(truncate=False)

# ============================================================================
# 1. ARRAY CREATION FUNCTIONS
# ============================================================================
print("=" * 60)
print("1. ARRAY CREATION FUNCTIONS")
print("=" * 60)

# array() - Create an array from columns
df_array = spark.createDataFrame([(1, 2, 3), (4, 5, 6)], ["a", "b", "c"])
df_array.select(array("a", "b", "c").alias("array_col")).show()

# array_repeat() - Create an array with element repeated n times
df.select("name", array_repeat(lit("PySpark"), 3).alias("repeated")).show(truncate=False)

# ============================================================================
# 2. ARRAY SIZE AND ELEMENT ACCESS
# ============================================================================
print("=" * 60)
print("2. ARRAY SIZE AND ELEMENT ACCESS")
print("=" * 60)

# size() - Get the size of array
df.select("name", "languages", size("languages").alias("num_languages")).show()

# element_at() - Get element at index (1-based index)
df.select("name", "languages", 
          element_at("languages", 1).alias("first_lang"),
          element_at("languages", -1).alias("last_lang")).show()

# array_position() - Find position of element (1-based, 0 if not found)
df.select("name", "languages", 
          array_position("languages", "Python").alias("python_position")).show()

# ============================================================================
# 3. ARRAY CONTAINS AND SEARCH
# ============================================================================
print("=" * 60)
print("3. ARRAY CONTAINS AND SEARCH")
print("=" * 60)

# array_contains() - Check if array contains an element
df.select("name", "languages", 
          array_contains("languages", "Python").alias("knows_python")).show()

# Filter rows where array contains specific element
print("People who know Python:")
df.filter(array_contains("languages", "Python")).show()

# ============================================================================
# 4. ARRAY TRANSFORMATION FUNCTIONS
# ============================================================================
print("=" * 60)
print("4. ARRAY TRANSFORMATION FUNCTIONS")
print("=" * 60)

# array_distinct() - Remove duplicates from array
df.select("name", "languages", 
          array_distinct("languages").alias("distinct_languages")).show(truncate=False)

# array_remove() - Remove all occurrences of element
df.select("name", "languages", 
          array_remove("languages", "Python").alias("without_python")).show(truncate=False)

# array_sort() / sort_array() - Sort array
df.select("name", "languages", 
          array_sort("languages").alias("sorted_asc"),
          sort_array("languages", asc=False).alias("sorted_desc")).show(truncate=False)

# reverse() - Reverse array
df.select("name", "languages", 
          reverse("languages").alias("reversed")).show(truncate=False)

# shuffle() - Randomly shuffle array
df.select("name", "languages", 
          shuffle("languages").alias("shuffled")).show(truncate=False)

# ============================================================================
# 5. ARRAY SLICING AND SUBSETTING
# ============================================================================
print("=" * 60)
print("5. ARRAY SLICING AND SUBSETTING")
print("=" * 60)

# slice() - Extract a portion of array (start index is 1-based)
df.select("name", "languages", 
          slice("languages", 1, 2).alias("first_two")).show(truncate=False)

# ============================================================================
# 6. ARRAY SET OPERATIONS
# ============================================================================
print("=" * 60)
print("6. ARRAY SET OPERATIONS")
print("=" * 60)

# Create another DataFrame for set operations
set_data = [
    (1, ["Python", "Java"], ["Java", "Scala"]),
    (2, ["Go", "Rust"], ["Go", "C++"]),
]
df_sets = spark.createDataFrame(set_data, ["id", "set1", "set2"])

# array_union() - Union of two arrays
df_sets.select("set1", "set2", 
               array_union("set1", "set2").alias("union")).show(truncate=False)

# array_intersect() - Intersection of two arrays
df_sets.select("set1", "set2", 
               array_intersect("set1", "set2").alias("intersection")).show(truncate=False)

# array_except() - Elements in first array but not in second
df_sets.select("set1", "set2", 
               array_except("set1", "set2").alias("except")).show(truncate=False)

# arrays_overlap() - Check if arrays have common elements
df_sets.select("set1", "set2", 
               arrays_overlap("set1", "set2").alias("has_overlap")).show()

# ============================================================================
# 7. ARRAY CONCATENATION AND JOINING
# ============================================================================
print("=" * 60)
print("7. ARRAY CONCATENATION AND JOINING")
print("=" * 60)

# concat() - Concatenate arrays
df_sets.select("set1", "set2", 
               concat("set1", "set2").alias("concatenated")).show(truncate=False)

# array_join() - Join array elements into string
df.select("name", "languages", 
          array_join("languages", ", ").alias("languages_string"),
          array_join("languages", " | ", "N/A").alias("with_null_replacement")).show(truncate=False)

# ============================================================================
# 8. ARRAY AGGREGATE FUNCTIONS
# ============================================================================
print("=" * 60)
print("8. ARRAY AGGREGATE FUNCTIONS")
print("=" * 60)

# array_max() - Maximum element in array
# array_min() - Minimum element in array
df.select("name", "scores", 
          array_max("scores").alias("max_score"),
          array_min("scores").alias("min_score")).show()

# ============================================================================
# 9. ARRAYS_ZIP - Combine Multiple Arrays
# ============================================================================
print("=" * 60)
print("9. ARRAYS_ZIP - Combine Multiple Arrays")
print("=" * 60)

df.select("name", "languages", "scores", 
          arrays_zip("languages", "scores").alias("lang_score_pairs")).show(truncate=False)

# ============================================================================
# 10. FLATTEN - Flatten Nested Arrays
# ============================================================================
print("=" * 60)
print("10. FLATTEN - Flatten Nested Arrays")
print("=" * 60)

nested_data = [
    (1, [["a", "b"], ["c", "d"]]),
    (2, [["e", "f"], ["g", "h", "i"]]),
]
df_nested = spark.createDataFrame(nested_data, ["id", "nested_array"])
df_nested.select("nested_array", 
                 flatten("nested_array").alias("flattened")).show(truncate=False)

# ============================================================================
# 11. HIGHER-ORDER FUNCTIONS (Spark 2.4+)
# ============================================================================
print("=" * 60)
print("11. HIGHER-ORDER FUNCTIONS (Spark 2.4+)")
print("=" * 60)

from pyspark.sql.functions import transform, filter, aggregate, exists, forall, zip_with

# transform() - Apply function to each element
df.select("name", "scores",
          transform("scores", lambda x: x * 2).alias("doubled_scores")).show(truncate=False)

# filter() - Filter array elements
df.select("name", "scores",
          filter("scores", lambda x: x > 85).alias("high_scores")).show(truncate=False)

# aggregate() - Reduce array to single value
df.select("name", "scores",
          aggregate("scores", lit(0), lambda acc, x: acc + x).alias("total_score")).show()

# exists() - Check if any element satisfies condition
df.select("name", "scores",
          exists("scores", lambda x: x > 90).alias("has_score_above_90")).show()

# forall() - Check if all elements satisfy condition
df.select("name", "scores",
          forall("scores", lambda x: x >= 80).alias("all_above_80")).show()

# zip_with() - Combine two arrays element-wise with function
zip_data = [(1, [1, 2, 3], [10, 20, 30])]
df_zip = spark.createDataFrame(zip_data, ["id", "arr1", "arr2"])
df_zip.select(
    zip_with("arr1", "arr2", lambda x, y: x + y).alias("sums"),
    zip_with("arr1", "arr2", lambda x, y: x * y).alias("products")
).show(truncate=False)

# ============================================================================
# 12. WORKING WITH ARRAY OF STRUCTS
# ============================================================================
print("=" * 60)
print("12. WORKING WITH ARRAY OF STRUCTS")
print("=" * 60)

struct_data = [
    (1, "Alice", [("Math", 90), ("Science", 85), ("English", 88)]),
    (2, "Bob", [("Math", 78), ("Science", 92), ("English", 80)]),
]
df_struct = spark.createDataFrame(struct_data, ["id", "name", "subjects"])
df_struct.printSchema()
df_struct.show(truncate=False)

# Access struct fields within array
df_struct.select(
    "name",
    transform("subjects", lambda x: x["_1"]).alias("subject_names"),
    transform("subjects", lambda x: x["_2"]).alias("subject_scores")
).show(truncate=False)

# ============================================================================
# Cleanup
# ============================================================================
spark.stop()
