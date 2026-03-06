"""PySpark RDD Transformations"""
from pyspark import SparkContext

sc = SparkContext("local", "RDDTransformations")

rdd = sc.parallelize([1, 2, 3, 4, 5])

mapped = rdd.map(lambda x: x * 2)
print("Map:", mapped.collect())

filtered = rdd.filter(lambda x: x % 2 == 0)
print("Filter:", filtered.collect())

total = rdd.reduce(lambda a, b: a + b)
print("Reduce (sum):", total)

words = sc.parallelize(["hello world", "pyspark rdd"])
flat = words.flatMap(lambda x: x.split(" "))
print("FlatMap:", flat.collect())

sc.stop()
