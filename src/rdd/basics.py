"""PySpark RDD Basics"""
from pyspark import SparkContext

sc = SparkContext("local", "RDDBasics")

data = [1, 2, 3, 4, 5]
rdd = sc.parallelize(data)

print("Collect:", rdd.collect())
print("Count:", rdd.count())
print("First:", rdd.first())
print("Take 3:", rdd.take(3))

sc.stop()
