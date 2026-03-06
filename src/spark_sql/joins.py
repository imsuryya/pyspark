"""PySpark SQL Joins"""
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("SparkSQLJoins").getOrCreate()

employees = spark.createDataFrame([
    (1, "Alice", 101),
    (2, "Bob", 102),
    (3, "Charlie", 101)
], ["id", "name", "dept_id"])

departments = spark.createDataFrame([
    (101, "HR"),
    (102, "IT"),
    (103, "Finance")
], ["dept_id", "dept_name"])

employees.createOrReplaceTempView("employees")
departments.createOrReplaceTempView("departments")

spark.sql("""
    SELECT e.name, d.dept_name 
    FROM employees e 
    JOIN departments d ON e.dept_id = d.dept_id
""").show()

spark.sql("""
    SELECT e.name, d.dept_name 
    FROM employees e 
    LEFT JOIN departments d ON e.dept_id = d.dept_id
""").show()

spark.stop()
