"""PySpark DataFrame Joins"""
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("DataFrameJoins").getOrCreate()

employees = spark.createDataFrame([
    (1, "Alice", 101, "New York"),
    (2, "Bob", 102, "Chicago"),
    (3, "Charlie", 101, "New York"),
    (4, "Diana", None, "Boston"),
    (5, "Eve", 104, "Seattle")
], ["emp_id", "name", "dept_id", "city"])

departments = spark.createDataFrame([
    (101, "HR", "Building A"),
    (102, "IT", "Building B"),
    (103, "Finance", "Building C")
], ["dept_id", "dept_name", "location"])

print("=== Inner Join ===")
employees.join(departments, "dept_id", "inner").show()

print("=== Left Join ===")
employees.join(departments, "dept_id", "left").show()

print("=== Right Join ===")
employees.join(departments, "dept_id", "right").show()

print("=== Full Outer Join ===")
employees.join(departments, "dept_id", "full").show()

print("=== Left Semi Join (rows in left that have match in right) ===")
employees.join(departments, "dept_id", "left_semi").show()

print("=== Left Anti Join (rows in left that have NO match in right) ===")
employees.join(departments, "dept_id", "left_anti").show()

print("=== Cross Join ===")
cities = spark.createDataFrame([("Remote",), ("On-site",)], ["work_type"])
departments.crossJoin(cities).show()

print("=== Join on multiple conditions ===")
offices = spark.createDataFrame([
    (101, "New York", "Floor 1"),
    (101, "Chicago", "Floor 2"),
    (102, "Chicago", "Floor 3")
], ["dept_id", "city", "floor"])

employees.join(
    offices,
    (employees.dept_id == offices.dept_id) & (employees.city == offices.city),
    "inner"
).select(employees.name, employees.city, offices.floor).show()

print("=== Self Join ===")
managers = spark.createDataFrame([
    (1, "Alice", None),
    (2, "Bob", 1),
    (3, "Charlie", 1),
    (4, "Diana", 2)
], ["emp_id", "name", "manager_id"])

emp = managers.alias("e")
mgr = managers.alias("m")
emp.join(mgr, emp.manager_id == mgr.emp_id, "left") \
    .select(emp.name.alias("employee"), mgr.name.alias("manager")) \
    .show()

spark.stop()
