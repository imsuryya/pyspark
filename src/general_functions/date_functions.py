"""PySpark Date and Time Functions"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, current_date, current_timestamp,
    datediff, date_add, date_sub, months_between, add_months,
    year, month, dayofmonth, dayofweek, dayofyear,
    hour, minute, second,
    date_format, to_date, to_timestamp,
    last_day, next_day, weekofyear, quarter,
    unix_timestamp, from_unixtime
)

spark = SparkSession.builder.appName("DateTimeFunctions").getOrCreate()

data = [
    ("Alice", "2024-01-15", "2024-01-15 08:30:45"),
    ("Bob", "2024-06-20", "2024-06-20 14:15:30"),
    ("Charlie", "2024-12-25", "2024-12-25 23:59:59")
]
df = spark.createDataFrame(data, ["name", "date_str", "timestamp_str"])

print("=== current_date() / current_timestamp() ===")
df.select("name", current_date().alias("today"), current_timestamp().alias("now")).show(truncate=False)

print("=== to_date() / to_timestamp() ===")
df.select(
    "name",
    to_date(col("date_str")).alias("date_val"),
    to_timestamp(col("timestamp_str")).alias("timestamp_val")
).show()

print("=== year() / month() / dayofmonth() ===")
df.select(
    "name",
    year(col("date_str")).alias("year"),
    month(col("date_str")).alias("month"),
    dayofmonth(col("date_str")).alias("day")
).show()

print("=== dayofweek() / dayofyear() / weekofyear() / quarter() ===")
df.select(
    "name",
    dayofweek(col("date_str")).alias("day_of_week"),
    dayofyear(col("date_str")).alias("day_of_year"),
    weekofyear(col("date_str")).alias("week_of_year"),
    quarter(col("date_str")).alias("quarter")
).show()

print("=== hour() / minute() / second() ===")
df.select(
    "name",
    hour(col("timestamp_str")).alias("hour"),
    minute(col("timestamp_str")).alias("minute"),
    second(col("timestamp_str")).alias("second")
).show()

print("=== date_add() / date_sub() ===")
df.select(
    "name",
    col("date_str"),
    date_add(col("date_str"), 7).alias("plus_7_days"),
    date_sub(col("date_str"), 7).alias("minus_7_days")
).show()

print("=== add_months() / months_between() ===")
df.select(
    "name",
    col("date_str"),
    add_months(col("date_str"), 3).alias("plus_3_months"),
    months_between(current_date(), col("date_str")).alias("months_diff")
).show()

print("=== datediff() ===")
df.select(
    "name",
    datediff(current_date(), col("date_str")).alias("days_since")
).show()

print("=== date_format() ===")
df.select(
    "name",
    date_format(col("date_str"), "dd-MM-yyyy").alias("dd_mm_yyyy"),
    date_format(col("date_str"), "EEEE").alias("day_name"),
    date_format(col("date_str"), "MMMM").alias("month_name"),
    date_format(col("timestamp_str"), "hh:mm:ss a").alias("time_12hr")
).show()

print("=== last_day() / next_day() ===")
df.select(
    "name",
    col("date_str"),
    last_day(col("date_str")).alias("month_end"),
    next_day(col("date_str"), "Monday").alias("next_monday")
).show()

print("=== unix_timestamp() / from_unixtime() ===")
df.select(
    "name",
    unix_timestamp(col("timestamp_str")).alias("epoch"),
    from_unixtime(unix_timestamp(col("timestamp_str")), "yyyy/MM/dd").alias("formatted")
).show()

spark.stop()
