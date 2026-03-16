"""Common PySpark imports and helpers for this repository.

Use this module to avoid repeating imports and Spark session setup in every file.
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.window import Window


def create_spark_session(app_name: str = "PySparkApp", master: str = "local[*]") -> SparkSession:
    """Create and return a SparkSession with consistent defaults.

    Args:
        app_name: Name displayed in Spark UI.
        master: Spark master URL.

    Returns:
        Configured SparkSession.
    """
    return (
        SparkSession.builder
        .appName(app_name)
        .master(master)
        .getOrCreate()
    )


__all__ = ["create_spark_session", "F", "T", "Window"]
