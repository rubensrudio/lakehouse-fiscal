from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession


def read_table(spark: SparkSession, table: str) -> DataFrame:
    return spark.read.table(table)
