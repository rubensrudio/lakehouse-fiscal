from __future__ import annotations

from pathlib import Path

from pyspark.sql import DataFrame, SparkSession


def build_dim_cfop(spark: SparkSession) -> DataFrame:
    path = Path(__file__).with_name("data") / "cfop_reference.csv"
    return spark.read.option("header", "true").option("inferSchema", "true").csv(str(path))
