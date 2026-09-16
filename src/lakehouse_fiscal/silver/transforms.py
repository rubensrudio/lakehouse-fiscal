from __future__ import annotations

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F


def digits_only(df: DataFrame, *columns: str) -> DataFrame:
    result = df
    for column in columns:
        result = result.withColumn(column, F.regexp_replace(F.col(column), "[^0-9]", ""))
    return result


def deduplicate(df: DataFrame, keys: tuple[str, ...], order_by: tuple[str, ...]) -> DataFrame:
    window = Window.partitionBy(*keys).orderBy(
        *(F.col(column).desc_nulls_last() for column in order_by)
    )
    return (
        df.withColumn("_rank", F.row_number().over(window))
        .filter(F.col("_rank") == 1)
        .drop("_rank")
    )


def derive_product_family(df: DataFrame) -> DataFrame:
    return df.withColumn(
        "familia_produto",
        F.when(F.col("ncm").startswith("84"), "ELETRONICOS")
        .when(F.col("ncm").startswith("85"), "ELETRONICOS")
        .when(F.col("ncm").startswith("94"), "MOVEIS")
        .when(F.col("ncm").startswith("61"), "VESTUARIO")
        .otherwise("UTILIDADES"),
    )
