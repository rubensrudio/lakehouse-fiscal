from __future__ import annotations

from collections.abc import Sequence

from delta.tables import DeltaTable
from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F

from lakehouse_fiscal.config.naming import resolve
from lakehouse_fiscal.config.profiles import load_settings
from lakehouse_fiscal.io.delta_writer import append
from lakehouse_fiscal.utils.ulid import new_ulid


def quarantine_rows(
    df: DataFrame,
    *,
    source_table: str,
    natural_key_expr: Column,
    failed_expectations: Column,
    batch_id: str,
) -> int:
    count = df.count()
    if count == 0:
        return 0
    target = resolve("silver", "quarantine_nfe", load_settings())
    payload_columns = [F.col(column) for column in df.columns]
    enriched = df.select(
        F.udf(lambda: new_ulid(), "string")().alias("quarantine_id"),
        F.lit(source_table).alias("source_table"),
        natural_key_expr.cast("string").alias("natural_key"),
        F.to_json(failed_expectations).alias("failed_expectations"),
        F.to_json(F.struct(*payload_columns)).alias("row_payload_json"),
        F.current_timestamp().alias("_quarantined_ts"),
        F.lit(False).alias("is_reprocessed"),
        F.lit(batch_id).alias("batch_id"),
    )
    append(enriched, target, batch_id=batch_id, stage="quarantine", merge_schema=True)
    return count


def mark_reprocessed(keys: Sequence[str], batch_id: str) -> int:
    spark = SparkSession.getActiveSession()
    if spark is None or not keys:
        return 0
    table = resolve("silver", "quarantine_nfe", load_settings())
    if not spark.catalog.tableExists(table):
        return 0
    count = (
        spark.table(table)
        .filter(F.col("natural_key").isin(list(keys)) & (F.col("batch_id") == batch_id))
        .count()
    )
    DeltaTable.forName(spark, table).update(
        condition=F.col("natural_key").isin(list(keys)) & (F.col("batch_id") == batch_id),
        set={"is_reprocessed": F.lit(True)},
    )
    return count
