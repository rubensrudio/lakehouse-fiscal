from __future__ import annotations

from datetime import UTC, datetime

from pyspark.sql import SparkSession

from lakehouse_fiscal.config.naming import resolve
from lakehouse_fiscal.config.profiles import load_settings
from lakehouse_fiscal.io.delta_writer import append, merge_upsert
from lakehouse_fiscal.schemas.contracts import BRONZE_CONTRACTS


def is_batch_applied(spark: SparkSession, *, batch_id: str, stage: str, target_table: str) -> bool:
    table = resolve("bronze", "_ingest_batches", load_settings())
    if not spark.catalog.tableExists(table):
        return False
    return bool(
        spark.table(table)
        .filter(
            (spark.table(table).batch_id == batch_id)
            & (spark.table(table).stage == stage)
            & (spark.table(table).target_table == target_table)
        )
        .limit(1)
        .count()
    )


def register_batch(
    spark: SparkSession,
    *,
    batch_id: str,
    stage: str,
    target_table: str,
    rows_written: int,
    profile: str,
) -> None:
    table = resolve("bronze", "_ingest_batches", load_settings())
    row = [(batch_id, stage, target_table, datetime.now(UTC), rows_written, profile)]
    append(
        spark.createDataFrame(row, BRONZE_CONTRACTS["_ingest_batches"]),
        table,
        batch_id=batch_id,
        stage="control",
    )


def read_watermark(spark: SparkSession, *, source: str, profile: str) -> int:
    table = resolve("bronze", "_cdc_watermark", load_settings())
    if not spark.catalog.tableExists(table):
        return -1
    row = (
        spark.table(table)
        .filter((spark.table(table).source == source) & (spark.table(table).profile == profile))
        .select("last_op_seq")
        .first()
    )
    return int(row.last_op_seq) if row else -1


def write_watermark(spark: SparkSession, *, source: str, profile: str, last_op_seq: int) -> None:
    table = resolve("bronze", "_cdc_watermark", load_settings())
    row = [(source, profile, last_op_seq, datetime.now(UTC))]
    df = spark.createDataFrame(row, BRONZE_CONTRACTS["_cdc_watermark"])
    merge_upsert(
        df,
        table,
        keys=("source", "profile"),
        order_by=("updated_ts",),
        batch_id=f"watermark-{last_op_seq}",
        stage="control",
    )
