from __future__ import annotations

import json
import time
from collections.abc import Sequence
from dataclasses import dataclass

from delta.tables import DeltaTable
from pyspark.errors import AnalysisException
from pyspark.sql import Column, DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from lakehouse_fiscal.config.naming import parse
from lakehouse_fiscal.config.paths import table_location
from lakehouse_fiscal.config.profiles import Profile, load_settings
from lakehouse_fiscal.utils.logging import get_logger

LOGGER = get_logger(__name__)


@dataclass(frozen=True)
class WriteResult:
    rows_written: int
    version: int


@dataclass(frozen=True)
class MergeResult:
    rows_inserted: int
    rows_updated: int
    rows_deleted: int
    version: int


def _prepare(df: DataFrame, table: str, batch_id: str, stage: str) -> None:
    settings = load_settings()
    catalog, layer, logical_table = parse(table, settings)
    df.sparkSession.conf.set(
        "spark.databricks.delta.commitInfo.userMetadata",
        json.dumps({"batch_id": batch_id, "stage": stage}, sort_keys=True),
    )
    if settings.profile is Profile.LOCAL:
        schema = f"{catalog}_{layer}"
        df.sparkSession.sql(f"CREATE DATABASE IF NOT EXISTS {schema}")
        location = table_location(layer, logical_table, settings)
        if not df.sparkSession.catalog.tableExists(table):
            if DeltaTable.isDeltaTable(df.sparkSession, location):
                df.sparkSession.sql(
                    f"CREATE TABLE IF NOT EXISTS {table} USING DELTA LOCATION '{location}'"
                )
            else:
                empty = df.limit(0)
                empty.write.format("delta").mode("overwrite").option("path", location).saveAsTable(
                    table
                )


def _version(table: str, spark: SparkSession) -> int:
    row = spark.sql(f"DESCRIBE HISTORY {table} LIMIT 1").select("version").first()
    return int(row.version) if row else -1


def append(
    df: DataFrame, table: str, *, batch_id: str, stage: str, merge_schema: bool = False
) -> WriteResult:
    _prepare(df, table, batch_id, stage)
    rows = df.count()
    (
        df.write.format("delta")
        .mode("append")
        .option("mergeSchema", str(merge_schema).lower())
        .saveAsTable(table)
    )
    return WriteResult(rows, _version(table, df.sparkSession))


def overwrite(
    df: DataFrame,
    table: str,
    *,
    batch_id: str,
    stage: str,
    partition_by: Sequence[str] = (),
) -> WriteResult:
    _prepare(df, table, batch_id, stage)
    writer = df.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
    if partition_by:
        writer = writer.partitionBy(*partition_by)
    rows = df.count()
    writer.saveAsTable(table)
    return WriteResult(rows, _version(table, df.sparkSession))


def _operation_metrics(table: str, df: DataFrame) -> dict[str, int]:
    row = (
        df.sparkSession.sql(f"DESCRIBE HISTORY {table} LIMIT 1").select("operationMetrics").first()
    )
    raw = row.operationMetrics if row and row.operationMetrics else {}
    return {str(key): int(value) for key, value in raw.items() if str(value).isdigit()}


def merge_upsert(
    df: DataFrame,
    table: str,
    *,
    keys: Sequence[str],
    order_by: Sequence[str],
    batch_id: str,
    stage: str,
    merge_schema: bool = False,
) -> MergeResult:
    if not keys:
        raise ValueError("merge_upsert requires at least one key")
    ordering = [F.col(column).desc_nulls_last() for column in order_by]
    source = (
        df.withColumn(
            "__row_number", F.row_number().over(Window.partitionBy(*keys).orderBy(*ordering))
        )
        .filter(F.col("__row_number") == 1)
        .drop("__row_number")
    )
    _prepare(source, table, batch_id, stage)
    if merge_schema:
        source.sparkSession.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")
    condition = " AND ".join(f"target.`{key}` <=> source.`{key}`" for key in keys)
    for attempt in range(1, 4):
        try:
            target = DeltaTable.forName(source.sparkSession, table)
            updates: dict[str, str | Column] = {
                column: f"source.`{column}`" for column in source.columns
            }
            ignored_for_change_detection = {
                *keys,
                "_batch_id",
                "_ingest_ts",
                "op_seq",
                "op_ts",
            }
            comparisons = [
                f"NOT (target.`{column}` <=> source.`{column}`)"
                for column in source.columns
                if column not in ignored_for_change_detection
            ]
            update_condition = " OR ".join(comparisons) or "false"
            (
                target.alias("target")
                .merge(source.alias("source"), condition)
                .whenMatchedUpdate(condition=update_condition, set=updates)
                .whenNotMatchedInsert(values=updates)
                .execute()
            )
            break
        except AnalysisException:
            source.write.format("delta").mode("overwrite").option(
                "overwriteSchema", "true"
            ).saveAsTable(table)
            break
        except Exception as exc:
            if "ConcurrentAppendException" not in type(
                exc
            ).__name__ and "ConcurrentAppendException" not in str(exc):
                raise
            if attempt == 3:
                raise
            delay = 2 ** (attempt - 1)
            LOGGER.warning(
                "ConcurrentAppendException on '%s', retry %d/3 in %ds.", table, attempt, delay
            )
            time.sleep(delay)
    metrics = _operation_metrics(table, source)
    return MergeResult(
        metrics.get("numTargetRowsInserted", metrics.get("numOutputRows", 0)),
        metrics.get("numTargetRowsUpdated", 0),
        metrics.get("numTargetRowsDeleted", 0),
        _version(table, source.sparkSession),
    )
