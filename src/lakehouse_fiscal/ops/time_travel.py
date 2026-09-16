from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

from pyspark.sql import DataFrame, SparkSession


class VersionOutOfWindowError(LookupError):
    pass


@dataclass(frozen=True)
class VersionDiff:
    rows_added: int
    rows_removed: int
    rows_changed: int


def history(spark: SparkSession, table: str, limit: int = 20) -> DataFrame:
    return spark.sql(f"DESCRIBE HISTORY {table} LIMIT {int(limit)}")


def _earliest(spark: SparkSession, table: str) -> int:
    row = history(spark, table, 10_000).selectExpr("min(version) AS version").first()
    return int(row.version) if row and row.version is not None else 0


def read_version(spark: SparkSession, table: str, version: int) -> DataFrame:
    try:
        return spark.read.format("delta").option("versionAsOf", version).table(table)
    except Exception as exc:
        earliest = _earliest(spark, table)
        raise VersionOutOfWindowError(
            f"Version {version} of '{table}' is no longer available (earliest retained version: "
            f"{earliest}). It was likely removed by VACUUM."
        ) from exc


def read_timestamp(spark: SparkSession, table: str, ts: datetime) -> DataFrame:
    return spark.read.format("delta").option("timestampAsOf", ts.isoformat()).table(table)


def diff(spark: SparkSession, table: str, v1: int, v2: int) -> VersionDiff:
    left = read_version(spark, table, v1)
    right = read_version(spark, table, v2)
    removed = left.exceptAll(right).count()
    added = right.exceptAll(left).count()
    return VersionDiff(added, removed, 0)
