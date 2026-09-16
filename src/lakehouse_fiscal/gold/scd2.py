from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass

from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F

from lakehouse_fiscal.io.delta_writer import overwrite


@dataclass(frozen=True)
class Scd2Config:
    target_table: str
    natural_keys: Sequence[str]
    tracked_columns: Sequence[str]
    surrogate_key: str
    unknown_member_sk: int = -1
    open_valid_to: str = "9999-12-31 23:59:59"


@dataclass(frozen=True)
class Scd2Result:
    rows_inserted: int
    rows_closed: int
    version: int


def compute_scd_hash(df: DataFrame, tracked_columns: Sequence[str]) -> DataFrame:
    values = [F.coalesce(F.col(column).cast("string"), F.lit("∅")) for column in tracked_columns]
    return df.withColumn("scd_hash", F.sha2(F.concat_ws("\u241f", *values), 256))


def _surrogate_key(cfg: Scd2Config) -> Column:
    return F.abs(F.xxhash64(*[F.col(key) for key in cfg.natural_keys], F.col("valid_from")))


def scd2_merge(
    source: DataFrame,
    cfg: Scd2Config,
    *,
    change_ts_col: str,
    tie_break_col: str,
    batch_id: str,
) -> Scd2Result:
    spark = source.sparkSession
    latest = (
        compute_scd_hash(source, cfg.tracked_columns)
        .withColumn(
            "_rank",
            F.row_number().over(
                Window.partitionBy(*cfg.natural_keys).orderBy(
                    F.col(change_ts_col).desc(), F.col(tie_break_col).desc()
                )
            ),
        )
        .filter(F.col("_rank") == 1)
        .drop("_rank")
    )
    open_to = F.to_timestamp(F.lit(cfg.open_valid_to))
    if not spark.catalog.tableExists(cfg.target_table):
        initial = (
            latest.withColumn("valid_from", F.col(change_ts_col).cast("timestamp"))
            .withColumn("valid_to", open_to)
            .withColumn("is_current", F.lit(True))
            .withColumn(cfg.surrogate_key, _surrogate_key(cfg).cast("long"))
        )
        written = overwrite(initial, cfg.target_table, batch_id=batch_id, stage="gold_scd2")
        return Scd2Result(initial.count(), 0, written.version)
    target = spark.table(cfg.target_table)
    current = target.filter("is_current = true").select(
        *[F.col(key) for key in cfg.natural_keys], F.col("scd_hash").alias("_current_hash")
    )
    joined = latest.join(current, list(cfg.natural_keys), "left")
    change_condition = F.col("_current_hash").isNull() | (
        F.col("scd_hash") != F.col("_current_hash")
    )
    if "op" in latest.columns:
        change_condition = change_condition | (F.col("op") == "D")
    changed = joined.filter(change_condition).drop("_current_hash")
    changed_count = changed.count()
    if changed_count == 0:
        history_row = spark.sql(f"DESCRIBE HISTORY {cfg.target_table} LIMIT 1").first()
        version = int(history_row.version) if history_row else -1
        return Scd2Result(0, 0, version)
    closing = changed.select(*cfg.natural_keys, F.col(change_ts_col).alias("_close_at"))
    closed_target = (
        target.join(closing, list(cfg.natural_keys), "left")
        .withColumn(
            "valid_to",
            F.when(
                F.col("is_current") & F.col("_close_at").isNotNull(), F.col("_close_at")
            ).otherwise(F.col("valid_to")),
        )
        .withColumn(
            "is_current",
            F.when(F.col("is_current") & F.col("_close_at").isNotNull(), F.lit(False)).otherwise(
                F.col("is_current")
            ),
        )
        .drop("_close_at")
    )
    prepared = changed
    if "op" in changed.columns and "situacao_cadastral" in changed.columns:
        prepared = prepared.withColumn(
            "situacao_cadastral",
            F.when(F.col("op") == "D", F.lit("BAIXADA")).otherwise(F.col("situacao_cadastral")),
        )
    new_versions = (
        prepared.withColumn("valid_from", F.col(change_ts_col).cast("timestamp"))
        .withColumn("valid_to", open_to)
        .withColumn("is_current", F.lit(True))
        .withColumn(cfg.surrogate_key, _surrogate_key(cfg).cast("long"))
    )
    final_df = closed_target.unionByName(new_versions, allowMissingColumns=True)
    written = overwrite(final_df, cfg.target_table, batch_id=batch_id, stage="gold_scd2")
    return Scd2Result(changed_count, changed_count, written.version)


def resolve_sk_as_of(
    facts: DataFrame,
    dim: DataFrame,
    cfg: Scd2Config,
    *,
    event_ts_col: str,
) -> DataFrame:
    fact = facts.alias("fact")
    dimension = dim.alias("dim")
    key_condition = F.lit(True)
    for key in cfg.natural_keys:
        key_condition = key_condition & (F.col(f"fact.{key}") == F.col(f"dim.{key}"))
    temporal = (F.col(f"fact.{event_ts_col}") >= F.col("dim.valid_from")) & (
        F.col(f"fact.{event_ts_col}") < F.col("dim.valid_to")
    )
    return fact.join(dimension, key_condition & temporal, "left").select(
        "fact.*",
        F.coalesce(F.col(f"dim.{cfg.surrogate_key}"), F.lit(cfg.unknown_member_sk)).alias(
            cfg.surrogate_key
        ),
    )
