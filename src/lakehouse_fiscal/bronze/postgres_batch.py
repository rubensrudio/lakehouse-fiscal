from __future__ import annotations

from dataclasses import dataclass

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from lakehouse_fiscal.config.naming import resolve
from lakehouse_fiscal.config.profiles import Settings
from lakehouse_fiscal.config.secrets import get_secret
from lakehouse_fiscal.control.control_tables import register_batch
from lakehouse_fiscal.io.delta_writer import MergeResult, merge_upsert
from lakehouse_fiscal.schemas.nfe import NFE_EVENTO_SCHEMA, NFE_ITEM_SCHEMA


@dataclass(frozen=True)
class BronzeBatchResult:
    headers: MergeResult
    items: MergeResult
    events: MergeResult
    emitentes: MergeResult

    @property
    def rows_written(self) -> int:
        return sum(
            result.rows_inserted + result.rows_updated
            for result in (self.headers, self.items, self.events, self.emitentes)
        )


def _jdbc(spark: SparkSession, settings: Settings, query: str) -> DataFrame:
    return (
        spark.read.format("jdbc")
        .option("url", settings.jdbc)
        .option("query", query)
        .option("user", get_secret(settings.secret_scope, "POSTGRES_USER"))
        .option("password", get_secret(settings.secret_scope, "POSTGRES_PASSWORD"))
        .option("driver", "org.postgresql.Driver")
        .load()
    )


def ingest_postgres_batch(
    spark: SparkSession, settings: Settings, batch_id: str
) -> BronzeBatchResult:
    header_df = (
        _jdbc(spark, settings, "SELECT * FROM nfe_cab")
        .withColumn("_batch_id", F.lit(batch_id))
        .withColumn("_ingest_ts", F.current_timestamp())
    )
    item_payload = _jdbc(spark, settings, "SELECT payload::text AS payload FROM nfe_item")
    item_df = (
        item_payload.select(F.from_json("payload", NFE_ITEM_SCHEMA).alias("row"))
        .select("row.*")
        .withColumn("_batch_id", F.lit(batch_id))
        .withColumn("_ingest_ts", F.current_timestamp())
    )
    event_payload = _jdbc(
        spark,
        settings,
        "SELECT payload_json::text AS payload FROM cdc_changelog WHERE table_name='nfe_evento'",
    )
    event_df = (
        event_payload.select(F.from_json("payload", NFE_EVENTO_SCHEMA).alias("row"))
        .select("row.*")
        .withColumn("_batch_id", F.lit(batch_id))
        .withColumn("_ingest_ts", F.current_timestamp())
    )
    emitente_df = (
        _jdbc(spark, settings, "SELECT * FROM emitente")
        .withColumn("op", F.lit("I"))
        .withColumn("op_seq", F.monotonically_increasing_id())
        .withColumn("op_ts", F.current_timestamp())
        .withColumn("_batch_id", F.lit(batch_id))
        .withColumn("_ingest_ts", F.current_timestamp())
    )
    header_table = resolve("bronze", "nfe_cab_raw", settings)
    item_table = resolve("bronze", "nfe_item_raw", settings)
    event_table = resolve("bronze", "nfe_evento_raw", settings)
    emitente_table = resolve("bronze", "emitente_cdc_raw", settings)
    headers = merge_upsert(
        header_df,
        header_table,
        keys=("chave_acesso",),
        order_by=("updated_at_source", "_ingest_ts"),
        batch_id=batch_id,
        stage="bronze_postgres",
        merge_schema=True,
    )
    items = merge_upsert(
        item_df,
        item_table,
        keys=("chave_acesso", "num_item"),
        order_by=("_ingest_ts",),
        batch_id=batch_id,
        stage="bronze_postgres",
        merge_schema=True,
    )
    events = merge_upsert(
        event_df,
        event_table,
        keys=("chave_acesso", "tipo_evento", "sequencia_evento"),
        order_by=("dh_evento",),
        batch_id=batch_id,
        stage="bronze_postgres",
        merge_schema=True,
    )
    emitentes = merge_upsert(
        emitente_df,
        emitente_table,
        keys=("cnpj_emitente",),
        order_by=("op_ts",),
        batch_id=batch_id,
        stage="bronze_postgres",
        merge_schema=True,
    )
    for table, result in (
        (header_table, headers),
        (item_table, items),
        (event_table, events),
        (emitente_table, emitentes),
    ):
        register_batch(
            spark,
            batch_id=batch_id,
            stage="bronze_postgres",
            target_table=table,
            rows_written=result.rows_inserted + result.rows_updated,
            profile=settings.profile.value,
        )
    return BronzeBatchResult(headers, items, events, emitentes)
