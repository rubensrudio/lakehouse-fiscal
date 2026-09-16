from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from lakehouse_fiscal.config.naming import resolve
from lakehouse_fiscal.config.profiles import Settings
from lakehouse_fiscal.io.delta_writer import MergeResult, merge_upsert
from lakehouse_fiscal.quality.expectations import apply_expectations
from lakehouse_fiscal.quality.quarantine import quarantine_rows
from lakehouse_fiscal.quality.rules_nfe import header_rules
from lakehouse_fiscal.silver.transforms import deduplicate, digits_only


def build_nfe_header(
    raw: DataFrame, events: DataFrame, settings: Settings, batch_id: str
) -> tuple[MergeResult, int]:
    normalized = digits_only(raw, "chave_acesso", "cnpj_emitente", "cnpj_destinatario")
    normalized = deduplicate(normalized, ("chave_acesso",), ("updated_at_source", "_ingest_ts"))
    passing, rejected, _ = apply_expectations(normalized, header_rules())
    quarantined = quarantine_rows(
        rejected,
        source_table="bronze.nfe_cab_raw",
        natural_key_expr=F.col("chave_acesso"),
        failed_expectations=F.col("_failed_expectations"),
        batch_id=batch_id,
    )
    cancellations = (
        events.filter((F.col("tipo_evento") == "CANCELAMENTO") & (F.length("justificativa") >= 15))
        .groupBy("chave_acesso")
        .agg(F.max("dh_evento").alias("dh_cancelamento"))
    )
    final_df = (
        passing.join(cancellations, "chave_acesso", "left")
        .withColumn("is_cancelada", F.col("dh_cancelamento").isNotNull())
        .withColumn(
            "status_nfe", F.when(F.col("is_cancelada"), "CANCELADA").otherwise(F.col("status_nfe"))
        )
        .drop("_dq_status")
    )
    result = merge_upsert(
        final_df,
        resolve("silver", "nfe_header", settings),
        keys=("chave_acesso",),
        order_by=("updated_at_source",),
        batch_id=batch_id,
        stage="silver_header",
        merge_schema=True,
    )
    return result, quarantined
