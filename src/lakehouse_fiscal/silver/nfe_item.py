from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from lakehouse_fiscal.config.naming import resolve
from lakehouse_fiscal.config.profiles import Settings
from lakehouse_fiscal.io.delta_writer import MergeResult, merge_upsert
from lakehouse_fiscal.quality.expectations import apply_expectations
from lakehouse_fiscal.quality.quarantine import quarantine_rows
from lakehouse_fiscal.quality.rules_nfe import item_rules
from lakehouse_fiscal.silver.transforms import deduplicate, derive_product_family, digits_only


def build_nfe_item(raw: DataFrame, settings: Settings, batch_id: str) -> tuple[MergeResult, int]:
    normalized = digits_only(raw, "chave_acesso", "cnpj_emitente", "ncm", "cest")
    normalized = derive_product_family(
        deduplicate(normalized, ("chave_acesso", "num_item"), ("_ingest_ts",))
    )
    passing, rejected, _ = apply_expectations(normalized, item_rules())
    quarantined = quarantine_rows(
        rejected,
        source_table="bronze.nfe_item_raw",
        natural_key_expr=F.concat_ws(":", "chave_acesso", "num_item"),
        failed_expectations=F.col("_failed_expectations"),
        batch_id=batch_id,
    )
    result = merge_upsert(
        passing.drop("_dq_status"),
        resolve("silver", "nfe_item", settings),
        keys=("chave_acesso", "num_item"),
        order_by=("_ingest_ts",),
        batch_id=batch_id,
        stage="silver_item",
        merge_schema=True,
    )
    return result, quarantined
