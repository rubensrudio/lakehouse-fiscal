from __future__ import annotations

import time

from pyspark.sql import SparkSession

from lakehouse_fiscal.config.naming import resolve
from lakehouse_fiscal.config.profiles import Settings, load_settings
from lakehouse_fiscal.gold.aggregates import build_aggregates
from lakehouse_fiscal.gold.dim_cfop import build_dim_cfop
from lakehouse_fiscal.gold.dim_data import build_dim_data
from lakehouse_fiscal.gold.dim_emitente import build_dim_emitente
from lakehouse_fiscal.gold.dim_produto import build_dim_produto
from lakehouse_fiscal.gold.fct_nfe_item import build_fct_nfe_item
from lakehouse_fiscal.io.delta_writer import overwrite
from lakehouse_fiscal.session.spark_session import get_spark
from lakehouse_fiscal.utils.logging import log_stage_summary
from lakehouse_fiscal.utils.ulid import new_ulid


def run(spark: SparkSession, settings: Settings, batch_id: str) -> int:
    started = time.monotonic()
    overwrite(
        build_dim_data(spark),
        resolve("gold", "dim_data", settings),
        batch_id=batch_id,
        stage="gold_dimension",
    )
    overwrite(
        build_dim_cfop(spark),
        resolve("gold", "dim_cfop", settings),
        batch_id=batch_id,
        stage="gold_dimension",
    )
    build_dim_emitente(
        spark.table(resolve("bronze", "emitente_cdc_raw", settings)), settings, batch_id
    )
    silver_items = spark.table(resolve("silver", "nfe_item", settings))
    build_dim_produto(silver_items, settings, batch_id)
    fact_result = build_fct_nfe_item(
        silver_items,
        spark.table(resolve("silver", "nfe_header", settings)),
        spark.table(resolve("gold", "dim_emitente", settings)),
        spark.table(resolve("gold", "dim_produto", settings)),
        settings,
        batch_id,
    )
    facts = spark.table(resolve("gold", "fct_nfe_item", settings))
    build_aggregates(facts, settings, batch_id)
    written = fact_result.rows_inserted + fact_result.rows_updated
    log_stage_summary(
        stage="gold",
        batch_id=batch_id,
        profile=settings.profile.value,
        table="gold",
        rows_in=silver_items.count(),
        rows_written=written,
        rows_quarantined=0,
        duration_ms=int((time.monotonic() - started) * 1000),
        status="SUCCESS",
    )
    return written


def main() -> None:
    settings = load_settings()
    run(get_spark(settings), settings, new_ulid())


if __name__ == "__main__":
    main()
