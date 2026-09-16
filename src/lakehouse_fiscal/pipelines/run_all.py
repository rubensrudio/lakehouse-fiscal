from __future__ import annotations

import argparse
from pathlib import Path

from pyspark.sql import SparkSession

from lakehouse_fiscal.config.naming import resolve
from lakehouse_fiscal.config.profiles import Settings, load_settings
from lakehouse_fiscal.pipelines import run_bronze, run_gold, run_silver
from lakehouse_fiscal.session.spark_session import get_spark
from lakehouse_fiscal.utils.logging import get_logger
from lakehouse_fiscal.utils.ulid import new_ulid

LOGGER = get_logger(__name__)


def run(spark: SparkSession, settings: Settings, batch_id: str) -> None:
    run_bronze.run(spark, settings, batch_id)
    run_silver.run(spark, settings, batch_id)
    run_gold.run(spark, settings, batch_id)


def verify(spark: SparkSession, settings: Settings) -> None:
    checks = [
        (
            "bronze headers exist",
            spark.table(resolve("bronze", "nfe_cab_raw", settings)).count() > 0,
        ),
        (
            "silver headers exist",
            spark.table(resolve("silver", "nfe_header", settings)).count() > 0,
        ),
        (
            "silver items reconcile",
            spark.table(resolve("bronze", "nfe_item_raw", settings)).count()
            == spark.table(resolve("silver", "nfe_item", settings)).count(),
        ),
        ("gold fact exists", spark.table(resolve("gold", "fct_nfe_item", settings)).count() > 0),
        (
            "one current issuer version",
            spark.table(resolve("gold", "dim_emitente", settings))
            .filter("is_current")
            .groupBy("cnpj_emitente")
            .count()
            .filter("count != 1")
            .count()
            == 0,
        ),
    ]
    for name, passed in checks:
        LOGGER.info("[%s] %s", "PASS" if passed else "FAIL", name)
    if not all(passed for _, passed in checks):
        raise RuntimeError("Lakehouse verification failed.")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--verify-only", action="store_true")
    parser.add_argument("--sql", type=Path)
    args = parser.parse_args()
    settings = load_settings()
    spark = get_spark(settings)
    if args.sql:
        spark.sql(args.sql.read_text(encoding="utf-8")).show(truncate=False)
    elif args.verify_only:
        verify(spark, settings)
    else:
        run(spark, settings, new_ulid())


if __name__ == "__main__":
    main()
