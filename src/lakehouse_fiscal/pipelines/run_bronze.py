from __future__ import annotations

import time

from pyspark.sql import SparkSession

from lakehouse_fiscal.bronze.postgres_batch import BronzeBatchResult, ingest_postgres_batch
from lakehouse_fiscal.config.profiles import Settings, load_settings
from lakehouse_fiscal.session.spark_session import get_spark
from lakehouse_fiscal.utils.logging import log_stage_summary
from lakehouse_fiscal.utils.ulid import new_ulid


def run(spark: SparkSession, settings: Settings, batch_id: str) -> BronzeBatchResult:
    started = time.monotonic()
    result = ingest_postgres_batch(spark, settings, batch_id)
    log_stage_summary(
        stage="bronze",
        batch_id=batch_id,
        profile=settings.profile.value,
        table="postgres_batch",
        rows_in=result.rows_written,
        rows_written=result.rows_written,
        rows_quarantined=0,
        duration_ms=int((time.monotonic() - started) * 1000),
        status="SUCCESS",
    )
    return result


def main() -> None:
    settings = load_settings()
    run(get_spark(settings), settings, new_ulid())


if __name__ == "__main__":
    main()
