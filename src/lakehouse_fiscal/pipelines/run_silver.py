from __future__ import annotations

import time
from dataclasses import dataclass

from pyspark.sql import SparkSession

from lakehouse_fiscal.config.naming import resolve
from lakehouse_fiscal.config.profiles import Settings, load_settings
from lakehouse_fiscal.session.spark_session import get_spark
from lakehouse_fiscal.silver.nfe_evento import build_nfe_evento
from lakehouse_fiscal.silver.nfe_header import build_nfe_header
from lakehouse_fiscal.silver.nfe_item import build_nfe_item
from lakehouse_fiscal.utils.logging import log_stage_summary
from lakehouse_fiscal.utils.ulid import new_ulid


@dataclass(frozen=True)
class SilverRunResult:
    rows_written: int
    rows_quarantined: int


def run(spark: SparkSession, settings: Settings, batch_id: str) -> SilverRunResult:
    started = time.monotonic()
    raw_events = spark.table(resolve("bronze", "nfe_evento_raw", settings))
    event_result = build_nfe_evento(raw_events, settings, batch_id)
    header_result, header_quarantine = build_nfe_header(
        spark.table(resolve("bronze", "nfe_cab_raw", settings)), raw_events, settings, batch_id
    )
    item_result, item_quarantine = build_nfe_item(
        spark.table(resolve("bronze", "nfe_item_raw", settings)), settings, batch_id
    )
    written = sum(
        (
            event_result.rows_inserted + event_result.rows_updated,
            header_result.rows_inserted + header_result.rows_updated,
            item_result.rows_inserted + item_result.rows_updated,
        )
    )
    quarantined = header_quarantine + item_quarantine
    log_stage_summary(
        stage="silver",
        batch_id=batch_id,
        profile=settings.profile.value,
        table="silver",
        rows_in=written + quarantined,
        rows_written=written,
        rows_quarantined=quarantined,
        duration_ms=int((time.monotonic() - started) * 1000),
        status="SUCCESS",
        dq_pass_rate=100.0
        if written + quarantined == 0
        else 100 * written / (written + quarantined),
    )
    return SilverRunResult(written, quarantined)


def main() -> None:
    settings = load_settings()
    run(get_spark(settings), settings, new_ulid())


if __name__ == "__main__":
    main()
