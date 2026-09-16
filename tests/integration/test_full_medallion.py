from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime, timedelta

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from lakehouse_fiscal.config.naming import resolve
from lakehouse_fiscal.config.profiles import load_settings
from lakehouse_fiscal.gold.scd2 import Scd2Config, resolve_sk_as_of, scd2_merge
from lakehouse_fiscal.ops.table_properties import apply_table_properties
from lakehouse_fiscal.ops.time_travel import (
    VersionOutOfWindowError,
    diff,
    history,
    read_timestamp,
    read_version,
)
from lakehouse_fiscal.pipelines import run_bronze, run_gold, run_silver
from lakehouse_fiscal.pipelines.run_all import verify
from lakehouse_fiscal.quality.expectations import (
    DataQualityGateError,
    DuplicateExpectationError,
    Expectation,
    apply_expectations,
    register,
)
from lakehouse_fiscal.quality.metrics import summarize, write_metrics
from lakehouse_fiscal.quality.quarantine import mark_reprocessed, quarantine_rows
from lakehouse_fiscal.utils.ulid import new_ulid


@pytest.mark.integration
def test_full_medallion_is_valid_and_idempotent(spark: SparkSession) -> None:
    settings = load_settings()
    verify(spark, settings)

    batch_id = new_ulid()
    bronze = run_bronze.run(spark, settings, batch_id)
    silver = run_silver.run(spark, settings, batch_id)
    gold_rows_written = run_gold.run(spark, settings, batch_id)

    assert bronze.rows_written == 0
    assert silver.rows_written == 0
    assert silver.rows_quarantined == 0
    assert gold_rows_written == 0
    verify(spark, settings)


@pytest.mark.integration
def test_data_quality_audit_and_time_travel(spark: SparkSession) -> None:
    settings = load_settings()
    batch_id = new_ulid()
    source = spark.createDataFrame([(1, 10), (2, -1), (3, 0)], ["id", "amount"])
    passing, rejected, results = apply_expectations(
        source,
        (
            Expectation("positive_amount", F.col("amount") > 0, "drop"),
            Expectation("large_amount", F.col("amount") >= 10, "warn"),
        ),
    )

    assert passing.count() == 1
    assert rejected.count() == 2
    assert summarize(results).pass_rate == pytest.approx(1 / 3)
    write_metrics(results, run_id=batch_id, table_name="integration_source", profile="local")
    assert (
        quarantine_rows(
            rejected,
            source_table="integration_source",
            natural_key_expr=F.col("id"),
            failed_expectations=F.col("_failed_expectations"),
            batch_id=batch_id,
        )
        == 2
    )
    assert mark_reprocessed(["2", "3"], batch_id) == 2

    fact = resolve("gold", "fct_nfe_item", settings)
    latest = history(spark, fact, 1).first()
    version = int(latest.version)
    assert read_version(spark, fact, version).count() > 0
    assert read_timestamp(spark, fact, latest.timestamp).count() > 0
    assert diff(spark, fact, version, version).rows_added == 0
    with pytest.raises(VersionOutOfWindowError):
        read_version(spark, fact, version + 1_000_000)
    assert apply_table_properties(spark, settings) >= 1


@pytest.mark.integration
def test_quality_edge_cases_and_scd2_lifecycle(
    spark: SparkSession, monkeypatch: pytest.MonkeyPatch
) -> None:
    unique = new_ulid().lower()
    rule = Expectation(f"unique_{unique}", F.lit(True), "warn")
    register(rule)
    with pytest.raises(DuplicateExpectationError):
        register(rule)

    empty = spark.createDataFrame([], "id int")
    passing, rejected, results = apply_expectations(empty, (rule,))
    assert passing.count() == rejected.count() == 0
    assert results[0].pass_rate == 1.0
    with pytest.raises(DataQualityGateError):
        apply_expectations(
            spark.createDataFrame([(1,)], ["id"]),
            (Expectation(f"gate_{unique}", F.lit(False), "fail"),),
        )

    settings = load_settings()
    isolated = replace(
        settings,
        catalog=f"test_{unique}",
        storage_root=f"{settings.storage_root}/_tests/{unique}",
    )
    monkeypatch.setattr("lakehouse_fiscal.io.delta_writer.load_settings", lambda: isolated)
    target = f"{isolated.catalog}_gold.dim_emitente"
    cfg = Scd2Config(
        target_table=target,
        natural_keys=("cnpj_emitente",),
        tracked_columns=("razao_social", "situacao_cadastral"),
        surrogate_key="sk_emitente",
    )
    start = datetime(2025, 1, 1, tzinfo=UTC)
    initial = spark.createDataFrame(
        [("11222333000181", "Empresa A", "ATIVA", "I", 1, start)],
        "cnpj_emitente string, razao_social string, situacao_cadastral string, "
        "op string, op_seq long, op_ts timestamp",
    )
    first = scd2_merge(initial, cfg, change_ts_col="op_ts", tie_break_col="op_seq", batch_id=unique)
    assert first.rows_inserted == 1

    deleted = spark.createDataFrame(
        [("11222333000181", "Empresa A", "ATIVA", "D", 2, start + timedelta(days=1))],
        initial.schema,
    )
    second = scd2_merge(
        deleted, cfg, change_ts_col="op_ts", tie_break_col="op_seq", batch_id=unique
    )
    assert second.rows_inserted == second.rows_closed == 1
    versions = spark.table(target).orderBy("valid_from")
    assert versions.count() == 2
    assert versions.filter("is_current AND situacao_cadastral = 'BAIXADA'").count() == 1

    facts = spark.createDataFrame(
        [
            ("11222333000181", start + timedelta(hours=1)),
            ("00000000000000", start + timedelta(hours=1)),
        ],
        "cnpj_emitente string, event_ts timestamp",
    )
    resolved = resolve_sk_as_of(facts, versions, cfg, event_ts_col="event_ts")
    assert resolved.filter("sk_emitente = -1").count() == 1
