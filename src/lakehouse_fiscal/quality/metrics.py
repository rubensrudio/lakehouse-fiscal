from __future__ import annotations

from collections.abc import Sequence
from dataclasses import asdict, dataclass

from pyspark.sql import SparkSession

from lakehouse_fiscal.config.naming import resolve
from lakehouse_fiscal.config.profiles import load_settings
from lakehouse_fiscal.io.delta_writer import append
from lakehouse_fiscal.quality.expectations import ExpectationResult


@dataclass(frozen=True)
class RunQualitySummary:
    rows_evaluated: int
    rows_failed: int
    pass_rate: float


def summarize(results: Sequence[ExpectationResult]) -> RunQualitySummary:
    evaluated = max((result.rows_evaluated for result in results), default=0)
    failed = sum(result.rows_failed for result in results if result.severity == "drop")
    return RunQualitySummary(
        evaluated, failed, 1.0 if evaluated == 0 else max(0.0, 1 - failed / evaluated)
    )


def write_metrics(
    results: Sequence[ExpectationResult], *, run_id: str, table_name: str, profile: str
) -> None:
    if not results:
        return
    spark = SparkSession.getActiveSession()
    if spark is None:
        raise RuntimeError("An active SparkSession is required to write DQ metrics.")
    rows = [
        {**asdict(result), "run_id": run_id, "table_name": table_name, "profile": profile}
        for result in results
    ]
    table = resolve("silver", "dq_metrics", load_settings())
    append(
        spark.createDataFrame(rows), table, batch_id=run_id, stage="dq_metrics", merge_schema=True
    )
