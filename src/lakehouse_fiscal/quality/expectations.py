from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from typing import Literal

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

Severity = Literal["warn", "drop", "fail"]


class DuplicateExpectationError(ValueError):
    pass


class DataQualityGateError(RuntimeError):
    pass


@dataclass(frozen=True)
class Expectation:
    name: str
    expr: Column
    severity: Severity


@dataclass(frozen=True)
class ExpectationResult:
    name: str
    severity: Severity
    rows_evaluated: int
    rows_passed: int
    rows_failed: int
    pass_rate: float


_REGISTRY: dict[str, Severity] = {}


def register(exp: Expectation) -> None:
    if exp.name in _REGISTRY:
        raise DuplicateExpectationError(f"Duplicate expectation '{exp.name}'.")
    _REGISTRY[exp.name] = exp.severity


def drop_rule_names() -> frozenset[str]:
    return frozenset(name for name, severity in _REGISTRY.items() if severity == "drop")


def apply_expectations(
    df: DataFrame, rules: Sequence[Expectation]
) -> tuple[DataFrame, DataFrame, list[ExpectationResult]]:
    total = df.count()
    results: list[ExpectationResult] = []
    drop_condition: Column = F.lit(False)
    warn_condition: Column = F.lit(False)
    failed_names = F.array().cast("array<string>")
    for rule in rules:
        failed = ~F.coalesce(rule.expr.cast("boolean"), F.lit(False))
        failed_count = df.filter(failed).count()
        result = ExpectationResult(
            rule.name,
            rule.severity,
            total,
            total - failed_count,
            failed_count,
            1.0 if total == 0 else (total - failed_count) / total,
        )
        results.append(result)
        if rule.severity == "fail" and failed_count:
            raise DataQualityGateError(
                f"Data quality gate FAILED: expectation '{rule.name}' violated by "
                f"{failed_count} row(s) on '{getattr(df, '_source_table', 'unknown')}'. "
                "Batch aborted, no data written."
            )
        if rule.severity == "drop":
            drop_condition = drop_condition | failed
            failed_names = F.when(
                failed, F.array_union(failed_names, F.array(F.lit(rule.name)))
            ).otherwise(failed_names)
        elif rule.severity == "warn":
            warn_condition = warn_condition | failed
    passing = df.filter(~drop_condition).withColumn(
        "_dq_status", F.when(warn_condition, F.lit("WARN")).otherwise(F.lit("PASS"))
    )
    quarantined = df.filter(drop_condition).withColumn("_failed_expectations", failed_names)
    return passing, quarantined, results
