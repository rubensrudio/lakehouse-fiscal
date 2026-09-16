from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from enum import StrEnum

from pyspark.sql.types import (
    DataType,
    DecimalType,
    DoubleType,
    FloatType,
    IntegerType,
    LongType,
    ShortType,
    StructType,
)


class DriftKind(StrEnum):
    NONE = "none"
    ADDITIVE = "additive"
    BREAKING = "breaking"


@dataclass(frozen=True)
class DriftReport:
    kind: DriftKind
    added: tuple[str, ...]
    changed: tuple[tuple[str, str, str], ...]
    dropped_key_columns: tuple[str, ...]


def _compatible(observed: DataType, expected: DataType) -> bool:
    if observed == expected:
        return True
    numeric_order = (ShortType, IntegerType, LongType, FloatType, DoubleType)
    observed_rank = next(
        (i for i, kind in enumerate(numeric_order) if isinstance(observed, kind)), -1
    )
    expected_rank = next(
        (i for i, kind in enumerate(numeric_order) if isinstance(expected, kind)), -1
    )
    if observed_rank >= expected_rank >= 0:
        return True
    return (
        isinstance(observed, DecimalType)
        and isinstance(expected, DecimalType)
        and observed.scale == expected.scale
        and observed.precision >= expected.precision
    )


def classify_drift(
    observed: StructType, expected: StructType, key_columns: Sequence[str]
) -> DriftReport:
    observed_fields = {field.name: field.dataType for field in observed.fields}
    expected_fields = {field.name: field.dataType for field in expected.fields}
    added = tuple(sorted(observed_fields.keys() - expected_fields.keys()))
    dropped_keys = tuple(sorted(key for key in key_columns if key not in observed_fields))
    changed = tuple(
        sorted(
            (name, expected_fields[name].simpleString(), observed_fields[name].simpleString())
            for name in observed_fields.keys() & expected_fields.keys()
            if not _compatible(observed_fields[name], expected_fields[name])
        )
    )
    kind = (
        DriftKind.BREAKING
        if changed or dropped_keys
        else DriftKind.ADDITIVE
        if added
        else DriftKind.NONE
    )
    return DriftReport(kind, added, changed, dropped_keys)
