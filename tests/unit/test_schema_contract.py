from __future__ import annotations

from pyspark.sql.types import IntegerType, LongType, StringType, StructField, StructType

from lakehouse_fiscal.quality.schema_contract import DriftKind, classify_drift


def test_schema_drift_classification() -> None:
    expected = StructType([StructField("id", IntegerType(), False)])
    assert classify_drift(expected, expected, ("id",)).kind is DriftKind.NONE
    additive = StructType([*expected.fields, StructField("note", StringType(), True)])
    assert classify_drift(additive, expected, ("id",)).kind is DriftKind.ADDITIVE
    widened = StructType([StructField("id", LongType(), False)])
    assert classify_drift(widened, expected, ("id",)).kind is DriftKind.NONE
    dropped = StructType([StructField("note", StringType(), True)])
    assert classify_drift(dropped, expected, ("id",)).kind is DriftKind.BREAKING
