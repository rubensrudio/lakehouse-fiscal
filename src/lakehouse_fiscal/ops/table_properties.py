from __future__ import annotations

from pyspark.sql import SparkSession

from lakehouse_fiscal.config.naming import LOGICAL_TABLES, resolve
from lakehouse_fiscal.config.profiles import Settings


def apply_table_properties(spark: SparkSession, settings: Settings) -> int:
    changed = 0
    for layer, tables in LOGICAL_TABLES.items():
        for table in tables:
            identifier = resolve(layer, table, settings)
            if not spark.catalog.tableExists(identifier):
                continue
            properties = {
                "delta.logRetentionDuration": "interval 7 days",
                "delta.deletedFileRetentionDuration": "interval 7 days",
                "delta.autoOptimize.optimizeWrite": "true",
            }
            if layer == "silver" and table == "nfe_header":
                properties["delta.enableChangeDataFeed"] = "true"
            assignments = ", ".join(f"'{key}' = '{value}'" for key, value in properties.items())
            spark.sql(f"ALTER TABLE {identifier} SET TBLPROPERTIES ({assignments})")
            changed += 1
    return changed
