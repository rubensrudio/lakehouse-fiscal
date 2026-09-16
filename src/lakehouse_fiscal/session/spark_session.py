from __future__ import annotations

import os
from pathlib import Path
from typing import Final, cast

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

from lakehouse_fiscal.config.profiles import Profile, Settings


class ObjectStorageUnreachableError(ConnectionError):
    """Raised when the local S3-compatible endpoint cannot be reached."""


LOCAL_SPARK_OPTIONS: Final[dict[str, str]] = {
    "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
    "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
    "spark.hadoop.fs.s3a.path.style.access": "true",
    "spark.hadoop.fs.s3a.connection.ssl.enabled": "false",
    "spark.sql.session.timeZone": "UTC",
    "spark.sql.shuffle.partitions": "8",
}


def get_spark(settings: Settings) -> SparkSession:
    if settings.profile is Profile.DATABRICKS:
        active = SparkSession.getActiveSession()
        if active is not None:
            return active
        from databricks.connect import DatabricksSession

        return cast(SparkSession, DatabricksSession.builder.getOrCreate())

    endpoint = os.environ.get("MINIO_ENDPOINT", "http://127.0.0.1:9000")
    access_key = os.environ.get("MINIO_ACCESS_KEY", "minioadmin")
    secret_key = os.environ.get("MINIO_SECRET_KEY", "minioadmin")
    builder = SparkSession.builder.appName("lakehouse-fiscal").master(
        os.environ.get("SPARK_MASTER", "local[4]")
    )
    for key, value in LOCAL_SPARK_OPTIONS.items():
        builder = builder.config(key, value)
    builder = builder.enableHiveSupport()
    builder = (
        builder.config("spark.hadoop.fs.s3a.endpoint", endpoint)
        .config("spark.hadoop.fs.s3a.access.key", access_key)
        .config("spark.hadoop.fs.s3a.secret.key", secret_key)
    )
    if any(Path("/opt/spark/jars").glob("delta-spark_*.jar")):
        spark = builder.getOrCreate()
    else:
        spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark
