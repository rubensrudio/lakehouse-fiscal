from __future__ import annotations

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from lakehouse_fiscal.config.naming import resolve
from lakehouse_fiscal.config.profiles import Settings
from lakehouse_fiscal.gold.scd2 import Scd2Config, Scd2Result, scd2_merge


def config(settings: Settings) -> Scd2Config:
    return Scd2Config(
        target_table=resolve("gold", "dim_produto", settings),
        natural_keys=("cnpj_emitente", "codigo_produto"),
        tracked_columns=(
            "descricao_produto",
            "ncm",
            "cest",
            "familia_produto",
            "unidade_comercial",
        ),
        surrogate_key="sk_produto",
    )


def build_dim_produto(source: DataFrame, settings: Settings, batch_id: str) -> Scd2Result:
    snapshot = source.withColumn(
        "change_ts",
        F.min("dh_emissao").over(Window.partitionBy("cnpj_emitente", "codigo_produto")),
    ).withColumn("op_seq", F.lit(1))
    return scd2_merge(
        snapshot,
        config(settings),
        change_ts_col="change_ts",
        tie_break_col="op_seq",
        batch_id=batch_id,
    )
