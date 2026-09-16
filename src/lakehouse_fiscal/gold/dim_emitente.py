from __future__ import annotations

from pyspark.sql import DataFrame

from lakehouse_fiscal.config.naming import resolve
from lakehouse_fiscal.config.profiles import Settings
from lakehouse_fiscal.gold.scd2 import Scd2Config, Scd2Result, scd2_merge


def config(settings: Settings) -> Scd2Config:
    return Scd2Config(
        target_table=resolve("gold", "dim_emitente", settings),
        natural_keys=("cnpj_emitente",),
        tracked_columns=(
            "razao_social",
            "nome_fantasia",
            "regime_tributario",
            "uf",
            "municipio",
            "cnae_principal",
            "situacao_cadastral",
        ),
        surrogate_key="sk_emitente",
    )


def build_dim_emitente(source: DataFrame, settings: Settings, batch_id: str) -> Scd2Result:
    return scd2_merge(
        source, config(settings), change_ts_col="op_ts", tie_break_col="op_seq", batch_id=batch_id
    )
