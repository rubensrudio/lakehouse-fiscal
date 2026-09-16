from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from lakehouse_fiscal.config.naming import resolve
from lakehouse_fiscal.config.profiles import Settings
from lakehouse_fiscal.io.delta_writer import MergeResult, merge_upsert


def build_nfe_evento(raw: DataFrame, settings: Settings, batch_id: str) -> MergeResult:
    valid = raw.filter((F.length("chave_acesso") == 44) & (F.length("justificativa") >= 15))
    return merge_upsert(
        valid,
        resolve("silver", "nfe_evento", settings),
        keys=("chave_acesso", "tipo_evento", "sequencia_evento"),
        order_by=("dh_evento",),
        batch_id=batch_id,
        stage="silver_evento",
        merge_schema=True,
    )
