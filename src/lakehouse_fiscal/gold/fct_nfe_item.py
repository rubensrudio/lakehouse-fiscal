from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from lakehouse_fiscal.config.naming import resolve
from lakehouse_fiscal.config.profiles import Settings
from lakehouse_fiscal.gold.dim_emitente import config as emitente_config
from lakehouse_fiscal.gold.dim_produto import config as produto_config
from lakehouse_fiscal.gold.scd2 import resolve_sk_as_of
from lakehouse_fiscal.io.delta_writer import MergeResult, merge_upsert


def build_fct_nfe_item(
    items: DataFrame,
    headers: DataFrame,
    dim_emitente: DataFrame,
    dim_produto: DataFrame,
    settings: Settings,
    batch_id: str,
) -> MergeResult:
    if items.groupBy("chave_acesso", "num_item").count().filter("count > 1").limit(1).count():
        raise ValueError("Duplicate fact grain (chave_acesso, num_item).")
    header_fields = headers.select(
        "chave_acesso", "uf_emitente", "uf_destinatario", "is_cancelada", "status_nfe"
    )
    facts = items.join(header_fields, "chave_acesso", "inner")
    facts = resolve_sk_as_of(
        facts, dim_emitente, emitente_config(settings), event_ts_col="dh_emissao"
    )
    facts = resolve_sk_as_of(
        facts, dim_produto, produto_config(settings), event_ts_col="dh_emissao"
    )
    facts = facts.withColumn(
        "sk_data", F.date_format("dh_emissao", "yyyyMMdd").cast("int")
    ).withColumn("ano_mes_emissao", F.date_format("dh_emissao", "yyyyMM"))
    return merge_upsert(
        facts,
        resolve("gold", "fct_nfe_item", settings),
        keys=("chave_acesso", "num_item"),
        order_by=("dh_emissao",),
        batch_id=batch_id,
        stage="gold_fact",
        merge_schema=True,
    )
