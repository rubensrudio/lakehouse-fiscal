from __future__ import annotations

from typing import Final

from pyspark.sql.types import LongType, StringType, StructField, StructType, TimestampType

from lakehouse_fiscal.schemas.nfe import (
    EMITENTE_CDC_SCHEMA,
    NFE_CAB_SCHEMA,
    NFE_EVENTO_SCHEMA,
    NFE_ITEM_SCHEMA,
    NFE_XML_SCHEMA,
    QUARANTINE_SCHEMA,
)

_INGEST = StructType(
    [
        StructField("batch_id", StringType(), False),
        StructField("stage", StringType(), False),
        StructField("target_table", StringType(), False),
        StructField("applied_ts", TimestampType(), False),
        StructField("rows_written", LongType(), False),
        StructField("profile", StringType(), False),
    ]
)
_WATERMARK = StructType(
    [
        StructField("source", StringType(), False),
        StructField("profile", StringType(), False),
        StructField("last_op_seq", LongType(), False),
        StructField("updated_ts", TimestampType(), False),
    ]
)
_EMPTY = StructType([StructField("_placeholder", StringType(), True)])

BRONZE_CONTRACTS: Final[dict[str, StructType]] = {
    "nfe_cab_raw": NFE_CAB_SCHEMA,
    "nfe_item_raw": NFE_ITEM_SCHEMA,
    "nfe_xml_raw": NFE_XML_SCHEMA,
    "nfe_evento_raw": NFE_EVENTO_SCHEMA,
    "emitente_cdc_raw": EMITENTE_CDC_SCHEMA,
    "_ingest_batches": _INGEST,
    "_cdc_watermark": _WATERMARK,
}
SILVER_CONTRACTS: Final[dict[str, StructType]] = {
    "nfe_header": NFE_CAB_SCHEMA,
    "nfe_item": NFE_ITEM_SCHEMA,
    "nfe_evento": NFE_EVENTO_SCHEMA,
    "emitente_current": _EMPTY,
    "quarantine_nfe": QUARANTINE_SCHEMA,
    "dq_metrics": _EMPTY,
}
GOLD_CONTRACTS: Final[dict[str, StructType]] = {
    "dim_emitente": _EMPTY,
    "dim_produto": _EMPTY,
    "dim_data": _EMPTY,
    "dim_cfop": _EMPTY,
    "fct_nfe_item": _EMPTY,
    "agg_faturamento_mensal_uf": _EMPTY,
    "agg_carga_tributaria_familia": _EMPTY,
}
