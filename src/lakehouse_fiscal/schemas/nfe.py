from __future__ import annotations

from typing import Final

from pyspark.sql.types import (
    BooleanType,
    DecimalType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

MONEY: Final = DecimalType(18, 2)

NFE_CAB_SCHEMA: Final = StructType(
    [
        StructField("chave_acesso", StringType(), False),
        StructField("numero_nfe", IntegerType(), False),
        StructField("serie", IntegerType(), False),
        StructField("dh_emissao", TimestampType(), False),
        StructField("cnpj_emitente", StringType(), False),
        StructField("cnpj_destinatario", StringType(), False),
        StructField("uf_emitente", StringType(), False),
        StructField("uf_destinatario", StringType(), False),
        StructField("valor_total", MONEY, False),
        StructField("status_nfe", StringType(), False),
        StructField("updated_at_source", TimestampType(), False),
    ]
)

NFE_ITEM_SCHEMA: Final = StructType(
    [
        StructField("chave_acesso", StringType(), False),
        StructField("num_item", IntegerType(), False),
        StructField("cnpj_emitente", StringType(), False),
        StructField("codigo_produto", StringType(), False),
        StructField("descricao_produto", StringType(), False),
        StructField("ncm", StringType(), False),
        StructField("cest", StringType(), True),
        StructField("familia_produto", StringType(), False),
        StructField("unidade_comercial", StringType(), False),
        StructField("cfop", StringType(), False),
        StructField("quantidade", DecimalType(18, 4), False),
        StructField("valor_unitario", DecimalType(18, 4), False),
        StructField("valor_produto", MONEY, False),
        StructField("cst_icms", StringType(), True),
        StructField("csosn", StringType(), True),
        StructField("valor_icms", MONEY, False),
        StructField("cst_ipi", StringType(), True),
        StructField("valor_ipi", MONEY, False),
        StructField("cst_pis", StringType(), True),
        StructField("valor_pis", MONEY, False),
        StructField("cst_cofins", StringType(), True),
        StructField("valor_cofins", MONEY, False),
        StructField("dh_emissao", TimestampType(), False),
    ]
)

NFE_EVENTO_SCHEMA: Final = StructType(
    [
        StructField("chave_acesso", StringType(), False),
        StructField("tipo_evento", StringType(), False),
        StructField("sequencia_evento", IntegerType(), False),
        StructField("dh_evento", TimestampType(), False),
        StructField("justificativa", StringType(), False),
    ]
)

EMITENTE_CDC_SCHEMA: Final = StructType(
    [
        StructField("cnpj_emitente", StringType(), False),
        StructField("op", StringType(), False),
        StructField("op_ts", TimestampType(), False),
    ]
)

NFE_XML_SCHEMA: Final = StructType([StructField("xml", StringType(), False)])

QUARANTINE_SCHEMA: Final = StructType(
    [
        StructField("quarantine_id", StringType(), False),
        StructField("source_table", StringType(), False),
        StructField("natural_key", StringType(), False),
        StructField("failed_expectations", StringType(), False),
        StructField("row_payload_json", StringType(), False),
        StructField("is_reprocessed", BooleanType(), False),
    ]
)
