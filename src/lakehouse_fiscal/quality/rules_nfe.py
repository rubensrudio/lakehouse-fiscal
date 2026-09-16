from __future__ import annotations

from pyspark.sql import functions as F
from pyspark.sql.types import BooleanType

from lakehouse_fiscal.generator.chave_acesso import is_valid_chave_acesso
from lakehouse_fiscal.generator.cnpj import is_valid_cnpj
from lakehouse_fiscal.quality.expectations import Expectation

_valid_cnpj = F.udf(is_valid_cnpj, BooleanType())
_valid_key = F.udf(is_valid_chave_acesso, BooleanType())


def header_rules() -> tuple[Expectation, ...]:
    return (
        Expectation("chave_acesso_valida", _valid_key(F.col("chave_acesso")), "drop"),
        Expectation("cnpj_emitente_valido", _valid_cnpj(F.col("cnpj_emitente")), "drop"),
        Expectation("valor_total_nao_negativo", F.col("valor_total") >= 0, "fail"),
        Expectation("uf_emitente_valida", F.col("uf_emitente").rlike("^[A-Z]{2}$"), "warn"),
    )


def item_rules() -> tuple[Expectation, ...]:
    return (
        Expectation("item_chave_acesso_valida", _valid_key(F.col("chave_acesso")), "drop"),
        Expectation("ncm_valido", F.col("ncm").rlike("^[0-9]{8}$"), "drop"),
        Expectation("cfop_valido", F.col("cfop").rlike("^[1-7][0-9]{3}$"), "drop"),
        Expectation("quantidade_positiva", F.col("quantidade") > 0, "fail"),
        Expectation("valor_produto_nao_negativo", F.col("valor_produto") >= 0, "fail"),
    )
