from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from lakehouse_fiscal.config.naming import resolve
from lakehouse_fiscal.config.profiles import Settings
from lakehouse_fiscal.io.delta_writer import WriteResult, overwrite


def build_aggregates(
    facts: DataFrame, settings: Settings, batch_id: str
) -> tuple[WriteResult, WriteResult]:
    valid_value = F.when(~F.col("is_cancelada"), F.col("valor_produto")).otherwise(F.lit(0))
    revenue = facts.groupBy("ano_mes_emissao", "uf_emitente", "uf_destinatario", "cfop").agg(
        F.sum(valid_value).alias("valor_liquido"),
        F.countDistinct(F.when(~F.col("is_cancelada"), F.col("chave_acesso"))).alias("qtd_notas"),
        F.countDistinct(F.when(F.col("is_cancelada"), F.col("chave_acesso"))).alias(
            "qtd_notas_canceladas_excluidas"
        ),
    )
    tax_value = (
        F.col("valor_icms") + F.col("valor_ipi") + F.col("valor_pis") + F.col("valor_cofins")
    )
    burden = (
        facts.filter(~F.col("is_cancelada"))
        .groupBy("ano_mes_emissao", "familia_produto")
        .agg(
            F.sum("valor_produto").alias("valor_produtos"),
            F.sum(tax_value).alias("valor_tributos"),
        )
        .withColumn(
            "carga_tributaria_efetiva",
            F.when(F.col("valor_produtos") != 0, F.col("valor_tributos") / F.col("valor_produtos")),
        )
    )
    return (
        overwrite(
            revenue,
            resolve("gold", "agg_faturamento_mensal_uf", settings),
            batch_id=batch_id,
            stage="gold_aggregate",
            partition_by=("ano_mes_emissao",),
        ),
        overwrite(
            burden,
            resolve("gold", "agg_carga_tributaria_familia", settings),
            batch_id=batch_id,
            stage="gold_aggregate",
            partition_by=("ano_mes_emissao",),
        ),
    )
