# Databricks notebook source
from lakehouse_fiscal.config.naming import resolve
from lakehouse_fiscal.config.profiles import load_settings
from lakehouse_fiscal.ops.time_travel import diff, history, read_version
from lakehouse_fiscal.session.spark_session import get_spark

settings = load_settings()
spark = get_spark(settings)
table = resolve("gold", "agg_faturamento_mensal_uf", settings)
versions = [row.version for row in history(spark, table).select("version").collect()]
if len(versions) >= 2:
    previous, current = min(versions), max(versions)
    prior_revenue = read_version(spark, table, previous).groupBy().sum("valor_liquido").first()[0]
    current_revenue = read_version(spark, table, current).groupBy().sum("valor_liquido").first()[0]
    change = diff(spark, table, previous, current)
    print({"previous": prior_revenue, "current": current_revenue, "diff": change})
