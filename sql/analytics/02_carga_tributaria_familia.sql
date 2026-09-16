SELECT ano_mes_emissao, familia_produto,
       SUM(valor_icms + valor_ipi + valor_pis + valor_cofins) / NULLIF(SUM(valor_produto), 0)
         AS carga_tributaria_efetiva
FROM lakehouse_fiscal_gold.fct_nfe_item
WHERE is_cancelada = false
GROUP BY ano_mes_emissao, familia_produto
ORDER BY ano_mes_emissao, familia_produto;

