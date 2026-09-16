SELECT ano_mes_emissao, uf_emitente, uf_destinatario, cfop,
       SUM(valor_produto) AS faturamento_liquido
FROM lakehouse_fiscal_gold.fct_nfe_item
WHERE is_cancelada = false
GROUP BY ano_mes_emissao, uf_emitente, uf_destinatario, cfop
ORDER BY ano_mes_emissao, uf_emitente, uf_destinatario, cfop;

