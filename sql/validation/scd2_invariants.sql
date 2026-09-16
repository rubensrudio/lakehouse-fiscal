WITH violations AS (
  SELECT 'dim_emitente' AS dimension, cnpj_emitente AS natural_key
  FROM lakehouse_fiscal_gold.dim_emitente
  GROUP BY cnpj_emitente
  HAVING SUM(CASE WHEN is_current THEN 1 ELSE 0 END) <> 1
  UNION ALL
  SELECT 'dim_produto', CONCAT(cnpj_emitente, ':', codigo_produto)
  FROM lakehouse_fiscal_gold.dim_produto
  GROUP BY cnpj_emitente, codigo_produto
  HAVING SUM(CASE WHEN is_current THEN 1 ELSE 0 END) <> 1
)
SELECT * FROM violations;

