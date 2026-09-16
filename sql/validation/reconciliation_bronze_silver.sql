SELECT 'nfe_header' AS entity,
       (SELECT COUNT(*) FROM lakehouse_fiscal_bronze.nfe_cab_raw) AS bronze_count,
       (SELECT COUNT(*) FROM lakehouse_fiscal_silver.nfe_header) AS silver_count
WHERE (SELECT COUNT(*) FROM lakehouse_fiscal_bronze.nfe_cab_raw) <>
      (SELECT COUNT(*) FROM lakehouse_fiscal_silver.nfe_header)
UNION ALL
SELECT 'nfe_item',
       (SELECT COUNT(*) FROM lakehouse_fiscal_bronze.nfe_item_raw),
       (SELECT COUNT(*) FROM lakehouse_fiscal_silver.nfe_item)
WHERE (SELECT COUNT(*) FROM lakehouse_fiscal_bronze.nfe_item_raw) <>
      (SELECT COUNT(*) FROM lakehouse_fiscal_silver.nfe_item);

