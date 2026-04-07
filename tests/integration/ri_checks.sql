-- Referential integrity checks: each row reports unmatched fact rows for a dimension join.
WITH checks AS (
  SELECT
    'fct_sales_orders -> dim_customers_current (customer_id)' AS check_name,
    COUNT(*) AS fact_rows,
    SUM(CASE WHEN d.customer_id IS NULL THEN 1 ELSE 0 END) AS unmatched_rows
  FROM workspace.silver_dev.fct_sales_orders f
  LEFT JOIN workspace.silver_dev.dim_customers_current d
    ON f.customer_id = d.customer_id

  UNION ALL

  SELECT
    'fct_sales_orders -> dim_distribution_centers_current (dc_id)' AS check_name,
    COUNT(*) AS fact_rows,
    SUM(CASE WHEN d.dc_id IS NULL THEN 1 ELSE 0 END) AS unmatched_rows
  FROM workspace.silver_dev.fct_sales_orders f
  LEFT JOIN workspace.silver_dev.dim_distribution_centers_current d
    ON f.dc_id = d.dc_id

  UNION ALL

  SELECT
    'fct_sales_order_lines -> dim_products_current (product_id)' AS check_name,
    COUNT(*) AS fact_rows,
    SUM(CASE WHEN d.product_id IS NULL THEN 1 ELSE 0 END) AS unmatched_rows
  FROM workspace.silver_dev.fct_sales_order_lines f
  LEFT JOIN workspace.silver_dev.dim_products_current d
    ON f.product_id = d.product_id

  UNION ALL

  SELECT
    'fct_inventory_snapshots -> dim_distribution_centers_current (dc_id)' AS check_name,
    COUNT(*) AS fact_rows,
    SUM(CASE WHEN d.dc_id IS NULL THEN 1 ELSE 0 END) AS unmatched_rows
  FROM workspace.silver_dev.fct_inventory_snapshots f
  LEFT JOIN workspace.silver_dev.dim_distribution_centers_current d
    ON f.dc_id = d.dc_id

  UNION ALL

  SELECT
    'fct_inventory_snapshots -> dim_products_current (product_id)' AS check_name,
    COUNT(*) AS fact_rows,
    SUM(CASE WHEN d.product_id IS NULL THEN 1 ELSE 0 END) AS unmatched_rows
  FROM workspace.silver_dev.fct_inventory_snapshots f
  LEFT JOIN workspace.silver_dev.dim_products_current d
    ON f.product_id = d.product_id

  UNION ALL

  SELECT
    'fct_shipments -> dim_distribution_centers_current (dc_id)' AS check_name,
    COUNT(*) AS fact_rows,
    SUM(CASE WHEN d.dc_id IS NULL THEN 1 ELSE 0 END) AS unmatched_rows
  FROM workspace.silver_dev.fct_shipments f
  LEFT JOIN workspace.silver_dev.dim_distribution_centers_current d
    ON f.dc_id = d.dc_id

  UNION ALL

  SELECT
    'fct_shipments -> dim_customers_current (customer_id)' AS check_name,
    COUNT(*) AS fact_rows,
    SUM(CASE WHEN d.customer_id IS NULL THEN 1 ELSE 0 END) AS unmatched_rows
  FROM workspace.silver_dev.fct_shipments f
  LEFT JOIN workspace.silver_dev.dim_customers_current d
    ON f.customer_id = d.customer_id

  UNION ALL

  SELECT
    'fct_returns -> dim_customers_current (customer_id)' AS check_name,
    COUNT(*) AS fact_rows,
    SUM(CASE WHEN d.customer_id IS NULL THEN 1 ELSE 0 END) AS unmatched_rows
  FROM workspace.silver_dev.fct_returns f
  LEFT JOIN workspace.silver_dev.dim_customers_current d
    ON f.customer_id = d.customer_id

  UNION ALL

  SELECT
    'fct_returns -> dim_products_current (product_id)' AS check_name,
    COUNT(*) AS fact_rows,
    SUM(CASE WHEN d.product_id IS NULL THEN 1 ELSE 0 END) AS unmatched_rows
  FROM workspace.silver_dev.fct_returns f
  LEFT JOIN workspace.silver_dev.dim_products_current d
    ON f.product_id = d.product_id
)
SELECT
  check_name,
  fact_rows,
  unmatched_rows,
  ROUND(100.0 * unmatched_rows / NULLIF(fact_rows, 0), 4) AS unmatched_pct
FROM checks
ORDER BY unmatched_rows DESC, check_name;
