-- Gold Layer: Pre-aggregated Dashboard Views
-- These provide simple, dashboard-ready aggregations from facts and dim_time

-- Daily Revenue Dashboard Data
CREATE OR REPLACE MATERIALIZED VIEW workspace.gold_dev.dashboard_daily_revenue AS
SELECT
  t.calendar_date,
  t.day_name,
  t.month_num,
  t.month_name,
  t.year_num,
  t.fiscal_year,
  t.is_weekend,
  SUM(l.line_amount) AS total_revenue,
  COUNT(DISTINCT l.order_id) AS order_count,
  COUNT(1) AS line_item_count,
  ROUND(AVG(l.line_amount), 2) AS avg_line_amount
FROM workspace.silver_dev.fct_sales_order_lines l
INNER JOIN workspace.silver_dev.fct_sales_orders o ON l.order_id = o.order_id
INNER JOIN workspace.silver_dev.dim_time t ON o.date_key = t.date_key
GROUP BY
  t.calendar_date, t.day_name, t.month_num, t.month_name, t.year_num, t.fiscal_year, t.is_weekend;

-- Monthly Revenue Dashboard Data
CREATE OR REPLACE MATERIALIZED VIEW workspace.gold_dev.dashboard_monthly_revenue AS
SELECT
  t.year_num,
  t.month_num,
  t.month_label,
  t.month_name,
  t.fiscal_year,
  t.fiscal_year_label,
  t.fiscal_month_num,
  t.fiscal_quarter_num,
  SUM(l.line_amount) AS total_revenue,
  COUNT(DISTINCT l.order_id) AS order_count,
  ROUND(SUM(l.line_amount) / NULLIF(COUNT(DISTINCT l.order_id), 0), 2) AS avg_order_value,
  SUM(l.ordered_qty_packs) AS units_ordered,
  SUM(l.confirmed_qty_packs) AS units_confirmed,
  ROUND(SUM(l.confirmed_qty_packs) / NULLIF(SUM(l.ordered_qty_packs), 0), 4) AS fulfillment_rate
FROM workspace.silver_dev.fct_sales_order_lines l
INNER JOIN workspace.silver_dev.fct_sales_orders o ON l.order_id = o.order_id
INNER JOIN workspace.silver_dev.dim_time t ON o.date_key = t.date_key
GROUP BY
  t.year_num, t.month_num, t.month_label, t.month_name, t.fiscal_year, t.fiscal_year_label, t.fiscal_month_num, t.fiscal_quarter_num;

-- Fiscal Year-to-Date Revenue
CREATE OR REPLACE MATERIALIZED VIEW workspace.gold_dev.dashboard_fiscal_ytd AS
SELECT
  t.fiscal_year,
  t.fiscal_year_label,
  t.fiscal_month_num,
  t.fiscal_quarter_num,
  CONCAT('FQ', t.fiscal_quarter_num) AS fiscal_quarter_label,
  SUM(l.line_amount) AS ytd_revenue,
  COUNT(DISTINCT l.order_id) AS ytd_order_count,
  ROUND(SUM(l.line_amount) / NULLIF(COUNT(DISTINCT l.order_id), 0), 2) AS ytd_avg_order_value
FROM workspace.silver_dev.fct_sales_order_lines l
INNER JOIN workspace.silver_dev.fct_sales_orders o ON l.order_id = o.order_id
INNER JOIN workspace.silver_dev.dim_time t ON o.date_key = t.date_key
WHERE t.calendar_date >= t.fiscal_year_start_date AND t.calendar_date <= CURRENT_DATE()
GROUP BY
  t.fiscal_year, t.fiscal_year_label, t.fiscal_month_num, t.fiscal_quarter_num;

-- Monthly Inventory Status
CREATE OR REPLACE MATERIALIZED VIEW workspace.gold_dev.dashboard_inventory_monthly AS
SELECT
  t.year_num,
  t.month_num,
  t.month_label,
  t.month_name,
  t.fiscal_year,
  ROUND(AVG(i.on_hand_packs), 0) AS avg_on_hand,
  ROUND(AVG(i.reserved_packs), 0) AS avg_reserved,
  ROUND(AVG(i.in_transit_packs), 0) AS avg_in_transit,
  ROUND(AVG(i.on_hand_packs - i.reserved_packs), 0) AS avg_available,
  COUNT(DISTINCT i.product_id) AS unique_products
FROM workspace.silver_dev.fct_inventory_snapshots i
INNER JOIN workspace.silver_dev.dim_time t ON i.date_key = t.date_key
GROUP BY
  t.year_num, t.month_num, t.month_label, t.month_name, t.fiscal_year;

-- Monthly Returns Analysis
CREATE OR REPLACE MATERIALIZED VIEW workspace.gold_dev.dashboard_returns_monthly AS
SELECT
  t.year_num,
  t.month_num,
  t.month_label,
  t.month_name,
  t.fiscal_year,
  r.reason_code,
  COUNT(1) AS return_count,
  SUM(r.credit_amount) AS total_credits,
  ROUND(AVG(r.credit_amount), 2) AS avg_credit
FROM workspace.silver_dev.fct_returns r
INNER JOIN workspace.silver_dev.dim_time t ON r.date_key = t.date_key
GROUP BY
  t.year_num, t.month_num, t.month_label, t.month_name, t.fiscal_year, r.reason_code;

-- Shipment Volume Trends
CREATE OR REPLACE MATERIALIZED VIEW workspace.gold_dev.dashboard_shipment_trends AS
SELECT
  t.year_num,
  t.month_num,
  t.month_label,
  t.month_name,
  t.fiscal_year,
  COUNT(1) AS shipment_count,
  COUNT(DISTINCT s.order_id) AS order_count,
  COUNT(DISTINCT s.customer_id) AS unique_customers
FROM workspace.silver_dev.fct_shipments s
INNER JOIN workspace.silver_dev.dim_time t ON s.ship_date_key = t.date_key
GROUP BY
  t.year_num, t.month_num, t.month_label, t.month_name, t.fiscal_year;

-- Current Year KPI Summary (Latest Month)
CREATE OR REPLACE MATERIALIZED VIEW workspace.gold_dev.dashboard_latest_kpis AS
WITH latest_month AS (
  SELECT
    YEAR(CURRENT_DATE()) AS current_year,
    MONTH(CURRENT_DATE()) AS current_month
)
SELECT
  'Revenue (Current Month)' AS kpi_name,
  ROUND(SUM(l.line_amount), 2) AS kpi_value,
  'USD' AS kpi_unit
FROM workspace.silver_dev.fct_sales_order_lines l
INNER JOIN workspace.silver_dev.fct_sales_orders o ON l.order_id = o.order_id
INNER JOIN workspace.silver_dev.dim_time t ON o.date_key = t.date_key
CROSS JOIN latest_month lm
WHERE t.year_num = lm.current_year AND t.month_num = lm.current_month

UNION ALL

SELECT
  'Orders (Current Month)',
  COUNT(DISTINCT o.order_id),
  'Count'
FROM workspace.silver_dev.fct_sales_orders o
INNER JOIN workspace.silver_dev.dim_time t ON o.date_key = t.date_key
CROSS JOIN latest_month lm
WHERE t.year_num = lm.current_year AND t.month_num = lm.current_month

UNION ALL

SELECT
  'Avg Order Value (Current Month)',
  ROUND(SUM(l.line_amount) / NULLIF(COUNT(DISTINCT l.order_id), 0), 2),
  'USD'
FROM workspace.silver_dev.fct_sales_order_lines l
INNER JOIN workspace.silver_dev.fct_sales_orders o ON l.order_id = o.order_id
INNER JOIN workspace.silver_dev.dim_time t ON o.date_key = t.date_key
CROSS JOIN latest_month lm
WHERE t.year_num = lm.current_year AND t.month_num = lm.current_month

UNION ALL

SELECT
  'Shipments (Current Month)',
  COUNT(1),
  'Count'
FROM workspace.silver_dev.fct_shipments s
INNER JOIN workspace.silver_dev.dim_time t ON s.ship_date_key = t.date_key
CROSS JOIN latest_month lm
WHERE t.year_num = lm.current_year AND t.month_num = lm.current_month

UNION ALL

SELECT
  'Returns (Current Month)',
  COUNT(1),
  'Count'
FROM workspace.silver_dev.fct_returns r
INNER JOIN workspace.silver_dev.dim_time t ON r.date_key = t.date_key
CROSS JOIN latest_month lm
WHERE t.year_num = lm.current_year AND t.month_num = lm.current_month;
