-- Example queries for time-based metrics
-- These demonstrate how to use the MEASURE() function with metric views

-- Daily Revenue Trends
SELECT
  `Calendar Date`,
  `Month`,
  `Day Name`,
  MEASURE(`Revenue`) AS daily_revenue,
  MEASURE(`Order Count`) AS orders,
  MEASURE(`Line Item Count`) AS line_items,
  MEASURE(`Average Line Amount`) AS avg_line_amount
FROM workspace.silver_dev.metrics_daily_revenue
WHERE YEAR(`Calendar Date`) = 2026
GROUP BY ALL
ORDER BY `Calendar Date` DESC
LIMIT 30;

-- Monthly Revenue with Fiscal Calendar Comparison
SELECT
  `Month Label`,
  `Fiscal Year Label`,
  `Fiscal Quarter`,
  MEASURE(`Revenue`) AS monthly_revenue,
  MEASURE(`Order Count`) AS order_count,
  MEASURE(`Average Order Value`) AS aov,
  MEASURE(`Fulfillment Rate`) AS fulfillment_pct
FROM workspace.silver_dev.metrics_monthly_revenue
WHERE `Fiscal Year` IN (2025, 2026)
GROUP BY ALL
ORDER BY `Fiscal Year Label` DESC, `Calendar Month` ASC;

-- Fiscal Year-to-Date Revenue Progress
SELECT
  `Fiscal Year Label`,
  `Fiscal Quarter`,
  MEASURE(`YTD Revenue`) AS ytd_revenue,
  MEASURE(`YTD Order Count`) AS ytd_orders,
  MEASURE(`YTD Average Order Value`) AS ytd_aov
FROM workspace.silver_dev.metrics_fiscal_ytd_revenue
WHERE `Fiscal Year` IN (2025, 2026)
GROUP BY ALL
ORDER BY `Fiscal Year`, `Fiscal Quarter` DESC;

-- Inventory Trend by Month
SELECT
  `Year Month`,
  MEASURE(`Total On Hand`) AS total_on_hand,
  MEASURE(`Total Reserved`) AS total_reserved,
  MEASURE(`Available for Sale`) AS available,
  MEASURE(`Product Count`) AS product_count
FROM workspace.silver_dev.metrics_inventory_by_date
WHERE `Fiscal Year` = 2026
GROUP BY ALL
ORDER BY `Year Month` DESC;

-- Shipment Volume by Status and Period
SELECT
  `Year Month`,
  `Shipment Status`,
  MEASURE(`Shipment Count`) AS shipment_count,
  MEASURE(`Order Count`) AS order_count,
  MEASURE(`Unique Customers`) AS customer_count
FROM workspace.silver_dev.metrics_shipments_by_date
WHERE `Fiscal Quarter` = 'FQ1'
GROUP BY ALL
ORDER BY `Year Month`, `Shipment Status`;

-- Returns Analysis by Reason Code
SELECT
  `Year Month`,
  `Return Reason`,
  MEASURE(`Return Count`) AS return_count,
  MEASURE(`Return Amount`) AS total_credits,
  MEASURE(`Average Return Amount`) AS avg_credit
FROM workspace.silver_dev.metrics_returns_by_date
WHERE `Fiscal Year` = 2026
GROUP BY ALL
ORDER BY MEASURE(`Return Count`) DESC, `Year Month` DESC;

-- Cross-Metric Comparison: Revenue vs Returns
-- (Requires subquery or CTE since metrics must be in same view)
WITH monthly_rev AS (
  SELECT
    `Month Label` AS period,
    MEASURE(`Revenue`) AS revenue,
    MEASURE(`Order Count`) AS orders
  FROM workspace.silver_dev.metrics_monthly_revenue
  WHERE `Fiscal Year` = 2026
  GROUP BY `Month Label`
),
monthly_ret AS (
  SELECT
    `Year Month` AS period,
    MEASURE(`Return Amount`) AS return_credits,
    MEASURE(`Return Count`) AS returns
  FROM workspace.silver_dev.metrics_returns_by_date
  WHERE `Fiscal Year` = 2026
  GROUP BY `Year Month`
)
SELECT
  rev.period,
  rev.revenue,
  rev.orders,
  ret.returns,
  ret.return_credits,
  ROUND(100.0 * ret.return_credits / NULLIF(rev.revenue, 0), 2) AS return_pct_of_revenue
FROM monthly_rev rev
LEFT JOIN monthly_ret ret ON rev.period = ret.period
ORDER BY rev.period DESC;
