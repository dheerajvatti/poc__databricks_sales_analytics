-- Time-Based Metric Views
-- These metric views enable standardized time-based analytics joined with dim_time

-- Daily Revenue Metrics
CREATE OR REPLACE VIEW workspace.silver_dev.metrics_daily_revenue
WITH METRICS
LANGUAGE YAML
AS $$
  version: 1.1
  comment: "Daily revenue metrics aggregated by calendar date with time attributes"
  source: workspace.silver_dev.fct_sales_order_lines
  joins:
    - name: order_header
      expr: fct_sales_order_lines.order_id = order_header.order_id
      source: workspace.silver_dev.fct_sales_orders AS order_header
    - name: time_dim
      expr: order_header.date_key = time_dim.date_key
      source: workspace.silver_dev.dim_time AS time_dim
  dimensions:
    - name: Calendar Date
      expr: time_dim.calendar_date
      comment: "Date the order was placed"
    - name: Year
      expr: time_dim.year_num
      comment: "Calendar year"
    - name: Month
      expr: time_dim.month_name
      comment: "Month name"
    - name: Month Num
      expr: time_dim.month_num
      comment: "Month number (1-12)"
    - name: Week Start
      expr: time_dim.week_start_date
      comment: "Start of week (Sunday)"
    - name: Day Name
      expr: time_dim.day_name
      comment: "Day of week name"
    - name: Is Weekend
      expr: time_dim.is_weekend
      comment: "Weekend flag"
  measures:
    - name: Revenue
      expr: SUM(line_amount)
      comment: "Total revenue (sum of line amounts)"
    - name: Order Count
      expr: COUNT(DISTINCT fct_sales_order_lines.order_id)
      comment: "Distinct count of orders"
    - name: Line Item Count
      expr: COUNT(1)
      comment: "Total line items"
    - name: Average Line Amount
      expr: AVG(line_amount)
      comment: "Average line item amount"
$$;

-- Monthly Revenue Metrics
CREATE OR REPLACE VIEW workspace.silver_dev.metrics_monthly_revenue
WITH METRICS
LANGUAGE YAML
AS $$
  version: 1.1
  comment: "Monthly revenue metrics with fiscal calendar support"
  source: workspace.silver_dev.fct_sales_order_lines
  joins:
    - name: order_header
      expr: fct_sales_order_lines.order_id = order_header.order_id
      source: workspace.silver_dev.fct_sales_orders AS order_header
    - name: time_dim
      expr: order_header.date_key = time_dim.date_key
      source: workspace.silver_dev.dim_time AS time_dim
  dimensions:
    - name: Calendar Year
      expr: time_dim.year_num
      comment: "Calendar year"
    - name: Calendar Month
      expr: time_dim.month_num
      comment: "Calendar month (1-12)"
    - name: Month Label
      expr: time_dim.month_label
      comment: "Year-month label (YYYY-MM)"
    - name: Fiscal Year
      expr: time_dim.fiscal_year
      comment: "Fiscal year"
    - name: Fiscal Year Label
      expr: time_dim.fiscal_year_label
      comment: "Fiscal year label (FY2026)"
    - name: Fiscal Month
      expr: time_dim.fiscal_month_num
      comment: "Fiscal month (1-12)"
    - name: Fiscal Quarter
      expr: time_dim.fiscal_quarter_num
      comment: "Fiscal quarter (1-4)"
  measures:
    - name: Revenue
      expr: SUM(line_amount)
      comment: "Total monthly revenue"
    - name: Order Count
      expr: COUNT(DISTINCT fct_sales_order_lines.order_id)
      comment: "Number of orders in month"
    - name: Average Order Value
      expr: SUM(line_amount) / COUNT(DISTINCT fct_sales_order_lines.order_id)
      comment: "Average revenue per order"
    - name: Units Ordered
      expr: SUM(ordered_qty_packs)
      comment: "Total packs ordered"
    - name: Units Confirmed
      expr: SUM(confirmed_qty_packs)
      comment: "Total packs confirmed"
    - name: Fulfillment Rate
      expr: SUM(confirmed_qty_packs) / NULLIF(SUM(ordered_qty_packs), 0)
      comment: "Confirmed / Ordered qty ratio"
$$;

-- Fiscal Year-to-Date Revenue
CREATE OR REPLACE VIEW workspace.silver_dev.metrics_fiscal_ytd_revenue
WITH METRICS
LANGUAGE YAML
AS $$
  version: 1.1
  comment: "Year-to-date revenue accumulation by fiscal year"
  source: workspace.silver_dev.fct_sales_order_lines
  joins:
    - name: order_header
      expr: fct_sales_order_lines.order_id = order_header.order_id
      source: workspace.silver_dev.fct_sales_orders AS order_header
    - name: time_dim
      expr: order_header.date_key = time_dim.date_key
      source: workspace.silver_dev.dim_time AS time_dim
  filter: |
    time_dim.calendar_date >= time_dim.fiscal_year_start_date AND
    time_dim.calendar_date <= CURDATE()
  dimensions:
    - name: Fiscal Year
      expr: time_dim.fiscal_year
      comment: "Fiscal year"
    - name: Fiscal Year Label
      expr: time_dim.fiscal_year_label
      comment: "Fiscal year display (FY2026)"
    - name: Fiscal Month
      expr: time_dim.fiscal_month_num
      comment: "Month within fiscal year"
    - name: Fiscal Quarter
      expr: time_dim.fiscal_quarter_num
      comment: "Quarter within fiscal year"
  measures:
    - name: YTD Revenue
      expr: SUM(line_amount)
      comment: "Fiscal year-to-date revenue"
    - name: YTD Order Count
      expr: COUNT(DISTINCT fct_sales_order_lines.order_id)
      comment: "Fiscal YTD order count"
    - name: YTD Average Order Value
      expr: SUM(line_amount) / COUNT(DISTINCT fct_sales_order_lines.order_id)
      comment: "Fiscal YTD average order value"
$$;

-- Inventory Snapshot Metrics by Date
CREATE OR REPLACE VIEW workspace.silver_dev.metrics_inventory_by_date
WITH METRICS
LANGUAGE YAML
AS $$
  version: 1.1
  comment: "Daily inventory levels aggregated from snapshots with calendar attributes"
  source: workspace.silver_dev.fct_inventory_snapshots
  joins:
    - name: time_dim
      expr: fct_inventory_snapshots.date_key = time_dim.date_key
      source: workspace.silver_dev.dim_time AS time_dim
  dimensions:
    - name: Calendar Date
      expr: time_dim.calendar_date
      comment: "Snapshot date"
    - name: Year Month
      expr: time_dim.month_label
      comment: "Year-month label"
    - name: Month
      expr: time_dim.month_name
      comment: "Month name"
    - name: Fiscal Year
      expr: time_dim.fiscal_year
      comment: "Fiscal year"
  measures:
    - name: Total On Hand
      expr: SUM(on_hand_packs)
      comment: "Total available inventory across all DCs and products"
    - name: Total Reserved
      expr: SUM(reserved_packs)
      comment: "Total reserved/allocated inventory"
    - name: Total In Transit
      expr: SUM(in_transit_packs)
      comment: "Total inventory in transit"
    - name: Available for Sale
      expr: SUM(on_hand_packs) - SUM(reserved_packs)
      comment: "Available inventory (on-hand minus reserved)"
    - name: Product Count
      expr: COUNT(DISTINCT product_id)
      comment: "Number of distinct products in snapshot"
$$;

-- Shipment Volume by Date
CREATE OR REPLACE VIEW workspace.silver_dev.metrics_shipments_by_date
WITH METRICS
LANGUAGE YAML
AS $$
  version: 1.1
  comment: "Daily shipment volume metrics with calendar breakdowns"
  source: workspace.silver_dev.fct_shipments
  joins:
    - name: time_dim
      expr: fct_shipments.ship_date_key = time_dim.date_key
      source: workspace.silver_dev.dim_time AS time_dim
  dimensions:
    - name: Ship Date
      expr: time_dim.calendar_date
      comment: "Date shipment was sent"
    - name: Year Month
      expr: time_dim.month_label
      comment: "Year-month label"
    - name: Fiscal Quarter
      expr: CONCAT('FQ', time_dim.fiscal_quarter_num)
      comment: "Fiscal quarter"
    - name: Shipment Status
      expr: fct_shipments.shipment_status
      comment: "Current shipment status"
  measures:
    - name: Shipment Count
      expr: COUNT(1)
      comment: "Number of shipments"
    - name: Order Count
      expr: COUNT(DISTINCT order_id)
      comment: "Distinct orders shipped"
    - name: Unique Customers
      expr: COUNT(DISTINCT customer_id)
      comment: "Distinct customers served"
$$;

-- Returns Analytics by Date
CREATE OR REPLACE VIEW workspace.silver_dev.metrics_returns_by_date
WITH METRICS
LANGUAGE YAML
AS $$
  version: 1.1
  comment: "Daily return volume and rates with temporal analysis"
  source: workspace.silver_dev.fct_returns
  joins:
    - name: time_dim
      expr: fct_returns.date_key = time_dim.date_key
      source: workspace.silver_dev.dim_time AS time_dim
  dimensions:
    - name: Return Date
      expr: time_dim.calendar_date
      comment: "Date of return"
    - name: Year Month
      expr: time_dim.month_label
      comment: "Year-month label"
    - name: Fiscal Year
      expr: time_dim.fiscal_year
      comment: "Fiscal year"
    - name: Return Reason
      expr: fct_returns.reason_code
      comment: "Reason for return"
  measures:
    - name: Return Count
      expr: COUNT(1)
      comment: "Number of returns"
    - name: Return Amount
      expr: SUM(credit_amount)
      comment: "Total credit issued for returns"
    - name: Average Return Amount
      expr: AVG(credit_amount)
      comment: "Average credit per return"
    - name: Units Returned
      expr: SUM(returned_qty_packs)
      comment: "Total packs returned"
$$;
