"""Deploy Unity Catalog metric views to the target silver schema.

This runs as a bundle job task because WITH METRICS LANGUAGE YAML DDL is not
supported inside SDP/DLT Python pipeline spark.sql execution.
"""

import argparse
import time
from databricks.sdk import WorkspaceClient
from databricks.sdk.service import sql as sqlsvc


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Deploy metric views")
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--silver-schema", required=True)
    parser.add_argument("--warehouse-id", required=True)
    return parser.parse_args()


def execute_statement(client: WorkspaceClient, warehouse_id: str, statement: str) -> None:
    response = client.statement_execution.execute_statement(
        warehouse_id=warehouse_id,
        statement=statement,
        wait_timeout="50s",
    )

    statement_id = response.statement_id
    status = response.status

    while status and status.state in {
        sqlsvc.StatementState.PENDING,
        sqlsvc.StatementState.RUNNING,
    }:
        time.sleep(2)
        response = client.statement_execution.get_statement(statement_id=statement_id)
        status = response.status

    if not status or status.state != sqlsvc.StatementState.SUCCEEDED:
        state = status.state.value if status and status.state else "UNKNOWN"
        message = status.error.message if status and status.error else "No error details provided"
        raise RuntimeError(f"Statement failed ({state}): {message}")


def build_metric_view_statements(source_prefix: str) -> list[str]:
    return [
        f"""
CREATE OR REPLACE VIEW {source_prefix}.metrics_daily_revenue
WITH METRICS
LANGUAGE YAML
AS $$
  version: 1.1
  comment: "Daily revenue metrics aggregated by calendar date with time attributes"
  source: {source_prefix}.fct_sales_order_lines
  joins:
    - name: time_dim
      on: source.date_key = time_dim.date_key
      source: {source_prefix}.dim_time
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
      expr: COUNT(DISTINCT source.order_id)
      comment: "Distinct count of orders"
    - name: Line Item Count
      expr: COUNT(1)
      comment: "Total line items"
    - name: Average Line Amount
      expr: AVG(line_amount)
      comment: "Average line item amount"
$$
""",
        f"""
CREATE OR REPLACE VIEW {source_prefix}.metrics_monthly_revenue
WITH METRICS
LANGUAGE YAML
AS $$
  version: 1.1
  comment: "Monthly revenue metrics with fiscal calendar support"
  source: {source_prefix}.fct_sales_order_lines
  joins:
    - name: time_dim
      on: source.date_key = time_dim.date_key
      source: {source_prefix}.dim_time
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
      expr: COUNT(DISTINCT source.order_id)
      comment: "Number of orders in month"
    - name: Average Order Value
      expr: SUM(line_amount) / COUNT(DISTINCT source.order_id)
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
$$
""",
        f"""
CREATE OR REPLACE VIEW {source_prefix}.metrics_fiscal_ytd_revenue
WITH METRICS
LANGUAGE YAML
AS $$
  version: 1.1
  comment: "Year-to-date revenue accumulation by fiscal year"
  source: {source_prefix}.fct_sales_order_lines
  joins:
    - name: time_dim
      on: source.date_key = time_dim.date_key
      source: {source_prefix}.dim_time
  filter: |
    time_dim.calendar_date >= time_dim.fiscal_year_start_date AND
    time_dim.calendar_date <= CURRENT_DATE()
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
      expr: COUNT(DISTINCT source.order_id)
      comment: "Fiscal YTD order count"
    - name: YTD Average Order Value
      expr: SUM(line_amount) / COUNT(DISTINCT source.order_id)
      comment: "Fiscal YTD average order value"
$$
""",
        f"""
CREATE OR REPLACE VIEW {source_prefix}.metrics_inventory_by_date
WITH METRICS
LANGUAGE YAML
AS $$
  version: 1.1
  comment: "Daily inventory levels aggregated from snapshots with calendar attributes"
  source: {source_prefix}.fct_inventory_snapshots
  joins:
    - name: time_dim
      on: source.date_key = time_dim.date_key
      source: {source_prefix}.dim_time
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
$$
""",
        f"""
CREATE OR REPLACE VIEW {source_prefix}.metrics_shipments_by_date
WITH METRICS
LANGUAGE YAML
AS $$
  version: 1.1
  comment: "Daily shipment volume metrics with calendar breakdowns"
  source: {source_prefix}.fct_shipments
  joins:
    - name: time_dim
      on: source.ship_date_key = time_dim.date_key
      source: {source_prefix}.dim_time
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
      expr: source.shipment_status
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
$$
""",
        f"""
CREATE OR REPLACE VIEW {source_prefix}.metrics_returns_by_date
WITH METRICS
LANGUAGE YAML
AS $$
  version: 1.1
  comment: "Daily return volume and rates with temporal analysis"
  source: {source_prefix}.fct_returns
  joins:
    - name: time_dim
      on: source.date_key = time_dim.date_key
      source: {source_prefix}.dim_time
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
      expr: source.reason_code
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
$$
""",
    ]


def main() -> None:
    args = parse_args()

    source_prefix = f"{args.catalog}.{args.silver_schema}"
    statements = build_metric_view_statements(source_prefix)

    client = WorkspaceClient()

    for index, statement in enumerate(statements, start=1):
        print(f"Deploying metric view {index}/{len(statements)}")
        execute_statement(client, args.warehouse_id, statement)

    print(f"Metric views deployed to {source_prefix}")


if __name__ == "__main__":
    main()
