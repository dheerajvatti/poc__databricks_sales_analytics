# Databricks Pharma Distribution Analytics

This repository contains a Databricks Asset Bundle for a pharma distribution analytics pipeline built on a Bronze, Silver, and Gold medallion design.

The project ingests a nested JSON landing file, normalizes it into entity-level Silver tables, builds Gold materialized views for analytics, and serves those metrics through a Databricks AI/BI dashboard.

## What This Project Includes

- A serverless Lakeflow Spark Declarative Pipeline managed through Databricks Asset Bundles
- Bronze ingestion for the raw landing dataset
- Silver tables for core business entities
- Gold materialized views for pre-aggregated analytics
- An AI/BI dashboard for KPI and visual reporting

## Project Structure

```text
.
├── bronze_dev/
│   └── landing/
│       └── pharma_distribution_landing.json
├── resources/
│   └── bronze_pharma_pipeline.pipeline.yml
├── src/
│   └── bronze_pharma_pipeline/
│       └── transformations/
│           ├── bronze_pharma_distribution_landing.py
│           ├── silver_*.py
│           └── gold_*.py
├── utilities/
│   ├── data_gen.py
│   └── upload_to_volume.py
└── databricks.yml
```

## Pipeline Configuration

- Bundle name: `bronze_pharma_pipeline`
- Target: `dev`
- Workspace host: `https://dbc-986cc365-9b2e.cloud.databricks.com`
- Catalog: `workspace`
- Base schema: `bronze_dev`
- Pipeline mode: serverless

The pipeline definition is in [resources/bronze_pharma_pipeline.pipeline.yml](/Users/dheerajvatti/databricks_projects/poc_distrbution_analytics/resources/bronze_pharma_pipeline.pipeline.yml), and the bundle configuration is in [databricks.yml](/Users/dheerajvatti/databricks_projects/poc_distrbution_analytics/databricks.yml).

## Data Model

### Bronze

- `workspace.bronze_dev.datawarehouse_raw`

This table stores the raw nested JSON payload with ingestion metadata.

### Silver

These tables flatten and validate the nested business entities:

- `workspace.silver_dev.dim_manufacturers`
- `workspace.silver_dev.dim_manufacturers_current`
- `workspace.silver_dev.dim_products`
- `workspace.silver_dev.dim_products_current`
- `workspace.silver_dev.dim_distribution_centers`
- `workspace.silver_dev.dim_distribution_centers_current`
- `workspace.silver_dev.dim_customers_hist`
- `workspace.silver_dev.dim_customers_current`
- `workspace.silver_dev.dim_time`
- `workspace.silver_dev.fct_sales_orders`
- `workspace.silver_dev.fct_sales_order_lines`
- `workspace.silver_dev.fct_inventory_snapshots`
- `workspace.silver_dev.fct_shipments`
- `workspace.silver_dev.fct_returns`

Common Silver patterns used in this project:

- `inline_outer(...)` for array-of-struct flattening
- `@dp.expect_or_drop(...)` for data quality expectations
- `withWatermark(...).dropDuplicates(...)` for streaming-safe deduplication
- explicit casting and trimming for standardized business columns

Conformed time join keys:

- `fct_sales_orders.date_key` -> `dim_time.date_key`
- `fct_sales_order_lines.date_key` -> `dim_time.date_key`
- `fct_inventory_snapshots.date_key` -> `dim_time.date_key`
- `fct_shipments.ship_date_key` -> `dim_time.date_key`
- `fct_returns.date_key` -> `dim_time.date_key`

### Gold

These materialized views provide pre-aggregated analytics for BI consumption:

- `workspace.gold_dev.agg_kpi_summary`
- `workspace.gold_dev.agg_revenue_monthly`
- `workspace.gold_dev.agg_revenue_by_therapeutic_area`
- `workspace.gold_dev.agg_revenue_by_customer_type`
- `workspace.gold_dev.agg_orders_by_status`
- `workspace.gold_dev.agg_inventory_availability_by_distribution_center`
- `workspace.gold_dev.agg_returns_by_reason`
- `workspace.gold_dev.agg_revenue_top_products`

## Star Schema

```mermaid
erDiagram
	DIM_CUSTOMERS_CURRENT {
		string customer_id PK
		string customer_name
		string customer_type
		string region
	}

	DIM_PRODUCTS_CURRENT {
		string product_id PK
		string product_name
		string therapeutic_area
		string dosage_form
		string manufacturer_id FK
	}

	DIM_MANUFACTURERS_CURRENT {
		string manufacturer_id PK
		string manufacturer_name
		string country
	}

	DIM_DISTRIBUTION_CENTERS_CURRENT {
		string dc_id PK
		string dc_name
		string region
	}

	DIM_TIME {
		int date_key PK
		date calendar_date
		string day_name
		int month_num
		string month_name
		int quarter_num
		int year_num
		int fiscal_year
		string fiscal_year_label
	}

	FCT_SALES_ORDERS {
		string order_id PK
		string customer_id FK
		string dc_id FK
		int date_key FK
		date order_date
		string order_status
		double order_total_amount
	}

	FCT_SALES_ORDER_LINES {
		string line_id PK
		string order_id FK
		string product_id FK
		int date_key FK
		int ordered_qty_packs
		int confirmed_qty_packs
		double line_amount
	}

	FCT_INVENTORY_SNAPSHOTS {
		string snapshot_id PK
		string dc_id FK
		string product_id FK
		int date_key FK
		timestamp snapshot_ts
		int on_hand_packs
		int reserved_packs
		int in_transit_packs
	}

	FCT_SHIPMENTS {
		string shipment_id PK
		string order_id FK
		string dc_id FK
		string customer_id FK
		int ship_date_key FK
		date ship_date
		date delivery_date
	}

	FCT_RETURNS {
		string return_id PK
		string original_order_id FK
		string customer_id FK
		string product_id FK
		int date_key FK
		date return_date
		string reason_code
		double credit_amount
	}

	DIM_MANUFACTURERS_CURRENT ||--o{ DIM_PRODUCTS_CURRENT : "manufacturer_id"

	DIM_CUSTOMERS_CURRENT ||--o{ FCT_SALES_ORDERS : "customer_id"
	DIM_DISTRIBUTION_CENTERS_CURRENT ||--o{ FCT_SALES_ORDERS : "dc_id"
	DIM_TIME ||--o{ FCT_SALES_ORDERS : "date_key"

	FCT_SALES_ORDERS ||--o{ FCT_SALES_ORDER_LINES : "order_id"
	DIM_PRODUCTS_CURRENT ||--o{ FCT_SALES_ORDER_LINES : "product_id"
	DIM_TIME ||--o{ FCT_SALES_ORDER_LINES : "date_key"

	DIM_DISTRIBUTION_CENTERS_CURRENT ||--o{ FCT_INVENTORY_SNAPSHOTS : "dc_id"
	DIM_PRODUCTS_CURRENT ||--o{ FCT_INVENTORY_SNAPSHOTS : "product_id"
	DIM_TIME ||--o{ FCT_INVENTORY_SNAPSHOTS : "date_key"

	FCT_SALES_ORDERS ||--o{ FCT_SHIPMENTS : "order_id"
	DIM_DISTRIBUTION_CENTERS_CURRENT ||--o{ FCT_SHIPMENTS : "dc_id"
	DIM_CUSTOMERS_CURRENT ||--o{ FCT_SHIPMENTS : "customer_id"
	DIM_TIME ||--o{ FCT_SHIPMENTS : "ship_date_key"

	FCT_SALES_ORDERS ||--o{ FCT_RETURNS : "original_order_id"
	DIM_CUSTOMERS_CURRENT ||--o{ FCT_RETURNS : "customer_id"
	DIM_PRODUCTS_CURRENT ||--o{ FCT_RETURNS : "product_id"
	DIM_TIME ||--o{ FCT_RETURNS : "date_key"
```

Notes:

- current dimensions (`dim_*_current`) are the BI join surfaces.
- historical SCD2 dimensions (`dim_*` / `dim_customers_hist`) support point-in-time analysis.
- conformed time dimension (`dim_time`) provides standard calendar and fiscal attributes for all facts via date_key joins.

### Gold Mart Lineage

```mermaid
flowchart LR
	D_CUST[dim_customers_current]
	D_PROD[dim_products_current]
	D_DC[dim_distribution_centers_current]

	F_ORD[fct_sales_orders]
	F_LINES[fct_sales_order_lines]
	F_INV[fct_inventory_snapshots]
	F_RET[fct_returns]

	G_KPI[agg_kpi_summary]
	G_MON[agg_revenue_monthly]
	G_TA[agg_revenue_by_therapeutic_area]
	G_CT[agg_revenue_by_customer_type]
	G_ST[agg_orders_by_status]
	G_INV[agg_inventory_availability_by_distribution_center]
	G_RET[agg_returns_by_reason]
	G_TOP[agg_revenue_top_products]

	F_ORD --> G_KPI
	F_LINES --> G_KPI
	D_CUST --> G_KPI

	F_ORD --> G_MON
	F_LINES --> G_MON

	F_LINES --> G_TA
	D_PROD --> G_TA

	F_ORD --> G_CT
	F_LINES --> G_CT
	D_CUST --> G_CT

	F_ORD --> G_ST

	F_INV --> G_INV
	D_DC --> G_INV
	D_PROD --> G_INV

	F_RET --> G_RET

	F_LINES --> G_TOP
	D_PROD --> G_TOP
```

## Dashboard

The Databricks AI/BI dashboard built from the Gold layer is available here:

- `https://dbc-986cc365-9b2e.cloud.databricks.com/sql/dashboardsv3/01f131db4d62156db234a46d4aaf0c75`

Dashboard coverage includes:

- KPI counters for revenue, orders, and average order value
- monthly revenue trend
- revenue by therapeutic area
- revenue by customer type
- order status funnel
- returns by reason code
- inventory by distribution center
- top products by revenue

## How To Deploy

Make sure the Databricks CLI is authenticated for the target workspace.

Validate the bundle:

```bash
databricks bundle validate
```

Deploy the bundle:

```bash
databricks bundle deploy --target dev
```

Run the pipeline:

```bash
databricks bundle run --target dev bronze_pharma_pipeline --full-refresh-all
```

## Development Notes

- Transformation files live under [src/bronze_pharma_pipeline/transformations](/Users/dheerajvatti/databricks_projects/poc_distrbution_analytics/src/bronze_pharma_pipeline/transformations)
- The pipeline automatically includes all transformation files via a glob pattern
- Local-only development artifacts are ignored via [.gitignore](/Users/dheerajvatti/databricks_projects/poc_distrbution_analytics/.gitignore)

## Mock Real-Time Sales Orders Stream

Use the streaming mock utility to continuously drop sales-order micro-batch files into the Bronze landing folder watched by Auto Loader.

Run from the repository root:

```bash
python3 utilities/mock_sales_orders_stream.py \
	--seed-file bronze_dev/landing/pharma_distribution_landing.json \
	--output-dir bronze_dev/landing \
	--batches 50 \
	--orders-per-batch 2 \
	--interval-seconds 2
```

What this does:

- writes one JSON file per batch in the same schema expected by Bronze
- emits only `sales_orders` records in each file (other arrays are empty)
- uses realistic DC, customer, and product references from your seed landing file
- creates unique real-time order IDs (`SO-RT-...`) per emitted order

## Next Improvements

- add a GitHub Actions workflow for bundle validation
- add data quality tests and table-level checks for Gold outputs
- add a business glossary for the Silver and Gold tables
- add parameterization for multi-environment deployment

## Integration Checks On Push

This repository includes an integration check workflow that runs on every push:

- workflow: `.github/workflows/integration-warehouse-checks.yml`
- SQL checks: `tests/integration/ri_checks.sql`
- runner: `tests/integration/run_ri_checks.py`

What it validates:

- `databricks bundle validate`
- referential integrity from facts to current dimensions in `silver_dev`

Required GitHub Actions secrets:

- `DATABRICKS_HOST`
- `DATABRICKS_TOKEN`
- `DATABRICKS_WAREHOUSE_ID`