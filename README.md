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

- `workspace.silver_dev.manufacturers`
- `workspace.silver_dev.products`
- `workspace.silver_dev.distribution_centers`
- `workspace.silver_dev.customers`
- `workspace.silver_dev.sales_orders`
- `workspace.silver_dev.sales_order_lines`
- `workspace.silver_dev.inventory_snapshots`
- `workspace.silver_dev.shipments`
- `workspace.silver_dev.returns`

Common Silver patterns used in this project:

- `inline_outer(...)` for array-of-struct flattening
- `@dp.expect_or_drop(...)` for data quality expectations
- `withWatermark(...).dropDuplicates(...)` for streaming-safe deduplication
- explicit casting and trimming for standardized business columns

### Gold

These materialized views provide pre-aggregated analytics for BI consumption:

- `workspace.gold_dev.kpi_summary`
- `workspace.gold_dev.monthly_revenue_trend`
- `workspace.gold_dev.revenue_by_therapeutic_area`
- `workspace.gold_dev.revenue_by_customer_type`
- `workspace.gold_dev.order_status_funnel`
- `workspace.gold_dev.inventory_availability`
- `workspace.gold_dev.return_analysis`
- `workspace.gold_dev.top_products_revenue`

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

## Next Improvements

- add a GitHub Actions workflow for bundle validation
- add data quality tests and table-level checks for Gold outputs
- add a business glossary for the Silver and Gold tables
- add parameterization for multi-environment deployment