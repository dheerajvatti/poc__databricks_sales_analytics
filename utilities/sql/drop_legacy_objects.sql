-- Drop legacy objects that are no longer used after naming-standard cutover.
-- Safe to run multiple times because every statement uses IF EXISTS.

USE CATALOG workspace;

-- Optional pre-checks for legacy Silver objects
SHOW TABLES IN silver_dev LIKE 'manufacturers';
SHOW TABLES IN silver_dev LIKE 'manufacturers_current';
SHOW TABLES IN silver_dev LIKE 'products';
SHOW TABLES IN silver_dev LIKE 'products_current';
SHOW TABLES IN silver_dev LIKE 'distribution_centers';
SHOW TABLES IN silver_dev LIKE 'distribution_centers_current';
SHOW TABLES IN silver_dev LIKE 'customers';
SHOW TABLES IN silver_dev LIKE 'customers_current';
SHOW TABLES IN silver_dev LIKE 'sales_orders';
SHOW TABLES IN silver_dev LIKE 'sales_order_lines';
SHOW TABLES IN silver_dev LIKE 'inventory_snapshots';
SHOW TABLES IN silver_dev LIKE 'shipments';
SHOW TABLES IN silver_dev LIKE 'returns';

-- Optional pre-checks for older iterations
SHOW TABLES IN silver_dev LIKE 'customers_scd2';
SHOW TABLES IN silver_dev LIKE 'customers_current_mv';
SHOW TABLES IN silver_dev LIKE 'manufacturers_current_mv';
SHOW TABLES IN silver_dev LIKE 'products_current_mv';
SHOW TABLES IN silver_dev LIKE 'distribution_centers_current_mv';

-- Optional pre-checks for legacy Gold objects
SHOW TABLES IN gold_dev LIKE 'kpi_summary';
SHOW TABLES IN gold_dev LIKE 'monthly_revenue_trend';
SHOW TABLES IN gold_dev LIKE 'revenue_by_therapeutic_area';
SHOW TABLES IN gold_dev LIKE 'revenue_by_customer_type';
SHOW TABLES IN gold_dev LIKE 'order_status_funnel';
SHOW TABLES IN gold_dev LIKE 'inventory_availability';
SHOW TABLES IN gold_dev LIKE 'return_analysis';
SHOW TABLES IN gold_dev LIKE 'top_products_revenue';

-- Legacy Silver objects (pre dim/fct naming)
DROP TABLE IF EXISTS silver_dev.manufacturers;
DROP MATERIALIZED VIEW IF EXISTS silver_dev.manufacturers_current;
DROP TABLE IF EXISTS silver_dev.products;
DROP MATERIALIZED VIEW IF EXISTS silver_dev.products_current;
DROP TABLE IF EXISTS silver_dev.distribution_centers;
DROP MATERIALIZED VIEW IF EXISTS silver_dev.distribution_centers_current;
DROP TABLE IF EXISTS silver_dev.customers;
DROP MATERIALIZED VIEW IF EXISTS silver_dev.customers_current;
DROP TABLE IF EXISTS silver_dev.sales_orders;
DROP TABLE IF EXISTS silver_dev.sales_order_lines;
DROP TABLE IF EXISTS silver_dev.inventory_snapshots;
DROP TABLE IF EXISTS silver_dev.shipments;
DROP TABLE IF EXISTS silver_dev.returns;

-- Legacy objects from earlier iterations
DROP TABLE IF EXISTS silver_dev.customers_scd2;
DROP MATERIALIZED VIEW IF EXISTS silver_dev.customers_current_mv;
DROP MATERIALIZED VIEW IF EXISTS silver_dev.manufacturers_current_mv;
DROP MATERIALIZED VIEW IF EXISTS silver_dev.products_current_mv;
DROP MATERIALIZED VIEW IF EXISTS silver_dev.distribution_centers_current_mv;

-- Legacy Gold objects (pre agg naming)
DROP MATERIALIZED VIEW IF EXISTS gold_dev.kpi_summary;
DROP MATERIALIZED VIEW IF EXISTS gold_dev.monthly_revenue_trend;
DROP MATERIALIZED VIEW IF EXISTS gold_dev.revenue_by_therapeutic_area;
DROP MATERIALIZED VIEW IF EXISTS gold_dev.revenue_by_customer_type;
DROP MATERIALIZED VIEW IF EXISTS gold_dev.order_status_funnel;
DROP MATERIALIZED VIEW IF EXISTS gold_dev.inventory_availability;
DROP MATERIALIZED VIEW IF EXISTS gold_dev.return_analysis;
DROP MATERIALIZED VIEW IF EXISTS gold_dev.top_products_revenue;
