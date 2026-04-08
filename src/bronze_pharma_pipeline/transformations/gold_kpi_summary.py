from pyspark import pipelines as dp
from pyspark.sql import functions as F
from _env_config import get_config

_c = get_config()


@dp.materialized_view(
    name=f"{_c['gold_schema']}.agg_kpi_summary",
    comment="Single-row overall KPI summary for dashboard counters"
)
def gold_kpi_summary():
    orders = spark.read.table(f"{_c['catalog']}.{_c['silver_schema']}.fct_sales_orders")
    lines = spark.read.table(f"{_c['catalog']}.{_c['silver_schema']}.fct_sales_order_lines")
    customers = spark.read.table(f"{_c['catalog']}.{_c['silver_schema']}.dim_customers_current")

    total_revenue = lines.agg(F.sum("line_amount").alias("total_revenue"))
    total_orders = orders.agg(F.countDistinct("order_id").alias("total_orders"))
    total_customers = customers.agg(F.countDistinct("customer_id").alias("total_customers"))
    avg_order_value = orders.agg(F.avg("order_total_amount").alias("avg_order_value"))

    return (
        total_revenue
            .crossJoin(total_orders)
            .crossJoin(total_customers)
            .crossJoin(avg_order_value)
    )
