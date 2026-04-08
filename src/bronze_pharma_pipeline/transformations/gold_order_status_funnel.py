from pyspark import pipelines as dp
from pyspark.sql import functions as F
from _env_config import get_config

_c = get_config()


@dp.materialized_view(
    name=f"{_c['gold_schema']}.agg_orders_by_status",
    comment="Order count and revenue by status (funnel view)"
)
def gold_order_status_funnel():
    orders = spark.read.table(f"{_c['catalog']}.{_c['silver_schema']}.fct_sales_orders")

    total_orders = orders.count()

    return (
        orders
            .groupBy("order_status")
            .agg(
                F.count("order_id").alias("order_count"),
                F.round(F.sum("order_total_amount"), 2).alias("revenue")
            )
            .withColumn("pct_of_total", F.round(F.col("order_count") / total_orders, 4))
            .orderBy(F.col("order_count").desc())
    )
