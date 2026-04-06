from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.materialized_view(
    name="gold_dev.order_status_funnel",
    comment="Order count and revenue by status (funnel view)"
)
def gold_order_status_funnel():
    orders = spark.read.table("workspace.silver_dev.sales_orders")

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
