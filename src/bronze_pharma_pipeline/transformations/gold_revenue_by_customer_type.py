from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.materialized_view(
    name="gold_dev.revenue_by_customer_type",
    comment="Revenue aggregated by customer type"
)
def gold_revenue_by_customer_type():
    orders = spark.read.table("workspace.silver_dev.sales_orders")
    lines = spark.read.table("workspace.silver_dev.sales_order_lines")
    customers = spark.read.table("workspace.silver_dev.customers")

    return (
        lines
            .join(orders.select("order_id", "customer_id"), on="order_id", how="inner")
            .join(customers.select("customer_id", "customer_type", "region"), on="customer_id", how="inner")
            .groupBy("customer_type", "region")
            .agg(
                F.round(F.sum("line_amount"), 2).alias("revenue"),
                F.countDistinct("order_id").alias("order_count")
            )
            .orderBy(F.col("revenue").desc())
    )
