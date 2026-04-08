from pyspark import pipelines as dp
from pyspark.sql import functions as F
from _env_config import get_config

_c = get_config()


@dp.materialized_view(
    name=f"{_c['gold_schema']}.agg_revenue_monthly",
    comment="Monthly revenue and order count trend"
)
def gold_monthly_revenue_trend():
    orders = spark.read.table(f"{_c['catalog']}.{_c['silver_schema']}.fct_sales_orders")
    lines = spark.read.table(f"{_c['catalog']}.{_c['silver_schema']}.fct_sales_order_lines")

    monthly_revenue = (
        lines
            .join(orders.select("order_id", "order_date"), on="order_id", how="inner")
            .withColumn("order_month", F.date_trunc("MONTH", F.col("order_date")))
            .groupBy("order_month")
            .agg(
                F.round(F.sum("line_amount"), 2).alias("revenue"),
                F.countDistinct("order_id").alias("order_count")
            )
            .orderBy("order_month")
    )
    return monthly_revenue
