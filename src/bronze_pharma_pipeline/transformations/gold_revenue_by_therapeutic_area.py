from pyspark import pipelines as dp
from pyspark.sql import functions as F
from _env_config import get_config

_c = get_config()


@dp.materialized_view(
    name=f"{_c['gold_schema']}.agg_revenue_by_therapeutic_area",
    comment="Revenue aggregated by therapeutic area"
)
def gold_revenue_by_therapeutic_area():
    lines = spark.read.table(f"{_c['catalog']}.{_c['silver_schema']}.fct_sales_order_lines")
    products = spark.read.table(f"{_c['catalog']}.{_c['silver_schema']}.dim_products_current")

    return (
        lines
            .join(products.select("product_id", "therapeutic_area"), on="product_id", how="inner")
            .groupBy("therapeutic_area")
            .agg(
                F.round(F.sum("line_amount"), 2).alias("revenue"),
                F.countDistinct("order_id").alias("order_count")
            )
            .orderBy(F.col("revenue").desc())
    )
