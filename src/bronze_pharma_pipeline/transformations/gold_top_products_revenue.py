from pyspark import pipelines as dp
from pyspark.sql import functions as F
from _env_config import get_config

_c = get_config()


@dp.materialized_view(
    name=f"{_c['gold_schema']}.agg_revenue_top_products",
    comment="Revenue and units sold per product, ranked by revenue"
)
def gold_top_products_revenue():
    lines = spark.read.table(f"{_c['catalog']}.{_c['silver_schema']}.fct_sales_order_lines")
    products = spark.read.table(f"{_c['catalog']}.{_c['silver_schema']}.dim_products_current")

    return (
        lines
            .join(
                products.select("product_id", "product_name", "therapeutic_area", "dosage_form", "manufacturer_id"),
                on="product_id",
                how="inner"
            )
            .groupBy("product_id", "product_name", "therapeutic_area", "dosage_form", "manufacturer_id")
            .agg(
                F.round(F.sum("line_amount"), 2).alias("revenue"),
                F.sum("ordered_qty_packs").alias("units_sold"),
                F.countDistinct("order_id").alias("order_count")
            )
            .orderBy(F.col("revenue").desc())
    )
