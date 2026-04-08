from pyspark import pipelines as dp
from pyspark.sql import functions as F
from _env_config import get_config

_c = get_config()


@dp.materialized_view(
    name=f"{_c['gold_schema']}.agg_returns_by_reason",
    comment="Return volume, credit, and rate by reason code"
)
def gold_return_analysis():
    returns = spark.read.table(f"{_c['catalog']}.{_c['silver_schema']}.fct_returns")
    lines = spark.read.table(f"{_c['catalog']}.{_c['silver_schema']}.fct_sales_order_lines")

    total_sold = lines.agg(F.sum("ordered_qty_packs").alias("total_sold_packs")).collect()[0]["total_sold_packs"]

    return (
        returns
            .groupBy("reason_code")
            .agg(
                F.count("return_id").alias("return_count"),
                F.sum("returned_qty_packs").alias("returned_packs"),
                F.round(F.sum("credit_amount"), 2).alias("total_credit")
            )
            .withColumn("return_rate", F.round(F.col("returned_packs") / total_sold, 4))
            .orderBy(F.col("return_count").desc())
    )
