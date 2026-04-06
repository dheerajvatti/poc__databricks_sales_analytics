from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.materialized_view(
    name="gold_dev.inventory_availability",
    comment="Available (on-hand minus reserved) packs by DC and product"
)
def gold_inventory_availability():
    snapshots = spark.read.table("workspace.silver_dev.inventory_snapshots")
    dcs = spark.read.table("workspace.silver_dev.distribution_centers")
    products = spark.read.table("workspace.silver_dev.products")

    return (
        snapshots
            .join(dcs.select("dc_id", "dc_name", "region"), on="dc_id", how="left")
            .join(products.select("product_id", "product_name", "therapeutic_area", "cold_chain_required"), on="product_id", how="left")
            .withColumn("available_packs", F.col("on_hand_packs") - F.col("reserved_packs"))
            .groupBy("dc_name", "region", "therapeutic_area", "cold_chain_required")
            .agg(
                F.sum("on_hand_packs").alias("total_on_hand"),
                F.sum("reserved_packs").alias("total_reserved"),
                F.sum("in_transit_packs").alias("total_in_transit"),
                F.sum("available_packs").alias("total_available")
            )
            .orderBy("dc_name", "therapeutic_area")
    )
