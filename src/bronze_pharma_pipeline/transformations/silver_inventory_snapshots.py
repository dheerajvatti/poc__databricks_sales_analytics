from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.types import DateType, IntegerType, StringType, StructField, StructType, TimestampType


table_schema = StructType([
    StructField("snapshot_id", StringType(), True),
    StructField("snapshot_ts", TimestampType(), True),
    StructField("dc_id", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("on_hand_packs", IntegerType(), True),
    StructField("reserved_packs", IntegerType(), True),
    StructField("in_transit_packs", IntegerType(), True),
    StructField("bronze_record_key", StringType(), True),
    StructField("_ingested_at", TimestampType(), True),
    StructField("_ingest_date", DateType(), True),
    StructField("_source_file", StringType(), True)
])


@dp.table(
    name="silver_dev.inventory_snapshots",
    schema=table_schema
)
@dp.expect_or_drop("valid_snapshot_id_not_null", "snapshot_id IS NOT NULL")
@dp.expect_or_drop("valid_snapshot_id_not_empty", "snapshot_id <> ''")
def silver_inventory_snapshots():
    bronze_df = spark.readStream.table("workspace.bronze_dev.datawarehouse_raw")

    exploded_df = (
        bronze_df
            .selectExpr(
                "key as bronze_record_key",
                "_ingested_at",
                "_ingest_date",
                "_source_file",
                "inline_outer(value.inventory_snapshots)"
            )
            .select(
                F.col("snapshot_id"),
                F.to_timestamp("snapshot_ts").alias("snapshot_ts"),
                F.col("dc_id"),
                F.col("product_id"),
                F.col("on_hand_packs").cast(IntegerType()),
                F.col("reserved_packs").cast(IntegerType()),
                F.col("in_transit_packs").cast(IntegerType()),
                F.col("bronze_record_key"),
                F.col("_ingested_at"),
                F.col("_ingest_date"),
                F.col("_source_file")
            )
            .withColumn("snapshot_id", F.trim(F.col("snapshot_id")))
    )

    return (
        exploded_df
            .withWatermark("_ingested_at", "7 days")
            .dropDuplicates(["snapshot_id"])
    )
