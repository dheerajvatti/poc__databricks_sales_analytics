from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.types import DateType, IntegerType, StringType, StructField, StructType, TimestampType
from _env_config import get_config

_c = get_config()


table_schema = StructType([
    StructField("shipment_id", StringType(), True),
    StructField("order_id", StringType(), True),
    StructField("dc_id", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("ship_date", TimestampType(), True),
    StructField("ship_date_key", IntegerType(), True),
    StructField("delivery_date", TimestampType(), True),
    StructField("shipment_status", StringType(), True),
    StructField("carrier_name", StringType(), True),
    StructField("tracking_number", StringType(), True),
    StructField("bronze_record_key", StringType(), True),
    StructField("_ingested_at", TimestampType(), True),
    StructField("_ingest_date", DateType(), True),
    StructField("_source_file", StringType(), True)
])


@dp.table(
    name=f"{_c['silver_schema']}.fct_shipments",
    schema=table_schema
)
@dp.expect_or_drop("valid_shipment_id_not_null", "shipment_id IS NOT NULL")
@dp.expect_or_drop("valid_shipment_id_not_empty", "shipment_id <> ''")
def silver_shipments():
    bronze_df = spark.readStream.table(f"{_c['catalog']}.{_c['bronze_schema']}.datawarehouse_raw")

    exploded_df = (
        bronze_df
            .selectExpr(
                "key as bronze_record_key",
                "_ingested_at",
                "_ingest_date",
                "_source_file",
                "inline_outer(value.shipments)"
            )
            .select(
                F.col("shipment_id"),
                F.col("order_id"),
                F.col("dc_id"),
                F.col("customer_id"),
                F.to_timestamp("ship_date").alias("ship_date"),
                F.to_timestamp("delivery_date").alias("delivery_date"),
                F.col("shipment_status"),
                F.col("carrier_name"),
                F.col("tracking_number"),
                F.col("bronze_record_key"),
                F.col("_ingested_at"),
                F.col("_ingest_date"),
                F.col("_source_file")
            )
            .withColumn("ship_date_key", F.date_format(F.col("ship_date"), "yyyyMMdd").cast(IntegerType()))
            .withColumn("shipment_id", F.trim(F.col("shipment_id")))
    )

    return (
        exploded_df
            .withWatermark("_ingested_at", "7 days")
            .dropDuplicates(["shipment_id"])
    )
