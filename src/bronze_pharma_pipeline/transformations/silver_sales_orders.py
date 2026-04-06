from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.types import DateType, DoubleType, StringType, StructField, StructType, TimestampType


table_schema = StructType([
    StructField("order_id", StringType(), True),
    StructField("order_date", TimestampType(), True),
    StructField("dc_id", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("order_status", StringType(), True),
    StructField("payment_terms", StringType(), True),
    StructField("incoterm", StringType(), True),
    StructField("currency", StringType(), True),
    StructField("order_total_amount", DoubleType(), True),
    StructField("bronze_record_key", StringType(), True),
    StructField("_ingested_at", TimestampType(), True),
    StructField("_ingest_date", DateType(), True),
    StructField("_source_file", StringType(), True)
])


@dp.table(
    name="silver_dev.sales_orders",
    schema=table_schema
)
@dp.expect_or_drop("valid_order_id_not_null", "order_id IS NOT NULL")
@dp.expect_or_drop("valid_order_id_not_empty", "order_id <> ''")
def silver_sales_orders():
    bronze_df = spark.readStream.table("workspace.bronze_dev.datawarehouse_raw")

    exploded_df = (
        bronze_df
            .selectExpr(
                "key as bronze_record_key",
                "_ingested_at",
                "_ingest_date",
                "_source_file",
                "inline_outer(value.sales_orders)"
            )
            .select(
                F.col("order_id"),
                F.to_timestamp("order_date").alias("order_date"),
                F.col("dc_id"),
                F.col("customer_id"),
                F.col("order_status"),
                F.col("payment_terms"),
                F.col("incoterm"),
                F.col("currency"),
                F.col("order_total_amount").cast(DoubleType()),
                F.col("bronze_record_key"),
                F.col("_ingested_at"),
                F.col("_ingest_date"),
                F.col("_source_file")
            )
            .withColumn("order_id", F.trim(F.col("order_id")))
    )

    return (
        exploded_df
            .withWatermark("_ingested_at", "7 days")
            .dropDuplicates(["order_id"])
    )
