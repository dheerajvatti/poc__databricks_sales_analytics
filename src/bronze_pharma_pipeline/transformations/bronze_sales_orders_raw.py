from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    DateType,
    FloatType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)
from _env_config import get_config

_c = get_config()

line_schema = ArrayType(
    StructType([
        StructField("line_id", StringType(), True),
        StructField("product_id", StringType(), True),
        StructField("ordered_qty_packs", IntegerType(), True),
        StructField("confirmed_qty_packs", IntegerType(), True),
        StructField("unit_price", FloatType(), True),
        StructField("line_amount", FloatType(), True),
        StructField("backorder_flag", BooleanType(), True),
    ])
)

sales_orders_schema = StructType([
    StructField("order_id", StringType(), True),
    StructField("order_date", TimestampType(), True),
    StructField("dc_id", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("order_status", StringType(), True),
    StructField("payment_terms", StringType(), True),
    StructField("incoterm", StringType(), True),
    StructField("currency", StringType(), True),
    StructField("order_total_amount", FloatType(), True),
    StructField("lines", line_schema, True),
    StructField("bronze_record_key", StringType(), True),
    StructField("_ingested_at", TimestampType(), True),
    StructField("_ingest_date", DateType(), True),
    StructField("_source_file", StringType(), True),
])

source_schema = StructType([
    StructField("key", StringType(), True),
    StructField("value", StructType([
        StructField("sales_orders", ArrayType(StructType([
            StructField("order_id", StringType(), True),
            StructField("order_date", StringType(), True),
            StructField("dc_id", StringType(), True),
            StructField("customer_id", StringType(), True),
            StructField("order_status", StringType(), True),
            StructField("payment_terms", StringType(), True),
            StructField("incoterm", StringType(), True),
            StructField("currency", StringType(), True),
            StructField("order_total_amount", FloatType(), True),
            StructField("lines", line_schema, True),
        ])), True),
    ]), True),
])


def _project_sales_orders(source_df):
    return (
        source_df
        .selectExpr(
            "key as bronze_record_key",
            "_ingested_at",
            "_ingest_date",
            "_source_file",
            "inline_outer(value.sales_orders)",
        )
        .select(
            F.trim(F.col("order_id")).alias("order_id"),
            F.to_timestamp("order_date").alias("order_date"),
            F.col("dc_id"),
            F.col("customer_id"),
            F.col("order_status"),
            F.col("payment_terms"),
            F.col("incoterm"),
            F.col("currency"),
            F.col("order_total_amount").cast(FloatType()),
            F.col("lines"),
            F.col("bronze_record_key"),
            F.col("_ingested_at"),
            F.col("_ingest_date"),
            F.col("_source_file"),
        )
        .withWatermark("_ingested_at", "7 days")
        .dropDuplicates(["order_id"])
    )


dp.create_streaming_table(
    "sales_orders_raw",
    schema=sales_orders_schema,
)


@dp.append_flow(target="sales_orders_raw", name="sales_orders_raw_live")
def bronze_sales_orders_raw_live():
    live_df = spark.readStream.table(f"{_c['catalog']}.{_c['bronze_schema']}.datawarehouse_raw")
    return _project_sales_orders(live_df)


@dp.append_flow(target="sales_orders_raw", name="sales_orders_raw_historical")
def bronze_sales_orders_raw_historical():
    bronze_df = (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("multiline", "true")
        .schema(source_schema)
        .load(f"/Volumes/{_c['catalog']}/{_c['bronze_schema']}/salesorders")
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_ingest_date", F.to_date("_ingested_at"))
        .withColumn("_source_file", F.col("_metadata.file_path"))
    )

    return _project_sales_orders(bronze_df)
