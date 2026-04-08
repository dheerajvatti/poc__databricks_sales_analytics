from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.types import BooleanType, DateType, DoubleType, IntegerType, StringType, StructField, StructType, TimestampType
from _env_config import get_config

_c = get_config()


table_schema = StructType([
    StructField("order_id", StringType(), True),
    StructField("line_id", StringType(), True),
    StructField("date_key", IntegerType(), True),
    StructField("product_id", StringType(), True),
    StructField("ordered_qty_packs", IntegerType(), True),
    StructField("confirmed_qty_packs", IntegerType(), True),
    StructField("unit_price", DoubleType(), True),
    StructField("line_amount", DoubleType(), True),
    StructField("backorder_flag", BooleanType(), True),
    StructField("bronze_record_key", StringType(), True),
    StructField("_ingested_at", TimestampType(), True),
    StructField("_ingest_date", DateType(), True),
    StructField("_source_file", StringType(), True)
])


@dp.table(
    name=f"{_c['silver_schema']}.fct_sales_order_lines",
    schema=table_schema
)
@dp.expect_or_drop("valid_order_id_not_null", "order_id IS NOT NULL")
@dp.expect_or_drop("valid_line_id_not_null", "line_id IS NOT NULL")
def silver_sales_order_lines():
    bronze_df = spark.readStream.table(f"{_c['catalog']}.{_c['bronze_schema']}.sales_orders_raw")

    # Expand lines array — each row is one order line, carrying order_id forward
    exploded_df = (
        bronze_df
            .selectExpr(
                "bronze_record_key",
                "_ingested_at",
                "_ingest_date",
                "_source_file",
                "order_id",
                "order_date",
                "inline_outer(lines)"
            )
            .select(
                F.col("order_id"),
                F.col("line_id"),
                F.date_format(F.to_timestamp("order_date"), "yyyyMMdd").cast(IntegerType()).alias("date_key"),
                F.col("product_id"),
                F.col("ordered_qty_packs").cast(IntegerType()),
                F.col("confirmed_qty_packs").cast(IntegerType()),
                F.col("unit_price").cast(DoubleType()),
                F.col("line_amount").cast(DoubleType()),
                F.col("backorder_flag"),
                F.col("bronze_record_key"),
                F.col("_ingested_at"),
                F.col("_ingest_date"),
                F.col("_source_file")
            )
            .withColumn("order_id", F.trim(F.col("order_id")))
            .withColumn("line_id", F.trim(F.col("line_id")))
    )

    return (
        exploded_df
            .withWatermark("_ingested_at", "7 days")
            .dropDuplicates(["order_id", "line_id"])
    )
