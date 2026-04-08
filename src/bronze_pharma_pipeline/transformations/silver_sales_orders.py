from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.types import DateType, DoubleType, IntegerType, StringType, StructField, StructType, TimestampType
from _env_config import get_config

_c = get_config()


table_schema = StructType([
    StructField("order_id", StringType(), True),
    StructField("order_date", TimestampType(), True),
    StructField("date_key", IntegerType(), True),
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
    name=f"{_c['silver_schema']}.fct_sales_orders",
    schema=table_schema
)
@dp.expect_or_drop("valid_order_id_not_null", "order_id IS NOT NULL")
@dp.expect_or_drop("valid_order_id_not_empty", "order_id <> ''")
def silver_sales_orders():
    bronze_df = spark.readStream.table(f"{_c['catalog']}.{_c['bronze_schema']}.sales_orders_raw")

    exploded_df = (
        bronze_df
            .select(
                F.col("order_id"),
                F.col("order_date"),
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
            .withColumn("date_key", F.date_format(F.col("order_date"), "yyyyMMdd").cast(IntegerType()))
            .withColumn("order_id", F.trim(F.col("order_id")))
    )

    return (
        exploded_df
            .withWatermark("_ingested_at", "7 days")
            .dropDuplicates(["order_id"])
    )
