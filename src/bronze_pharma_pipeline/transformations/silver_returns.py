from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.types import DateType, DoubleType, IntegerType, StringType, StructField, StructType, TimestampType


table_schema = StructType([
    StructField("return_id", StringType(), True),
    StructField("original_order_id", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("return_date", TimestampType(), True),
    StructField("reason_code", StringType(), True),
    StructField("returned_qty_packs", IntegerType(), True),
    StructField("credit_amount", DoubleType(), True),
    StructField("bronze_record_key", StringType(), True),
    StructField("_ingested_at", TimestampType(), True),
    StructField("_ingest_date", DateType(), True),
    StructField("_source_file", StringType(), True)
])


@dp.table(
    name="silver_dev.fct_returns",
    schema=table_schema
)
@dp.expect_or_drop("valid_return_id_not_null", "return_id IS NOT NULL")
@dp.expect_or_drop("valid_return_id_not_empty", "return_id <> ''")
def silver_returns():
    bronze_df = spark.readStream.table("workspace.bronze_dev.datawarehouse_raw")

    exploded_df = (
        bronze_df
            .selectExpr(
                "key as bronze_record_key",
                "_ingested_at",
                "_ingest_date",
                "_source_file",
                "inline_outer(value.returns)"
            )
            .select(
                F.col("return_id"),
                F.col("original_order_id"),
                F.col("customer_id"),
                F.col("product_id"),
                F.to_timestamp("return_date").alias("return_date"),
                F.col("reason_code"),
                F.col("returned_qty_packs").cast(IntegerType()),
                F.col("credit_amount").cast(DoubleType()),
                F.col("bronze_record_key"),
                F.col("_ingested_at"),
                F.col("_ingest_date"),
                F.col("_source_file")
            )
            .withColumn("return_id", F.trim(F.col("return_id")))
    )

    return (
        exploded_df
            .withWatermark("_ingested_at", "7 days")
            .dropDuplicates(["return_id"])
    )
