from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.types import BooleanType, DateType, DoubleType, IntegerType, StringType, StructField, StructType, TimestampType


table_schema = StructType([
    StructField("order_id", StringType(), True),
    StructField("line_id", StringType(), True),
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
    name="silver_dev.sales_order_lines",
    schema=table_schema
)
@dp.expect_or_drop("valid_order_id_not_null", "order_id IS NOT NULL")
@dp.expect_or_drop("valid_line_id_not_null", "line_id IS NOT NULL")
def silver_sales_order_lines():
    bronze_df = spark.readStream.table("workspace.bronze_dev.datawarehouse_raw")

    # Step 1: expand sales_orders array — each row is one order with a lines array column
    orders_df = bronze_df.selectExpr(
        "key as bronze_record_key",
        "_ingested_at",
        "_ingest_date",
        "_source_file",
        "inline_outer(value.sales_orders)"
    )

    # Step 2: expand lines array — each row is one order line, carrying order_id forward
    exploded_df = (
        orders_df
            .selectExpr(
                "bronze_record_key",
                "_ingested_at",
                "_ingest_date",
                "_source_file",
                "order_id",
                "inline_outer(lines)"
            )
            .select(
                F.col("order_id"),
                F.col("line_id"),
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
