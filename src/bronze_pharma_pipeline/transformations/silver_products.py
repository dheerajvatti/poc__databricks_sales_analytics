from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.types import BooleanType, DateType, DoubleType, IntegerType, StringType, StructField, StructType, TimestampType


table_schema = StructType([
    StructField("product_id", StringType(), True),
    StructField("manufacturer_id", StringType(), True),
    StructField("product_name", StringType(), True),
    StructField("molecule", StringType(), True),
    StructField("therapeutic_area", StringType(), True),
    StructField("dosage_form", StringType(), True),
    StructField("strength", StringType(), True),
    StructField("pack_size", IntegerType(), True),
    StructField("cold_chain_required", BooleanType(), True),
    StructField("list_price", DoubleType(), True),
    StructField("bronze_record_key", StringType(), True),
    StructField("_ingested_at", TimestampType(), True),
    StructField("_ingest_date", DateType(), True),
    StructField("_source_file", StringType(), True)
])


@dp.table(
    name="silver_dev.products",
    schema=table_schema
)
@dp.expect_or_drop("valid_product_id_not_null", "product_id IS NOT NULL")
@dp.expect_or_drop("valid_product_id_not_empty", "product_id <> ''")
@dp.expect_or_drop("valid_manufacturer_id_not_null", "manufacturer_id IS NOT NULL")
def silver_products():
    bronze_df = spark.readStream.table("workspace.bronze_dev.datawarehouse_raw")

    exploded_df = (
        bronze_df
            .selectExpr(
                "key as bronze_record_key",
                "_ingested_at",
                "_ingest_date",
                "_source_file",
                "inline_outer(value.products)"
            )
            .select(
                F.col("product_id"),
                F.col("manufacturer_id"),
                F.col("product_name"),
                F.col("molecule"),
                F.col("therapeutic_area"),
                F.col("dosage_form"),
                F.col("strength"),
                F.col("pack_size").cast(IntegerType()),
                F.col("cold_chain_required"),
                F.col("list_price").cast(DoubleType()),
                F.col("bronze_record_key"),
                F.col("_ingested_at"),
                F.col("_ingest_date"),
                F.col("_source_file")
            )
            .withColumn("product_id", F.trim(F.col("product_id")))
            .withColumn("manufacturer_id", F.trim(F.col("manufacturer_id")))
            .withColumn("product_name", F.trim(F.col("product_name")))
    )

    return (
        exploded_df
            .withWatermark("_ingested_at", "7 days")
            .dropDuplicates(["product_id"])
    )
