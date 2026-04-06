from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.types import BooleanType, DateType, StringType, StructField, StructType, TimestampType


table_schema = StructType([
    StructField("manufacturer_id", StringType(), True),
    StructField("manufacturer_name", StringType(), True),
    StructField("country", StringType(), True),
    StructField("gmp_certified", BooleanType(), True),
    StructField("created_at", TimestampType(), True),
    StructField("bronze_record_key", StringType(), True),
    StructField("_ingested_at", TimestampType(), True),
    StructField("_ingest_date", DateType(), True),
    StructField("_source_file", StringType(), True)
])


@dp.table(
    name="silver_dev.manufacturers",
    schema=table_schema
)
@dp.expect_or_drop("valid_manufacturer_id_not_null", "manufacturer_id IS NOT NULL")
@dp.expect_or_drop("valid_manufacturer_id_not_empty", "manufacturer_id <> ''")
def silver_manufacturers():
    bronze_df = spark.readStream.table("workspace.bronze_dev.datawarehouse_raw")

    exploded_df = (
        bronze_df
            .selectExpr(
                "key as bronze_record_key",
                "_ingested_at",
                "_ingest_date",
                "_source_file",
                "inline_outer(value.manufacturers)"
            )
            .select(
                F.col("manufacturer_id"),
                F.col("manufacturer_name"),
                F.col("country"),
                F.col("gmp_certified"),
                F.to_timestamp("created_at").alias("created_at"),
                F.col("bronze_record_key"),
                F.col("_ingested_at"),
                F.col("_ingest_date"),
                F.col("_source_file")
            )
            .withColumn("manufacturer_id", F.trim(F.col("manufacturer_id")))
            .withColumn("manufacturer_name", F.trim(F.col("manufacturer_name")))
    )

    return (
        exploded_df
            .withWatermark("_ingested_at", "7 days")
            .dropDuplicates(["manufacturer_id"])
    )