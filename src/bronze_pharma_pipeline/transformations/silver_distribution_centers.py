from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, DateType, StringType, StructField, StructType, TimestampType


table_schema = StructType([
    StructField("dc_id", StringType(), True),
    StructField("dc_name", StringType(), True),
    StructField("region", StringType(), True),
    StructField("address_street", StringType(), True),
    StructField("address_city", StringType(), True),
    StructField("address_state", StringType(), True),
    StructField("address_postal_code", StringType(), True),
    StructField("address_country", StringType(), True),
    StructField("temperature_zones", ArrayType(StringType()), True),
    StructField("bronze_record_key", StringType(), True),
    StructField("_ingested_at", TimestampType(), True),
    StructField("_ingest_date", DateType(), True),
    StructField("_source_file", StringType(), True)
])


@dp.table(
    name="silver_dev.distribution_centers",
    schema=table_schema
)
@dp.expect_or_drop("valid_dc_id_not_null", "dc_id IS NOT NULL")
@dp.expect_or_drop("valid_dc_id_not_empty", "dc_id <> ''")
def silver_distribution_centers():
    bronze_df = spark.readStream.table("workspace.bronze_dev.datawarehouse_raw")

    exploded_df = (
        bronze_df
            .selectExpr(
                "key as bronze_record_key",
                "_ingested_at",
                "_ingest_date",
                "_source_file",
                "inline_outer(value.distribution_centers)"
            )
            .select(
                F.col("dc_id"),
                F.col("dc_name"),
                F.col("region"),
                F.col("address.street").alias("address_street"),
                F.col("address.city").alias("address_city"),
                F.col("address.state").alias("address_state"),
                F.col("address.postal_code").alias("address_postal_code"),
                F.col("address.country").alias("address_country"),
                F.col("temperature_zones"),
                F.col("bronze_record_key"),
                F.col("_ingested_at"),
                F.col("_ingest_date"),
                F.col("_source_file")
            )
            .withColumn("dc_id", F.trim(F.col("dc_id")))
            .withColumn("dc_name", F.trim(F.col("dc_name")))
    )

    return (
        exploded_df
            .withWatermark("_ingested_at", "7 days")
            .dropDuplicates(["dc_id"])
    )
