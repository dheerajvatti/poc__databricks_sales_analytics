from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.types import BooleanType, DateType, DoubleType, StringType, StructField, StructType, TimestampType


table_schema = StructType([
    StructField("customer_id", StringType(), True),
    StructField("customer_name", StringType(), True),
    StructField("customer_type", StringType(), True),
    StructField("region", StringType(), True),
    StructField("credit_limit", DoubleType(), True),
    StructField("is_340b_eligible", BooleanType(), True),
    StructField("address_street", StringType(), True),
    StructField("address_city", StringType(), True),
    StructField("address_state", StringType(), True),
    StructField("address_postal_code", StringType(), True),
    StructField("address_country", StringType(), True),
    StructField("bronze_record_key", StringType(), True),
    StructField("_ingested_at", TimestampType(), True),
    StructField("_ingest_date", DateType(), True),
    StructField("_source_file", StringType(), True)
])


@dp.table(
    name="silver_dev.customers",
    schema=table_schema
)
@dp.expect_or_drop("valid_customer_id_not_null", "customer_id IS NOT NULL")
@dp.expect_or_drop("valid_customer_id_not_empty", "customer_id <> ''")
def silver_customers():
    bronze_df = spark.readStream.table("workspace.bronze_dev.datawarehouse_raw")

    exploded_df = (
        bronze_df
            .selectExpr(
                "key as bronze_record_key",
                "_ingested_at",
                "_ingest_date",
                "_source_file",
                "inline_outer(value.customers)"
            )
            .select(
                F.col("customer_id"),
                F.col("customer_name"),
                F.col("customer_type"),
                F.col("region"),
                F.col("credit_limit").cast(DoubleType()),
                F.col("is_340b_eligible"),
                F.col("address.street").alias("address_street"),
                F.col("address.city").alias("address_city"),
                F.col("address.state").alias("address_state"),
                F.col("address.postal_code").alias("address_postal_code"),
                F.col("address.country").alias("address_country"),
                F.col("bronze_record_key"),
                F.col("_ingested_at"),
                F.col("_ingest_date"),
                F.col("_source_file")
            )
            .withColumn("customer_id", F.trim(F.col("customer_id")))
            .withColumn("customer_name", F.trim(F.col("customer_name")))
    )

    return (
        exploded_df
            .withWatermark("_ingested_at", "7 days")
            .dropDuplicates(["customer_id"])
    )
