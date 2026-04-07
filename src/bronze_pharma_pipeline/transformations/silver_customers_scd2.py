try:
    from pyspark import pipelines as dp
except ImportError:
    class _DPStub:
        @staticmethod
        def temporary_view(*args, **kwargs):
            def _decorator(func):
                return func
            return _decorator

        @staticmethod
        def create_streaming_table(*args, **kwargs):
            return None

        @staticmethod
        def create_auto_cdc_flow(*args, **kwargs):
            return None

    dp = _DPStub()

from pyspark.sql import functions as F


@dp.temporary_view()
def customers_scd2_source():
    bronze_df = spark.readStream.table("workspace.bronze_dev.datawarehouse_raw")

    return transform_customers_scd2_source_from_bronze_df(bronze_df)


def transform_customers_scd2_source_from_bronze_df(bronze_df):
    """Pure transform for SCD2 source rows, reused by unit tests."""

    return (
        bronze_df
            .selectExpr(
                "key as bronze_record_key",
                "_ingested_at",
                "_ingest_date",
                "_source_file",
                "inline_outer(value.customers)"
            )
            .select(
                F.trim(F.col("customer_id")).alias("customer_id"),
                F.trim(F.col("customer_name")).alias("customer_name"),
                F.col("customer_type"),
                F.col("region"),
                F.col("credit_limit").cast("double").alias("credit_limit"),
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
            .filter(F.col("customer_id").isNotNull())
            .filter(F.col("customer_id") != "")
    )


# Target SCD2 history table. AUTO CDC manages __START_AT and __END_AT columns.
dp.create_streaming_table("silver_dev.dim_customers_hist")


dp.create_auto_cdc_flow(
    target="silver_dev.dim_customers_hist",
    source="customers_scd2_source",
    keys=["customer_id"],
    sequence_by=F.col("_ingested_at"),
    stored_as_scd_type=2,
    except_column_list=["bronze_record_key", "_ingested_at", "_ingest_date", "_source_file"]
)
