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
def manufacturers_scd2_source():
    bronze_df = spark.readStream.table("workspace.bronze_dev.datawarehouse_raw")

    return transform_manufacturers_scd2_source_from_bronze_df(bronze_df)


def transform_manufacturers_scd2_source_from_bronze_df(bronze_df):
    """Pure transform for SCD2 source rows, reused by unit tests."""

    return (
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
            .filter(F.col("manufacturer_id").isNotNull())
            .filter(F.col("manufacturer_id") != "")
    )


# Target SCD2 history table. AUTO CDC manages __START_AT and __END_AT columns.
dp.create_streaming_table("silver_dev.dim_manufacturers")


dp.create_auto_cdc_flow(
    target="silver_dev.dim_manufacturers",
    source="manufacturers_scd2_source",
    keys=["manufacturer_id"],
    sequence_by=F.col("_ingested_at"),
    stored_as_scd_type=2,
    except_column_list=["bronze_record_key", "_ingested_at", "_ingest_date", "_source_file"]
)