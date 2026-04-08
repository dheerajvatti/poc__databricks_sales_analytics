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
from _env_config import get_config

_c = get_config()


@dp.temporary_view()
def distribution_centers_scd2_source():
    bronze_df = spark.readStream.table(f"{_c['catalog']}.{_c['bronze_schema']}.datawarehouse_raw")

    return transform_distribution_centers_scd2_source_from_bronze_df(bronze_df)


def transform_distribution_centers_scd2_source_from_bronze_df(bronze_df):
    """Pure transform for SCD2 source rows, reused by unit tests."""

    return (
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
            .filter(F.col("dc_id").isNotNull())
            .filter(F.col("dc_id") != "")
    )


# Target SCD2 history table. AUTO CDC manages __START_AT and __END_AT columns.
dp.create_streaming_table(f"{_c['silver_schema']}.dim_distribution_centers")


dp.create_auto_cdc_flow(
    target=f"{_c['silver_schema']}.dim_distribution_centers",
    source="distribution_centers_scd2_source",
    keys=["dc_id"],
    sequence_by=F.col("_ingested_at"),
    stored_as_scd_type=2,
    except_column_list=["bronze_record_key", "_ingested_at", "_ingest_date", "_source_file"]
)
