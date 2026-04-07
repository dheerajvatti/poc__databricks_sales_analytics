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
def products_scd2_source():
    bronze_df = spark.readStream.table("workspace.bronze_dev.datawarehouse_raw")

    return transform_products_scd2_source_from_bronze_df(bronze_df)


def transform_products_scd2_source_from_bronze_df(bronze_df):
    """Pure transform for SCD2 source rows, reused by unit tests."""

    return (
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
                F.col("pack_size").cast("int"),
                F.col("cold_chain_required"),
                F.col("list_price").cast("double"),
                F.col("bronze_record_key"),
                F.col("_ingested_at"),
                F.col("_ingest_date"),
                F.col("_source_file")
            )
            .withColumn("product_id", F.trim(F.col("product_id")))
            .withColumn("manufacturer_id", F.trim(F.col("manufacturer_id")))
            .withColumn("product_name", F.trim(F.col("product_name")))
            .filter(F.col("product_id").isNotNull())
            .filter(F.col("product_id") != "")
            .filter(F.col("manufacturer_id").isNotNull())
            .filter(F.col("manufacturer_id") != "")
    )


# Target SCD2 history table. AUTO CDC manages __START_AT and __END_AT columns.
dp.create_streaming_table("silver_dev.dim_products")


dp.create_auto_cdc_flow(
    target="silver_dev.dim_products",
    source="products_scd2_source",
    keys=["product_id"],
    sequence_by=F.col("_ingested_at"),
    stored_as_scd_type=2,
    except_column_list=["bronze_record_key", "_ingested_at", "_ingest_date", "_source_file"]
)
