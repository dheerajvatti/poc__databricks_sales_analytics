try:
    from pyspark import pipelines as dp
except ImportError:
    class _DPStub:
        @staticmethod
        def materialized_view(*args, **kwargs):
            def _decorator(func):
                return func
            return _decorator

    dp = _DPStub()

from pyspark.sql import functions as F
from pyspark.sql.types import BooleanType, DoubleType, IntegerType, StringType, StructField, StructType


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
])


def project_current_products_from_scd2(scd2_df):
    return (
        scd2_df
            .filter(F.col("__END_AT").isNull())
            .select(
                F.col("product_id"),
                F.col("manufacturer_id"),
                F.col("product_name"),
                F.col("molecule"),
                F.col("therapeutic_area"),
                F.col("dosage_form"),
                F.col("strength"),
                F.col("pack_size").cast(IntegerType()).alias("pack_size"),
                F.col("cold_chain_required"),
                F.col("list_price").cast(DoubleType()).alias("list_price")
            )
    )


@dp.materialized_view(
    name="silver_dev.dim_products_current",
    schema=table_schema
)
def silver_products_current():
    scd2_df = spark.read.table("workspace.silver_dev.dim_products")
    return project_current_products_from_scd2(scd2_df)
