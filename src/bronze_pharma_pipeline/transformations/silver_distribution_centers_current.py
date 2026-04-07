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
from pyspark.sql.types import ArrayType, StringType, StructField, StructType


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
])


def project_current_distribution_centers_from_scd2(scd2_df):
    return (
        scd2_df
            .filter(F.col("__END_AT").isNull())
            .select(
                F.col("dc_id"),
                F.col("dc_name"),
                F.col("region"),
                F.col("address_street"),
                F.col("address_city"),
                F.col("address_state"),
                F.col("address_postal_code"),
                F.col("address_country"),
                F.col("temperature_zones")
            )
    )


@dp.materialized_view(
    name="silver_dev.dim_distribution_centers_current",
    schema=table_schema
)
def silver_distribution_centers_current():
    scd2_df = spark.read.table("workspace.silver_dev.dim_distribution_centers")
    return project_current_distribution_centers_from_scd2(scd2_df)
