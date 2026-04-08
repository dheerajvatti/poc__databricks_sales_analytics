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
from pyspark.sql.types import BooleanType, StringType, StructField, StructType, TimestampType
from _env_config import get_config

_c = get_config()


table_schema = StructType([
    StructField("manufacturer_id", StringType(), True),
    StructField("manufacturer_name", StringType(), True),
    StructField("country", StringType(), True),
    StructField("gmp_certified", BooleanType(), True),
    StructField("created_at", TimestampType(), True),
])


def project_current_manufacturers_from_scd2(scd2_df):
    return (
        scd2_df
            .filter(F.col("__END_AT").isNull())
            .select(
                F.col("manufacturer_id"),
                F.col("manufacturer_name"),
                F.col("country"),
                F.col("gmp_certified"),
                F.col("created_at")
            )
    )


@dp.materialized_view(
    name=f"{_c['silver_schema']}.dim_manufacturers_current",
    schema=table_schema
)
def silver_manufacturers_current():
    scd2_df = spark.read.table(f"{_c['catalog']}.{_c['silver_schema']}.dim_manufacturers")
    return project_current_manufacturers_from_scd2(scd2_df)
