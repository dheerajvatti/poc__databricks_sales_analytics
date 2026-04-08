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
from pyspark.sql.types import BooleanType, DoubleType, StringType, StructField, StructType
from _env_config import get_config

_c = get_config()


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
])


def project_current_customers_from_scd2(scd2_df):
    return (
        scd2_df
            .filter(F.col("__END_AT").isNull())
            .select(
                F.col("customer_id"),
                F.col("customer_name"),
                F.col("customer_type"),
                F.col("region"),
                F.col("credit_limit").cast(DoubleType()).alias("credit_limit"),
                F.col("is_340b_eligible"),
                F.col("address_street"),
                F.col("address_city"),
                F.col("address_state"),
                F.col("address_postal_code"),
                F.col("address_country")
            )
    )


@dp.materialized_view(
    name=f"{_c['silver_schema']}.dim_customers_current",
    schema=table_schema
)
def silver_customers_current():
    scd2_df = spark.read.table(f"{_c['catalog']}.{_c['silver_schema']}.dim_customers_hist")
    return project_current_customers_from_scd2(scd2_df)
