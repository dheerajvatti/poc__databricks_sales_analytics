from pyspark import pipelines as dp
from pyspark.sql import functions as F
from _env_config import get_config

_c = get_config()


# Configurable boundaries for pre-materialized reporting dates.
START_DATE = "2015-01-01"
END_DATE = "2035-12-31"
FISCAL_START_MONTH = 1


@dp.materialized_view(
    name=f"{_c['silver_schema']}.dim_time",
    comment="Conformed date dimension for reporting with calendar and fiscal attributes",
)
def silver_dim_time():
    base_dates = spark.sql(
        f"""
        SELECT explode(sequence(to_date('{START_DATE}'), to_date('{END_DATE}'), interval 1 day)) AS calendar_date
        """
    )

    fiscal_month_num = (
        (F.month(F.col("calendar_date")) - F.lit(FISCAL_START_MONTH) + F.lit(12)) % F.lit(12)
    ) + F.lit(1)
    fiscal_quarter_num = F.floor((fiscal_month_num - F.lit(1)) / F.lit(3)) + F.lit(1)

    fiscal_year_num = (
        F.year(F.col("calendar_date"))
        + F.when(
            (F.lit(FISCAL_START_MONTH) > F.lit(1))
            & (F.month(F.col("calendar_date")) >= F.lit(FISCAL_START_MONTH)),
            F.lit(1),
        ).otherwise(F.lit(0))
    )

    fiscal_year_start_date = F.make_date(
        F.when(
            F.month(F.col("calendar_date")) >= F.lit(FISCAL_START_MONTH),
            F.year(F.col("calendar_date")),
        ).otherwise(F.year(F.col("calendar_date")) - F.lit(1)),
        F.lit(FISCAL_START_MONTH),
        F.lit(1),
    )
    fiscal_year_end_date = F.date_sub(F.add_months(fiscal_year_start_date, 12), 1)

    return (
        base_dates
        .select(
            F.date_format(F.col("calendar_date"), "yyyyMMdd").cast("int").alias("date_key"),
            F.col("calendar_date"),
            F.date_format(F.col("calendar_date"), "EEEE").alias("day_name"),
            F.date_format(F.col("calendar_date"), "E").alias("day_name_short"),
            F.dayofweek(F.col("calendar_date")).alias("day_of_week_num"),
            F.dayofmonth(F.col("calendar_date")).alias("day_of_month_num"),
            F.dayofyear(F.col("calendar_date")).alias("day_of_year_num"),
            F.weekofyear(F.col("calendar_date")).alias("week_of_year_num"),
            F.date_trunc("week", F.col("calendar_date")).cast("date").alias("week_start_date"),
            F.date_add(F.date_trunc("week", F.col("calendar_date")).cast("date"), 6).alias("week_end_date"),
            F.month(F.col("calendar_date")).alias("month_num"),
            F.date_format(F.col("calendar_date"), "MMMM").alias("month_name"),
            F.date_format(F.col("calendar_date"), "MMM").alias("month_name_short"),
            F.concat(F.year(F.col("calendar_date")), F.lit("-"), F.lpad(F.month(F.col("calendar_date")), 2, "0")).alias("month_label"),
            F.quarter(F.col("calendar_date")).alias("quarter_num"),
            F.concat(F.lit("Q"), F.quarter(F.col("calendar_date"))).alias("quarter_name"),
            F.year(F.col("calendar_date")).alias("year_num"),
            F.date_trunc("month", F.col("calendar_date")).cast("date").alias("month_start_date"),
            F.last_day(F.col("calendar_date")).alias("month_end_date"),
            F.date_trunc("quarter", F.col("calendar_date")).cast("date").alias("quarter_start_date"),
            F.date_sub(F.add_months(F.date_trunc("quarter", F.col("calendar_date")).cast("date"), 3), 1).alias("quarter_end_date"),
            F.date_trunc("year", F.col("calendar_date")).cast("date").alias("year_start_date"),
            F.date_sub(F.add_months(F.date_trunc("year", F.col("calendar_date")).cast("date"), 12), 1).alias("year_end_date"),
            F.when(F.dayofweek(F.col("calendar_date")).isin(1, 7), F.lit(True)).otherwise(F.lit(False)).alias("is_weekend"),
            (F.col("calendar_date") == F.date_trunc("month", F.col("calendar_date")).cast("date")).alias("is_month_start"),
            (F.col("calendar_date") == F.last_day(F.col("calendar_date"))).alias("is_month_end"),
            (F.col("calendar_date") == F.date_trunc("quarter", F.col("calendar_date")).cast("date")).alias("is_quarter_start"),
            (F.col("calendar_date") == F.date_sub(F.add_months(F.date_trunc("quarter", F.col("calendar_date")).cast("date"), 3), 1)).alias("is_quarter_end"),
            (F.col("calendar_date") == F.date_trunc("year", F.col("calendar_date")).cast("date")).alias("is_year_start"),
            (F.col("calendar_date") == F.date_sub(F.add_months(F.date_trunc("year", F.col("calendar_date")).cast("date"), 12), 1)).alias("is_year_end"),
            fiscal_month_num.alias("fiscal_month_num"),
            F.date_format(F.add_months(F.col("calendar_date"), 1 - FISCAL_START_MONTH), "MMMM").alias("fiscal_month_name"),
            fiscal_quarter_num.alias("fiscal_quarter_num"),
            F.concat(F.lit("FQ"), fiscal_quarter_num).alias("fiscal_quarter_name"),
            fiscal_year_num.alias("fiscal_year"),
            F.concat(F.lit("FY"), fiscal_year_num.cast("string")).alias("fiscal_year_label"),
            fiscal_year_start_date.alias("fiscal_year_start_date"),
            fiscal_year_end_date.alias("fiscal_year_end_date"),
        )
    )