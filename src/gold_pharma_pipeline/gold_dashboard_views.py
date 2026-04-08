from pyspark import pipelines as dp
from pyspark.sql import functions as F
from _env_config import get_config

_c = get_config()
_CAT = _c["catalog"]
_SIL = f"{_CAT}.{_c['silver_schema']}"
_GOLD = _c["gold_schema"]


@dp.materialized_view(
    name=f"{_GOLD}.dashboard_daily_revenue",
    comment="Daily revenue aggregated by calendar date with time attributes"
)
def dashboard_daily_revenue():
    lines = spark.read.table(f"{_SIL}.fct_sales_order_lines")
    orders = spark.read.table(f"{_SIL}.fct_sales_orders")
    time = spark.read.table(f"{_SIL}.dim_time")

    return (
        lines
        .join(orders.select("order_id", "date_key"), on="order_id", how="inner")
        .join(time, on="date_key", how="inner")
        .groupBy(
            "calendar_date", "day_name", "month_num", "month_name",
            "year_num", "fiscal_year", "is_weekend"
        )
        .agg(
            F.round(F.sum("line_amount"), 2).alias("total_revenue"),
            F.countDistinct("order_id").alias("order_count"),
            F.count(F.lit(1)).alias("line_item_count"),
            F.round(F.avg("line_amount"), 2).alias("avg_line_amount"),
        )
    )


@dp.materialized_view(
    name=f"{_GOLD}.dashboard_monthly_revenue",
    comment="Monthly revenue with fiscal calendar support"
)
def dashboard_monthly_revenue():
    lines = spark.read.table(f"{_SIL}.fct_sales_order_lines")
    orders = spark.read.table(f"{_SIL}.fct_sales_orders")
    time = spark.read.table(f"{_SIL}.dim_time")

    return (
        lines
        .join(orders.select("order_id", "date_key"), on="order_id", how="inner")
        .join(time, on="date_key", how="inner")
        .groupBy(
            "year_num", "month_num", "month_label", "month_name",
            "fiscal_year", "fiscal_year_label", "fiscal_month_num", "fiscal_quarter_num"
        )
        .agg(
            F.round(F.sum("line_amount"), 2).alias("total_revenue"),
            F.countDistinct("order_id").alias("order_count"),
            F.round(
                F.sum("line_amount") / F.nullif(F.countDistinct("order_id"), F.lit(0)), 2
            ).alias("avg_order_value"),
            F.sum("ordered_qty_packs").alias("units_ordered"),
            F.sum("confirmed_qty_packs").alias("units_confirmed"),
            F.round(
                F.sum("confirmed_qty_packs") / F.nullif(F.sum("ordered_qty_packs"), F.lit(0)), 4
            ).alias("fulfillment_rate"),
        )
    )


@dp.materialized_view(
    name=f"{_GOLD}.dashboard_fiscal_ytd",
    comment="Fiscal year-to-date revenue accumulation"
)
def dashboard_fiscal_ytd():
    lines = spark.read.table(f"{_SIL}.fct_sales_order_lines")
    orders = spark.read.table(f"{_SIL}.fct_sales_orders")
    time = spark.read.table(f"{_SIL}.dim_time")

    return (
        lines
        .join(orders.select("order_id", "date_key"), on="order_id", how="inner")
        .join(time, on="date_key", how="inner")
        .where("calendar_date >= fiscal_year_start_date AND calendar_date <= current_date()")
        .groupBy(
            "fiscal_year", "fiscal_year_label", "fiscal_month_num", "fiscal_quarter_num"
        )
        .agg(
            F.round(F.sum("line_amount"), 2).alias("ytd_revenue"),
            F.countDistinct("order_id").alias("ytd_order_count"),
            F.round(
                F.sum("line_amount") / F.nullif(F.countDistinct("order_id"), F.lit(0)), 2
            ).alias("ytd_avg_order_value"),
        )
        .withColumn("fiscal_quarter_label", F.concat(F.lit("FQ"), F.col("fiscal_quarter_num")))
    )


@dp.materialized_view(
    name=f"{_GOLD}.dashboard_inventory_monthly",
    comment="Monthly average inventory levels across all DCs and products"
)
def dashboard_inventory_monthly():
    inventory = spark.read.table(f"{_SIL}.fct_inventory_snapshots")
    time = spark.read.table(f"{_SIL}.dim_time")

    return (
        inventory
        .join(time, on="date_key", how="inner")
        .groupBy("year_num", "month_num", "month_label", "month_name", "fiscal_year")
        .agg(
            F.round(F.avg("on_hand_packs"), 0).alias("avg_on_hand"),
            F.round(F.avg("reserved_packs"), 0).alias("avg_reserved"),
            F.round(F.avg("in_transit_packs"), 0).alias("avg_in_transit"),
            F.round(F.avg(F.col("on_hand_packs") - F.col("reserved_packs")), 0).alias("avg_available"),
            F.countDistinct("product_id").alias("unique_products"),
        )
    )


@dp.materialized_view(
    name=f"{_GOLD}.dashboard_returns_monthly",
    comment="Monthly returns analysis by reason code"
)
def dashboard_returns_monthly():
    returns = spark.read.table(f"{_SIL}.fct_returns")
    time = spark.read.table(f"{_SIL}.dim_time")

    return (
        returns
        .join(time, on="date_key", how="inner")
        .groupBy("year_num", "month_num", "month_label", "month_name", "fiscal_year", "reason_code")
        .agg(
            F.count(F.lit(1)).alias("return_count"),
            F.sum("credit_amount").alias("total_credits"),
            F.round(F.avg("credit_amount"), 2).alias("avg_credit"),
        )
    )


@dp.materialized_view(
    name=f"{_GOLD}.dashboard_shipment_trends",
    comment="Monthly shipment volume trends"
)
def dashboard_shipment_trends():
    shipments = spark.read.table(f"{_SIL}.fct_shipments")
    time = spark.read.table(f"{_SIL}.dim_time")

    return (
        shipments
        .join(time, shipments["ship_date_key"] == time["date_key"], how="inner")
        .groupBy("year_num", "month_num", "month_label", "month_name", "fiscal_year")
        .agg(
            F.count(F.lit(1)).alias("shipment_count"),
            F.countDistinct("order_id").alias("order_count"),
            F.countDistinct("customer_id").alias("unique_customers"),
        )
    )


@dp.materialized_view(
    name=f"{_GOLD}.dashboard_latest_kpis",
    comment="Current month KPI summary across revenue, orders, shipments and returns"
)
def dashboard_latest_kpis():
    from pyspark.sql.types import StructType, StructField, StringType, DoubleType

    lines = spark.read.table(f"{_SIL}.fct_sales_order_lines")
    orders = spark.read.table(f"{_SIL}.fct_sales_orders")
    shipments = spark.read.table(f"{_SIL}.fct_shipments")
    returns = spark.read.table(f"{_SIL}.fct_returns")
    time = spark.read.table(f"{_SIL}.dim_time")

    cur_year = F.year(F.current_date())
    cur_month = F.month(F.current_date())

    time_current = time.where(
        (F.col("year_num") == cur_year) & (F.col("month_num") == cur_month)
    ).select("date_key")

    revenue = (
        lines
        .join(orders.select("order_id", "date_key"), on="order_id")
        .join(time_current, on="date_key")
        .agg(F.round(F.sum("line_amount"), 2).alias("v"))
        .withColumn("kpi_name", F.lit("Revenue (Current Month)"))
        .withColumn("kpi_unit", F.lit("USD"))
        .select("kpi_name", F.col("v").cast("double").alias("kpi_value"), "kpi_unit")
    )

    order_cnt = (
        orders
        .join(time_current, on="date_key")
        .agg(F.countDistinct("order_id").cast("double").alias("v"))
        .withColumn("kpi_name", F.lit("Orders (Current Month)"))
        .withColumn("kpi_unit", F.lit("Count"))
        .select("kpi_name", F.col("v").alias("kpi_value"), "kpi_unit")
    )

    avg_order = (
        lines
        .join(orders.select("order_id", "date_key"), on="order_id")
        .join(time_current, on="date_key")
        .agg(
            F.round(
                F.sum("line_amount") / F.nullif(F.countDistinct("order_id"), F.lit(0)), 2
            ).alias("v")
        )
        .withColumn("kpi_name", F.lit("Avg Order Value (Current Month)"))
        .withColumn("kpi_unit", F.lit("USD"))
        .select("kpi_name", F.col("v").cast("double").alias("kpi_value"), "kpi_unit")
    )

    ship_cnt = (
        shipments
        .join(time.where(
            (F.col("year_num") == cur_year) & (F.col("month_num") == cur_month)
        ).select(F.col("date_key").alias("ship_date_key")), on="ship_date_key")
        .agg(F.count(F.lit(1)).cast("double").alias("v"))
        .withColumn("kpi_name", F.lit("Shipments (Current Month)"))
        .withColumn("kpi_unit", F.lit("Count"))
        .select("kpi_name", F.col("v").alias("kpi_value"), "kpi_unit")
    )

    ret_cnt = (
        returns
        .join(time_current, on="date_key")
        .agg(F.count(F.lit(1)).cast("double").alias("v"))
        .withColumn("kpi_name", F.lit("Returns (Current Month)"))
        .withColumn("kpi_unit", F.lit("Count"))
        .select("kpi_name", F.col("v").alias("kpi_value"), "kpi_unit")
    )

    return revenue.union(order_cnt).union(avg_order).union(ship_cnt).union(ret_cnt)
