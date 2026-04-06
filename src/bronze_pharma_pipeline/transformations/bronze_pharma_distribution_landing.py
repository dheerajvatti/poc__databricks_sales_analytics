from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, BooleanType, DateType, FloatType, IntegerType, StringType, StructField, StructType, TimestampType

payload_schema = StructType([
    StructField("manufacturers", ArrayType(StructType([
        StructField("manufacturer_id", StringType(), True),
        StructField("manufacturer_name", StringType(), True),
        StructField("country", StringType(), True),
        StructField("gmp_certified", BooleanType(), True),
        StructField("created_at", StringType(), True)
    ])), True),
    StructField("products", ArrayType(StructType([
        StructField("product_id", StringType(), True),
        StructField("manufacturer_id", StringType(), True),
        StructField("product_name", StringType(), True),
        StructField("molecule", StringType(), True),
        StructField("therapeutic_area", StringType(), True),
        StructField("dosage_form", StringType(), True),
        StructField("strength", StringType(), True),
        StructField("pack_size", IntegerType(), True),
        StructField("cold_chain_required", BooleanType(), True),
        StructField("list_price", FloatType(), True)
    ])), True),
    StructField("distribution_centers", ArrayType(StructType([
        StructField("dc_id", StringType(), True),
        StructField("dc_name", StringType(), True),
        StructField("region", StringType(), True),
        StructField("address", StructType([
            StructField("street", StringType(), True),
            StructField("city", StringType(), True),
            StructField("state", StringType(), True),
            StructField("postal_code", StringType(), True),
            StructField("country", StringType(), True)
        ]), True),
        StructField("temperature_zones", ArrayType(StringType()), True)
    ])), True),
    StructField("customers", ArrayType(StructType([
        StructField("customer_id", StringType(), True),
        StructField("customer_name", StringType(), True),
        StructField("customer_type", StringType(), True),
        StructField("region", StringType(), True),
        StructField("credit_limit", FloatType(), True),
        StructField("is_340b_eligible", BooleanType(), True),
        StructField("address", StructType([
            StructField("street", StringType(), True),
            StructField("city", StringType(), True),
            StructField("state", StringType(), True),
            StructField("postal_code", StringType(), True),
            StructField("country", StringType(), True)
        ]), True)
    ])), True),
    StructField("sales_orders", ArrayType(StructType([
        StructField("order_id", StringType(), True),
        StructField("order_date", StringType(), True),
        StructField("dc_id", StringType(), True),
        StructField("customer_id", StringType(), True),
        StructField("order_status", StringType(), True),
        StructField("payment_terms", StringType(), True),
        StructField("incoterm", StringType(), True),
        StructField("currency", StringType(), True),
        StructField("order_total_amount", FloatType(), True),
        StructField("lines", ArrayType(StructType([
            StructField("line_id", StringType(), True),
            StructField("product_id", StringType(), True),
            StructField("ordered_qty_packs", IntegerType(), True),
            StructField("confirmed_qty_packs", IntegerType(), True),
            StructField("unit_price", FloatType(), True),
            StructField("line_amount", FloatType(), True),
            StructField("backorder_flag", BooleanType(), True)
        ])), True)
    ])), True),
    StructField("inventory_snapshots", ArrayType(StructType([
        StructField("snapshot_id", StringType(), True),
        StructField("snapshot_ts", StringType(), True),
        StructField("dc_id", StringType(), True),
        StructField("product_id", StringType(), True),
        StructField("on_hand_packs", IntegerType(), True),
        StructField("reserved_packs", IntegerType(), True),
        StructField("in_transit_packs", IntegerType(), True)
    ])), True),
    StructField("shipments", ArrayType(StructType([
        StructField("shipment_id", StringType(), True),
        StructField("order_id", StringType(), True),
        StructField("dc_id", StringType(), True),
        StructField("customer_id", StringType(), True),
        StructField("ship_date", StringType(), True),
        StructField("delivery_date", StringType(), True),
        StructField("shipment_status", StringType(), True),
        StructField("carrier_name", StringType(), True),
        StructField("tracking_number", StringType(), True)
    ])), True),
    StructField("returns", ArrayType(StructType([
        StructField("return_id", StringType(), True),
        StructField("original_order_id", StringType(), True),
        StructField("customer_id", StringType(), True),
        StructField("product_id", StringType(), True),
        StructField("return_date", StringType(), True),
        StructField("reason_code", StringType(), True),
        StructField("returned_qty_packs", IntegerType(), True),
        StructField("credit_amount", FloatType(), True)
    ])), True)
])

source_schema = StructType([
    StructField("key", StringType(), True),
    StructField("value", payload_schema, True)
])

table_schema = StructType([
    StructField("key", StringType(), True),
    StructField("value", payload_schema, True),
    StructField("_ingested_at", TimestampType(), True),
    StructField("_ingest_date", DateType(), True),
    StructField("_source_file", StringType(), True),
    StructField("_rescued_data", StringType(), True)
])


@dp.table(
    name="datawarehouse_raw",
    schema=table_schema
)
def bronze_pharma_distribution_landing():
    return (
        spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "json")
            .option("multiline", "true")
            .option("rescuedDataColumn", "_rescued_data")
            .schema(source_schema)
            .load("/Volumes/workspace/bronze_dev/landing")
            .withColumn("_ingested_at", F.current_timestamp())
            .withColumn("_ingest_date", F.to_date("_ingested_at"))
            .withColumn("_source_file", F.col("_metadata.file_path"))
    )
