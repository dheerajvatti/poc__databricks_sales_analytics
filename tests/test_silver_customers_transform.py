import sys
import unittest
from datetime import datetime, date
from pathlib import Path

try:
    from pyspark.sql import SparkSession
    from pyspark.sql.types import (
        ArrayType,
        BooleanType,
        DateType,
        DoubleType,
        StringType,
        StructField,
        StructType,
        TimestampType,
    )
    HAS_PYSPARK = True
except ModuleNotFoundError:
    HAS_PYSPARK = False


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

if HAS_PYSPARK:
    from src.bronze_pharma_pipeline.transformations.silver_customers_current import project_current_customers_from_scd2
    from src.bronze_pharma_pipeline.transformations.silver_customers_scd2 import transform_customers_scd2_source_from_bronze_df


@unittest.skipUnless(HAS_PYSPARK, "pyspark is required for Spark transformation unit tests")
class TestSilverCustomersTransform(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = (
            SparkSession.builder.master("local[1]")
            .appName("test-silver-customers-transform")
            .getOrCreate()
        )

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def _bronze_schema(self):
        customer_schema = StructType([
            StructField("customer_id", StringType(), True),
            StructField("customer_name", StringType(), True),
            StructField("customer_type", StringType(), True),
            StructField("region", StringType(), True),
            StructField("credit_limit", DoubleType(), True),
            StructField("is_340b_eligible", BooleanType(), True),
            StructField("address", StructType([
                StructField("street", StringType(), True),
                StructField("city", StringType(), True),
                StructField("state", StringType(), True),
                StructField("postal_code", StringType(), True),
                StructField("country", StringType(), True),
            ]), True),
        ])

        value_schema = StructType([
            StructField("customers", ArrayType(customer_schema), True),
        ])

        return StructType([
            StructField("key", StringType(), True),
            StructField("value", value_schema, True),
            StructField("_ingested_at", TimestampType(), True),
            StructField("_ingest_date", DateType(), True),
            StructField("_source_file", StringType(), True),
        ])

    def _scd2_schema(self):
        return StructType([
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
            StructField("__START_AT", TimestampType(), True),
            StructField("__END_AT", TimestampType(), True),
        ])

    def test_customers_flatten_and_trim(self):
        rows = [
            (
                "k1",
                {
                    "customers": [
                        {
                            "customer_id": "  CUST-1001  ",
                            "customer_name": "  Customer One  ",
                            "customer_type": "HOSPITAL",
                            "region": "NORTHEAST",
                            "credit_limit": 12345.67,
                            "is_340b_eligible": True,
                            "address": {
                                "street": "1 Main St",
                                "city": "Boston",
                                "state": "MA",
                                "postal_code": "02101",
                                "country": "US",
                            },
                        }
                    ]
                },
                datetime(2026, 4, 7, 12, 0, 0),
                date(2026, 4, 7),
                "/tmp/file1.json",
            )
        ]

        bronze_df = self.spark.createDataFrame(rows, schema=self._bronze_schema())
        result = transform_customers_scd2_source_from_bronze_df(bronze_df).collect()

        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["customer_id"], "CUST-1001")
        self.assertEqual(result[0]["customer_name"], "Customer One")
        self.assertEqual(result[0]["address_street"], "1 Main St")
        self.assertEqual(result[0]["address_city"], "Boston")
        self.assertAlmostEqual(result[0]["credit_limit"], 12345.67)

    def test_customers_multiple_entries_expand(self):
        rows = [
            (
                "k2",
                {
                    "customers": [
                        {
                            "customer_id": "CUST-1002",
                            "customer_name": "Customer Two",
                            "customer_type": "WHOLESALER",
                            "region": "MIDWEST",
                            "credit_limit": 20000.0,
                            "is_340b_eligible": False,
                            "address": {
                                "street": "2 Main St",
                                "city": "Chicago",
                                "state": "IL",
                                "postal_code": "60601",
                                "country": "US",
                            },
                        },
                        {
                            "customer_id": "CUST-1003",
                            "customer_name": "Customer Three",
                            "customer_type": "RETAIL_PHARMACY",
                            "region": "SOUTHEAST",
                            "credit_limit": 30000.0,
                            "is_340b_eligible": False,
                            "address": {
                                "street": "3 Main St",
                                "city": "Atlanta",
                                "state": "GA",
                                "postal_code": "30301",
                                "country": "US",
                            },
                        },
                    ]
                },
                datetime(2026, 4, 7, 12, 5, 0),
                date(2026, 4, 7),
                "/tmp/file2.json",
            )
        ]

        bronze_df = self.spark.createDataFrame(rows, schema=self._bronze_schema())
        result_df = transform_customers_scd2_source_from_bronze_df(bronze_df)

        self.assertEqual(result_df.count(), 2)
        ids = sorted([r["customer_id"] for r in result_df.collect()])
        self.assertEqual(ids, ["CUST-1002", "CUST-1003"])

    def test_null_customer_id_dropped_in_scd2_source(self):
        rows = [
            (
                "k3",
                {
                    "customers": [
                        {
                            "customer_id": None,
                            "customer_name": "No Id Customer",
                            "customer_type": "HOSPITAL",
                            "region": "NORTHEAST",
                            "credit_limit": 11111.0,
                            "is_340b_eligible": False,
                            "address": {
                                "street": "4 Main St",
                                "city": "Cleveland",
                                "state": "OH",
                                "postal_code": "44101",
                                "country": "US",
                            },
                        }
                    ]
                },
                datetime(2026, 4, 7, 12, 10, 0),
                date(2026, 4, 7),
                "/tmp/file3.json",
            )
        ]

        bronze_df = self.spark.createDataFrame(rows, schema=self._bronze_schema())
        result = transform_customers_scd2_source_from_bronze_df(bronze_df).collect()

        self.assertEqual(len(result), 0)

    def test_project_current_customers_from_scd2(self):
        rows = [
            (
                "CUST-1001",
                "Customer One",
                "HOSPITAL",
                "NORTHEAST",
                12000.0,
                True,
                "1 Main St",
                "Boston",
                "MA",
                "02101",
                "US",
                datetime(2026, 4, 7, 10, 0, 0),
                datetime(2026, 4, 7, 11, 0, 0),
            ),
            (
                "CUST-1001",
                "Customer One",
                "HOSPITAL",
                "MIDWEST",
                15000.0,
                True,
                "2 Main St",
                "Chicago",
                "IL",
                "60601",
                "US",
                datetime(2026, 4, 7, 11, 0, 0),
                None,
            ),
        ]

        scd2_df = self.spark.createDataFrame(rows, schema=self._scd2_schema())
        current_df = project_current_customers_from_scd2(scd2_df)
        result = current_df.collect()

        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["customer_id"], "CUST-1001")
        self.assertEqual(result[0]["region"], "MIDWEST")
        self.assertAlmostEqual(result[0]["credit_limit"], 15000.0)


if __name__ == "__main__":
    unittest.main()
