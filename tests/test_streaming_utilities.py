import json
import os
import sys
import tempfile
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from utilities.emit_customer_update_event import apply_updates, find_customer, write_update_event
from utilities.mock_sales_orders_stream import load_reference_data, make_order, write_batch


class TestStreamingUtilities(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.work_dir = Path(self.temp_dir.name)
        self.seed_file = self.work_dir / "seed.json"

        self.seed_payload = {
            "key": "seed",
            "value": {
                "distribution_centers": [{"dc_id": "DC-01"}],
                "customers": [
                    {
                        "customer_id": "CUST-1003",
                        "customer_name": "Customer 3",
                        "region": "NORTHEAST",
                        "credit_limit": 100000.0,
                    }
                ],
                "products": [{"product_id": "PRD-0001", "list_price": 50.0}],
            },
        }

        with open(self.seed_file, "w", encoding="utf-8") as f:
            json.dump(self.seed_payload, f)

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_load_reference_data(self):
        dcs, customers, products = load_reference_data(str(self.seed_file))
        self.assertEqual(len(dcs), 1)
        self.assertEqual(len(customers), 1)
        self.assertEqual(len(products), 1)
        self.assertEqual(customers[0]["customer_id"], "CUST-1003")

    def test_make_order(self):
        dcs, customers, products = load_reference_data(str(self.seed_file))
        order = make_order(7, dcs, customers, products)

        self.assertTrue(order["order_id"].startswith("SO-RT-"))
        self.assertTrue(order["order_id"].endswith("-000007"))
        self.assertEqual(order["customer_id"], "CUST-1003")
        self.assertGreaterEqual(len(order["lines"]), 1)

        line_sum = round(sum(line["line_amount"] for line in order["lines"]), 2)
        self.assertEqual(order["order_total_amount"], line_sum)

    def test_write_batch(self):
        sample_orders = [{"order_id": "SO-RT-TEST-000001"}]
        out_path = write_batch(str(self.work_dir), 1, sample_orders)

        self.assertTrue(os.path.exists(out_path))
        self.assertIn("sales_orders_stream_", os.path.basename(out_path))
        self.assertTrue(out_path.endswith("_00001.json"))

        with open(out_path, "r", encoding="utf-8") as f:
            payload = json.load(f)

        self.assertEqual(payload["key"], "pharma_distribution_sales_orders_stream")
        self.assertEqual(len(payload["value"]["sales_orders"]), 1)
        self.assertEqual(payload["value"]["sales_orders"][0]["order_id"], "SO-RT-TEST-000001")

    def test_find_customer_and_apply_updates(self):
        customer = find_customer(self.seed_payload["value"], "CUST-1003")
        updated = apply_updates(customer, credit_limit_increase_pct=15.0, new_region="MIDWEST")

        self.assertEqual(customer["credit_limit"], 100000.0)
        self.assertEqual(customer["region"], "NORTHEAST")

        self.assertEqual(updated["credit_limit"], 115000.0)
        self.assertEqual(updated["region"], "MIDWEST")

    def test_write_update_event(self):
        customer = {
            "customer_id": "CUST-1003",
            "customer_name": "Customer 3",
            "region": "MIDWEST",
            "credit_limit": 115000.0,
        }

        out_path = write_update_event(str(self.work_dir), customer)
        self.assertTrue(os.path.exists(out_path))
        self.assertIn("customer_update_event_CUST-1003_", os.path.basename(out_path))

        with open(out_path, "r", encoding="utf-8") as f:
            payload = json.load(f)

        self.assertEqual(payload["key"], "pharma_distribution_customer_update_event")
        self.assertEqual(len(payload["value"]["customers"]), 1)
        self.assertEqual(payload["value"]["customers"][0]["customer_id"], "CUST-1003")


if __name__ == "__main__":
    unittest.main()
