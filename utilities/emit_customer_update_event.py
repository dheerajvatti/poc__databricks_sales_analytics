import argparse
import json
import os
from datetime import datetime, timezone


def load_seed_payload(seed_file: str):
    with open(seed_file, "r", encoding="utf-8") as f:
        root = json.load(f)

    return root.get("value", root)


def find_customer(payload: dict, customer_id: str):
    customers = payload.get("customers", [])
    for c in customers:
        if c.get("customer_id") == customer_id:
            return c
    raise ValueError(f"Customer {customer_id} not found in seed file")


def apply_updates(customer: dict, credit_limit_increase_pct: float, new_region: str | None):
    updated = json.loads(json.dumps(customer))

    if isinstance(updated.get("credit_limit"), (int, float)):
        current_limit = float(updated["credit_limit"])
        updated["credit_limit"] = round(current_limit * (1.0 + credit_limit_increase_pct / 100.0), 2)

    if new_region:
        updated["region"] = new_region

    return updated


def write_update_event(output_dir: str, updated_customer: dict):
    now = datetime.now(timezone.utc)
    ts = now.strftime("%Y%m%dT%H%M%S")
    file_name = f"customer_update_event_{updated_customer['customer_id']}_{ts}.json"
    path = os.path.join(output_dir, file_name)

    event_payload = {
        "key": "pharma_distribution_customer_update_event",
        "value": {
            "manufacturers": [],
            "products": [],
            "distribution_centers": [],
            "customers": [updated_customer],
            "sales_orders": [],
            "inventory_snapshots": [],
            "shipments": [],
            "returns": [],
        },
    }

    with open(path, "w", encoding="utf-8") as f:
        json.dump(event_payload, f)

    return path


def main():
    parser = argparse.ArgumentParser(description="Emit a customer update event file for SCD2 testing.")
    parser.add_argument(
        "--seed-file",
        default="bronze_dev/landing/pharma_distribution_landing.json",
        help="Seed JSON file containing baseline customers",
    )
    parser.add_argument(
        "--output-dir",
        default="bronze_dev/landing",
        help="Folder watched by Bronze Auto Loader",
    )
    parser.add_argument("--customer-id", required=True, help="Customer ID to update, e.g. CUST-1003")
    parser.add_argument(
        "--credit-limit-increase-pct",
        type=float,
        default=10.0,
        help="Percent increase to apply to credit_limit",
    )
    parser.add_argument(
        "--new-region",
        default=None,
        help="Optional new region value to force a second changed attribute",
    )

    args = parser.parse_args()

    payload = load_seed_payload(args.seed_file)
    original_customer = find_customer(payload, args.customer_id)
    updated_customer = apply_updates(
        original_customer,
        credit_limit_increase_pct=args.credit_limit_increase_pct,
        new_region=args.new_region,
    )

    os.makedirs(args.output_dir, exist_ok=True)
    path = write_update_event(args.output_dir, updated_customer)

    print(f"Wrote customer update event for {args.customer_id}: {path}")
    print(
        f"credit_limit: {original_customer.get('credit_limit')} -> {updated_customer.get('credit_limit')}, "
        f"region: {original_customer.get('region')} -> {updated_customer.get('region')}"
    )


if __name__ == "__main__":
    main()
