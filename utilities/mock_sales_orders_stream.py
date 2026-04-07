import argparse
import json
import os
import random
import time
from datetime import datetime, timedelta, timezone


def load_reference_data(seed_file: str):
    with open(seed_file, "r", encoding="utf-8") as f:
        root = json.load(f)

    payload = root.get("value", root)
    dcs = payload.get("distribution_centers", [])
    customers = payload.get("customers", [])
    products = payload.get("products", [])

    if not dcs or not customers or not products:
        raise ValueError(
            "Seed file must contain distribution_centers, customers, and products under value."
        )

    return dcs, customers, products


def make_order(order_seq: int, dcs, customers, products):
    now = datetime.now(timezone.utc)
    dc = random.choice(dcs)
    customer = random.choice(customers)

    order_id = f"SO-RT-{now.strftime('%Y%m%d%H%M%S')}-{order_seq:06d}"
    num_lines = random.randint(1, 4)
    lines = []
    order_total = 0.0

    for idx in range(1, num_lines + 1):
        product = random.choice(products)
        ordered_qty = random.randint(1, 20)
        confirmed_qty = ordered_qty if random.random() < 0.92 else max(0, ordered_qty - random.randint(1, ordered_qty))
        unit_price = round(float(product.get("list_price", 100.0)) * random.uniform(0.9, 1.1), 2)
        line_amount = round(confirmed_qty * unit_price, 2)
        order_total += line_amount

        lines.append(
            {
                "line_id": str(idx),
                "product_id": product["product_id"],
                "ordered_qty_packs": ordered_qty,
                "confirmed_qty_packs": confirmed_qty,
                "unit_price": unit_price,
                "line_amount": line_amount,
                "backorder_flag": confirmed_qty < ordered_qty,
            }
        )

    return {
        "order_id": order_id,
        "order_date": now.isoformat().replace("+00:00", "Z"),
        "dc_id": dc["dc_id"],
        "customer_id": customer["customer_id"],
        "order_status": random.choice(["CREATED", "ALLOCATED", "SHIPPED", "INVOICED"]),
        "payment_terms": random.choice(["NET_15", "NET_30", "NET_45"]),
        "incoterm": random.choice(["FOB", "CIF"]),
        "currency": "USD",
        "order_total_amount": round(order_total, 2),
        "lines": lines,
    }


def write_batch(out_dir: str, batch_id: int, orders):
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
    file_name = f"sales_orders_stream_{ts}_{batch_id:05d}.json"
    out_path = os.path.join(out_dir, file_name)

    payload = {
        "key": "pharma_distribution_sales_orders_stream",
        "value": {
            "manufacturers": [],
            "products": [],
            "distribution_centers": [],
            "customers": [],
            "sales_orders": orders,
            "inventory_snapshots": [],
            "shipments": [],
            "returns": [],
        },
    }

    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(payload, f)

    return out_path


def main():
    parser = argparse.ArgumentParser(description="Mock a real-time sales orders stream into the Bronze landing folder.")
    parser.add_argument(
        "--seed-file",
        default="bronze_dev/landing/pharma_distribution_landing.json",
        help="Seed JSON with reference master data (products/customers/DCs)",
    )
    parser.add_argument(
        "--output-dir",
        default="bronze_dev/landing",
        help="Destination folder watched by Bronze Auto Loader",
    )
    parser.add_argument("--batches", type=int, default=20, help="Number of micro-batches to emit")
    parser.add_argument("--orders-per-batch", type=int, default=2, help="Sales orders per file")
    parser.add_argument("--interval-seconds", type=float, default=2.0, help="Delay between files")
    parser.add_argument("--start-seq", type=int, default=1, help="Starting sequence for unique order ids")

    args = parser.parse_args()

    random.seed(42)
    dcs, customers, products = load_reference_data(args.seed_file)
    os.makedirs(args.output_dir, exist_ok=True)

    order_seq = args.start_seq
    for batch_id in range(1, args.batches + 1):
        orders = []
        for _ in range(args.orders_per_batch):
            orders.append(make_order(order_seq, dcs, customers, products))
            order_seq += 1

        written_file = write_batch(args.output_dir, batch_id, orders)
        print(f"Batch {batch_id}/{args.batches}: wrote {len(orders)} orders -> {written_file}")

        if batch_id < args.batches:
            time.sleep(args.interval_seconds)

    print("Done emitting mock real-time sales order stream.")


if __name__ == "__main__":
    main()
