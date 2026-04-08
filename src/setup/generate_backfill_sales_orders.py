import json
import random
from datetime import datetime, timedelta
from pathlib import Path


def generate_backfill_orders(count: int = 100_000) -> dict:
    random.seed(20260123)

    start_dt = datetime(2023, 1, 1, 0, 0, 0)
    end_dt = datetime(2025, 1, 22, 23, 59, 59)
    total_seconds = int((end_dt - start_dt).total_seconds())

    orders = []
    statuses = ["CREATED", "CONFIRMED", "PARTIALLY_SHIPPED", "SHIPPED"]
    payment_terms = ["NET30", "NET45", "NET60"]
    incoterms = ["FOB", "CIF", "DAP"]

    for i in range(1, count + 1):
        order_id = f"BF-ORD-{i:06d}"
        line_id = f"BF-LN-{i:06d}-001"
        dc_id = f"DC-{(i % 15) + 1:03d}"
        customer_id = f"CUST-{(i % 5000) + 1:05d}"
        product_id = f"PROD-{(i % 3000) + 1:05d}"

        order_dt = start_dt + timedelta(seconds=random.randint(0, total_seconds))
        order_dt_str = order_dt.strftime("%Y-%m-%d %H:%M:%S")

        ordered_qty = random.randint(1, 50)
        confirmed_qty = max(0, ordered_qty - random.randint(0, 4))
        unit_price = round(random.uniform(8.0, 450.0), 2)
        line_amount = round(confirmed_qty * unit_price, 2)

        orders.append(
            {
                "order_id": order_id,
                "order_date": order_dt_str,
                "dc_id": dc_id,
                "customer_id": customer_id,
                "order_status": random.choice(statuses),
                "payment_terms": random.choice(payment_terms),
                "incoterm": random.choice(incoterms),
                "currency": "USD",
                "order_total_amount": line_amount,
                "lines": [
                    {
                        "line_id": line_id,
                        "product_id": product_id,
                        "ordered_qty_packs": ordered_qty,
                        "confirmed_qty_packs": confirmed_qty,
                        "unit_price": unit_price,
                        "line_amount": line_amount,
                        "backorder_flag": confirmed_qty < ordered_qty,
                    }
                ],
            }
        )

    payload = {
        "key": "backfill_sales_orders_pre_2025_01_23",
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
    return payload


def main() -> None:
    output_path = Path("data/backfill/backfill_sales_orders_pre_2025_01_23.json")
    output_path.parent.mkdir(parents=True, exist_ok=True)

    payload = generate_backfill_orders(count=100_000)
    with output_path.open("w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=True)

    orders = payload["value"]["sales_orders"]
    min_date = min(o["order_date"] for o in orders)
    max_date = max(o["order_date"] for o in orders)
    print(f"Wrote: {output_path}")
    print(f"Orders: {len(orders)}")
    print(f"Min order_date: {min_date}")
    print(f"Max order_date: {max_date}")


if __name__ == "__main__":
    main()
