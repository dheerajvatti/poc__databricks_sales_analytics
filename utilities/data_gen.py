import json
import os
import random
from datetime import datetime, timedelta

# -----------------------------
# CONFIG
# -----------------------------
num_manufacturers = 3
num_products = 10
num_dcs = 3
num_customers = 8
num_orders = 50         # total orders
max_lines_per_order = 4

preferred_output_dir = "/Volumes/workspace/bronze_dev/landing"  # adjust as needed
fallback_output_dir = os.path.join(os.getcwd(), "bronze_dev", "landing")
output_filename = "pharma_distribution_landing.json"

output_dir = preferred_output_dir if os.access(os.path.dirname(preferred_output_dir), os.W_OK) or os.path.isdir(preferred_output_dir) else fallback_output_dir
output_path = os.path.join(output_dir, output_filename)

random.seed(42)

# -----------------------------
# HELPERS
# -----------------------------
def random_date(start_dt, end_dt):
    delta = end_dt - start_dt
    offset = random.randint(0, delta.days)
    return start_dt + timedelta(days=offset, seconds=random.randint(0, 86400))

start_window = datetime(2025, 1, 1)
end_window = datetime(2026, 3, 31)

therapeutic_areas = ["Cardiology", "Oncology", "Endocrinology", "Neurology", "Anti-Infectives"]
dosage_forms = ["tablet", "capsule", "injectable", "suspension"]
molecules = ["amoxicillin", "atorvastatin", "metformin", "trastuzumab", "adalimumab", "omeprazole"]

regions = ["NORTHEAST", "SOUTHEAST", "MIDWEST"]
customer_types = ["HOSPITAL", "RETAIL_PHARMACY", "WHOLESALER"]
order_statuses = ["CREATED", "ALLOCATED", "SHIPPED", "INVOICED", "CANCELLED"]
ship_statuses = ["IN_TRANSIT", "DELIVERED", "DELAYED"]

# -----------------------------
# MANUFACTURERS
# -----------------------------
manufacturers = []
for i in range(1, num_manufacturers + 1):
    manufacturers.append({
        "manufacturer_id": f"MFG-{i:03d}",
        "manufacturer_name": f"Manufacturer-{i}",
        "country": random.choice(["US", "DE", "IN", "IE"]),
        "gmp_certified": random.choice([True, True, True, False]),
        "created_at": random_date(start_window, end_window).isoformat() + "Z"
    })

# -----------------------------
# PRODUCTS
# -----------------------------
products = []
for i in range(1, num_products + 1):
    manu = random.choice(manufacturers)
    molecule = random.choice(molecules)
    products.append({
        "product_id": f"PRD-{i:04d}",
        "manufacturer_id": manu["manufacturer_id"],
        "product_name": f"{molecule.capitalize()}-Brand-{i}",
        "molecule": molecule,
        "therapeutic_area": random.choice(therapeutic_areas),
        "dosage_form": random.choice(dosage_forms),
        "strength": random.choice(["10 mg", "20 mg", "50 mg", "100 mg", "150 mg", "500 mg"]),
        "pack_size": random.choice([10, 20, 30, 60, 90]),
        "cold_chain_required": random.choice([False, False, True]),
        "list_price": round(random.uniform(15.0, 2000.0), 2)
    })

# -----------------------------
# DISTRIBUTION CENTERS
# -----------------------------
dcs = []
for i in range(1, num_dcs + 1):
    reg = regions[i % len(regions)]
    dcs.append({
        "dc_id": f"DC-{reg[:2]}-{i:02d}",
        "dc_name": f"{reg.title()} Pharma DC {i}",
        "region": reg,
        "address": {
            "street": f"{100 + i} Logistics Park Dr",
            "city": random.choice(["Pittsburgh", "Cleveland", "Atlanta", "Chicago"]),
            "state": random.choice(["PA", "OH", "GA", "IL"]),
            "postal_code": f"{15000 + i}",
            "country": "US"
        },
        "temperature_zones": ["ambient"] if random.random() < 0.3 else ["ambient", "cold_chain"]
    })

# -----------------------------
# CUSTOMERS
# -----------------------------
customers = []
for i in range(1, num_customers + 1):
    reg = random.choice(regions)
    customers.append({
        "customer_id": f"CUST-{1000 + i}",
        "customer_name": f"Customer-{i}",
        "customer_type": random.choice(customer_types),
        "region": reg,
        "credit_limit": round(random.uniform(50000, 300000), 2),
        "is_340b_eligible": random.choice([True, False]),
        "address": {
            "street": f"{200 + i} Healthway Ave",
            "city": random.choice(["Pittsburgh", "Cleveland", "Atlanta", "Chicago"]),
            "state": random.choice(["PA", "OH", "GA", "IL"]),
            "postal_code": f"{14000 + i}",
            "country": "US"
        }
    })

# -----------------------------
# ORDERS + LINES
# -----------------------------
sales_orders = []
shipments = []
returns = []
inventory_snapshots = []

for i in range(1, num_orders + 1):
    order_dt = random_date(start_window, end_window)
    dc = random.choice(dcs)
    cust = random.choice(customers)
    status = random.choice(order_statuses)

    order_id = f"SO-{order_dt.strftime('%Y%m%d')}-{i:04d}"

    num_lines = random.randint(1, max_lines_per_order)
    lines = []
    order_total = 0.0

    for line_idx in range(1, num_lines + 1):
        prod = random.choice(products)
        qty = random.randint(1, 20)
        confirmed_qty = qty if random.random() < 0.9 else max(qty - random.randint(1, qty), 0)
        price = prod["list_price"] * random.uniform(0.85, 1.1)
        price = round(price, 2)
        line_amount = round(confirmed_qty * price, 2)

        lines.append({
            "line_id": str(line_idx),
            "product_id": prod["product_id"],
            "ordered_qty_packs": qty,
            "confirmed_qty_packs": confirmed_qty,
            "unit_price": price,
            "line_amount": line_amount,
            "backorder_flag": confirmed_qty < qty
        })
        order_total += line_amount

        # inventory snapshot (very simple)
        if random.random() < 0.3:
            inventory_snapshots.append({
                "snapshot_id": f"INV-{order_dt.strftime('%Y%m%d')}-{dc['dc_id']}-{prod['product_id']}",
                "snapshot_ts": order_dt.isoformat() + "Z",
                "dc_id": dc["dc_id"],
                "product_id": prod["product_id"],
                "on_hand_packs": random.randint(100, 5000),
                "reserved_packs": random.randint(0, 200),
                "in_transit_packs": random.randint(0, 100)
            })

        # possible return
        if random.random() < 0.05 and confirmed_qty > 0:
            ret_qty = random.randint(1, confirmed_qty)
            returns.append({
                "return_id": f"RET-{order_dt.strftime('%Y%m%d')}-{i:04d}-{line_idx}",
                "original_order_id": order_id,
                "customer_id": cust["customer_id"],
                "product_id": prod["product_id"],
                "return_date": (order_dt + timedelta(days=random.randint(5, 60))).isoformat() + "Z",
                "reason_code": random.choice(["DAMAGED", "EXPIRED", "RECALL"]),
                "returned_qty_packs": ret_qty,
                "credit_amount": round(ret_qty * price, 2)
            })

    order_total = round(order_total, 2)

    sales_orders.append({
        "order_id": order_id,
        "order_date": order_dt.isoformat() + "Z",
        "dc_id": dc["dc_id"],
        "customer_id": cust["customer_id"],
        "order_status": status,
        "payment_terms": random.choice(["NET_15", "NET_30", "NET_45"]),
        "incoterm": random.choice(["FOB", "CIF"]),
        "currency": "USD",
        "order_total_amount": order_total,
        "lines": lines
    })

    # shipment
    if status in ["SHIPPED", "INVOICED", "ALLOCATED"] and random.random() < 0.9:
        ship_dt = order_dt + timedelta(days=random.randint(0, 3))
        deliv_dt = ship_dt + timedelta(days=random.randint(1, 5))
        shipments.append({
            "shipment_id": f"SHP-{ship_dt.strftime('%Y%m%d')}-{i:04d}",
            "order_id": order_id,
            "dc_id": dc["dc_id"],
            "customer_id": cust["customer_id"],
            "ship_date": ship_dt.isoformat() + "Z",
            "delivery_date": deliv_dt.isoformat() + "Z",
            "shipment_status": random.choice(ship_statuses),
            "carrier_name": random.choice(["MedRoute", "PolarLine", "HealthTrans"]),
            "tracking_number": f"TRK-{random.randint(1000000, 9999999)}"
        })

# de-duplicate snapshots (optional)
inventory_snapshots = {x["snapshot_id"]: x for x in inventory_snapshots}.values()

# -----------------------------
# BUILD SINGLE NESTED JSON OBJECT
# -----------------------------
nested_obj = {
    "manufacturers": manufacturers,
    "products": products,
    "distribution_centers": dcs,
    "customers": customers,
    "sales_orders": sales_orders,
    "inventory_snapshots": list(inventory_snapshots),
    "shipments": shipments,
    "returns": returns
}

# -----------------------------
# WRITE AS SINGLE JSON FILE TO VOLUME
# -----------------------------
os.makedirs(output_dir, exist_ok=True)
with open(output_path, "w", encoding="utf-8") as f:
    json.dump(nested_obj, f, indent=2)

print(f"Wrote synthetic nested JSON to {output_path}")