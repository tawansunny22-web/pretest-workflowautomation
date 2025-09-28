
import argparse
from datetime import datetime, timedelta
import numpy as np
import pandas as pd
import os

def make_sku(brand, rng, prefixes0, SKU_TYPE0):
    weights = rng.random(len(SKU_TYPE0))
    weights = weights / weights.sum()
    kind = rng.choice(SKU_TYPE0, p=weights)
    num = int(rng.integers(1, 60))
    return f"{prefixes0[brand]}-{kind[:4]}-{num:02d}"

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rows", type=int, default=10_000_000)
    ap.add_argument("--out", type=str, default="data/large_order_data_2024_10M.csv")
    ap.add_argument("--start", type=str, default="2024-01-01")
    ap.add_argument("--months", type=int, default=12)
    ap.add_argument("--customers", type=int, default=10_000)
    ap.add_argument("--gzip", action="store_true")
    args = ap.parse_args()

    base_dir = os.path.dirname(os.path.abspath(__file__))  # path ของสคริปต์นี้
    file_path = os.path.join(base_dir, "data", "large_order_data_2024.csv")
    df = pd.read_csv(file_path)

    BRANDS = list(set(df['brand']))
    BRANCHES = list(set(df['branch']))
    prefixes = {brand: brand[:2].upper() for brand in BRANDS}
    SKU_TYPE = list(set(df['sku'].str.split('-').str[1]))

    rng = np.random.default_rng(123)
    n = args.rows
    start = datetime.fromisoformat(args.start)

    month_offsets = rng.integers(0, args.months, size=n)
    days_in_month = 28
    day_offsets = rng.integers(0, days_in_month, size=n)
    base_ts = [start.replace(day=1) + timedelta(days=int(m*days_in_month + d)) for m,d in zip(month_offsets, day_offsets)]
    seconds = rng.integers(0, 24*3600, size=n)
    timestamps = [ts + timedelta(seconds=int(s)) for ts,s in zip(base_ts, seconds)]

    group_sizes = rng.choice([1,2,3,4,5,6], size=n, p=[0.52,0.16,0.12,0.10,0.06,0.04])
    cum = np.cumsum(group_sizes)
    idx = np.searchsorted(cum, np.arange(n))
    order_ids = idx + 100000
    order_no = np.array([f"ORD{i}" for i in order_ids])

    customers = rng.integers(1, args.customers+1, size=n)
    customer_no = np.array([f"CUST{c:04d}" for c in customers])
    branch = rng.choice(BRANCHES, size=n)
    brand = rng.choice(BRANDS, size=n, p=[0.26,0.14,0.24,0.22,0.14])
    sku = [make_sku(b, rng, prefixes, SKU_TYPE) for b in brand]
    quantity = rng.integers(1, 6, size=n)
    amount = np.round(rng.lognormal(mean=3.2, sigma=0.9, size=n) * quantity, 2)

    df = pd.DataFrame({
        "order_no": order_no,
        "amount": amount.astype(float),
        "customer_no": customer_no,
        "branch": branch,
        "brand": brand,
        "sku": sku,
        "quantity": quantity.astype(int),
        "transaction_datetime": [ts.strftime("%Y-%m-%d %H:%M:%S") for ts in timestamps],
    })

    out = args.out
    # if args.gzip and not out.endswith(".gz"):
    #     out = out + ".gz"
    df.to_csv(out, index=False, compression="infer")

if __name__ == "__main__":
    main()
