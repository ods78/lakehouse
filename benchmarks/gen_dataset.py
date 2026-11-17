#!/usr/bin/env python3
"""Generate a deterministic TPC-DS-like dataset (customer, product, lineorder)
as CSV for the Phase-1 latency benchmark. Same rows are loaded into ClickHouse
and Iceberg so both engines run identical queries over identical data."""
import csv
import random
import sys
from datetime import date, timedelta

SEED = 42
N_CUSTOMER = 100_000
N_PRODUCT = 50_000
N_LINEORDER = 2_000_000
OUT = "/home/darabi/Dockers/LakeHouse/benchmarks/data"

rng = random.Random(SEED)

REGIONS = ["AFRICA", "AMERICA", "ASIA", "EUROPE", "MIDDLEEAST"]
NATIONS = ["ALGERIA", "ARGENTINA", "BRAZIL", "CANADA", "EGYPT", "ETHIOPIA",
           "FRANCE", "GERMANY", "INDIA", "INDONESIA", "IRAN", "IRAQ",
           "JAPAN", "JORDAN", "KENYA", "MEXICO", "MOROCCO", "MOZAMBIQUE",
           "PERU", "CHINA", "RUSSIA", "SAUDI ARABIA", "VIETNAM", "UNITED KINGDOM", "UNITED STATES"]
SEGMENTS = ["AUTOMOBILE", "BUILDING", "FURNITURE", "HOUSEHOLD", "MACHINERY"]
CATEGORIES = ["electr", "mech", "pharm", "furni", "apparel"]
BRANDS = [f"Brand#{i:02d}" for i in range(1, 26)]
YEARS = [2021, 2022, 2023, 2024]
BASE_DATE = date(2021, 1, 1)

with open(f"{OUT}/customer.csv", "w", newline="") as f:
    w = csv.writer(f)
    w.writerow(["c_id", "c_name", "c_region", "c_nation", "c_mktsegment"])
    for i in range(1, N_CUSTOMER + 1):
        w.writerow([i, f"Customer#{i:07d}", rng.choice(REGIONS),
                    rng.choice(NATIONS), rng.choice(SEGMENTS)])

with open(f"{OUT}/product.csv", "w", newline="") as f:
    w = csv.writer(f)
    w.writerow(["p_id", "p_name", "p_brand", "p_category", "p_size"])
    for i in range(1, N_PRODUCT + 1):
        w.writerow([i, f"Product#{i:07d}", rng.choice(BRANDS),
                    rng.choice(CATEGORIES), rng.randint(1, 50)])

with open(f"{OUT}/lineorder.csv", "w", newline="") as f:
    w = csv.writer(f)
    w.writerow(["lo_id", "lo_custkey", "lo_prodkey", "lo_quantity",
                "lo_discount", "lo_price", "lo_datekey", "lo_year"])
    for i in range(1, N_LINEORDER + 1):
        year = rng.choice(YEARS)
        doy = rng.randint(1, 365)
        d = BASE_DATE + timedelta(days=(year - 2021) * 365 + doy)
        w.writerow([i, rng.randint(1, N_CUSTOMER), rng.randint(1, N_PRODUCT),
                    rng.randint(1, 50), rng.choice([0, 1, 2, 3, 4]),
                    rng.randint(1000, 100_000), d.isoformat(), year])

print(f"generated: {N_CUSTOMER} customers, {N_PRODUCT} products, {N_LINEORDER} lineorders")
