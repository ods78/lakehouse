#!/usr/bin/env python3
"""Phase 1 — Query Latency & Federation Benchmark.

Runs 5 TPC-DS-style analytical queries against the hot tier (ClickHouse) and
the federated/cold tier (Trino over Iceberg on Ceph RGW), same data, same
schema. Each query is executed once as warmup, then 5 measured iterations.
Results are appended to benchmarks/benchmark_results.json.
"""
import json
import os
import statistics
import time
from datetime import datetime, timezone

import clickhouse_connect
from trino.dbapi import connect as trino_connect

BASE = os.path.dirname(os.path.abspath(__file__))

# ---- connections -----------------------------------------------------------
import sys

sys.path.insert(0, BASE)
ENV = {}
with open(os.path.join(BASE, "..", ".env")) as f:
    for line in f:
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            k, v = line.split("=", 1)
            ENV[k.strip()] = v.strip()

ch = clickhouse_connect.get_client(
    host="localhost", port=int(ENV["CLICKHOUSE_HTTP_HOST_PORT"]),
    username=ENV["CLICKHOUSE_USER"], password=ENV["CLICKHOUSE_PASSWORD"],
    database="bench",
)

trino = trino_connect(
    host="localhost", port=int(ENV["TRINO_HOST_PORT"]),
    user="admin", catalog="lakehouse", schema="bench",
    http_scheme="http",
)

# ---- queries (identical semantics on both engines, portable SQL) -----------
QUERIES = {
    "Q1_revenue_by_region_join_customer": """
        SELECT c.c_region, SUM(l.lo_price) AS revenue, COUNT(*) AS orders
        FROM lineorder l JOIN customer c ON l.lo_custkey = c.c_id
        WHERE l.lo_year IN (2022, 2023, 2024)
        GROUP BY c.c_region ORDER BY c.c_region
    """,
    "Q2_category_revenue_join_product": """
        SELECT p.p_category, SUM(l.lo_quantity * l.lo_price) AS gross,
               AVG(l.lo_discount) AS avg_disc
        FROM lineorder l JOIN product p ON l.lo_prodkey = p.p_id
        GROUP BY p.p_category ORDER BY p.p_category
    """,
    "Q3_three_table_join_having": """
        SELECT c.c_region, p.p_category, SUM(l.lo_price) AS revenue
        FROM lineorder l
        JOIN customer c ON l.lo_custkey = c.c_id
        JOIN product  p ON l.lo_prodkey = p.p_id
        WHERE l.lo_quantity BETWEEN 10 AND 40
        GROUP BY c.c_region, p.p_category
        HAVING SUM(l.lo_price) > 1000000
        ORDER BY revenue DESC
    """,
    "Q4_top3_nations_per_region_window": """
        SELECT region, nation, total FROM (
            SELECT c.c_region AS region, c.c_nation AS nation,
                   SUM(l.lo_price) AS total,
                   ROW_NUMBER() OVER (
                       PARTITION BY c.c_region ORDER BY SUM(l.lo_price) DESC) AS rn
            FROM lineorder l JOIN customer c ON l.lo_custkey = c.c_id
            GROUP BY c.c_region, c.c_nation
        ) t WHERE rn <= 3
        ORDER BY region, rn
    """,
    "Q5_segment_distinct_customers": """
        SELECT c.c_mktsegment, COUNT(DISTINCT l.lo_custkey) AS buyers,
               COUNT(*) AS orders, SUM(l.lo_discount * l.lo_price) AS disc_value
        FROM lineorder l JOIN customer c ON l.lo_custkey = c.c_id
        WHERE l.lo_year = 2021 AND l.lo_discount >= 2
        GROUP BY c.c_mktsegment ORDER BY c.c_mktsegment
    """,
}

WARMUP = 1
REPEATS = 5

results = {"phase": 1, "generated_at": datetime.now(timezone.utc).isoformat(),
           "dataset": {"lineorder_rows": 2_000_000, "customer_rows": 100_000,
                       "product_rows": 50_000},
           "method": "1 warmup + 5 measured iterations per engine per query",
           "engines": {"trino": {}, "clickhouse": {}}}


def time_trino(sql):
    cur = trino.cursor()
    t0 = time.perf_counter()
    cur.execute(sql)
    rows = cur.fetchall()
    dt = time.perf_counter() - t0
    return dt, rows


def time_clickhouse(sql):
    t0 = time.perf_counter()
    rows = ch.query(sql).result_rows
    dt = time.perf_counter() - t0
    return dt, rows


for qname, sql in QUERIES.items():
    for engine, runner, unit in (("trino", time_trino, "s"),
                                 ("clickhouse", time_clickhouse, "s")):
        # sanity + warmup
        dt_w, rows = runner(sql)
        times, row_counts = [], []
        for _ in range(REPEATS):
            dt, rows = runner(sql)
            times.append(round(dt, 4))
            row_counts.append(len(rows))
        results["engines"][engine][qname] = {
            "warmup_s": round(dt_w, 4),
            "runs_s": times,
            "median_s": round(statistics.median(times), 4),
            "mean_s": round(statistics.mean(times), 4),
            "stdev_s": round(statistics.stdev(times), 4) if REPEATS > 1 else 0,
            "rows_returned": rows[0] if False else rows and len(rows) or 0,
        }
        print(f"{qname:45s} {engine:10s} median={statistics.median(times):.3f}s")

out_path = os.path.join(BASE, "benchmark_results.json")
existing = {}
if os.path.exists(out_path):
    with open(out_path) as f:
        existing = json.load(f)
existing["phase1_query_latency"] = results
with open(out_path, "w") as f:
    json.dump(existing, f, indent=2)
print(f"\nsaved -> {out_path}")
