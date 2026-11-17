#!/usr/bin/env python3
"""Phase 2 — CDC end-to-end ingestion benchmark.

Measures: SQL Server -> Debezium -> Kafka -> ClickHouse (temporary Kafka-engine
consumer). Inserts 1,000,000 rows into dbo.bench_cdc_orders server-side, polls
the ClickHouse destination table (bench.cdc_orders) until 1,000,000 rows land,
and records the full timeline (first-row latency, Kafka lag, completion).
Also collects on-disk / compressed size from system.parts and the source-side
SQL Server size, then appends everything to benchmarks/benchmark_results.json.
"""
import json
import os
import subprocess
import time
from datetime import datetime, timezone

import clickhouse_connect

BASE = os.path.dirname(os.path.abspath(__file__))
ENV = {}
with open(os.path.join(BASE, "..", ".env")) as f:
    for line in f:
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            k, v = line.split("=", 1)
            ENV[k.strip()] = v.strip()

N = 1_000_000
ch = clickhouse_connect.get_client(
    host="localhost", port=int(ENV["CLICKHOUSE_HTTP_HOST_PORT"]),
    username=ENV["CLICKHOUSE_USER"], password=ENV["CLICKHOUSE_PASSWORD"],
    database="bench",
)

start_count = ch.query("SELECT count() FROM bench.cdc_orders").result_rows[0][0]
print(f"initial dest count: {start_count}")
target = start_count + N

SQL_BATCH = r"""
USE lakehouse_src;
SET NOCOUNT ON;
IF OBJECT_ID('tempdb..#n') IS NOT NULL DROP TABLE #n;
CREATE TABLE #n (n INT PRIMARY KEY);
INSERT #n VALUES (1);
WHILE (SELECT MAX(n) FROM #n) < 1000000
    INSERT #n SELECT n + (SELECT MAX(n) FROM #n) FROM #n;
DELETE FROM #n WHERE n > 1000000;
INSERT INTO dbo.bench_cdc_orders (name,email,city,amount,created_at)
SELECT
  'User#' + CAST(n AS varchar(10)),
  'user' + CAST(n AS varchar(10)) + '@example.com',
  CASE n % 10 WHEN 0 THEN N'Berlin' WHEN 1 THEN N'Singapore' WHEN 2 THEN N'Paris'
              WHEN 3 THEN N'Milano' WHEN 4 THEN N'Tokyo' WHEN 5 THEN N'Oslo'
              WHEN 6 THEN N'Madrid' WHEN 7 THEN N'Toronto' WHEN 8 THEN N'Dubai'
              ELSE N'Sao Paulo' END,
  CAST(n % 999900 + 100 AS decimal(12,2)),
  DATEADD(SECOND, n % 31536000, CAST('2026-01-01' AS datetime2))
FROM #n;
DROP TABLE #n;
"""

t0 = time.perf_counter()
print(f"[{time.strftime('%H:%M:%S')}] submitting 1M-row INSERT to SQL Server ...")
ins = subprocess.run(
    ["docker", "exec", "lakehouse-mssql", "/opt/mssql-tools18/bin/sqlcmd",
     "-C", "-S", "localhost", "-U", "sa", "-P", ENV["MSSQL_SA_PASSWORD"],
     "-Q", SQL_BATCH],
    capture_output=True, text=True, timeout=1800,
)
t_insert_done = time.perf_counter()
insert_wall = t_insert_done - t0
if ins.returncode != 0:
    print("INSERT FAILED:", ins.stdout[-500:], ins.stderr[-500:])
    raise SystemExit(1)
rows_done = (ins.stdout.count("rows affected") and True) or True
print(f"[{time.strftime('%H:%M:%S')}] SQL Server insert finished in {insert_wall:.1f}s")
print(ins.stdout.strip().splitlines()[-2:])

def kafka_end_offset():
    r = subprocess.run(
        ["docker", "exec", "lakehouse-kafka", "/opt/kafka/bin/kafka-get-offsets.sh",
         "--bootstrap-server", "localhost:9092",
         "--topic", "benchcdc.lakehouse_src.dbo.bench_cdc_orders", "--time", "-1"],
        capture_output=True, text=True, timeout=30)
    for line in r.stdout.splitlines():
        parts = line.split(":")
        if len(parts) == 3 and parts[0].endswith("bench_cdc_orders"):
            return int(parts[2])
    return None

timeline = []          # (elapsed_s, ch_count)
kafka_timeline = []    # (elapsed_s, kafka_end_offset)
first_row_at = None
kafka_done_at = None
t_done = None
deadline = time.time() + 45 * 60
poll = time.perf_counter()
while time.time() < deadline:
    el = time.perf_counter() - t0
    c = ch.query("SELECT count() FROM bench.cdc_orders").result_rows[0][0]
    timeline.append((round(el, 2), c))
    if c > start_count and first_row_at is None:
        first_row_at = el
        print(f"[{time.strftime('%H:%M:%S')}] first row in ClickHouse at {el:.1f}s")
    if c >= target and t_done is None:
        t_done = el
        print(f"[{time.strftime('%H:%M:%S')}] REACHED {c} rows at {el:.1f}s")
        break
    if el - (kafka_timeline[-1][0] if kafka_timeline else 0) >= 10 or not kafka_timeline:
        off = kafka_end_offset()
        if off is not None:
            kafka_timeline.append((round(el, 2), off))
            if off >= N and kafka_done_at is None:
                kafka_done_at = el
                print(f"[{time.strftime('%H:%M:%S')}] Kafka topic fully populated (offset {off}) at {el:.1f}s")
    poll = time.perf_counter()
    time.sleep(1)

e2e = t_done if t_done is not None else None
print(f"\nE2E latency (insert start -> 1M rows in ClickHouse): {e2e:.1f}s" if e2e else "TIMEOUT")
print(f"SQL Server insert wall time: {insert_wall:.1f}s")
print(f"first-row latency: {first_row_at:.1f}s" if first_row_at else "no rows observed")

# final kafka offset
final_off = kafka_end_offset()

# compression / size metrics (ClickHouse)
q = ("SELECT sum(bytes_on_disk), sum(data_compressed_bytes), sum(data_uncompressed_bytes), "
     "sum(rows), count() FROM system.parts WHERE active AND database='bench' AND table='cdc_orders'")
b_disk, b_comp, b_uncomp, rws, nparts = ch.query(q).result_rows[0]
# source-side SQL Server size (reserved pages * 8KB)
mssql = subprocess.run(
    ["docker", "exec", "lakehouse-mssql", "/opt/mssql-tools18/bin/sqlcmd",
     "-C", "-S", "localhost", "-U", "sa", "-P", ENV["MSSQL_SA_PASSWORD"], "-h", "-1",
     "-Q", "USE lakehouse_src; SELECT SUM(reserved_page_count)*8/1024.0 FROM sys.dm_db_partition_stats WHERE object_id = OBJECT_ID('dbo.bench_cdc_orders')"],
    capture_output=True, text=True, timeout=60)
mssql_mb = None
for line in mssql.stdout.splitlines():
    try:
        mssql_mb = round(float(line.strip()), 1)
    except ValueError:
        pass

# CH consumer throughput from timeline: linear fit over the last 80% of samples
import statistics
counts = [(e, c) for e, c in timeline if c > start_count]
if len(counts) >= 3:
    (e1, c1), (e2, c2) = counts[0], counts[-1]
    ch_rate = (c2 - c1) / (e2 - e1) if e2 > e1 else 0
else:
    ch_rate = 0

results = {
    "phase": 2,
    "generated_at": datetime.now(timezone.utc).isoformat(),
    "pipeline": "SQL Server (dbo.bench_cdc_orders) -> Debezium 2.7.3 (mssql-cdc-bench, JSON) -> Kafka (benchcdc.lakehouse_src.dbo.bench_cdc_orders, 1 partition) -> ClickHouse Kafka-engine + materialized view -> bench.cdc_orders (MergeTree)",
    "rows_inserted": N,
    "mssql_insert_wall_s": round(insert_wall, 2),
    "e2e_latency_s": round(e2e, 2) if e2e else None,
    "first_row_in_ch_s": round(first_row_at, 2) if first_row_at else None,
    "kafka_all_events_at_s": round(kafka_done_at, 2) if kafka_done_at else None,
    "ch_consumer_effective_rows_per_s": round(ch_rate, 0),
    "final_kafka_end_offset": final_off,
    "final_ch_count": rws,
    "kafka_timeline": kafka_timeline,
    "ch_timeline": timeline,
    "clickhouse_size": {
        "rows": int(rws),
        "bytes_on_disk": int(b_disk),
        "on_disk_mb": round(int(b_disk) / 1e6, 2),
        "compressed_bytes": int(b_comp),
        "uncompressed_bytes": int(b_uncomp),
        "uncompressed_mb": round(int(b_uncomp) / 1e6, 2),
        "compression_ratio": round(b_uncomp / b_comp, 2) if b_comp else None,
        "parts": int(nparts),
    },
    "mssql_size_mb_reserved": mssql_mb,
    "settings_note": "Debezium defaults (max.batch.size=2000, poll 500ms); MSSQL CDC capture job default 5s polling; ClickHouse Kafka engine 1 consumer, stream_flush_interval_ms=1000",
}
out = os.path.join(BASE, "benchmark_results.json")
data = json.load(open(out)) if os.path.exists(out) else {}
data["phase2_cdc_ingestion"] = results
json.dump(data, open(out, "w"), indent=2)
print(f"\nsaved -> {out}")
print(json.dumps({k: v for k, v in results.items() if k not in ("ch_timeline", "kafka_timeline")}, indent=2))
