#!/usr/bin/env python3
"""Phase 3 — Fault tolerance / availability test through HAProxy.

Continuous query loop (6 workers) against the ClickHouse HTTP interface via
the HAProxy edge (Host: clickhouse.lakehouse.local). After a baseline window
the script restarts the lakehouse-clickhouse container (single-node serving
tier) and keeps querying until the backend is healthy again.

Metrics: unavailability window (client view), failed requests, recovery time.
"""
import base64
import json
import os
import statistics
import subprocess
import threading
import time
from collections import defaultdict
from datetime import datetime, timezone

BASE = os.path.dirname(os.path.abspath(__file__))
ENV = {}
with open(os.path.join(BASE, "..", ".env")) as f:
    for line in f:
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            k, v = line.split("=", 1)
            ENV[k.strip()] = v.strip()

PROXY = "http://localhost:10080/"
HOST = "clickhouse.lakehouse.local"
AUTH = "Basic " + base64.b64encode(
    f"{ENV['CLICKHOUSE_USER']}:{ENV['CLICKHOUSE_PASSWORD']}".encode()).decode()
QUERY = "SELECT lo_year, count() FROM bench.lineorder GROUP BY lo_year ORDER BY lo_year FORMAT JSONCompact"

BASELINE_S = 15          # healthy traffic before the failure is injected
SETTLE_S = 20            # consecutive-success seconds needed to declare recovery
REQ_TIMEOUT = 3          # per-request client timeout
MAX_RUNTIME = 240        # hard cap for the whole test

log = []
lock = threading.Lock()
stop = threading.Event()


def worker(wid, t0):
    import http.client
    while not stop.is_set():
        ts = time.perf_counter() - t0
        try:
            # fresh connection per request: a dead backend must not wedge the
            # worker's connection object (realistic client behaviour)
            conn = http.client.HTTPConnection("localhost", 10080, timeout=REQ_TIMEOUT)
            conn.request("POST", "/", body=QUERY, headers={
                "Host": HOST, "Authorization": AUTH,
                "Content-Type": "text/plain; charset=utf-8"})
            resp = conn.getresponse()
            body = resp.read()
            ok = resp.status == 200 and body.count(b"[") >= 4
            entry = {"t": round(ts, 3), "ok": ok, "status": resp.status,
                     "worker": wid, "ms": round((time.perf_counter() - t0 - ts) * 1000, 1)}
            conn.close()
        except Exception as e:
            entry = {"t": round(ts, 3), "ok": False, "status": 0,
                     "error": f"{type(e).__name__}: {e}"[:80], "worker": wid}
            time.sleep(0.25)  # backoff so failures don't spin the loop
        with lock:
            log.append(entry)


def main():
    t0 = time.perf_counter()
    workers = [threading.Thread(target=worker, args=(i, t0), daemon=True) for i in range(6)]
    for w in workers:
        w.start()
    print(f"[{time.strftime('%H:%M:%S')}] baseline traffic for {BASELINE_S}s ...")
    time.sleep(BASELINE_S)

    print(f"[{time.strftime('%H:%M:%S')}] >>> docker restart lakehouse-clickhouse <<<")
    t_kill_epoch = datetime.now(timezone.utc).isoformat()
    subprocess.run(["docker", "restart", "lakehouse-clickhouse"],
                   capture_output=True, timeout=120)

    print(f"[{time.strftime('%H:%M:%S')}] waiting for recovery ...")
    deadline = t0 + MAX_RUNTIME
    consec_ok_start = None
    while time.perf_counter() < deadline:
        time.sleep(0.5)
        with lock:
            recent = [e for e in log if e["t"] > time.perf_counter() - t0 - SETTLE_S]
        if recent and all(e["ok"] for e in recent) and len(recent) >= 10:
            if consec_ok_start is None:
                consec_ok_start = recent[0]["t"]
            if time.perf_counter() - t0 - consec_ok_start >= SETTLE_S:
                break
        else:
            consec_ok_start = None
    stop.set()
    time.sleep(1.5)

    # ---- analysis ---------------------------------------------------------
    fails = [e for e in log if not e["ok"]]
    first_fail = fails[0]["t"] if fails else None
    first_ok_after = None
    if first_fail is not None:
        for e in log:
            if e["t"] > first_fail and e["ok"]:
                first_ok_after = e["t"]
                break
    # recovery point: start of the sustained-success window after last failure
    recovery_at = None
    if fails:
        last_fail = fails[-1]["t"]
        streak = None
        for e in log:
            if e["t"] <= last_fail:
                continue
            if e["ok"]:
                streak = streak or e["t"]
            else:
                streak = None
        recovery_at = streak
    with lock:
        errs = defaultdict(int)
        for e in fails:
            errs[e.get("error", f"http_{e.get('status')}")] += 1

    ok_times = [e["ms"] for e in log if e["ok"]]
    downtime = (recovery_at - first_fail) if (first_fail is not None and recovery_at is not None) else None
    results = {
        "phase": 3,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "test": "single-node serving-tier failure (docker restart lakehouse-clickhouse) under continuous load through HAProxy edge",
        "client_path": "python http client -> HAProxy :10080 (Host: clickhouse.lakehouse.local) -> backend be_clickhouse -> clickhouse:8123",
        "concurrency": 6,
        "query": "SELECT lo_year, count() FROM bench.lineorder (2M rows) GROUP BY lo_year",
        "baseline_s": BASELINE_S,
        "failure_injected_at": t_kill_epoch,
        "unavailability_window_s": round(downtime, 2) if downtime else None,
        "recovery_time_s_from_restart": round(recovery_at - (first_fail if fails else 0), 2) if recovery_at else None,
        "first_failure_at_s": round(first_fail, 2) if first_fail is not None else None,
        "first_success_after_failure_s": round(first_ok_after, 2) if first_ok_after else None,
        "sustained_recovery_at_s": round(recovery_at, 2) if recovery_at else None,
        "failed_requests": len(fails),
        "successful_requests": len(log) - len(fails),
        "total_requests": len(log),
        "failure_breakdown": dict(errs),
        "availability_pct_during_test": round(100 * (len(log) - len(fails)) / len(log), 3),
        "healthy_latency_ms": {
            "median": round(statistics.median(ok_times), 1),
            "p95": round(sorted(ok_times)[int(0.95 * len(ok_times))], 1),
        },
    }
    out = os.path.join(BASE, "benchmark_results.json")
    data = json.load(open(out)) if os.path.exists(out) else {}
    data["phase3_failover"] = results
    json.dump(data, open(out, "w"), indent=2)
    print(json.dumps(results, indent=2))
    print(f"\nsaved -> {out}")


if __name__ == "__main__":
    main()
