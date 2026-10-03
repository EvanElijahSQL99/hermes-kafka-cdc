"""Unit tests for the stream-processing logic (no Kafka needed)."""
from hermes.core import Processor, Rule, parse_debezium, pct

T0 = 1_700_000_000_000  # ms


def order_event(op="c", status="placed", before_status=None, total=10.0, source="workload", trace_id=None,
                commit=T0, capture=T0 + 40, oid=1):
    after = {"id": oid, "status": status, "total": total, "source": source, "trace_id": trace_id}
    return {"op": op, "ts_ms": capture,
            "before": {"id": oid, "status": before_status} if before_status else None,
            "after": None if op == "d" else after,
            "source": {"table": "orders", "ts_ms": commit, "lsn": 123, "txId": 9}}


def telemetry(blocked_s=0, oldest_s=0.1, app="shop-workload", slot_lag=0, xid_age=1000):
    return {"ts": T0, "sessions": {"active": 1}, "xid_age": xid_age,
            "blocked": [{"pid": 7, "blocked_by": [5], "app": "demo-restock-job", "seconds": blocked_s}] if blocked_s else [],
            "blocked_count": 1 if blocked_s else 0,
            "oldest_xact": {"pid": 5, "app": app, "seconds": oldest_s},
            "rates": {"commits_per_s": 4.0, "cache_hit_pct": 99.9},
            "slots": [{"slot": "hermes_slot", "active": True, "lag_bytes": slot_lag}], "dead_tuples": []}


def test_parse_debezium_envelope_and_tombstone():
    ev = parse_debezium(order_event())
    assert ev["op"] == "c" and ev["table"] == "orders"
    assert ev["commit_ts"] == T0 and ev["capture_ts"] == T0 + 40 and ev["lsn"] == 123
    assert parse_debezium(None) is None
    assert parse_debezium({"schema": {}}) is None


def test_pct_interpolates():
    assert pct([], 0.5) is None
    assert pct([10], 0.95) == 10
    assert pct([0, 10, 20, 30, 40], 0.5) == 20
    assert pct(list(range(101)), 0.95) == 95


def test_snapshot_reads_are_not_live_traffic():
    p = Processor()
    p.handle_cdc({**order_event(), "op": "r"}, None, T0 + 100)
    m, _ = p.tick(T0 + 1000)
    assert p.totals["snapshot_reads"] == 1 and m["totals"]["events"] == 0


def test_latency_funnel_and_revenue():
    p = Processor()
    now = T0 + 100
    p.handle_cdc(order_event(total=25.5), None, now)                                  # placed
    p.handle_cdc(order_event(op="u", status="paid", before_status="placed"), None, now)
    p.handle_cdc(order_event(op="u", status="paid", before_status="paid"), None, now)  # no status change
    p.handle_cdc(order_event(op="u", status="shipped", before_status="paid"), None, now)
    p.handle_cdc(order_event(op="d", before_status="shipped"), None, now)
    p.handle_cdc({"op": "c", "ts_ms": T0 + 40, "after": {"id": 1}, "source": {"table": "payments", "ts_ms": T0}}, None, now)
    m, _ = p.tick(T0 + 1000)
    assert m["funnel_60s"] == {"placed": 1, "paid": 1, "shipped": 1, "deleted": 1}
    assert m["throughput"]["revenue_per_min"] == 25.5
    assert m["totals"] == {"events": 6, "orders": 1, "snapshot_reads": 0}
    lat = m["latency_ms"]
    assert lat["commit_to_capture"]["p50"] == 40
    assert lat["capture_to_process"]["p50"] == 60
    assert lat["end_to_end"]["max"] == 100


def test_visitor_order_produces_trace():
    p = Processor()
    tr = p.handle_cdc(order_event(source="visitor", trace_id="abc", oid=42), T0 + 50, T0 + 90)
    assert tr == {"trace_id": "abc", "order_id": 42, "commit_ts": T0, "capture_ts": T0 + 40,
                  "kafka_ts": T0 + 50, "processed_ts": T0 + 90}
    assert p.handle_cdc(order_event(), T0 + 50, T0 + 90) is None


def test_rule_hysteresis_and_hold():
    r = Rule("x", "warning", "X", open_at=10, close_at=5, hold_s=2)
    d = lambda v: f"v={v}"
    assert r.evaluate(11, 0, d) is None            # breach starts, hold not met
    assert r.evaluate(12, 1000, d) is None
    opened = r.evaluate(12, 2000, d)
    assert opened["state"] == "open" and opened["detail"] == "v=12"
    assert r.evaluate(7, 3000, d) is None          # between close and open: stays open
    resolved = r.evaluate(4, 4000, d)
    assert resolved["state"] == "resolved" and resolved["id"] == opened["id"]
    assert r.evaluate(None, 5000, d) is None


def test_blocking_alert_opens_and_resolves():
    p = Processor()
    p.handle_telemetry(telemetry(blocked_s=6))
    _, ev = p.tick(T0)
    assert [(e["rule"], e["state"]) for e in ev] == [("blocking", "open")]
    assert "demo-restock-job" in ev[0]["detail"] and "pid 5" in ev[0]["detail"]
    assert p.active_alerts() == ["blocking"]
    p.handle_telemetry(telemetry())
    _, ev = p.tick(T0 + 1000)
    assert [(e["rule"], e["state"]) for e in ev] == [("blocking", "resolved")]


def test_long_transaction_alert_names_app():
    p = Processor()
    p.handle_telemetry(telemetry(oldest_s=31, app="demo-report-query"))
    m, ev = p.tick(T0)
    assert ev[0]["rule"] == "long_xact" and "demo-report-query" in ev[0]["detail"]
    assert m["db"]["oldest_xact_app"] == "demo-report-query"


def test_slot_lag_alert():
    p = Processor()
    p.handle_telemetry(telemetry(slot_lag=40 * 1024 * 1024))
    m, ev = p.tick(T0)
    assert ev[0]["rule"] == "slot_lag" and "40.0 MB" in ev[0]["detail"]
    assert m["db"]["slot_lag_bytes"] == 40 * 1024 * 1024


def test_throughput_drop_needs_baseline_then_fires():
    p = Processor()
    # 2 minutes of steady 2 orders/s, then silence.
    for s in range(120):
        for i in range(2):
            p.handle_cdc(order_event(oid=s * 10 + i), None, T0 + s * 1000)
    m, ev = p.tick(T0 + 120_000)
    assert m["throughput"]["baseline_orders_per_s"] > 1.5 and not ev
    fired = None
    for s in range(121, 140):
        _, ev = p.tick(T0 + s * 1000)
        if ev:
            fired = (s, ev[0])
            break
    assert fired, "throughput alert should open once orders stop"
    assert fired[1]["rule"] == "throughput" and fired[1]["state"] == "open"
    assert 125 <= fired[0] <= 135


def test_no_throughput_alert_without_baseline():
    p = Processor()
    for s in range(20):
        _, ev = p.tick(T0 + s * 1000)
        assert not ev


def test_old_buckets_are_dropped():
    p = Processor()
    p.handle_cdc(order_event(), None, T0)
    p.tick(T0 + 500_000)
    assert len(p.buckets) == 0
