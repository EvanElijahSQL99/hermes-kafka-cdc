"""Stream-processing logic, independent of Kafka I/O so it can be unit-tested.

Processor.handle_cdc()        — one Debezium change event
Processor.handle_telemetry()  — one collector snapshot
Processor.tick()              — once per second: returns (metrics, alert_events)
"""
from __future__ import annotations

import statistics
import uuid
from collections import defaultdict, deque

WINDOW_S = 180  # seconds of per-second buckets kept in memory


def parse_debezium(value: dict | None):
    """Normalize a Debezium envelope (JsonConverter, schemas disabled). Returns None for tombstones."""
    if not value or "op" not in value:
        return None
    src = value.get("source") or {}
    return {
        "op": value["op"],                  # c=insert u=update d=delete r=snapshot read
        "table": src.get("table"),
        "before": value.get("before"),
        "after": value.get("after"),
        "commit_ts": src.get("ts_ms"),      # when the transaction committed in Postgres
        "capture_ts": value.get("ts_ms"),   # when Debezium read it from the WAL
        "lsn": src.get("lsn"),
        "txid": src.get("txId"),
    }


def pct(values, p):
    if not values:
        return None
    v = sorted(values)
    k = (len(v) - 1) * p
    lo, hi = int(k), min(int(k) + 1, len(v) - 1)
    return round(v[lo] + (v[hi] - v[lo]) * (k - lo), 1)


class Bucket:
    __slots__ = ("events", "orders", "payments", "inventory", "revenue", "placed", "paid", "shipped",
                 "deleted", "lat_commit_capture", "lat_capture_process", "lat_e2e")

    def __init__(self):
        self.events = self.orders = self.payments = self.inventory = 0
        self.revenue = 0.0
        self.placed = self.paid = self.shipped = self.deleted = 0
        self.lat_commit_capture, self.lat_capture_process, self.lat_e2e = [], [], []


class Rule:
    """A threshold rule with hysteresis: opens after `hold_s` above `open_at`, resolves below `close_at`."""

    def __init__(self, rule_id, severity, title, open_at, close_at, hold_s=0):
        self.id, self.severity, self.title = rule_id, severity, title
        self.open_at, self.close_at, self.hold_s = open_at, close_at, hold_s
        self.active = False
        self.breach_since = None
        self.alert_id = None

    def evaluate(self, value, now_ms, detail_fn):
        if value is None:
            return None
        if not self.active:
            if value >= self.open_at:
                if self.breach_since is None:
                    self.breach_since = now_ms
                if now_ms - self.breach_since >= self.hold_s * 1000:
                    self.active, self.alert_id = True, uuid.uuid4().hex[:10]
                    return self._event("open", now_ms, value, detail_fn(value))
            else:
                self.breach_since = None
        elif value < self.close_at:
            self.active, self.breach_since = False, None
            return self._event("resolved", now_ms, value, detail_fn(value))
        return None

    def _event(self, state, now_ms, value, detail):
        return {"id": self.alert_id, "rule": self.id, "severity": self.severity, "state": state,
                "title": self.title, "detail": detail, "value": value, "ts": now_ms}


class Processor:
    def __init__(self, thresholds: dict | None = None):
        t = {"blocking_s": 5, "long_xact_s": 30, "slot_lag_mb": 32, "xid_age": 1_500_000_000,
             "drop_ratio": 0.25, **(thresholds or {})}
        self.buckets: dict[int, Bucket] = defaultdict(Bucket)
        self.telemetry = None
        self.paused = False
        self.totals = {"events": 0, "orders": 0, "snapshot_reads": 0}
        self.traces = deque(maxlen=100)
        self.rules = {
            "blocking": Rule("blocking", "critical", "Blocking chain", t["blocking_s"], 0.5),
            "long_xact": Rule("long_xact", "warning", "Long-running transaction", t["long_xact_s"], t["long_xact_s"]),
            "slot_lag": Rule("slot_lag", "warning", "CDC replication slot falling behind",
                             t["slot_lag_mb"] * 1024 * 1024, t["slot_lag_mb"] * 1024 * 1024 / 4),
            "xid_age": Rule("xid_age", "critical", "Transaction ID wraparound risk", t["xid_age"], t["xid_age"] * 0.9),
            "throughput": Rule("throughput", "warning", "Order throughput dropped", 1 - t["drop_ratio"], 0.4, hold_s=5),
        }

    # ── input ────────────────────────────────────────────────────
    def handle_cdc(self, value: dict, kafka_ts: int | None, now_ms: int):
        ev = parse_debezium(value)
        if ev is None:
            return None
        if ev["op"] == "r":                       # initial snapshot rows: count, but not as live traffic
            self.totals["snapshot_reads"] += 1
            return None
        b = self.buckets[now_ms // 1000]
        b.events += 1
        self.totals["events"] += 1
        commit, capture = ev["commit_ts"], ev["capture_ts"]
        if commit and capture:
            b.lat_commit_capture.append(max(capture - commit, 0))
            b.lat_capture_process.append(max(now_ms - capture, 0))
            b.lat_e2e.append(max(now_ms - commit, 0))
        table, after, before = ev["table"], ev["after"] or {}, ev["before"] or {}
        if table == "orders":
            b.orders += 1
            if ev["op"] == "c":
                b.placed += 1
                b.revenue += float(after.get("total") or 0)
                self.totals["orders"] += 1
                if after.get("source") == "visitor" and after.get("trace_id"):
                    trace = {"trace_id": after["trace_id"], "order_id": after.get("id"),
                             "commit_ts": commit, "capture_ts": capture, "kafka_ts": kafka_ts, "processed_ts": now_ms}
                    self.traces.append(trace)
                    return trace
            elif ev["op"] == "u" and after.get("status") != before.get("status"):
                if after.get("status") == "paid":
                    b.paid += 1
                elif after.get("status") == "shipped":
                    b.shipped += 1
            elif ev["op"] == "d":
                b.deleted += 1
        elif table == "payments":
            b.payments += 1
        elif table == "inventory":
            b.inventory += 1
        return None

    def handle_telemetry(self, snap: dict):
        self.telemetry = snap

    # ── output ───────────────────────────────────────────────────
    def _window(self, now_s, seconds, skip=0):
        return [self.buckets[s] for s in range(now_s - skip - seconds, now_s - skip) if s in self.buckets]

    def tick(self, now_ms: int):
        now_s = now_ms // 1000
        for s in [s for s in self.buckets if s < now_s - WINDOW_S]:
            del self.buckets[s]

        last10 = self._window(now_s, 10)
        last60 = self._window(now_s, 60)
        baseline = self._window(now_s, 110, skip=10)
        orders10 = sum(b.placed for b in last10) / 10
        base_rate = (sum(b.placed for b in baseline) / 110) if baseline else 0

        def lat(attr):
            vals = [v for b in last10 for v in getattr(b, attr)]
            return {"p50": pct(vals, 0.5), "p95": pct(vals, 0.95), "max": max(vals) if vals else None}

        tel = self.telemetry or {}
        oldest = tel.get("oldest_xact") or {}
        slot_lag = max((s["lag_bytes"] for s in tel.get("slots", [])), default=None)
        blocked_s = max((b["seconds"] for b in tel.get("blocked", [])), default=0) if tel else None

        metrics = {
            "ts": now_ms,
            "paused": self.paused,
            "throughput": {
                "events_per_s": round(sum(b.events for b in last10) / 10, 2),
                "orders_per_s": round(orders10, 2),
                "payments_per_s": round(sum(b.payments for b in last10) / 10, 2),
                "revenue_per_min": round(sum(b.revenue for b in last60), 2),
                "baseline_orders_per_s": round(base_rate, 2),
            },
            "latency_ms": {"commit_to_capture": lat("lat_commit_capture"),
                           "capture_to_process": lat("lat_capture_process"),
                           "end_to_end": lat("lat_e2e")},
            "funnel_60s": {k: sum(getattr(b, k) for b in last60) for k in ("placed", "paid", "shipped", "deleted")},
            "totals": dict(self.totals),
            "db": {
                "sessions": tel.get("sessions"),
                "blocked_count": tel.get("blocked_count"),
                "blocked": tel.get("blocked"),
                "oldest_xact_s": oldest.get("seconds"),
                "oldest_xact_app": oldest.get("app"),
                "xid_age": tel.get("xid_age"),
                "commits_per_s": (tel.get("rates") or {}).get("commits_per_s"),
                "cache_hit_pct": (tel.get("rates") or {}).get("cache_hit_pct"),
                "slot_lag_bytes": slot_lag,
                "dead_tuples": tel.get("dead_tuples"),
                "telemetry_ts": tel.get("ts"),
            },
        }

        events = []
        if tel:
            def blocked_detail(v):
                bl = tel.get("blocked") or []
                if not bl:
                    return "No sessions are waiting on locks any more."
                head = sorted({p for b in bl for p in b["blocked_by"]})
                return (f"{len(bl)} session(s) waiting on a lock for up to {v:.0f}s; blocked by pid "
                        f"{', '.join(map(str, head))}. Waiting: " + "; ".join(f"{b['app'] or 'pid ' + str(b['pid'])}" for b in bl))
            events.append(self.rules["blocking"].evaluate(blocked_s, now_ms, blocked_detail))
            app = oldest.get("app") or f"pid {oldest.get('pid')}"
            events.append(self.rules["long_xact"].evaluate(
                oldest.get("seconds") if oldest.get("app") != "hermes-collector" else 0, now_ms,
                lambda v: f"Transaction from {app} open {v:.0f}s. It pins the xmin horizon, so vacuum can't remove "
                          f"dead tuples created since it began." if v >= self.rules['long_xact'].close_at
                          else "The long transaction has ended; vacuum can clean up again."))
            events.append(self.rules["slot_lag"].evaluate(slot_lag, now_ms,
                lambda v: f"Debezium's slot is {v / 1048576:.1f} MB behind the current WAL position; Postgres must "
                          f"retain that WAL until it's confirmed."))
            events.append(self.rules["xid_age"].evaluate(tel.get("xid_age"), now_ms,
                lambda v: f"datfrozenxid age is {v:,}; anti-wraparound vacuum needs attention."))
        # Throughput rule uses a ratio: 1 - current/baseline (1.0 = stopped). Only meaningful with a baseline.
        if base_rate >= 0.5 and len(baseline) >= 60:
            drop = 1 - (orders10 / base_rate)
            events.append(self.rules["throughput"].evaluate(round(drop, 2), now_ms,
                lambda v: f"Orders are at {orders10:.1f}/s against a {base_rate:.1f}/s baseline "
                          f"({max(v, 0) * 100:.0f}% drop)." if v >= 0.4 else
                          f"Orders recovered to {orders10:.1f}/s."))
        return metrics, [e for e in events if e]

    def active_alerts(self):
        return [r.id for r in self.rules.values() if r.active]


def median(xs):
    return statistics.median(xs) if xs else None
