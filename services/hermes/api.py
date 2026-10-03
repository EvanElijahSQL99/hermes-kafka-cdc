"""Dashboard API: streams pipeline state to browsers (SSE) and runs visitor-triggered scenarios.

GET  /api/stream          Server-sent events: a snapshot, then one tick per second
GET  /api/snapshot        Same payload for polling clients (?since=<seq>)
POST /api/actions/order   Place an order and trace it through the pipeline
POST /api/actions/blocking | longtx | pause | resume   Demo scenarios (one at a time, rate-limited)
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import threading
import time
import uuid
from collections import deque
from contextlib import asynccontextmanager

import psycopg
from confluent_kafka import Consumer, TopicPartition
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import FileResponse, JSONResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles

from . import config
from .kafka_util import loads, producer, send_json, wait_for_kafka

log = logging.getLogger("hermes.api")
UI_DIR = os.environ.get("UI_DIR", "/app/ui")


# ── shared state, filled by background threads ──────────────────────
class State:
    def __init__(self):
        self.lock = threading.Lock()
        self.seq = 0
        self.metrics = None
        self.history = deque(maxlen=180)    # one compact point per second
        self.alerts = deque(maxlen=40)      # alert open/resolve events
        self.active_alerts: dict[str, dict] = {}
        self.feed = deque(maxlen=40)        # recent raw CDC events
        self.traces = deque(maxlen=200)
        self.lag = {"total": None, "partitions": [], "ts": None}
        self.scenario = None                # {"name", "ends_at", "started_at"}
        self.visitor_orders = 0
        self.started = time.time()

    def _next(self):
        self.seq += 1
        return self.seq

    def add(self, kind: str, item: dict):
        with self.lock:
            item = {**item, "seq": self._next()}
            getattr(self, kind).append(item)
            if kind == "alerts":
                if item["state"] == "open":
                    self.active_alerts[item["rule"]] = item
                else:
                    self.active_alerts.pop(item["rule"], None)

    def set_metrics(self, m: dict):
        with self.lock:
            self.metrics = m
            t, lat, db = m["throughput"], m["latency_ms"], m.get("db") or {}
            self.history.append({
                "ts": m["ts"], "events": t["events_per_s"], "orders": t["orders_per_s"],
                "baseline": t.get("baseline_orders_per_s"),
                "e2e_p50": lat["end_to_end"]["p50"], "e2e_p95": lat["end_to_end"]["p95"],
                "lag": self.lag["total"], "blocked": db.get("blocked_count"),
                "commits": db.get("commits_per_s"), "paused": m.get("paused"),
            })

    def payload(self, since: int = 0, full: bool = False):
        with self.lock:
            sc = self.scenario
            if sc and time.time() > sc["ends_at"] + 5:
                self.scenario = sc = None
            out = {
                "seq": self.seq, "now": int(time.time() * 1000), "metrics": self.metrics, "lag": self.lag,
                "scenario": sc, "active_alerts": list(self.active_alerts.values()),
                "visitor_orders": self.visitor_orders,
                "alerts": [a for a in self.alerts if a["seq"] > since],
                "feed": [f for f in self.feed if f["seq"] > since],
                "traces": [t for t in self.traces if t["seq"] > since],
            }
            if full:
                out["history"] = list(self.history)
            return out


S = State()


# ── background consumers ────────────────────────────────────────────
def compact_cdc(topic, m, value):
    src = value.get("source") or {}
    row = value.get("after") or value.get("before") or {}
    keep = {k: row[k] for k in ("id", "order_id", "product_id", "status", "total", "amount", "quantity",
                                "on_hand", "source", "method") if k in row}
    return {"topic": topic, "partition": m.partition(), "offset": m.offset(), "op": value.get("op"),
            "table": src.get("table"), "lsn": src.get("lsn"), "txid": src.get("txId"),
            "commit_ts": src.get("ts_ms"), "row": keep,
            "before_status": (value.get("before") or {}).get("status")}


def consume_loop():
    c = Consumer({"bootstrap.servers": config.BOOTSTRAP, "group.id": f"hermes-api-{uuid.uuid4().hex[:8]}",
                  "auto.offset.reset": "latest", "enable.auto.commit": False,
                  "topic.metadata.refresh.interval.ms": 10000, "fetch.wait.max.ms": 50})
    c.subscribe([config.METRICS_TOPIC, config.ALERTS_TOPIC, config.TRACE_TOPIC, "^shop\\.public\\..*"])
    while True:
        for m in c.consume(num_messages=200, timeout=0.5):
            if m.error():
                continue
            v = loads(m.value())
            if not v:
                continue
            t = m.topic()
            if t == config.METRICS_TOPIC:
                S.set_metrics(v)
            elif t == config.ALERTS_TOPIC:
                S.add("alerts", v)
            elif t == config.TRACE_TOPIC:
                S.add("traces", {**v, "api_ts": int(time.time() * 1000)})
            elif t.startswith("shop.public.") and v.get("op") != "r":
                S.add("feed", compact_cdc(t, m, v))


def lag_loop():
    """Consumer lag of the processor group = log-end offset − committed offset, per CDC partition."""
    c = Consumer({"bootstrap.servers": config.BOOTSTRAP, "group.id": config.PROCESSOR_GROUP,
                  "enable.auto.commit": False})
    while True:
        try:
            md = c.list_topics(timeout=5)
            tps = [TopicPartition(t, p) for t, tm in md.topics.items() if t.startswith("shop.public.")
                   for p in tm.partitions]
            parts, total = [], 0
            if tps:
                for tp in c.committed(tps, timeout=5):
                    lo, hi = c.get_watermark_offsets(tp, timeout=5, cached=False)
                    committed = tp.offset if tp.offset >= 0 else lo
                    lag = max(hi - committed, 0)
                    total += lag
                    parts.append({"topic": tp.topic, "partition": tp.partition, "end": hi, "committed": committed, "lag": lag})
            with S.lock:
                S.lag = {"total": total, "partitions": parts, "ts": int(time.time() * 1000)}
        except Exception as e:  # noqa: BLE001
            log.warning("lag check failed: %s", e)
        time.sleep(1)


# ── app ─────────────────────────────────────────────────────────────
PRODUCER = None
# How long each demo scenario lasts (seconds). Module-level so tests can shorten them.
SCENARIO_SECONDS = {"blocking": 25, "longtx": 50, "pause": 30}


@asynccontextmanager
async def lifespan(_app):
    global PRODUCER
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(message)s")
    if os.environ.get("HERMES_NO_KAFKA") != "1":      # tests run the API without a broker
        wait_for_kafka(config.BOOTSTRAP)
        PRODUCER = producer(config.BOOTSTRAP)
        threading.Thread(target=consume_loop, daemon=True).start()
        threading.Thread(target=lag_loop, daemon=True).start()
    yield


app = FastAPI(title="Hermes API", docs_url=None, redoc_url=None, lifespan=lifespan)


@app.get("/api/health")
def health():
    return {"ok": True, "metrics_age_s": round(time.time() - S.metrics["ts"] / 1000, 1) if S.metrics else None}


@app.get("/api/snapshot")
def snapshot(since: int = 0):
    return S.payload(since, full=since == 0)


@app.get("/api/stream")
async def stream(request: Request):
    async def gen():
        snap = S.payload(0, full=True)
        cursor = snap["seq"]
        yield f"event: snapshot\ndata: {json.dumps(snap, default=str)}\n\n"
        while not await request.is_disconnected():
            await asyncio.sleep(1)
            p = S.payload(cursor)
            cursor = p["seq"]
            yield f"event: tick\ndata: {json.dumps(p, default=str)}\n\n"
    return StreamingResponse(gen(), media_type="text/event-stream",
                             headers={"Cache-Control": "no-cache, no-transform", "X-Accel-Buffering": "no"})


# ── guard rails for a public demo ───────────────────────────────────
HITS: dict[str, deque] = {}
HITS_LOCK = threading.Lock()


def client_ip(req: Request) -> str:
    fwd = req.headers.get("x-forwarded-for", "")
    return fwd.split(",")[0].strip() if fwd else (req.client.host if req.client else "?")


def rate_limit(req: Request, per_10min=40, min_gap_s=1.5):
    ip, now = client_ip(req), time.time()
    with HITS_LOCK:
        q = HITS.setdefault(ip, deque())
        while q and now - q[0] > 600:
            q.popleft()
        if q and now - q[-1] < min_gap_s:
            raise HTTPException(429, "Easy there — one action every couple of seconds.")
        if len(q) >= per_10min:
            raise HTTPException(429, "Demo action limit reached for now. Try again in a few minutes.")
        q.append(now)


def start_scenario(name: str, seconds: float):
    with S.lock:
        if S.scenario and time.time() < S.scenario["ends_at"]:
            raise HTTPException(409, f"The '{S.scenario['name']}' scenario is already running — watch it play out.")
        S.scenario = {"name": name, "started_at": int(time.time() * 1000), "ends_at": time.time() + seconds}


def end_scenario(name: str):
    with S.lock:
        if S.scenario and S.scenario["name"] == name:
            S.scenario["ends_at"] = time.time()


def pg(app_name: str, **kw):
    return psycopg.connect(config.PG_DSN, application_name=app_name, **kw)


@app.post("/api/actions/order")
def place_order(req: Request):
    rate_limit(req, per_10min=60, min_gap_s=0.8)
    from .workload import place_order as insert_order
    trace_id = str(uuid.uuid4())
    t0 = int(time.time() * 1000)
    try:
        with pg("dashboard-visitor") as conn, conn.cursor() as cur:
            cur.execute("SET lock_timeout = '4s'")
            cur.execute("SELECT count(*) FROM inventory")
            n_products = cur.fetchone()[0]
            order_id = insert_order(cur, n_products, 200, source="visitor", trace_id=trace_id)
            conn.commit()
    except psycopg.errors.LockNotAvailable:
        raise HTTPException(409, "Your order is stuck behind the blocking chain (lock_timeout hit after 4s) — "
                                 "that's the scenario working. Try again when it clears.")
    with S.lock:
        S.visitor_orders += 1
    return {"trace_id": trace_id, "order_id": order_id, "api_ts": t0, "committed_ts": int(time.time() * 1000)}


@app.post("/api/actions/blocking")
def blocking(req: Request):
    rate_limit(req)
    hold = SCENARIO_SECONDS["blocking"]
    start_scenario("blocking", hold)

    def holder():
        try:
            with pg("demo-lock-holder") as conn, conn.cursor() as cur:
                cur.execute("SELECT on_hand FROM inventory WHERE product_id = 1 FOR UPDATE")
                time.sleep(hold)
                conn.rollback()
        finally:
            end_scenario("blocking")

    def waiter():
        time.sleep(1)
        try:
            with pg("demo-restock-job") as conn, conn.cursor() as cur:
                cur.execute("SET lock_timeout = '40s'")
                cur.execute("UPDATE inventory SET on_hand = on_hand WHERE product_id = 1")
                conn.rollback()
        except psycopg.Error:
            pass

    threading.Thread(target=holder, daemon=True).start()
    threading.Thread(target=waiter, daemon=True).start()
    return {"ok": True, "seconds": hold,
            "message": f"A session now holds a row lock on the hot product for {hold}s. Watch blocked sessions, "
                       "order throughput and the alerts."}


@app.post("/api/actions/longtx")
def long_tx(req: Request):
    rate_limit(req)
    hold = SCENARIO_SECONDS["longtx"]
    start_scenario("longtx", hold)

    def run():
        try:
            # autocommit so our explicit BEGIN really opens the transaction (psycopg would otherwise start a
            # READ COMMITTED one first, whose snapshot is released after each statement and pins nothing)
            with pg("demo-report-query", autocommit=True) as conn, conn.cursor() as cur:
                cur.execute("BEGIN ISOLATION LEVEL REPEATABLE READ")
                cur.execute("SELECT count(*), sum(total) FROM orders")   # takes a snapshot → pins xmin
                time.sleep(hold)
                cur.execute("COMMIT")
        finally:
            end_scenario("longtx")

    threading.Thread(target=run, daemon=True).start()
    return {"ok": True, "seconds": hold,
            "message": f"A reporting transaction is now open for {hold}s. After 30s the long-transaction alert fires; "
                       "watch dead tuples climb while it pins the xmin horizon."}


@app.post("/api/actions/pause")
def pause(req: Request):
    rate_limit(req)
    secs = SCENARIO_SECONDS["pause"]
    start_scenario("pause", secs)
    send_json(PRODUCER, config.CONTROL_TOPIC, {"action": "pause", "seconds": secs})
    PRODUCER.flush(5)
    return {"ok": True, "seconds": secs,
            "message": f"The processor stopped reading CDC topics for {secs}s. Lag builds, then drains when it resumes."}


@app.post("/api/actions/resume")
def resume(req: Request):
    rate_limit(req)
    send_json(PRODUCER, config.CONTROL_TOPIC, {"action": "resume"})
    PRODUCER.flush(5)
    end_scenario("pause")
    return {"ok": True}


@app.exception_handler(psycopg.OperationalError)
def db_down(_req, exc):
    return JSONResponse({"detail": f"Database unavailable: {exc}"}, status_code=503)


# Static dashboard (built React app); unknown paths fall back to index.html.
if os.path.isdir(UI_DIR):
    app.mount("/assets", StaticFiles(directory=os.path.join(UI_DIR, "assets")), name="assets")

    @app.get("/{path:path}")
    def spa(path: str):
        f = os.path.join(UI_DIR, path)
        if path and os.path.isfile(f) and os.path.realpath(f).startswith(os.path.realpath(UI_DIR)):
            return FileResponse(f)
        return FileResponse(os.path.join(UI_DIR, "index.html"))
