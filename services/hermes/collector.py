"""Polls PostgreSQL health views every few seconds and publishes a snapshot to Kafka.

The same signals a DBA watches by hand: session states, blocking chains, the oldest
open transaction (which pins the xmin horizon), transaction-ID age, throughput,
cache hit ratio, dead tuples, and how far the Debezium replication slot is behind.
"""
import logging
import time

import psycopg

from . import config
from .kafka_util import ensure_topics, producer, send_json, wait_for_kafka

log = logging.getLogger("hermes.collector")

SQL = {
    "sessions": """
        SELECT coalesce(state, 'unknown'), count(*)
        FROM pg_stat_activity
        WHERE datname = current_database() AND backend_type = 'client backend' AND pid <> pg_backend_pid()
        GROUP BY 1""",
    "blocked": """
        SELECT pid, pg_blocking_pids(pid), coalesce(application_name, ''), wait_event_type,
               extract(epoch FROM now() - query_start)::float, left(regexp_replace(query, '\\s+', ' ', 'g'), 120)
        FROM pg_stat_activity
        WHERE cardinality(pg_blocking_pids(pid)) > 0
        ORDER BY query_start LIMIT 5""",
    "oldest_xact": """
        SELECT pid, coalesce(application_name, ''), state,
               extract(epoch FROM now() - xact_start)::float, age(backend_xmin)
        FROM pg_stat_activity
        WHERE datname = current_database() AND xact_start IS NOT NULL AND pid <> pg_backend_pid()
          AND backend_type = 'client backend'
        ORDER BY xact_start LIMIT 1""",
    "xid_age": "SELECT age(datfrozenxid) FROM pg_database WHERE datname = current_database()",
    "db": """
        SELECT xact_commit, xact_rollback, tup_inserted, tup_updated, tup_deleted, blks_hit, blks_read, deadlocks
        FROM pg_stat_database WHERE datname = current_database()""",
    "slots": """
        SELECT slot_name, active, coalesce(pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn), 0)::bigint
        FROM pg_replication_slots""",
    "dead": """
        SELECT relname, n_live_tup, n_dead_tup, last_autovacuum
        FROM pg_stat_user_tables ORDER BY n_dead_tup DESC LIMIT 3""",
}


class Collector:
    """Turns raw catalog reads into a snapshot; keeps the previous counters to compute rates."""

    def __init__(self):
        self.prev = None  # (time, counters)

    def snapshot(self, cur) -> dict:
        now = time.time()
        cur.execute(SQL["sessions"])
        sessions = {state: n for state, n in cur.fetchall()}

        cur.execute(SQL["blocked"])
        blocked = [{"pid": r[0], "blocked_by": list(r[1]), "app": r[2], "wait": r[3],
                    "seconds": round(r[4] or 0, 1), "query": r[5]} for r in cur.fetchall()]

        cur.execute(SQL["oldest_xact"])
        r = cur.fetchone()
        oldest = {"pid": r[0], "app": r[1], "state": r[2], "seconds": round(r[3] or 0, 1),
                  "xmin_age": int(r[4]) if r[4] is not None else None} if r else None

        cur.execute(SQL["xid_age"])
        xid_age = int(cur.fetchone()[0])

        cur.execute(SQL["db"])
        c = cur.fetchone()
        counters = dict(zip(["commits", "rollbacks", "inserted", "updated", "deleted", "blks_hit", "blks_read", "deadlocks"], c))
        rates = {}
        if self.prev:
            dt = max(now - self.prev[0], 1e-3)
            for k in ("commits", "rollbacks", "inserted", "updated", "deleted"):
                rates[k + "_per_s"] = round(max(counters[k] - self.prev[1][k], 0) / dt, 2)
            hit = counters["blks_hit"] - self.prev[1]["blks_hit"]
            read = counters["blks_read"] - self.prev[1]["blks_read"]
            rates["cache_hit_pct"] = round(100 * hit / (hit + read), 2) if hit + read else 100.0
        self.prev = (now, counters)

        cur.execute(SQL["slots"])
        slots = [{"slot": s, "active": a, "lag_bytes": int(lag)} for s, a, lag in cur.fetchall()]

        cur.execute(SQL["dead"])
        dead = [{"table": t, "live": int(l), "dead": int(d), "last_autovacuum": str(v) if v else None}
                for t, l, d, v in cur.fetchall()]

        return {
            "instance": config.INSTANCE,
            "ts": int(now * 1000),
            "sessions": sessions,
            "blocked": blocked,
            "blocked_count": len(blocked),
            "oldest_xact": oldest,
            "xid_age": xid_age,
            "rates": rates,
            "deadlocks_total": counters["deadlocks"],
            "slots": slots,
            "dead_tuples": dead,
        }


def run():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(message)s")
    admin = wait_for_kafka(config.BOOTSTRAP)
    ensure_topics(admin, {config.TELEMETRY_TOPIC: (1, {"retention.ms": str(6 * 3600 * 1000)})})
    p = producer(config.BOOTSTRAP)
    col = Collector()
    while True:
        try:
            with psycopg.connect(config.PG_DSN, application_name="hermes-collector", autocommit=True) as conn:
                with conn.cursor() as cur:
                    cur.execute("SET statement_timeout = '2s'")
                    while True:
                        t0 = time.time()
                        snap = col.snapshot(cur)
                        send_json(p, config.TELEMETRY_TOPIC, snap, key=config.INSTANCE)
                        p.poll(0)
                        time.sleep(max(config.COLLECT_INTERVAL_S - (time.time() - t0), 0.1))
        except psycopg.Error as e:
            log.warning("collector error (%s); retrying", e)
            time.sleep(3)


if __name__ == "__main__":
    run()
