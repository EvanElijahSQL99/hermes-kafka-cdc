import { useEffect, useRef, useState } from "react";
import { createSimulator } from "./mock.js";

// Mock mode (?mock) runs the dashboard against an in-browser simulator: handy for UI work without Docker.
export const MOCK = typeof window !== "undefined" && new URLSearchParams(window.location.search).has("mock");
const sim = MOCK ? createSimulator() : null;

const EMPTY = {
  status: "connecting",
  seq: 0,
  metrics: null,
  lag: { total: null, partitions: [] },
  scenario: null,
  activeAlerts: [],
  alerts: [],
  feed: [],
  traces: {},
  history: [],
  visitorOrders: 0,
  batch: { key: 0, items: [] },
  serverSkew: 0,
};

function apply(prev, p, full) {
  const next = { ...prev, seq: p.seq, metrics: p.metrics ?? prev.metrics, lag: p.lag ?? prev.lag,
    scenario: p.scenario, activeAlerts: p.active_alerts ?? [], visitorOrders: p.visitor_orders ?? 0,
    serverSkew: p.now ? p.now - Date.now() : prev.serverSkew };
  const newFeed = (p.feed || []).slice().reverse();
  next.feed = full ? newFeed.slice(0, 40) : [...newFeed, ...prev.feed].slice(0, 40);
  const newAlerts = (p.alerts || []).slice().reverse();
  next.alerts = full ? newAlerts.slice(0, 30) : [...newAlerts, ...prev.alerts].slice(0, 30);
  if (p.traces?.length) {
    next.traces = { ...prev.traces };
    for (const t of p.traces) next.traces[t.trace_id] = { ...t, browser_ts: Date.now() + next.serverSkew };
  }
  if (full && p.history) {
    next.history = p.history;
  } else if (p.metrics && p.metrics.ts !== prev.history.at(-1)?.ts) {
    const m = p.metrics;
    next.history = [...prev.history, {
      ts: m.ts, events: m.throughput.events_per_s, orders: m.throughput.orders_per_s,
      baseline: m.throughput.baseline_orders_per_s, e2e_p50: m.latency_ms.end_to_end.p50,
      e2e_p95: m.latency_ms.end_to_end.p95, lag: p.lag?.total ?? null, blocked: m.db?.blocked_count,
      paused: m.paused,
    }].slice(-180);
  }
  // New CDC events for the pipeline animation (skipped on a full snapshot: those already happened).
  next.batch = { key: p.seq, items: full ? [] : (p.feed || []) };
  return next;
}

export function useHermes() {
  const [state, setState] = useState(EMPTY);
  const cursor = useRef(0);

  useEffect(() => {
    let es = null, pollTimer = null, retryTimer = null, watchdog = null, closed = false;
    let lastMessage = Date.now();

    const handle = (p, full) => {
      lastMessage = Date.now();
      cursor.current = p.seq;
      setState((prev) => apply(prev, p, full));
    };

    if (MOCK) {
      handle(sim.payload(0, true), true);
      setState((s) => ({ ...s, status: "live" }));
      const t = setInterval(() => handle(sim.payload(cursor.current, false), false), 1000);
      return () => clearInterval(t);
    }

    const startPolling = () => {
      if (pollTimer || closed) return;
      setState((s) => ({ ...s, status: "polling" }));
      const poll = async () => {
        try {
          const full = cursor.current === 0;
          const r = await fetch(`/api/snapshot?since=${cursor.current}`, { cache: "no-store" });
          if (!r.ok) throw new Error(r.status);
          handle(await r.json(), full);
          setState((s) => (s.status === "offline" ? { ...s, status: "polling" } : s));
        } catch {
          setState((s) => ({ ...s, status: "offline" }));
        }
      };
      poll();
      pollTimer = setInterval(poll, 1500);
      retryTimer = setTimeout(() => { stopPolling(); connect(); }, 30000);   // try streaming again later
    };
    const stopPolling = () => { clearInterval(pollTimer); clearTimeout(retryTimer); pollTimer = null; };

    const connect = () => {
      if (closed) return;
      es = new EventSource("/api/stream");
      es.addEventListener("snapshot", (e) => {
        stopPolling();
        handle(JSON.parse(e.data), true);
        setState((s) => ({ ...s, status: "live" }));
      });
      es.addEventListener("tick", (e) => handle(JSON.parse(e.data), false));
      es.onerror = () => { es.close(); es = null; startPolling(); };
    };

    // A proxy that buffers the stream looks like silence: fall back to polling if nothing arrives.
    watchdog = setInterval(() => {
      if (es && Date.now() - lastMessage > 6000) { es.close(); es = null; startPolling(); }
    }, 2000);

    connect();
    return () => { closed = true; es?.close(); stopPolling(); clearInterval(watchdog); };
  }, []);

  return state;
}

export async function postAction(name) {
  if (MOCK) return sim.action(name);
  const r = await fetch(`/api/actions/${name}`, { method: "POST" });
  let body = {};
  try { body = await r.json(); } catch { /* empty */ }
  if (!r.ok) {
    const err = new Error(body.detail || `The server answered ${r.status}.`);
    err.status = r.status;
    throw err;
  }
  return body;
}
