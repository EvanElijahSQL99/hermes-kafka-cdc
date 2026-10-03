// In-browser stand-in for the Hermes API (?mock). Produces payloads in the same shape as /api/snapshot,
// including the visitor scenarios, so the dashboard can be developed and screenshotted without Docker.

export function createSimulator() {
  let seq = 0, offset = { orders: [1200, 1180, 1215], payments: [610, 640, 598], inventory: [900, 880, 905] };
  let lsn = 24_117_000, nextOrder = 5000, scenario = null, pausedUntil = 0, visitorOrders = 0;
  const lag = { orders: [0, 0, 0], payments: [0, 0, 0], inventory: [0, 0, 0] };
  const alerts = [], feed = [], traces = [], history = [];
  const active = {};
  const pendingTraces = [];
  let blockedSince = null, longSince = null, baseline = 2.1, drop = 0;
  const totals = { events: 18234, orders: 6120, snapshot_reads: 2214 };

  const add = (list, item, max = 60) => { seq += 1; list.push({ ...item, seq }); if (list.length > max) list.shift(); };
  const open = (rule, severity, title, detail, value) => {
    if (active[rule]) return;
    const a = { id: Math.random().toString(16).slice(2, 12), rule, severity, state: "open", title, detail, value, ts: Date.now() };
    active[rule] = a; add(alerts, a);
  };
  const resolve = (rule, detail) => {
    if (!active[rule]) return;
    add(alerts, { ...active[rule], state: "resolved", detail, ts: Date.now() });
    delete active[rule];
  };

  // Seed three minutes of history.
  const t0 = Date.now() - 180_000;
  for (let i = 0; i < 180; i++) {
    const o = 2 + Math.sin(i / 9) * 0.3 + Math.random() * 0.3;
    history.push({ ts: t0 + i * 1000, events: +(o * 2.6).toFixed(2), orders: +o.toFixed(2), baseline: 2.1,
      e2e_p50: 70 + Math.random() * 30, e2e_p95: 150 + Math.random() * 60, lag: 0, blocked: 0, paused: false });
  }

  function step() {
    const now = Date.now();
    const paused = now < pausedUntil;
    const blocking = scenario?.name === "blocking" && now < scenario.ends_at * 1000;
    const longtx = scenario?.name === "longtx" && now < scenario.ends_at * 1000;
    if (scenario && now > scenario.ends_at * 1000 + 5000) scenario = null;

    const rate = blocking ? 0.15 : 2 + Math.random() * 0.6;
    const n = Math.max(0, Math.round(rate * 2.6 + (Math.random() - 0.5) * 2));
    const tables = ["orders", "orders", "payments", "orders", "inventory"];
    for (let i = 0; i < n; i++) {
      const table = tables[Math.floor(Math.random() * tables.length)];
      const p = Math.floor(Math.random() * 3);
      offset[table][p] += 1;
      lsn += 300 + Math.floor(Math.random() * 900);
      const op = table === "orders" ? (Math.random() < 0.45 ? "c" : "u") : table === "payments" ? "c" : "u";
      const row = table === "orders"
        ? { id: op === "c" ? nextOrder++ : nextOrder - 1 - Math.floor(Math.random() * 40), status: op === "c" ? "placed" : (Math.random() < 0.5 ? "paid" : "shipped"),
            total: +(20 + Math.random() * 300).toFixed(2), quantity: 1 + Math.floor(Math.random() * 4), product_id: 1 + Math.floor(Math.random() * 40), source: "workload" }
        : table === "payments" ? { id: 3000 + seq, order_id: nextOrder - 3, amount: +(20 + Math.random() * 300).toFixed(2), method: "card" }
        : { product_id: 1 + Math.floor(Math.random() * 40), on_hand: 300 + Math.floor(Math.random() * 600) };
      if (paused) lag[table][p] += 1;
      add(feed, { topic: `shop.public.${table}`, partition: p, offset: offset[table][p], op, table, lsn, txid: 90000 + seq,
        commit_ts: now - 60, row, before_status: op === "u" && table === "orders" ? "placed" : null });
    }
    if (!paused) for (const t in lag) lag[t] = lag[t].map((v) => Math.max(0, v - 40));
    const lagTotal = Object.values(lag).flat().reduce((a, b) => a + b, 0);

    for (const pt of pendingTraces.splice(0)) {
      if (blocking) { pendingTraces.push(pt); continue; }
      const c = pt.commit_ts;
      add(traces, { trace_id: pt.trace_id, order_id: pt.order_id, commit_ts: c, capture_ts: c + 18 + Math.random() * 30,
        kafka_ts: c + 40 + Math.random() * 20, processed_ts: c + 70 + Math.random() * 40 + (paused ? 30000 : 0),
        api_ts: c + 95 + Math.random() * 40 });
    }

    // Alerts follow the same rules as the processor, roughly.
    blockedSince = blocking ? (blockedSince ?? now) : null;
    const blockedS = blockedSince ? (now - blockedSince) / 1000 + 1 : 0;
    if (blockedS >= 5) open("blocking", "critical", "Blocking chain", `1 session(s) waiting on a lock for up to ${blockedS.toFixed(0)}s; blocked by pid 4182. Waiting: demo-restock-job`, blockedS);
    else resolve("blocking", "No sessions are waiting on locks any more.");
    drop = blocking ? Math.min(1, drop + 0.12) : Math.max(0, drop - 0.15);
    if (drop >= 0.75 && blockedS > 10) open("throughput", "warning", "Order throughput dropped", `Orders are at 0.2/s against a ${baseline}/s baseline (92% drop).`, 0.92);
    else if (drop < 0.4) resolve("throughput", "Orders recovered to 2.1/s.");
    longSince = longtx ? (longSince ?? now) : null;
    const longS = longSince ? (now - longSince) / 1000 : 0.1;
    if (longS >= 30) open("long_xact", "warning", "Long-running transaction", `Transaction from demo-report-query open ${longS.toFixed(0)}s. It pins the xmin horizon, so vacuum can't remove dead tuples created since it began.`, longS);
    else resolve("long_xact", "The long transaction has ended; vacuum can clean up again.");

    const orders = blocking ? Math.max(0.1, 2.2 * (1 - drop)) : 2 + Math.random() * 0.5;
    const p50 = paused ? null : 60 + Math.random() * 40, p95 = paused ? null : 140 + Math.random() * 70;
    const metrics = {
      ts: now, paused,
      throughput: { events_per_s: +(orders * 2.6).toFixed(2), orders_per_s: +orders.toFixed(2), payments_per_s: 1.1,
        revenue_per_min: 21_000 + Math.random() * 3000, baseline_orders_per_s: baseline },
      latency_ms: {
        commit_to_capture: { p50: 22, p95: 41, max: 60 },
        capture_to_process: { p50: paused ? null : 48, p95: paused ? null : 120, max: 200 },
        end_to_end: { p50: p50 && +p50.toFixed(1), p95: p95 && +p95.toFixed(1), max: 300 },
      },
      funnel_60s: { placed: 128, paid: 71, shipped: 64, deleted: 52 },
      totals: { ...totals, events: (totals.events += n) },
      db: {
        sessions: { active: blocking ? 3 : 1, idle: 4, "idle in transaction": longtx ? 1 : 0 },
        blocked_count: blockedS ? 1 : 0,
        blocked: blockedS ? [{ pid: 4190, blocked_by: [4182], app: "demo-restock-job", seconds: blockedS }] : [],
        oldest_xact_s: longtx ? longS : blocking ? blockedS + 1 : 0.02, oldest_xact_app: longtx ? "demo-report-query" : blocking ? "demo-lock-holder" : "shop-workload",
        xid_age: 41_223 + seq, commits_per_s: blocking ? 0.4 : 4.1, cache_hit_pct: 99.97, slot_lag_bytes: 2048 + Math.floor(Math.random() * 30000),
        dead_tuples: [{ table: "orders", live: 6100, dead: longtx ? Math.floor(400 + longS * 21) : 180 }, { table: "inventory", live: 40, dead: 31 }],
        telemetry_ts: now,
      },
    };
    history.push({ ts: now, events: metrics.throughput.events_per_s, orders: metrics.throughput.orders_per_s, baseline,
      e2e_p50: p50, e2e_p95: p95, lag: lagTotal, blocked: metrics.db.blocked_count, paused });
    if (history.length > 180) history.shift();
    return { metrics, lagTotal };
  }

  let last = { metrics: null, lagTotal: 0 };
  let lastStep = 0;

  return {
    payload(since, full) {
      if (Date.now() - lastStep > 900) { last = step(); lastStep = Date.now(); }
      const parts = [];
      for (const t of ["orders", "payments", "inventory"]) lag[t].forEach((l, p) =>
        parts.push({ topic: `shop.public.${t}`, partition: p, end: offset[t][p], committed: offset[t][p] - l, lag: l }));
      const out = {
        seq, now: Date.now(), metrics: last.metrics, lag: { total: last.lagTotal, partitions: parts, ts: Date.now() },
        scenario, active_alerts: Object.values(active), visitor_orders: visitorOrders,
        alerts: alerts.filter((a) => a.seq > since), feed: feed.filter((f) => f.seq > since).slice(-40),
        traces: traces.filter((t) => t.seq > since),
      };
      if (full) out.history = history.slice();
      return out;
    },
    async action(name) {
      await new Promise((r) => setTimeout(r, 120));
      const now = Date.now();
      const busy = scenario && now < scenario.ends_at * 1000;
      const start = (n, s) => {
        if (busy) { const e = new Error(`The '${scenario.name}' scenario is already running. Watch it play out.`); e.status = 409; throw e; }
        scenario = { name: n, started_at: now, ends_at: now / 1000 + s };
      };
      if (name === "order") {
        const trace_id = crypto.randomUUID();
        const order_id = nextOrder++;
        visitorOrders += 1;
        pendingTraces.push({ trace_id, order_id, commit_ts: now + 30 });
        return { trace_id, order_id, api_ts: now, committed_ts: now + 30 };
      }
      if (name === "blocking") { start("blocking", 25); return { ok: true, seconds: 25 }; }
      if (name === "longtx") { start("longtx", 50); return { ok: true, seconds: 50 }; }
      if (name === "pause") { start("pause", 30); pausedUntil = now + 30000; return { ok: true, seconds: 30 }; }
      if (name === "resume") { pausedUntil = 0; if (scenario?.name === "pause") scenario.ends_at = now / 1000; return { ok: true }; }
      throw new Error("Unknown action");
    },
  };
}
