import { useEffect, useMemo, useState } from "react";
import Pipeline, { TABLES } from "./Pipeline.jsx";
import LineChart from "./LineChart.jsx";
import { postAction } from "./useHermes.js";
import { ago, clock, fmtBytes, fmtMs, fmtNum, fmtSecs } from "./format.js";

const COLOR = Object.fromEntries(TABLES.map((t) => [t.key, t.color]));

function useNow(ms = 1000) {
  const [now, setNow] = useState(Date.now());
  useEffect(() => { const t = setInterval(() => setNow(Date.now()), ms); return () => clearInterval(t); }, [ms]);
  return now;
}

export default function Dashboard({ s }) {
  const m = s.metrics;
  const p50 = m?.latency_ms?.end_to_end?.p50;
  return (
    <main className="wrap">
      <section className="hero">
        <h1>
          Every change to this Postgres database is processed{" "}
          <span className="ms">{p50 == null ? "–" : Math.round(p50)}&nbsp;ms</span> after it commits.
        </h1>
        <p>
          Debezium reads the write-ahead log, Kafka carries each row change, and a Python consumer turns the stream
          into metrics and alerts. Below, you can place an order and follow it, or break something and watch the
          pipeline notice.
        </p>
      </section>

      <Pipeline s={s} />

      <div className="cols">
        <Actions s={s} />
        <div className="panel">
          <h2>Alerts</h2>
          <p className="lede">Rules run on the stream with hysteresis, so an alert opens once and resolves once.</p>
          <Alerts s={s} />
          <Health db={m?.db} />
        </div>
      </div>

      <Charts s={s} />
      <Feed s={s} />
    </main>
  );
}

// ── visitor actions ─────────────────────────────────────────────
const SCENARIOS = [
  { name: "blocking", label: "Create a blocking chain", body: <>A session locks the inventory row of the best-selling product for 25 seconds. A third of the orders need that row, so the workload stalls behind it. <b>Watch</b> blocked sessions, the orders chart and the alerts.</> },
  { name: "longtx", label: "Hold a long transaction", body: <>A reporting query keeps a REPEATABLE READ snapshot open for 50 seconds, which pins the xmin horizon. <b>Watch</b> dead tuples pile up and the long-transaction alert fire at 30 seconds.</> },
  { name: "pause", label: "Pause the consumer", body: <>The processor stops reading the change topics for 30 seconds while Debezium keeps writing. <b>Watch</b> consumer lag build in the Kafka partitions, then drain when it resumes.</> },
];

function Actions({ s }) {
  const now = useNow(500);
  const [busy, setBusy] = useState(null);
  const [notes, setNotes] = useState({});
  const [mine, setMine] = useState(null);
  const sc = s.scenario;
  const serverNow = now + s.serverSkew;
  const running = sc && sc.ends_at * 1000 > serverNow ? sc : null;

  const run = async (name) => {
    setBusy(name);
    setNotes((n) => ({ ...n, [name]: null }));
    const c0 = Date.now();
    try {
      const r = await postAction(name);
      if (name === "order") setMine({ ...r, c0, c1: Date.now() });
      else if (r.message) setNotes((n) => ({ ...n, [name]: { text: r.message } }));
    } catch (e) {
      setNotes((n) => ({ ...n, [name]: { text: e.message, err: true } }));
      if (name === "order") setMine(null);
    } finally {
      setBusy(null);
    }
  };

  return (
    <div className="panel">
      <h2>Try it</h2>
      <p className="lede">These run against the real database. One scenario at a time, shared by everyone viewing the page.</p>
      <div className="actions">
        <div className="action">
          <button className="btn visitor" onClick={() => run("order")} disabled={busy === "order"}>
            {busy === "order" ? "Placing order…" : "Place an order"}
          </button>
          <p>Inserts a real order tagged with a trace ID, then times it at each hop: Postgres commit, Debezium, Kafka, the processor and this page.</p>
        </div>
        {notes.order && <p className="notice err" role="alert">{notes.order.text}</p>}
        {mine && <Trace mine={mine} trace={s.traces[mine.trace_id]} now={now} />}

        {SCENARIOS.map((x) => {
          const mineRunning = running?.name === x.name;
          const pct = mineRunning ? Math.min(100, ((serverNow - running.started_at) / (running.ends_at * 1000 - running.started_at)) * 100) : 0;
          const left = mineRunning ? Math.max(0, Math.ceil(running.ends_at - serverNow / 1000)) : 0;
          return (
            <div className="action" key={x.name}>
              <div>
                {x.name === "pause" && mineRunning ? (
                  <button className="btn secondary" onClick={() => run("resume")} disabled={busy === "resume"}>Resume now</button>
                ) : (
                  <button className="btn" onClick={() => run(x.name)} disabled={!!running || busy === x.name}
                          title={running && !mineRunning ? `Wait for the ${running.name} scenario to finish` : undefined}>
                    {mineRunning ? `Running, ${left} s left` : x.label}
                  </button>
                )}
                {mineRunning && <div className="progress" aria-hidden="true"><i style={{ width: `${pct}%` }} /></div>}
              </div>
              <div>
                <p>{x.body}</p>
                {notes[x.name] && <p className={`notice${notes[x.name].err ? " err" : ""}`} role="status">{notes[x.name].text}</p>}
              </div>
            </div>
          );
        })}
      </div>
    </div>
  );
}

function Trace({ mine, trace, now }) {
  const base = trace?.commit_ts;
  const step = (label, ts) => ({ label, t: ts != null && base != null ? Math.max(0, ts - base) : null });
  const steps = [
    step("Committed in Postgres", base),
    step("Read from the WAL by Debezium", trace?.capture_ts),
    step("Appended to Kafka", trace?.kafka_ts),
    step("Consumed by the processor", trace?.processed_ts),
    step("Picked up by the dashboard API", trace?.api_ts),
  ];
  const max = Math.max(1, ...steps.map((x) => x.t ?? 0));
  const shown = trace ? trace.browser_ts - mine.c0 : null;   // approximate; mixes browser and server clocks
  const waiting = !trace && now - mine.c1 > 8000;
  return (
    <div className="trace" aria-live="polite">
      <h3><i />Order #{mine.order_id}</h3>
      <ol>
        {steps.map((x, i) => (
          <li key={x.label} className={x.t != null ? "done" : ""}>
            <span className="n">{i + 1}</span>
            <span>{x.label}</span>
            <span className="t">{x.t == null ? "…" : i === 0 ? clock(base) : `+${fmtMs(x.t)}`}</span>
            {i > 0 && x.t != null && <span className="bar"><i style={{ left: 0, width: `${(x.t / max) * 100}%` }} /></span>}
          </li>
        ))}
      </ol>
      <p className="foot">
        {trace
          ? <>Times are measured from the Postgres commit on the server&rsquo;s clock. From your click to this page: about {fmtMs(Math.max(shown, 0))}, including the once-a-second update.</>
          : waiting
            ? "Still waiting. If the consumer is paused, the order is sitting in Kafka and will arrive when it resumes."
            : "Waiting for the change event…"}
      </p>
    </div>
  );
}

// ── alerts ───────────────────────────────────────────────────────
const Icon = {
  critical: <svg viewBox="0 0 20 20" aria-hidden="true"><path d="M6.5 1.5h7l5 5v7l-5 5h-7l-5-5v-7z" fill="var(--critical)" /><path d="M10 5.5v5.5M10 13.6v.4" stroke="#fff" strokeWidth="2.2" strokeLinecap="round" /></svg>,
  warning: <svg viewBox="0 0 20 20" aria-hidden="true"><path d="M10 1.8 19 17.8H1z" fill="var(--warning)" strokeLinejoin="round" /><path d="M10 7v5M10 14.6v.3" stroke="#2a1d00" strokeWidth="2.1" strokeLinecap="round" /></svg>,
  resolved: <svg viewBox="0 0 20 20" aria-hidden="true"><circle cx="10" cy="10" r="8.5" fill="var(--good)" /><path d="m6 10.3 2.7 2.6L14 7.6" stroke="#fff" strokeWidth="2.1" fill="none" strokeLinecap="round" strokeLinejoin="round" /></svg>,
};

function Alerts({ s }) {
  useNow(5000);
  const items = useMemo(() => {
    const open = s.activeAlerts.map((a) => ({ ...a, state: "open" }));
    const seen = new Set(open.map((a) => a.id));
    const resolved = s.alerts.filter((a) => a.state === "resolved" && !seen.has(a.id) && (seen.add(a.id), true));
    return [...open, ...resolved].slice(0, 5);
  }, [s.activeAlerts, s.alerts]);
  if (!items.length) return <p className="empty">No alerts. Try a scenario on the left: the blocking chain raises one within about ten seconds.</p>;
  return (
    <ul className="alerts">
      {items.map((a) => (
        <li key={`${a.id}-${a.state}`} className={`alert ${a.state} ${a.severity}`}>
          {a.state === "open" ? Icon[a.severity] : Icon.resolved}
          <div>
            <div className="title">{a.title}<span className="sev">{a.state === "open" ? (a.severity === "critical" ? "Critical" : "Warning") : "Resolved"}</span></div>
            <div className="detail">{a.detail}</div>
          </div>
          <span className="when">{ago(a.ts)}</span>
        </li>
      ))}
    </ul>
  );
}

function Health({ db }) {
  const d = db || {};
  const sessions = d.sessions || {};
  const orders = (d.dead_tuples || []).find((t) => t.table === "orders");
  const blocked = d.blocked || [];
  return (
    <dl className="health" aria-label="Database health">
      <div><dt>Sessions</dt><dd>{fmtNum(Object.values(sessions).reduce((a, b) => a + b, 0) || null)}
        <small>{sessions.active ?? 0} active, {sessions["idle in transaction"] ?? 0} idle in tx</small></dd></div>
      <div><dt>Blocked sessions</dt><dd className={blocked.length ? "hot" : undefined}>{fmtNum(d.blocked_count)}
        <small>{blocked.length ? `${blocked[0].app || "pid " + blocked[0].pid}, ${fmtSecs(blocked[0].seconds)}` : "Nobody waiting on a lock"}</small></dd></div>
      <div><dt>Oldest transaction</dt><dd>{fmtSecs(d.oldest_xact_s)}<small>{d.oldest_xact_app || "–"}</small></dd></div>
      <div><dt>Replication slot lag</dt><dd>{fmtBytes(d.slot_lag_bytes)}<small>Unconfirmed WAL</small></dd></div>
      <div><dt>Dead tuples in orders</dt><dd>{fmtNum(orders?.dead)}<small>{orders ? `${fmtNum(orders.live)} live rows` : "–"}</small></dd></div>
      <div><dt>Buffer cache hit</dt><dd>{d.cache_hit_pct == null ? "–" : `${fmtNum(d.cache_hit_pct, 1)}%`}<small>XID age {fmtNum(d.xid_age)}</small></dd></div>
    </dl>
  );
}

// ── charts ───────────────────────────────────────────────────────
function Charts({ s }) {
  const m = s.metrics;
  const h = s.history;
  const rate = (v, axis) => (v == null ? "–" : axis ? fmtNum(v, v < 10 && v % 1 ? 1 : 0) : `${fmtNum(v, 2)}/s`);
  const ms = (v, axis) => (v == null ? "–" : axis ? (v >= 1000 ? `${v / 1000}s` : `${Math.round(v)}`) : fmtMs(v));
  const count = (v) => (v == null ? "–" : fmtNum(v));
  return (
    <section className="section">
      <h2>Last three minutes</h2>
      <p className="lede">Computed by the processor once a second from the change stream and the collector&rsquo;s snapshots.</p>
      <div className="charts">
        <LineChart title="Orders per second" sub="10-second rate, against the trailing two-minute baseline"
                   now={<>{fmtNum(m?.throughput?.orders_per_s, 1)}<small>/s</small></>}
                   series={[{ key: "orders", label: "Orders", color: "var(--series-1)" }, { key: "baseline", label: "Baseline", color: "var(--series-2)" }]}
                   data={h} fmt={rate} minMax={3} />
        <LineChart title="Commit to processed" sub="Milliseconds from Postgres commit to the processor, 10-second window"
                   now={<>{fmtMs(m?.latency_ms?.end_to_end?.p95)}<small>p95</small></>}
                   series={[{ key: "e2e_p50", label: "Median", color: "var(--series-1)" }, { key: "e2e_p95", label: "95th percentile", color: "var(--series-2)" }]}
                   data={h} fmt={ms} minMax={200} />
        <LineChart title="Consumer lag" sub="Change events in Kafka the processor hasn&rsquo;t committed yet"
                   now={<>{fmtNum(s.lag.total)}<small>events</small></>}
                   series={[{ key: "lag", label: "Lag", color: "var(--series-1)" }]}
                   data={h} fmt={count} minMax={10} band={{ key: "paused", label: "Consumer paused" }} />
      </div>
    </section>
  );
}

// ── change feed ──────────────────────────────────────────────────
const OPS = { c: "Insert", u: "Update", d: "Delete" };

function summary(f) {
  const r = f.row || {};
  if (f.table === "orders") {
    const st = f.op === "u" && f.before_status && f.before_status !== r.status ? `${f.before_status} to ${r.status}` : r.status;
    return `order ${r.id}, ${st}${r.total != null ? `, $${fmtNum(r.total, 2)}` : ""}${r.source === "visitor" ? ", placed from this page" : ""}`;
  }
  if (f.table === "payments") return `payment for order ${r.order_id}, $${fmtNum(r.amount, 2)} by ${r.method}`;
  return `product ${r.product_id}, ${fmtNum(r.on_hand)} on hand`;
}

function Feed({ s }) {
  const fresh = new Set(s.batch.items.map((f) => f.seq));
  const rows = s.feed.slice(0, 14);
  return (
    <section className="section">
      <h2>Change feed</h2>
      <p className="lede">The newest Debezium events, as read from Kafka. Each one is a row change captured from the write-ahead log.</p>
      <div className="feedwrap">
        <table className="feed">
          <thead>
            <tr><th>Committed</th><th>Topic</th><th className="hide-sm">Partition, offset</th><th>Change</th><th>Row</th><th className="hide-sm">LSN</th></tr>
          </thead>
          <tbody>
            {rows.length === 0 && <tr><td colSpan={6} className="empty">Waiting for the first change event…</td></tr>}
            {rows.map((f) => (
              <tr key={f.seq} className={`${fresh.has(f.seq) ? "fresh" : ""}${f.row?.source === "visitor" ? " visitor" : ""}`}>
                <td className="num">{clock(f.commit_ts)}</td>
                <td><span className="tkey" style={{ "--c": COLOR[f.table] }}><i />{f.table}</span></td>
                <td className="mono hide-sm">p{f.partition} @ {f.offset}</td>
                <td className="op">{OPS[f.op] || f.op}</td>
                <td className="row-summary">{summary(f)}</td>
                <td className="mono hide-sm">{f.lsn ?? "–"}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </section>
  );
}
