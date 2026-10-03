import { useEffect, useRef } from "react";
import { fmtBytes, fmtMs, fmtNum } from "./format.js";

export const TABLES = [
  { key: "orders", label: "orders", color: "var(--t-orders)" },
  { key: "payments", label: "payments", color: "var(--t-payments)" },
  { key: "inventory", label: "inventory", color: "var(--t-inventory)" },
];
const COLOR = Object.fromEntries(TABLES.map((t) => [t.key, t.color]));
const HOP = 380;          // ms per wire
const MAX_PER_BATCH = 24; // keep the animation legible when traffic spikes

function launch(wire, color, visitor, delay) {
  if (!wire) return;
  setTimeout(() => {
    const p = document.createElement("span");
    p.className = visitor ? "particle visitor" : "particle";
    p.style.setProperty("--c", color);
    p.style.setProperty("--d", `${HOP}ms`);
    p.addEventListener("animationend", () => p.remove());
    wire.appendChild(p);
  }, delay);
}

/** The live pipeline: Postgres → Debezium → Kafka (topics × partitions) → processor → this page. */
export default function Pipeline({ s }) {
  const wires = useRef([]);
  const cells = useRef({});
  const m = s.metrics;
  const db = m?.db || {};
  const paused = !!m?.paused;

  useEffect(() => {
    if (window.matchMedia?.("(prefers-reduced-motion: reduce)").matches) return;
    const items = s.batch.items.slice(-MAX_PER_BATCH);
    items.forEach((ev) => {
      const visitor = ev.row?.source === "visitor";
      const color = visitor ? "var(--visitor)" : COLOR[ev.table] || "var(--flow)";
      const start = visitor ? 0 : Math.random() * 850;
      launch(wires.current[0], color, visitor, start);
      launch(wires.current[1], color, visitor, start + HOP);
      setTimeout(() => {
        const cell = cells.current[`${ev.table}-${ev.partition}`];
        if (cell) { cell.classList.add("hit"); setTimeout(() => cell.classList.remove("hit"), 140); }
      }, start + 2 * HOP);
      if (!paused) {
        launch(wires.current[2], color, visitor, start + 2 * HOP + 60);
        launch(wires.current[3], color, visitor, start + 3 * HOP + 60);
      }
    });
  }, [s.batch.key]); // eslint-disable-line react-hooks/exhaustive-deps

  const parts = Object.fromEntries((s.lag.partitions || []).map((p) => [`${p.topic.split(".").pop()}-${p.partition}`, p]));
  const maxLag = Math.max(1, ...(s.lag.partitions || []).map((p) => p.lag));
  const lat = m?.latency_ms || {};
  const slot = db.slot_lag_bytes;

  return (
    <section className="pipeline" aria-label="Live pipeline">
      <div className="stage">
        <h3>PostgreSQL 17</h3>
        <div className="role">shop-db, with an order workload running against it</div>
        <div className="big">{fmtNum(db.commits_per_s, 1)}<small>commits/s</small></div>
        <div className="sub">{fmtNum(m?.throughput?.orders_per_s, 1)} orders/s, {db.sessions?.active ?? "–"} active sessions</div>
      </div>
      <div className="wire" ref={(el) => (wires.current[0] = el)}><span className="lbl">write-ahead log</span></div>
      <div className="stage">
        <h3>Debezium</h3>
        <div className="role">Kafka Connect source, reading the WAL through a replication slot</div>
        <div className="big">{fmtMs(lat.commit_to_capture?.p50)}</div>
        <div className="sub">commit to capture, median. Slot {slot == null ? "–" : fmtBytes(slot)} behind</div>
      </div>
      <div className="wire" ref={(el) => (wires.current[1] = el)}><span className="lbl">change events</span></div>
      <div className="stage kafka">
        <h3>Kafka</h3>
        <div className="role">One topic per table, three partitions each. Cells show the newest offset.</div>
        <div className="topics" role="table" aria-label="Kafka partitions">
          <span role="columnheader" />
          {[0, 1, 2].map((p) => <span key={p} className="phead" role="columnheader">p{p}</span>)}
          {TABLES.map((t) => (
            <Row key={t.key} t={t} parts={parts} maxLag={maxLag} cells={cells} />
          ))}
        </div>
        <div className="legend-note">
          {s.lag.total ? <>Consumer lag: <b className="num">{fmtNum(s.lag.total)}</b> events waiting</> : "Consumer lag: none, the processor is keeping up"}
        </div>
      </div>
      <div className="wire" ref={(el) => (wires.current[2] = el)}><span className="lbl">consumer group</span></div>
      <div className={`stage${paused ? " paused" : ""}`}>
        <h3>Processor</h3>
        <div className="role">Python consumer: latency, funnel, alert rules</div>
        {paused && <span className="flag">Paused</span>}
        <div className="big">{fmtNum(m?.throughput?.events_per_s, 1)}<small>events/s</small></div>
        <div className="sub">capture to processed, median {fmtMs(lat.capture_to_process?.p50)}</div>
      </div>
      <div className="wire" ref={(el) => (wires.current[3] = el)}><span className="lbl">server-sent events</span></div>
      <div className="stage">
        <h3>This page</h3>
        <div className="role">Metrics, alerts and the change feed</div>
        <div className="big">{fmtMs(lat.end_to_end?.p95)}</div>
        <div className="sub">commit to processed, 95th percentile</div>
      </div>
    </section>
  );
}

function Row({ t, parts, maxLag, cells }) {
  return (
    <>
      <span className="tname" role="rowheader" style={{ "--c": t.color }}><i />{t.label}</span>
      {[0, 1, 2].map((p) => {
        const info = parts[`${t.key}-${p}`];
        const lag = info?.lag || 0;
        return (
          <span key={p} role="cell" ref={(el) => (cells.current[`${t.key}-${p}`] = el)}
                className={`cell${lag ? " lagging" : ""}`}
                title={info ? `${t.label} partition ${p}: end offset ${info.end}, processor at ${info.committed}, lag ${lag}` : undefined}>
            {lag ? `+${fmtNum(lag)}` : info ? fmtNum(info.end) : "–"}
            <span className="lagbar" style={{ width: `${(lag / maxLag) * 100}%` }} />
          </span>
        );
      })}
    </>
  );
}
