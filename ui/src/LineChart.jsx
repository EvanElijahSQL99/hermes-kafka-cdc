import { useEffect, useMemo, useRef, useState } from "react";
import { clock } from "./format.js";

const H = 140, AXIS = 20, PAD_L = 40, PAD_R = 8, WINDOW = 180_000;

function niceMax(v) {
  if (v <= 0) return 1;
  const exp = 10 ** Math.floor(Math.log10(v));
  for (const m of [1, 1.5, 2, 2.5, 3, 4, 5, 6, 8, 10]) if (m * exp >= v) return m * exp;
  return 10 * exp;
}

/**
 * One measure per chart (never two y-scales). Series share the unit; a single series gets no legend.
 * Hover or arrow keys move a crosshair; "Show table" gives the same numbers without hovering.
 */
export default function LineChart({ title, sub, now, series, data, fmt, minMax = 1, band }) {
  const box = useRef(null);
  const [w, setW] = useState(360);
  const [hover, setHover] = useState(null);
  const [table, setTable] = useState(false);

  useEffect(() => {
    const ro = new ResizeObserver(([e]) => setW(Math.max(220, e.contentRect.width)));
    ro.observe(box.current);
    return () => ro.disconnect();
  }, []);

  const end = data.at(-1)?.ts ?? Date.now();
  const start = end - WINDOW;
  const pts = data.filter((d) => d.ts >= start);
  const max = niceMax(Math.max(minMax, ...pts.flatMap((d) => series.map((s) => d[s.key] ?? 0))));
  const x = (t) => PAD_L + ((t - start) / WINDOW) * (w - PAD_L - PAD_R);
  const y = (v) => H - (v / max) * (H - 8);

  const paths = useMemo(() => series.map((s) => {
    let d = "", pen = false;
    for (const p of pts) {
      const v = p[s.key];
      if (v == null) { pen = false; continue; }
      d += `${pen ? "L" : "M"}${x(p.ts).toFixed(1)},${y(v).toFixed(1)}`;
      pen = true;
    }
    return d;
  }), [pts, series, w, max]); // eslint-disable-line react-hooks/exhaustive-deps

  // Shaded spans (e.g. while the consumer was paused).
  const bands = [];
  if (band) {
    let open = null;
    for (const p of pts) {
      if (p[band.key] && open == null) open = p.ts;
      if (!p[band.key] && open != null) { bands.push([open, p.ts]); open = null; }
    }
    if (open != null) bands.push([open, end]);
  }

  const idx = hover == null ? null : Math.min(hover, pts.length - 1);
  const hp = idx != null ? pts[idx] : null;

  const pick = (clientX) => {
    const r = box.current.getBoundingClientRect();
    const t = start + ((clientX - r.left - PAD_L) / (w - PAD_L - PAD_R)) * WINDOW;
    let best = 0;
    pts.forEach((p, i) => { if (Math.abs(p.ts - t) < Math.abs(pts[best].ts - t)) best = i; });
    setHover(best);
  };
  const onKey = (e) => {
    if (!pts.length) return;
    if (e.key === "ArrowLeft") { setHover((h) => Math.max(0, (h ?? pts.length) - 1)); e.preventDefault(); }
    if (e.key === "ArrowRight") { setHover((h) => Math.min(pts.length - 1, (h ?? pts.length - 2) + 1)); e.preventDefault(); }
    if (e.key === "Escape") setHover(null);
  };

  const ticks = [0, max / 2, max];

  return (
    <figure className="chart" style={{ margin: 0 }}>
      <header>
        <h3>{title}</h3>
        <span className="now">{now}</span>
      </header>
      <p className="sub">{sub}</p>
      {(series.length > 1 || band) && (
        <div className="legend">
          {series.length > 1 && series.map((s) => <span key={s.key}><i style={{ "--c": s.color }} />{s.label}</span>)}
          {band && <span><i className="band" />{band.label}</span>}
        </div>
      )}
      <div ref={box} style={{ position: "relative" }}>
        <svg height={H + AXIS} width={w} role="img" tabIndex={0} onKeyDown={onKey}
             aria-label={`${title}. Use the left and right arrow keys to read values, or show the table.`}
             onPointerMove={(e) => pick(e.clientX)} onPointerLeave={() => setHover(null)} onBlur={() => setHover(null)}>
          {bands.map(([a, b], i) => <rect key={i} className="pausedband" x={x(a)} y={0} width={Math.max(2, x(b) - x(a))} height={H} />)}
          {ticks.map((t) => (
            <g key={t}>
              <line className="gridline" x1={PAD_L} x2={w - PAD_R} y1={y(t)} y2={y(t)} />
              <text className="axis" x={PAD_L - 6} y={y(t) + 4} textAnchor="end">{fmt(t, true)}</text>
            </g>
          ))}
          {[180, 120, 60, 0].map((s) => (
            <text key={s} className="axis" x={x(end - s * 1000)} y={H + 15}
                  textAnchor={s === 180 ? "start" : s === 0 ? "end" : "middle"}>{s ? `−${s / 60} min` : "now"}</text>
          ))}
          {paths.map((d, i) => <path key={series[i].key} className="line" d={d} stroke={series[i].color} />)}
          {series.map((s) => {
            const last = [...pts].reverse().find((p) => p[s.key] != null);
            return last && <circle key={s.key} cx={x(last.ts)} cy={y(last[s.key])} r={4} fill={s.color} stroke="var(--surface)" strokeWidth={2} />;
          })}
          {hp && (
            <g>
              <line className="cross" x1={x(hp.ts)} x2={x(hp.ts)} y1={0} y2={H} />
              {series.map((s) => hp[s.key] != null &&
                <circle key={s.key} cx={x(hp.ts)} cy={y(hp[s.key])} r={4} fill={s.color} stroke="var(--surface)" strokeWidth={2} />)}
            </g>
          )}
        </svg>
        {hp && (
          <div className="tip" style={{ left: Math.min(Math.max(x(hp.ts), 70), w - 70), top: 0 }}>
            <div style={{ opacity: 0.75 }}>{clock(hp.ts)}{band && hp[band.key] ? `, ${band.label.toLowerCase()}` : ""}</div>
            {series.map((s) => (
              <div key={s.key}><span className="k" style={{ "--c": s.color }} /><b>{fmt(hp[s.key])}</b> {series.length > 1 ? s.label : ""}</div>
            ))}
          </div>
        )}
      </div>
      <button className="linkbtn" onClick={() => setTable((t) => !t)} aria-expanded={table}>
        {table ? "Hide table" : "Show table"}
      </button>
      {table && (
        <table className="data" style={{ marginTop: 6 }}>
          <thead><tr><th>Time</th>{series.map((s) => <th key={s.key}>{s.label}</th>)}</tr></thead>
          <tbody>
            {pts.slice(-10).reverse().map((p) => (
              <tr key={p.ts}><td>{clock(p.ts)}</td>{series.map((s) => <td key={s.key}>{fmt(p[s.key])}</td>)}</tr>
            ))}
          </tbody>
        </table>
      )}
    </figure>
  );
}
