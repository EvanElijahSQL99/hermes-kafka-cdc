export const fmtNum = (v, digits = 0) =>
  v == null || Number.isNaN(v) ? "–" : Number(v).toLocaleString("en-US", { maximumFractionDigits: digits, minimumFractionDigits: digits });

export const fmtMs = (v) => {
  if (v == null) return "–";
  if (v >= 10_000) return `${(v / 1000).toFixed(0)} s`;
  if (v >= 1000) return `${(v / 1000).toFixed(1)} s`;
  return `${Math.round(v)} ms`;
};

export const fmtBytes = (b) => {
  if (b == null) return "–";
  if (b < 1024) return `${b} B`;
  if (b < 1024 ** 2) return `${(b / 1024).toFixed(1)} KB`;
  if (b < 1024 ** 3) return `${(b / 1024 ** 2).toFixed(1)} MB`;
  return `${(b / 1024 ** 3).toFixed(2)} GB`;
};

export const fmtCompact = (v) =>
  v == null ? "–" : Intl.NumberFormat("en-US", { notation: "compact", maximumFractionDigits: 1 }).format(v);

export const fmtSecs = (s) => {
  if (s == null) return "–";
  if (s < 1) return "under 1 s";
  if (s < 90) return `${Math.round(s)} s`;
  return `${Math.floor(s / 60)} min ${Math.round(s % 60)} s`;
};

export const clock = (ts) =>
  ts ? new Date(ts).toLocaleTimeString("en-US", { hour12: false, hour: "2-digit", minute: "2-digit", second: "2-digit" }) : "";

export const ago = (ts, now = Date.now()) => {
  const s = Math.max(0, Math.round((now - ts) / 1000));
  if (s < 5) return "just now";
  if (s < 60) return `${s} s ago`;
  return `${Math.floor(s / 60)} min ago`;
};
