import { useEffect, useState } from "react";
import Dashboard from "./Dashboard.jsx";
import HowItWorks from "./HowItWorks.jsx";
import { MOCK, useHermes } from "./useHermes.js";

import { REPO } from "./site.js";

const ROUTES = { "/": "Live pipeline", "/how-it-works": "How it works" };

function usePath() {
  const [path, setPath] = useState(window.location.pathname in ROUTES ? window.location.pathname : "/");
  useEffect(() => {
    const on = () => setPath(window.location.pathname in ROUTES ? window.location.pathname : "/");
    window.addEventListener("popstate", on);
    return () => window.removeEventListener("popstate", on);
  }, []);
  const go = (p) => (e) => {
    if (e.metaKey || e.ctrlKey || e.shiftKey) return;
    e.preventDefault();
    window.history.pushState({}, "", p + window.location.search);
    setPath(p);
    window.scrollTo(0, 0);
  };
  return [path, go];
}

const STATUS = {
  connecting: "Connecting…",
  live: "Live",
  polling: "Live, updating every 1.5 s",
  offline: "Can't reach the server. Retrying",
};

export default function App() {
  const s = useHermes();
  const [path, go] = usePath();
  useEffect(() => {
    document.title = path === "/" ? "Hermes: live database telemetry on Kafka" : "How Hermes works";
  }, [path]);

  return (
    <>
      <header className="wrap top">
        <a className="brand" href="/" onClick={go("/")}>
          <svg viewBox="0 0 32 32" aria-hidden="true"><rect width="32" height="32" rx="7" fill="var(--ink)" /><path d="M7 21h18M7 16h12M7 11h7" stroke="var(--flow-soft)" strokeWidth="2.6" strokeLinecap="round" /><circle cx="25" cy="11" r="2.6" fill="var(--visitor)" /></svg>
          Hermes
        </a>
        <nav className="nav" aria-label="Pages">
          {Object.entries(ROUTES).map(([p, label]) => (
            <a key={p} href={p} onClick={go(p)} aria-current={path === p ? "page" : undefined}>{label}</a>
          ))}
        </nav>
        <span className="spacer" />
        <span className={`conn ${s.status}`} role="status"><i />{MOCK ? "Simulated data" : STATUS[s.status]}</span>
        <a className="gh" href={REPO} target="_blank" rel="noreferrer">Source on GitHub</a>
      </header>

      {path === "/" ? <Dashboard s={s} /> : <HowItWorks />}

      <footer className="wrap foot">
        <span>Hermes is a portfolio project by Evan Elijah.</span>
        <span>PostgreSQL 17, Debezium 3, Apache Kafka in KRaft mode, Python, React.</span>
        <a href={REPO} target="_blank" rel="noreferrer">Source and run-it-yourself instructions</a>
      </footer>
    </>
  );
}
