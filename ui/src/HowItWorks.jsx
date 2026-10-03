import { REPO } from "./site.js";

export default function HowItWorks() {
  return (
    <main className="wrap">
      <article className="doc">
        <h1>How a row change gets from Postgres to this page</h1>
        <p className="intro">
          Hermes is change data capture (CDC) plus stream processing, in miniature. A shop database takes a steady
          stream of orders. Debezium turns every committed change into an event in Kafka. A consumer turns those
          events into the numbers and alerts on the dashboard. Everything runs as eight containers on one small
          server.
        </p>

        <h2>The path of one order</h2>
        <ol className="steps">
          <li><b>The order commits in PostgreSQL</b>
            The workload, or you, inserts a row into <code>orders</code> and decrements <code>inventory</code> in
            one transaction. Postgres writes the change to its write-ahead log (WAL) before it reports the commit.</li>
          <li><b>Debezium reads it from the WAL</b>
            Debezium runs inside Kafka Connect and holds a logical replication slot (<code>hermes_slot</code>) with
            the built-in <code>pgoutput</code> plugin. Postgres streams decoded changes to it only after commit, so
            rolled-back work never appears. Because the tables use <code>REPLICA IDENTITY FULL</code>, updates carry
            the full before and after images of the row.</li>
          <li><b>The event lands in a Kafka topic</b>
            One topic per table (<code>shop.public.orders</code> and so on), three partitions each, keyed by primary
            key. A key always maps to the same partition, so all changes to one order stay in order.</li>
          <li><b>The processor consumes it</b>
            A Python consumer in the <code>hermes-processor</code> group reads the change topics and a telemetry topic
            the collector fills with Postgres health snapshots every two seconds. Once a second it publishes metrics
            and evaluates alert rules, writing to <code>hermes.metrics</code> and <code>hermes.alerts</code>.</li>
          <li><b>The dashboard API fans it out</b>
            The API consumes the output topics and streams them to your browser with server-sent events. If a proxy
            buffers the stream, the page falls back to polling without losing anything.</li>
        </ol>
        <p>
          The trace under &ldquo;Place an order&rdquo; measures those hops for one real row. The order carries a
          trace ID column, the processor recognizes it, and each stage&rsquo;s timestamp comes from the event
          itself: <code>source.ts_ms</code> for the commit, the envelope&rsquo;s <code>ts_ms</code> for the Debezium
          capture, and the Kafka record timestamp for the append.
        </p>

        <h2>What the scenarios demonstrate</h2>
        <h3>Blocking chain</h3>
        <p>
          A session takes <code>SELECT … FOR UPDATE</code> on the inventory row of the best-selling product and holds
          it. A third of the orders need that row, and the workload processes orders one at a time, so throughput
          collapses. The collector sees the waiting sessions through <code>pg_blocking_pids()</code>. Two alerts open:
          the blocking chain right away, and the throughput drop once orders fall 75% below their two-minute
          baseline for five seconds. This is the hot-row contention pattern behind many real incidents.
        </p>
        <h3>Long transaction</h3>
        <p>
          A <code>REPEATABLE READ</code> transaction keeps its snapshot, so Postgres can&rsquo;t vacuum any row
          version that snapshot might still need. Watch dead tuples in <code>orders</code> climb while it is open,
          then fall when autovacuum catches up. Idle-in-transaction sessions do the same thing in production, and an
          open replication slot can too.
        </p>
        <h3>Paused consumer</h3>
        <p>
          The dashboard sends a command on <code>hermes.control</code>. The processor pauses only its change-topic
          partitions and keeps reading telemetry, so alerts stay live. Debezium keeps writing, so lag (the newest
          offset minus the group&rsquo;s committed offset) grows in every partition. On resume, the consumer reads
          the backlog in batches and lag drains in a few seconds. Nothing is lost, because Kafka retains the events
          independently of who has read them.
        </p>

        <h2>Design decisions</h2>
        <div className="decisions">
          <div><b>Log-based CDC instead of polling the tables</b>
            <p>Reading the WAL catches every change, including deletes and intermediate states, without extra queries on the database and without an <code>updated_at</code> convention.</p></div>
          <div><b>Alerts with hysteresis</b>
            <p>Each rule opens above one threshold and resolves below a lower one, some only after a hold period. A noisy signal near the line produces one alert, not a flapping stream.</p></div>
          <div><b>Replication slot lag is a first-class metric</b>
            <p>A slot whose consumer stops makes Postgres retain WAL until the disk fills. Hermes alerts when Debezium falls more than 32 MB behind.</p></div>
          <div><b>The processing logic has no Kafka in it</b>
            <p>Parsing, windows, percentiles and rules are pure functions over events, unit-tested without a broker. The Kafka loop around them is about a hundred lines.</p></div>
          <div><b>Guard rails for a public demo</b>
            <p>Scenarios are rate-limited per visitor, run one at a time, and end on their own. Postgres also kills sessions idle in a transaction for more than two minutes.</p></div>
        </div>

        <h2>Run it yourself</h2>
        <p>You need Docker with Compose and about 3 GB of free memory.</p>
        <pre><code>{`git clone ${REPO}.git
cd hermes-kafka-cdc
docker compose up -d --build
# open http://localhost:8080 (Debezium needs about a minute to start)`}</code></pre>
        <dl className="facts">
          <dt>Database</dt><dd>PostgreSQL 17, <code>wal_level=logical</code></dd>
          <dt>Capture</dt><dd>Debezium PostgreSQL connector on Kafka Connect</dd>
          <dt>Broker</dt><dd>Apache Kafka, single node in KRaft mode (no ZooKeeper)</dd>
          <dt>Services</dt><dd>Python with confluent-kafka, psycopg 3 and FastAPI</dd>
          <dt>Dashboard</dt><dd>React, with hand-drawn SVG charts and server-sent events</dd>
          <dt>Hosting</dt><dd>One EC2 instance behind CloudFront</dd>
        </dl>
      </article>
    </main>
  );
}
