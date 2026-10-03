-- Hermes demo database: a small "shop" OLTP schema that Debezium captures.
-- Runs once, on first container start (docker-entrypoint-initdb.d).

CREATE TABLE customers (
  id      serial PRIMARY KEY,
  name    text NOT NULL,
  region  text NOT NULL
);

CREATE TABLE inventory (
  product_id  serial PRIMARY KEY,
  sku         text NOT NULL UNIQUE,
  name        text NOT NULL,
  price       numeric(10,2) NOT NULL,
  on_hand     integer NOT NULL,
  updated_at  timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE orders (
  id           bigserial PRIMARY KEY,
  customer_id  integer NOT NULL REFERENCES customers(id),
  product_id   integer NOT NULL REFERENCES inventory(product_id),
  quantity     integer NOT NULL,
  total        numeric(12,2) NOT NULL,
  status       text NOT NULL DEFAULT 'placed',   -- placed → paid → shipped
  source       text NOT NULL DEFAULT 'workload', -- 'visitor' = placed from the dashboard
  trace_id     uuid,                             -- set for visitor orders; follows the row through the pipeline
  created_at   timestamptz NOT NULL DEFAULT now(),
  updated_at   timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX orders_status_created ON orders (status, created_at);

CREATE TABLE payments (
  id          bigserial PRIMARY KEY,
  order_id    bigint NOT NULL REFERENCES orders(id) ON DELETE CASCADE,
  amount      numeric(12,2) NOT NULL,
  method      text NOT NULL,
  created_at  timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX payments_order ON payments (order_id);

-- Full row images on UPDATE/DELETE so CDC events carry the "before" state too.
ALTER TABLE orders    REPLICA IDENTITY FULL;
ALTER TABLE inventory REPLICA IDENTITY FULL;

INSERT INTO customers (name, region)
SELECT 'Customer ' || g,
       (ARRAY['us-east','us-west','eu-central','ap-south'])[1 + (g % 4)]
FROM generate_series(1, 200) g;

INSERT INTO inventory (sku, name, price, on_hand)
SELECT 'SKU-' || lpad(g::text, 4, '0'),
       (ARRAY['Widget','Gadget','Sprocket','Gizmo','Doohickey','Flange','Valve','Bracket'])[1 + (g % 8)] || ' ' || g,
       round((5 + random() * 195)::numeric, 2),
       500 + (random() * 500)::int
FROM generate_series(1, 40) g;
