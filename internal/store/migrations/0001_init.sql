-- +goose Up
CREATE TABLE applications (
  id           text PRIMARY KEY,
  name         text NOT NULL,
  api_key_hash bytea NOT NULL UNIQUE,
  created_at   timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE endpoints (
  id                   text PRIMARY KEY,
  application_id       text NOT NULL REFERENCES applications (id) ON DELETE CASCADE,
  url                  text NOT NULL,
  secret               text NOT NULL,
  event_types          text[] NOT NULL DEFAULT '{}',
  status               text NOT NULL DEFAULT 'enabled',
  consecutive_failures int NOT NULL DEFAULT 0,
  created_at           timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX endpoints_app ON endpoints (application_id);

CREATE TABLE messages (
  id              text PRIMARY KEY,
  application_id  text NOT NULL REFERENCES applications (id) ON DELETE CASCADE,
  event_type      text NOT NULL,
  payload         jsonb NOT NULL,
  idempotency_key text,
  created_at      timestamptz NOT NULL DEFAULT now()
);
CREATE UNIQUE INDEX messages_idem ON messages (application_id, idempotency_key)
  WHERE idempotency_key IS NOT NULL;

CREATE TABLE deliveries (
  id               text PRIMARY KEY,
  message_id       text NOT NULL REFERENCES messages (id) ON DELETE CASCADE,
  endpoint_id      text NOT NULL REFERENCES endpoints (id) ON DELETE CASCADE,
  status           text NOT NULL DEFAULT 'pending',
  attempt_count    int NOT NULL DEFAULT 0,
  next_attempt_at  timestamptz NOT NULL DEFAULT now(),
  claimed_at       timestamptz,
  last_enqueued_at timestamptz,
  traceparent      text,
  last_status_code int,
  last_error       text,
  dead_reason      text,
  created_at       timestamptz NOT NULL DEFAULT now(),
  succeeded_at     timestamptz
);
CREATE INDEX deliveries_due ON deliveries (next_attempt_at) WHERE status IN ('pending','failed');
CREATE INDEX deliveries_endpoint_status ON deliveries (endpoint_id, status);
CREATE INDEX deliveries_message ON deliveries (message_id);

CREATE TABLE delivery_attempts (
  id               text PRIMARY KEY,
  delivery_id      text NOT NULL REFERENCES deliveries (id) ON DELETE CASCADE,
  attempted_at     timestamptz NOT NULL DEFAULT now(),
  duration_ms      int NOT NULL,
  status_code      int,
  error            text,
  response_snippet text
);
CREATE INDEX attempts_delivery ON delivery_attempts (delivery_id);

-- +goose Down
DROP TABLE delivery_attempts;
DROP TABLE deliveries;
DROP TABLE messages;
DROP TABLE endpoints;
DROP TABLE applications;
