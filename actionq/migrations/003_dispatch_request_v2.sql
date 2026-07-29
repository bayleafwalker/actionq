-- Durable Action-root dispatch enqueue contract v2.  The bytea column is the
-- authoritative canonical request representation; jsonb is intentionally not
-- used for the snapshot because it cannot preserve serialization bytes.
CREATE TABLE IF NOT EXISTS {{schema}}.dispatch_requests (
    action_id               BIGINT      PRIMARY KEY REFERENCES {{schema}}.actions(id),
    request_ref             TEXT        NOT NULL UNIQUE,
    normalized_snapshot     BYTEA       NOT NULL,
    schema_version          TEXT        NOT NULL CHECK (schema_version = 'v2'),
    canonicalization_version TEXT       NOT NULL,
    request_sha256          TEXT        NOT NULL CHECK (request_sha256 ~ '^[a-f0-9]{64}$'),
    identity                TEXT        NOT NULL,
    environment             TEXT        NOT NULL,
    operation               TEXT        NOT NULL CHECK (operation = 'execution.dispatch.enqueue'),
    idempotency_key         TEXT        NOT NULL,
    created_at              TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (identity, environment, operation, idempotency_key)
);

CREATE INDEX IF NOT EXISTS idx_dispatch_requests_created
    ON {{schema}}.dispatch_requests(created_at, action_id);
