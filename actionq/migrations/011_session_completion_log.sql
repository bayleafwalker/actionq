-- #2141: separate durable observation log for session completion alerts.
-- This relation never participates in action claim or settlement state.
CREATE TABLE {{schema}}.session_completion_watermarks (
    singleton                INTEGER PRIMARY KEY CHECK (singleton = 1),
    recovery_floor           BIGINT NOT NULL DEFAULT 0 CHECK (recovery_floor >= 0),
    last_cursor              BIGINT NOT NULL DEFAULT 0 CHECK (last_cursor >= 0),
    last_acknowledged_at     TIMESTAMPTZ,
    retention_seconds        INTEGER NOT NULL DEFAULT 2592000 CHECK (retention_seconds > 0)
);

INSERT INTO {{schema}}.session_completion_watermarks (singleton)
VALUES (1)
ON CONFLICT (singleton) DO NOTHING;

CREATE TABLE {{schema}}.session_completion_events (
    server_cursor            BIGSERIAL PRIMARY KEY,
    event_id                 UUID NOT NULL UNIQUE,
    origin_stream_id         UUID NOT NULL,
    origin_sequence          BIGINT NOT NULL CHECK (origin_sequence >= 1),
    event_digest             TEXT NOT NULL CHECK (event_digest ~ '^sha256:[0-9a-f]{64}$'),
    payload                  JSONB NOT NULL,
    received_at              TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE {{schema}}.session_completion_stream_positions (
    origin_stream_id         UUID NOT NULL,
    origin_sequence          BIGINT NOT NULL CHECK (origin_sequence >= 1),
    -- Kept after event retention so a reused producer position remains a
    -- digest-checked conflict rather than silently becoming a new fact.
    event_id                 UUID NOT NULL UNIQUE,
    server_cursor            BIGINT NOT NULL UNIQUE,
    event_digest             TEXT NOT NULL CHECK (event_digest ~ '^sha256:[0-9a-f]{64}$'),
    PRIMARY KEY (origin_stream_id, origin_sequence)
);

CREATE TABLE {{schema}}.session_completion_acknowledgements (
    -- Acknowledgements are durable independently of event-row retention.
    event_id                 UUID PRIMARY KEY,
    server_cursor            BIGINT NOT NULL,
    event_digest             TEXT NOT NULL CHECK (event_digest ~ '^sha256:[0-9a-f]{64}$'),
    acknowledged_at          TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE {{schema}}.session_completion_quarantine (
    id                       BIGSERIAL PRIMARY KEY,
    event_id                 UUID,
    origin_stream_id         UUID,
    origin_sequence          BIGINT,
    event_digest             TEXT,
    conflict_kind            TEXT NOT NULL,
    summary                  JSONB NOT NULL DEFAULT '{}'::jsonb,
    quarantined_at           TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_session_completion_events_received
    ON {{schema}}.session_completion_events(received_at, server_cursor);
CREATE INDEX idx_session_completion_events_origin
    ON {{schema}}.session_completion_events(origin_stream_id, origin_sequence);
CREATE INDEX idx_session_completion_quarantine_time
    ON {{schema}}.session_completion_quarantine(quarantined_at);
