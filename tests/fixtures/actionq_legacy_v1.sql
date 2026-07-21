-- actionq schema v1.
-- The runtime migrator applies this shape to ACTIONQ_SCHEMA, default `actionq`.

CREATE TABLE actions (
    id              BIGSERIAL PRIMARY KEY,
    action_type     TEXT        NOT NULL,
    project         TEXT,
    target_ref      TEXT,
    source_refs     JSONB       NOT NULL DEFAULT '[]'::jsonb,
    priority        INTEGER     NOT NULL DEFAULT 100,
    status          TEXT        NOT NULL DEFAULT 'pending'
                                CHECK (status IN ('pending', 'claimed', 'completed', 'failed', 'rejected', 'cancelled')),
    parent_id       BIGINT      REFERENCES actions(id),
    chain_depth     INTEGER     NOT NULL DEFAULT 0,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    claimed_at      TIMESTAMPTZ,
    claimed_by      TEXT,
    claim_deadline  TIMESTAMPTZ,
    completed_at    TIMESTAMPTZ,
    result_ref      TEXT,
    failure_reason  TEXT,
    created_by      TEXT        NOT NULL
);

CREATE TABLE events (
    id          BIGSERIAL   PRIMARY KEY,
    action_id   BIGINT      REFERENCES actions(id),
    event_type  TEXT        NOT NULL,
    timestamp   TIMESTAMPTZ NOT NULL DEFAULT now(),
    actor       TEXT,
    payload     JSONB       NOT NULL DEFAULT '{}'::jsonb
);

CREATE INDEX idx_actions_claim_lookup ON actions(status, priority, created_at)
    WHERE status = 'pending';
CREATE INDEX idx_actions_parent ON actions(parent_id);
CREATE INDEX idx_actions_project ON actions(project);
CREATE INDEX idx_actions_deadline ON actions(claim_deadline)
    WHERE status = 'claimed';
CREATE INDEX idx_events_action ON events(action_id);
CREATE INDEX idx_events_timestamp ON events(timestamp DESC);
CREATE INDEX idx_events_type_time ON events(event_type, timestamp DESC);
