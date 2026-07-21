-- actionq execution-domain schema v1.
-- Applied only by the deployment migration entrypoint. {{schema}} is replaced
-- with a validated, quoted ACTIONQ_SCHEMA identifier by actionq.schema.

CREATE TABLE IF NOT EXISTS {{schema}}.actions (
    id              BIGSERIAL PRIMARY KEY,
    action_type     TEXT        NOT NULL,
    project         TEXT,
    target_ref      TEXT,
    source_refs     JSONB       NOT NULL DEFAULT '[]'::jsonb,
    priority        INTEGER     NOT NULL DEFAULT 100,
    status          TEXT        NOT NULL DEFAULT 'pending'
                                CHECK (status IN ('pending', 'claimed', 'completed', 'failed', 'rejected', 'cancelled')),
    parent_id       BIGINT      REFERENCES {{schema}}.actions(id),
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

CREATE TABLE IF NOT EXISTS {{schema}}.events (
    id          BIGSERIAL   PRIMARY KEY,
    action_id   BIGINT      REFERENCES {{schema}}.actions(id),
    event_type  TEXT        NOT NULL,
    timestamp   TIMESTAMPTZ NOT NULL DEFAULT now(),
    actor       TEXT,
    payload     JSONB       NOT NULL DEFAULT '{}'::jsonb
);

CREATE INDEX IF NOT EXISTS idx_actionq_actions_claim_lookup
    ON {{schema}}.actions(status, priority, created_at)
    WHERE status = 'pending';
CREATE INDEX IF NOT EXISTS idx_actionq_actions_parent ON {{schema}}.actions(parent_id);
CREATE INDEX IF NOT EXISTS idx_actionq_actions_project ON {{schema}}.actions(project);
CREATE INDEX IF NOT EXISTS idx_actionq_actions_deadline ON {{schema}}.actions(claim_deadline)
    WHERE status = 'claimed';
CREATE INDEX IF NOT EXISTS idx_actionq_events_action ON {{schema}}.events(action_id);
CREATE INDEX IF NOT EXISTS idx_actionq_events_timestamp ON {{schema}}.events(timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_actionq_events_type_time ON {{schema}}.events(event_type, timestamp DESC);
