-- #2034: immutable bounded execution groups projected over ordinary actions.
CREATE TABLE {{schema}}.execution_groups (
    id UUID PRIMARY KEY,
    plan_ref TEXT NOT NULL UNIQUE,
    spec_sha256 TEXT NOT NULL UNIQUE,
    spec_snapshot BYTEA NOT NULL,
    max_parallel INTEGER NOT NULL CHECK (max_parallel BETWEEN 1 AND 32),
    failure_policy TEXT NOT NULL CHECK (failure_policy = 'continue-independent'),
    claim_state TEXT NOT NULL DEFAULT 'active' CHECK (claim_state IN ('active', 'stopped')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    created_by TEXT NOT NULL,
    stopped_at TIMESTAMPTZ,
    stopped_by TEXT,
    stop_reason TEXT
);

CREATE TABLE {{schema}}.execution_group_members (
    group_id UUID NOT NULL REFERENCES {{schema}}.execution_groups(id) ON DELETE RESTRICT,
    action_id BIGINT NOT NULL UNIQUE REFERENCES {{schema}}.actions(id) ON DELETE RESTRICT,
    ordinal INTEGER NOT NULL CHECK (ordinal >= 0),
    envelope_digest TEXT NOT NULL,
    envelope_snapshot BYTEA NOT NULL,
    PRIMARY KEY (group_id, action_id),
    UNIQUE (group_id, ordinal)
);

CREATE INDEX idx_execution_group_members_group
    ON {{schema}}.execution_group_members(group_id);
CREATE INDEX idx_execution_group_members_action
    ON {{schema}}.execution_group_members(action_id);
