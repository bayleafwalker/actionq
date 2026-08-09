-- Frozen actionq.action-resource-owner/v1 projection storage.
CREATE TABLE {{schema}}.action_resources (
    resource_ref TEXT PRIMARY KEY CHECK (resource_ref ~ '^aqr1_[A-Za-z0-9_-]{43}$'),
    action_id BIGINT NOT NULL UNIQUE REFERENCES {{schema}}.actions(id) ON DELETE RESTRICT,
    principal_scope TEXT NOT NULL,
    operation TEXT NOT NULL,
    idempotency_key TEXT NOT NULL,
    request_digest TEXT NOT NULL CHECK (request_digest ~ '^[a-f0-9]{64}$'),
    request_snapshot BYTEA NOT NULL,
    revision BIGINT NOT NULL CHECK (revision >= 1),
    enqueue_revision BIGINT NOT NULL CHECK (enqueue_revision = 1),
    recovery_floor BIGINT NOT NULL DEFAULT 0 CHECK (recovery_floor >= 0 AND recovery_floor <= revision),
    state TEXT NOT NULL CHECK (state IN ('pending','claimed','completed','failed','rejected','cancelled')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (principal_scope, operation, idempotency_key)
);

CREATE TABLE {{schema}}.action_resource_changes (
    resource_ref TEXT NOT NULL REFERENCES {{schema}}.action_resources(resource_ref) ON DELETE RESTRICT,
    revision BIGINT NOT NULL CHECK (revision >= 1),
    kind TEXT NOT NULL CHECK (kind = 'state_changed'),
    state TEXT NOT NULL CHECK (state IN ('pending','claimed','completed','failed','rejected','cancelled')),
    occurred_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (resource_ref, revision)
);

CREATE TABLE {{schema}}.action_resource_sessions (
    resource_ref TEXT NOT NULL REFERENCES {{schema}}.action_resources(resource_ref) ON DELETE RESTRICT,
    ordinal INTEGER NOT NULL CHECK (ordinal >= 1),
    state TEXT NOT NULL,
    started_at TIMESTAMPTZ,
    ended_at TIMESTAMPTZ,
    PRIMARY KEY (resource_ref, ordinal)
);

CREATE INDEX idx_action_resource_changes_lookup
    ON {{schema}}.action_resource_changes(resource_ref, revision);
