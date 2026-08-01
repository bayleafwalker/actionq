-- #2035: durable, byte-authoritative compiler requests for ordinary actions.
-- CAS locators are transport addresses only and PostgreSQL snapshots below
-- are the authority consulted by claim-time validation.
CREATE TABLE {{schema}}.immutable_action_specs (
    spec_ref TEXT PRIMARY KEY CHECK (spec_ref ~ '^artifact:sha256:[0-9a-f]{64}$'),
    spec_digest TEXT NOT NULL UNIQUE CHECK (spec_digest ~ '^sha256:[0-9a-f]{64}$'),
    spec_snapshot BYTEA NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CHECK (substring(spec_ref FROM 17) = substring(spec_digest FROM 8))
);

CREATE TABLE {{schema}}.immutable_action_requests (
    action_id BIGINT PRIMARY KEY REFERENCES {{schema}}.actions(id) ON DELETE RESTRICT,
    request_ref TEXT NOT NULL UNIQUE CHECK (request_ref ~ '^artifact:sha256:[0-9a-f]{64}$'),
    request_digest TEXT NOT NULL UNIQUE CHECK (request_digest ~ '^sha256:[0-9a-f]{64}$'),
    request_snapshot BYTEA NOT NULL,
    plan_ref TEXT NOT NULL CHECK (plan_ref ~ '^artifact:sha256:[0-9a-f]{64}$'),
    topology TEXT NOT NULL CHECK (topology IN ('independent', 'stacked', 'wave-integrated')),
    role TEXT NOT NULL CHECK (role IN ('candidate-verification', 'candidate-integration', 'candidate-review')),
    subject TEXT NOT NULL,
    spec_ref TEXT NOT NULL REFERENCES {{schema}}.immutable_action_specs(spec_ref) ON DELETE RESTRICT,
    spec_digest TEXT NOT NULL CHECK (spec_digest ~ '^sha256:[0-9a-f]{64}$'),
    input_set_digest TEXT NOT NULL CHECK (input_set_digest ~ '^sha256:[0-9a-f]{64}$'),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (plan_ref, topology, role, subject),
    CHECK (substring(request_ref FROM 17) = substring(request_digest FROM 8)),
    CHECK (substring(spec_ref FROM 17) = substring(spec_digest FROM 8))
);

CREATE TABLE {{schema}}.immutable_action_runtime_grants (
    action_id BIGINT PRIMARY KEY REFERENCES {{schema}}.actions(id) ON DELETE RESTRICT,
    request_digest TEXT NOT NULL CHECK (request_digest ~ '^sha256:[0-9a-f]{64}$'),
    spec_digest TEXT NOT NULL CHECK (spec_digest ~ '^sha256:[0-9a-f]{64}$'),
    input_set_digest TEXT NOT NULL CHECK (input_set_digest ~ '^sha256:[0-9a-f]{64}$'),
    grant_digest TEXT NOT NULL CHECK (grant_digest ~ '^sha256:[0-9a-f]{64}$'),
    issued_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_immutable_action_requests_created
    ON {{schema}}.immutable_action_requests(created_at, action_id);
