CREATE TABLE {{schema}}.federation_resources (
    resource_ref TEXT PRIMARY KEY,
    owner_principal_id TEXT NOT NULL,
    state TEXT NOT NULL CHECK (state IN ('registered', 'evidence-recorded', 'accepted', 'rejected', 'superseded')),
    revision BIGINT NOT NULL CHECK (revision > 0),
    recovery_floor BIGINT NOT NULL DEFAULT 0 CHECK (recovery_floor = 0),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE {{schema}}.federation_resource_changes (
    resource_ref TEXT NOT NULL REFERENCES {{schema}}.federation_resources(resource_ref) ON DELETE RESTRICT,
    revision BIGINT NOT NULL CHECK (revision > 0),
    operation TEXT NOT NULL,
    state TEXT NOT NULL CHECK (state IN ('registered', 'evidence-recorded', 'accepted', 'rejected', 'superseded')),
    actor_principal_id TEXT NOT NULL,
    payload_bytes BYTEA NOT NULL,
    payload_digest TEXT NOT NULL CHECK (payload_digest ~ '^sha256:[0-9a-f]{64}$'),
    occurred_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (resource_ref, revision)
);

CREATE TABLE {{schema}}.federation_relations (
    source_ref TEXT NOT NULL REFERENCES {{schema}}.federation_resources(resource_ref) ON DELETE RESTRICT,
    relation_type TEXT NOT NULL CHECK (relation_type IN ('parent-of', 'depends-on', 'derived-from', 'supersedes')),
    target_ref TEXT NOT NULL REFERENCES {{schema}}.federation_resources(resource_ref) ON DELETE RESTRICT,
    source_revision BIGINT NOT NULL CHECK (source_revision > 0),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (source_ref, relation_type, target_ref),
    CHECK (source_ref <> target_ref)
);

CREATE TABLE {{schema}}.federation_execution_refs (
    resource_ref TEXT NOT NULL REFERENCES {{schema}}.federation_resources(resource_ref) ON DELETE RESTRICT,
    execution_ref TEXT NOT NULL,
    assurance_type TEXT NOT NULL,
    source_revision BIGINT NOT NULL CHECK (source_revision > 0),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (resource_ref, execution_ref)
);

CREATE TABLE {{schema}}.federation_evidence (
    resource_ref TEXT NOT NULL REFERENCES {{schema}}.federation_resources(resource_ref) ON DELETE RESTRICT,
    evidence_ref TEXT NOT NULL,
    evidence_digest TEXT NOT NULL CHECK (evidence_digest ~ '^sha256:[0-9a-f]{64}$'),
    assurance_type TEXT NOT NULL,
    source_revision BIGINT NOT NULL CHECK (source_revision > 0),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (resource_ref, evidence_ref)
);

CREATE TABLE {{schema}}.federation_acceptance_decisions (
    resource_ref TEXT NOT NULL REFERENCES {{schema}}.federation_resources(resource_ref) ON DELETE RESTRICT,
    source_revision BIGINT NOT NULL CHECK (source_revision > 0),
    outcome TEXT NOT NULL CHECK (outcome IN ('accepted', 'rejected')),
    policy_ref TEXT NOT NULL,
    evidence_ref TEXT,
    decided_by TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (resource_ref, source_revision)
);

CREATE TABLE {{schema}}.federation_settlements (
    resource_ref TEXT NOT NULL REFERENCES {{schema}}.federation_resources(resource_ref) ON DELETE RESTRICT,
    source_revision BIGINT NOT NULL CHECK (source_revision > 0),
    fact_ref TEXT NOT NULL,
    reconciled_by TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (resource_ref, source_revision)
);

CREATE TABLE {{schema}}.federation_idempotency_bindings (
    environment TEXT NOT NULL,
    principal_id TEXT NOT NULL,
    operation TEXT NOT NULL,
    idempotency_key TEXT NOT NULL,
    request_digest TEXT NOT NULL CHECK (request_digest ~ '^sha256:[0-9a-f]{64}$'),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (environment, principal_id, operation, idempotency_key)
);

CREATE TABLE {{schema}}.federation_command_decisions (
    environment TEXT NOT NULL,
    principal_id TEXT NOT NULL,
    operation TEXT NOT NULL,
    idempotency_key TEXT NOT NULL,
    request_digest TEXT NOT NULL CHECK (request_digest ~ '^sha256:[0-9a-f]{64}$'),
    status TEXT NOT NULL CHECK (status IN ('accepted', 'rejected')),
    code TEXT NOT NULL,
    message TEXT NOT NULL,
    response_bytes BYTEA NOT NULL,
    response_digest TEXT NOT NULL CHECK (response_digest ~ '^sha256:[0-9a-f]{64}$'),
    resource_ref TEXT,
    before_revision BIGINT,
    after_revision BIGINT,
    decided_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (environment, principal_id, operation, idempotency_key, request_digest),
    CHECK (before_revision IS NULL OR before_revision >= 0),
    CHECK (after_revision IS NULL OR after_revision >= 0)
);

CREATE INDEX federation_relations_target_idx ON {{schema}}.federation_relations(target_ref, relation_type, source_ref);
