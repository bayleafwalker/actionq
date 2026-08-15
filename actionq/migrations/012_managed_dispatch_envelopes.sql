-- Immutable managed-enqueue envelope attached 1:1 to its ordinary v2 root.
-- The runtime can read and append it once; it cannot alter a capsule after
-- admission or obtain a prompt through a mutable event projection.
CREATE TABLE IF NOT EXISTS {{schema}}.managed_dispatch_envelopes (
    action_id BIGINT PRIMARY KEY REFERENCES {{schema}}.actions(id) ON DELETE RESTRICT,
    envelope_sha256 TEXT NOT NULL CHECK (envelope_sha256 ~ '^[a-f0-9]{64}$'),
    envelope_snapshot BYTEA NOT NULL,
    capsule_sha256 TEXT NOT NULL CHECK (capsule_sha256 ~ '^[a-f0-9]{64}$'),
    rendered_prompt_sha256 TEXT NOT NULL CHECK (rendered_prompt_sha256 ~ '^[a-f0-9]{64}$'),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
