-- #2027 Stage A: durable cursor recovery floor for v2 dispatch-root actions.
-- The floor is stored independently of retained event rows so an observer can
-- fail closed after partial or complete pruning instead of inferring history
-- from MIN(events.id).
CREATE TABLE IF NOT EXISTS {{schema}}.dispatch_observation_watermarks (
    action_id BIGINT PRIMARY KEY REFERENCES {{schema}}.actions(id) ON DELETE RESTRICT,
    first_retained_event_id BIGINT NOT NULL CHECK (first_retained_event_id >= 0),
    -- Survives complete event pruning for snapshot recovery.
    last_observed_event_id BIGINT NOT NULL CHECK (last_observed_event_id >= 0),
    watermark_updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

INSERT INTO {{schema}}.dispatch_observation_watermarks
    (action_id, first_retained_event_id, last_observed_event_id)
SELECT
    d.action_id,
    COALESCE(MIN(e.id), COALESCE(MAX(e.id), 0) + 1),
    COALESCE(MAX(e.id), 0)
FROM {{schema}}.dispatch_requests AS d
LEFT JOIN {{schema}}.events AS e ON e.action_id = d.action_id
GROUP BY d.action_id
ON CONFLICT (action_id) DO NOTHING;
