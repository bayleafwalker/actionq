-- Actionq execution-domain schema v10: bind a verified result to the
-- currently authoritative claim incarnation.  This is not a secret, and it
-- is the public attempt identity that workers must echo in dispatch-result/v1.
ALTER TABLE {{schema}}.actions
    ADD COLUMN IF NOT EXISTS claim_attempt_id TEXT;
