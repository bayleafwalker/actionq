-- #2031: cancellation fences a live claim before runner shutdown is observed.
ALTER TABLE {{schema}}.actions
    DROP CONSTRAINT IF EXISTS actions_status_check;
ALTER TABLE {{schema}}.actions
    ADD CONSTRAINT actions_status_check
    CHECK (status IN ('pending', 'claimed', 'cancelling', 'completed', 'failed', 'rejected', 'cancelled'));
ALTER TABLE {{schema}}.actions
    ADD COLUMN IF NOT EXISTS cancel_request_id UUID,
    ADD COLUMN IF NOT EXISTS cancel_stop_deadline TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS stop_acknowledged BOOLEAN,
    ADD COLUMN IF NOT EXISTS cancel_former_claimed_by TEXT,
    ADD COLUMN IF NOT EXISTS cancel_former_receipt_digest TEXT;
CREATE INDEX IF NOT EXISTS idx_actions_cancelling_deadline
    ON {{schema}}.actions(cancel_stop_deadline) WHERE status = 'cancelling';
