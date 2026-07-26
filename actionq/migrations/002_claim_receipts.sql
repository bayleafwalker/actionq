-- Actionq execution-domain schema v2: fence terminal settlement to the
-- current claim incarnation.  A receipt is minted for every successful claim
-- and cleared only when the claim is swept/released by a terminal transition.
ALTER TABLE {{schema}}.actions
    ADD COLUMN IF NOT EXISTS claim_receipt TEXT;
