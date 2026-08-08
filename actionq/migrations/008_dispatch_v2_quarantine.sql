-- #2027 Stage B: v7 recovery floors were derived from retained event rows.
--
-- This follow-on deliberately has no data rewrite.  A v7 dispatch root can
-- already have lost the event history needed to prove a recovery floor, so a
-- migration must not manufacture one from MIN(events.id) (or any other
-- retained-row heuristic).  Operators first drain/remove every v2 dispatch
-- root under the deployment preflight; terminal action/event history is not
-- a dispatch root and is intentionally retained.
--
-- The Python migration entrypoint performs the same check with a diagnostic
-- before this statement is reached.  Keep this SQL guard as the transactional
-- defence-in-depth gate for any alternate caller.
SELECT 1 / CASE
    WHEN EXISTS (SELECT 1 FROM {{schema}}.dispatch_requests) THEN 0
    ELSE 1
END;
