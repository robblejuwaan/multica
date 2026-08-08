-- BLU-472: retain one holder if historical duplicates exist, so adding the
-- cross-replica unique index cannot fail during upgrade. Their task rows are
-- left intact; this only makes the run history reflect that those occurrences
-- have been superseded by the new admission invariant.
WITH ranked AS (
    SELECT id,
           row_number() OVER (PARTITION BY autopilot_id ORDER BY triggered_at ASC, id ASC) AS ordinal
    FROM autopilot_run
    WHERE status IN ('issue_created', 'running')
)
UPDATE autopilot_run r
SET status = 'failed',
    completed_at = now(),
    failure_reason = 'superseded by BLU-472 active-run lock migration'
FROM ranked
WHERE r.id = ranked.id
  AND ranked.ordinal > 1;
