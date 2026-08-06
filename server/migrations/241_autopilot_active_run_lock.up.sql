-- BLU-472: retain one holder if historical duplicates exist, so adding the
-- cross-replica unique index cannot fail during upgrade. Their task rows are
-- left intact; this only makes the run history reflect that those occurrences
-- have been superseded by the new admission invariant.
WITH ranked AS (
    SELECT id,
           row_number() OVER (PARTITION BY autopilot_id ORDER BY triggered_at ASC, id ASC) AS ordinal
    FROM autopilot_run
    WHERE status IN ('pending', 'issue_created', 'running')
)
UPDATE autopilot_run r
SET status = 'failed',
    completed_at = now(),
    failure_reason = 'superseded by BLU-472 active-run lock migration'
FROM ranked
WHERE r.id = ranked.id
  AND ranked.ordinal > 1;

-- One active holder is the database admission lock. Terminal history remains
-- unconstrained so skipped/failed/completed runs stay human-readable.
CREATE UNIQUE INDEX IF NOT EXISTS uq_autopilot_run_one_active_per_autopilot
    ON autopilot_run (autopilot_id)
    WHERE status IN ('pending', 'issue_created', 'running');
