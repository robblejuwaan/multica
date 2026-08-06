-- BLU-472: PostgreSQL requires CREATE INDEX CONCURRENTLY to be the only
-- statement in its migration. Keep the hot autopilot_run table writable while
-- the cross-replica admission invariant is installed.
CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS uq_autopilot_run_one_active_per_autopilot
    ON autopilot_run (autopilot_id)
    WHERE status IN ('issue_created', 'running');
