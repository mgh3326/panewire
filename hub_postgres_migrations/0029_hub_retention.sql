-- hk 5297 (design pw-4898-hub-store-mysql §1): phase 0 retention.
-- quota_refresh_failures (0011) has no Go reader or writer, and SQLite never
-- created it, so it is dropped outright.
DROP TABLE IF EXISTS quota_refresh_failures;
-- A terminal spawn operation records when it became terminal, so the 1-day and
-- 30-day windows have a clock. Rows terminal before this migration are dated
-- now: they keep the full window from the upgrade, never less.
ALTER TABLE spawn_operations ADD COLUMN IF NOT EXISTS terminal_ms BIGINT NOT NULL DEFAULT 0;
UPDATE spawn_operations SET terminal_ms=(EXTRACT(EPOCH FROM clock_timestamp())*1000)::BIGINT WHERE terminal_ms=0 AND status IN ('succeeded','failed','refused','given_up','cancelled');
CREATE INDEX IF NOT EXISTS spawn_operations_terminal ON spawn_operations(terminal_ms) WHERE status IN ('succeeded','failed','refused','given_up','cancelled');
-- A pool action records when it became final (done or refused). A final row is
-- the dedupe fence for its job (hasPoolAction, supersedeActionsOf), so it is
-- deleted only after its window and only once the job has no live spawn row.
-- Final rows existing at this migration are dated now, for the same reason.
ALTER TABLE pool_actions ADD COLUMN IF NOT EXISTS final_ms BIGINT NOT NULL DEFAULT 0;
UPDATE pool_actions SET final_ms=(EXTRACT(EPOCH FROM clock_timestamp())*1000)::BIGINT WHERE final_ms=0 AND phase IN ('done','refused');
CREATE INDEX IF NOT EXISTS pool_actions_final ON pool_actions(final_ms) WHERE phase IN ('done','refused');
