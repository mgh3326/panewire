-- Panewire R7 teardown for the retired peer-monitoring schema.
--
-- Apply after every node has been redeployed with hub-based notification:
--   psql "$DATABASE_URL" -v ON_ERROR_STOP=1 -f provision/sentinel-teardown.sql
--
-- The sequence is idempotent.  It removes only the retired heartbeat/alert
-- tables and their RPC/check-validation functions; stage-2 queue objects are
-- intentionally untouched.

BEGIN;

DROP FUNCTION IF EXISTS panewire.panewire_sentinel_claim_alert(text, timestamptz, integer);
DROP FUNCTION IF EXISTS panewire.panewire_sentinel_mark_alert_delivered(text, timestamptz);
DROP TABLE IF EXISTS panewire.sentinel_alerts;
DROP TABLE IF EXISTS panewire.sentinel_heartbeats;
DROP FUNCTION IF EXISTS panewire.sentinel_checks_valid(jsonb);

COMMIT;
