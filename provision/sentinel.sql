-- Panewire R5 sentinel (L2 peer monitoring) incremental schema.
--
-- Apply only after provision/schema.sql:
--   psql "$DATABASE_URL" -v ON_ERROR_STOP=1 -f provision/sentinel.sql
--
-- This file is deliberately separate from schema.sql.  It is idempotent and
-- contains no body, command output, credential, or Telegram data.

BEGIN;

-- The check result vocabulary is closed before an authenticated client can
-- write it.  Command argv and stdout/stderr are local-only and never appear in
-- this table.
CREATE OR REPLACE FUNCTION panewire.sentinel_checks_valid(p_checks jsonb)
RETURNS boolean
LANGUAGE sql
IMMUTABLE
SET search_path = pg_catalog, pg_temp
AS $$
    SELECT COALESCE(
        jsonb_typeof(p_checks) = 'object'
        AND NOT EXISTS (
            SELECT 1
              FROM jsonb_each(p_checks) AS entry(check_name, check_status)
             WHERE check_name !~ '^[a-z][a-z0-9._-]{0,63}$'
                OR jsonb_typeof(check_status) <> 'string'
                OR check_status #>> '{}' NOT IN ('ok', 'fail', 'skip')
        ),
        false
    )
$$;

CREATE TABLE IF NOT EXISTS panewire.sentinel_heartbeats (
    machine_id  text PRIMARY KEY REFERENCES panewire.machine_registry(machine_id),
    seen_at     timestamptz NOT NULL,
    checks_json jsonb NOT NULL DEFAULT '{}'::jsonb
                CHECK (panewire.sentinel_checks_valid(checks_json)),
    version     text NOT NULL CHECK (version ~ '^[A-Za-z0-9._-]{1,64}$'),
    updated_at  timestamptz NOT NULL DEFAULT clock_timestamp()
);

CREATE INDEX IF NOT EXISTS panewire_sentinel_heartbeats_seen_idx
    ON panewire.sentinel_heartbeats (seen_at DESC);

-- A claim is initially a lease.  A successful Telegram send marks it durable;
-- an unmarked lease can be reclaimed after claim_expires_at, so a failed sender
-- cannot create permanent alert silence.  The unique pair implements exactly
-- one shared candidate for each incident/window bucket.
CREATE TABLE IF NOT EXISTS panewire.sentinel_alerts (
    incident_key        text NOT NULL
                        CHECK (incident_key ~ '^sentinel:(incident|recovery):[a-z0-9][a-z0-9._-]{0,62}:(stale|checks_fail)$'),
    alert_window        timestamptz NOT NULL,
    claimant_machine_id text NOT NULL REFERENCES panewire.machine_registry(machine_id),
    claim_expires_at    timestamptz NOT NULL,
    delivered_at        timestamptz,
    created_at          timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (incident_key, alert_window),
    CONSTRAINT panewire_sentinel_alert_claim_shape CHECK (claim_expires_at > created_at)
);

CREATE INDEX IF NOT EXISTS panewire_sentinel_alerts_expiry_idx
    ON panewire.sentinel_alerts (claim_expires_at)
    WHERE delivered_at IS NULL;

ALTER TABLE panewire.sentinel_heartbeats ENABLE ROW LEVEL SECURITY;
ALTER TABLE panewire.sentinel_alerts ENABLE ROW LEVEL SECURITY;

-- Unlike message_queue's destination-scoped policy, every authenticated node
-- may read every heartbeat.  Peer visibility is the prerequisite for mutual
-- observation; a publishable/anonymous client receives no heartbeat privilege.
DROP POLICY IF EXISTS panewire_sentinel_heartbeats_authenticated_read ON panewire.sentinel_heartbeats;
DROP POLICY IF EXISTS panewire_sentinel_heartbeats_self_insert ON panewire.sentinel_heartbeats;
DROP POLICY IF EXISTS panewire_sentinel_heartbeats_self_update ON panewire.sentinel_heartbeats;
CREATE POLICY panewire_sentinel_heartbeats_authenticated_read
    ON panewire.sentinel_heartbeats
    FOR SELECT TO authenticated
    USING (true);
CREATE POLICY panewire_sentinel_heartbeats_self_insert
    ON panewire.sentinel_heartbeats
    FOR INSERT TO authenticated
    WITH CHECK (machine_id = panewire.current_machine_id());
CREATE POLICY panewire_sentinel_heartbeats_self_update
    ON panewire.sentinel_heartbeats
    FOR UPDATE TO authenticated
    USING (machine_id = panewire.current_machine_id())
    WITH CHECK (machine_id = panewire.current_machine_id());

-- Alert rows are private to their claimant for direct table access.  Cross-node
-- arbitration occurs only in the SECURITY DEFINER claim RPC below.
DROP POLICY IF EXISTS panewire_sentinel_alerts_self_select ON panewire.sentinel_alerts;
DROP POLICY IF EXISTS panewire_sentinel_alerts_self_insert ON panewire.sentinel_alerts;
DROP POLICY IF EXISTS panewire_sentinel_alerts_self_update ON panewire.sentinel_alerts;
CREATE POLICY panewire_sentinel_alerts_self_select
    ON panewire.sentinel_alerts
    FOR SELECT TO authenticated
    USING (claimant_machine_id = panewire.current_machine_id());
CREATE POLICY panewire_sentinel_alerts_self_insert
    ON panewire.sentinel_alerts
    FOR INSERT TO authenticated
    WITH CHECK (claimant_machine_id = panewire.current_machine_id());
CREATE POLICY panewire_sentinel_alerts_self_update
    ON panewire.sentinel_alerts
    FOR UPDATE TO authenticated
    USING (claimant_machine_id = panewire.current_machine_id())
    WITH CHECK (claimant_machine_id = panewire.current_machine_id());

-- This is deliberately a database-side delete-then-unique-insert operation.
-- It makes lease expiry retryable while keeping the winning identity bound to
-- auth.uid() -> machine_registry instead of a client-supplied machine ID.
CREATE OR REPLACE FUNCTION panewire.panewire_sentinel_claim_alert(
    p_incident_key text,
    p_alert_window timestamptz,
    p_claim_ttl_seconds integer DEFAULT 90
)
RETURNS TABLE (claimed boolean)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, panewire, pg_temp
AS $$
DECLARE
    v_machine text;
    v_claimed boolean := false;
BEGIN
    v_machine := panewire.current_machine_id();
    IF v_machine IS NULL THEN
        RAISE EXCEPTION 'authenticated machine registry entry is required'
            USING ERRCODE = '42501';
    END IF;
    IF p_incident_key IS NULL
       OR p_incident_key !~ '^sentinel:(incident|recovery):[a-z0-9][a-z0-9._-]{0,62}:(stale|checks_fail)$'
       OR p_alert_window IS NULL THEN
        RAISE EXCEPTION 'sentinel alert key is invalid' USING ERRCODE = '22023';
    END IF;
    IF p_claim_ttl_seconds NOT BETWEEN 5 AND 600 THEN
        RAISE EXCEPTION 'sentinel alert TTL is outside the allowed range' USING ERRCODE = '22023';
    END IF;

    DELETE FROM panewire.sentinel_alerts
     WHERE incident_key = p_incident_key
       AND alert_window = p_alert_window
       AND delivered_at IS NULL
       AND claim_expires_at <= clock_timestamp();

    INSERT INTO panewire.sentinel_alerts (
        incident_key, alert_window, claimant_machine_id, claim_expires_at
    ) VALUES (
        p_incident_key, p_alert_window, v_machine,
        clock_timestamp() + make_interval(secs => p_claim_ttl_seconds)
    )
    ON CONFLICT (incident_key, alert_window) DO NOTHING
    RETURNING true INTO v_claimed;

    RETURN QUERY SELECT COALESCE(v_claimed, false);
END;
$$;

CREATE OR REPLACE FUNCTION panewire.panewire_sentinel_mark_alert_delivered(
    p_incident_key text,
    p_alert_window timestamptz
)
RETURNS TABLE (delivered boolean)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, panewire, pg_temp
AS $$
DECLARE
    v_machine text;
    v_delivered boolean := false;
BEGIN
    v_machine := panewire.current_machine_id();
    IF v_machine IS NULL THEN
        RAISE EXCEPTION 'authenticated machine registry entry is required'
            USING ERRCODE = '42501';
    END IF;
    UPDATE panewire.sentinel_alerts
       SET delivered_at = clock_timestamp()
     WHERE incident_key = p_incident_key
       AND alert_window = p_alert_window
       AND claimant_machine_id = v_machine
       AND delivered_at IS NULL
    RETURNING true INTO v_delivered;
    RETURN QUERY SELECT COALESCE(v_delivered, false);
END;
$$;

REVOKE ALL ON TABLE panewire.sentinel_heartbeats FROM PUBLIC, anon, authenticated;
REVOKE ALL ON TABLE panewire.sentinel_alerts FROM PUBLIC, anon, authenticated;
GRANT SELECT, INSERT, UPDATE ON TABLE panewire.sentinel_heartbeats TO authenticated;
GRANT SELECT, INSERT, UPDATE ON TABLE panewire.sentinel_alerts TO authenticated;
GRANT ALL PRIVILEGES ON TABLE panewire.sentinel_heartbeats, panewire.sentinel_alerts TO service_role;

REVOKE ALL ON FUNCTION panewire.sentinel_checks_valid(jsonb) FROM PUBLIC;
REVOKE ALL ON FUNCTION panewire.panewire_sentinel_claim_alert(text, timestamptz, integer) FROM PUBLIC;
REVOKE ALL ON FUNCTION panewire.panewire_sentinel_mark_alert_delivered(text, timestamptz) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION panewire.sentinel_checks_valid(jsonb) TO authenticated, service_role;
GRANT EXECUTE ON FUNCTION panewire.panewire_sentinel_claim_alert(text, timestamptz, integer) TO authenticated, service_role;
GRANT EXECUTE ON FUNCTION panewire.panewire_sentinel_mark_alert_delivered(text, timestamptz) TO authenticated, service_role;

COMMIT;
