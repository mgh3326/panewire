-- LOCAL POSTGRESQL VALIDATION ONLY -- requires local-auth-stub.sql,
-- schema.sql, and sentinel.sql.  It prints no Telegram, credential, command,
-- or message-body data.

INSERT INTO panewire.machine_registry (machine_id, auth_user_id)
VALUES
    ('fixture-a', 'aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa'),
    ('fixture-b', 'bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb')
ON CONFLICT (machine_id) DO NOTHING;

INSERT INTO panewire.sentinel_heartbeats (machine_id, seen_at, checks_json, version)
VALUES ('fixture-b', clock_timestamp(), '{"service":"ok"}'::jsonb, 'r5-fixture')
ON CONFLICT (machine_id) DO UPDATE
   SET seen_at = EXCLUDED.seen_at,
       checks_json = EXCLUDED.checks_json,
       version = EXCLUDED.version;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_policies
         WHERE schemaname = 'panewire'
           AND tablename = 'sentinel_heartbeats'
           AND policyname = 'panewire_sentinel_heartbeats_authenticated_read'
           AND cmd = 'SELECT'
    ) THEN
        RAISE EXCEPTION 'sentinel heartbeat mutual-read policy is missing';
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
         WHERE conrelid = 'panewire.sentinel_alerts'::regclass
           AND contype = 'p'
    ) THEN
        RAISE EXCEPTION 'sentinel alert unique claim key is missing';
    END IF;
    IF has_table_privilege('anon', 'panewire.sentinel_heartbeats', 'SELECT') THEN
        RAISE EXCEPTION 'anon unexpectedly has sentinel heartbeat SELECT';
    END IF;
END;
$$;

SET ROLE authenticated;
SET request.jwt.claim.sub = 'aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa';

DO $$
DECLARE
    visible integer;
    claimed_once boolean;
    delivered_once boolean;
BEGIN
    INSERT INTO panewire.sentinel_heartbeats (machine_id, seen_at, checks_json, version)
    VALUES ('fixture-a', clock_timestamp(), '{"service":"fail","disk":"ok"}'::jsonb, 'r5-fixture')
    ON CONFLICT (machine_id) DO UPDATE
       SET seen_at = EXCLUDED.seen_at,
           checks_json = EXCLUDED.checks_json,
           version = EXCLUDED.version;

    SELECT count(*) INTO visible FROM panewire.sentinel_heartbeats;
    IF visible <> 2 THEN
        RAISE EXCEPTION 'authenticated mutual heartbeat read returned % rows', visible;
    END IF;
    BEGIN
        INSERT INTO panewire.sentinel_heartbeats (machine_id, seen_at, checks_json, version)
        VALUES ('fixture-b', clock_timestamp(), '{"service":"ok"}'::jsonb, 'r5-fixture');
        RAISE EXCEPTION 'foreign heartbeat write unexpectedly succeeded';
    EXCEPTION WHEN insufficient_privilege THEN
        NULL;
    END;

    SELECT claimed INTO claimed_once
      FROM panewire.panewire_sentinel_claim_alert(
          'sentinel:incident:fixture-b:stale',
          '2026-08-31T01:00:00Z'::timestamptz,
          5
      );
    IF claimed_once IS DISTINCT FROM true THEN
        RAISE EXCEPTION 'initial sentinel alert claim failed';
    END IF;
    SELECT delivered INTO delivered_once
      FROM panewire.panewire_sentinel_mark_alert_delivered(
          'sentinel:incident:fixture-b:stale',
          '2026-08-31T01:00:00Z'::timestamptz
      );
    IF delivered_once IS DISTINCT FROM true THEN
        RAISE EXCEPTION 'sentinel alert delivery mark failed';
    END IF;
END;
$$;

RESET ROLE;
UPDATE panewire.sentinel_alerts
   SET created_at = clock_timestamp() - interval '10 seconds',
       claim_expires_at = clock_timestamp() - interval '1 second'
 WHERE incident_key = 'sentinel:incident:fixture-b:checks_fail';

SET ROLE authenticated;
SET request.jwt.claim.sub = 'aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa';
DO $$
DECLARE
    claimed_once boolean;
BEGIN
    SELECT claimed INTO claimed_once
      FROM panewire.panewire_sentinel_claim_alert(
          'sentinel:incident:fixture-b:checks_fail',
          '2026-08-31T01:00:00Z'::timestamptz,
          5
      );
    IF claimed_once IS DISTINCT FROM true THEN
        RAISE EXCEPTION 'unmarked sentinel claim did not start';
    END IF;
END;
$$;

RESET ROLE;
UPDATE panewire.sentinel_alerts
   SET created_at = clock_timestamp() - interval '10 seconds',
       claim_expires_at = clock_timestamp() - interval '1 second'
 WHERE incident_key = 'sentinel:incident:fixture-b:checks_fail';

SET ROLE authenticated;
SET request.jwt.claim.sub = 'bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb';
DO $$
DECLARE
    durable_claim boolean;
    reclaimed boolean;
BEGIN
    SELECT claimed INTO durable_claim
      FROM panewire.panewire_sentinel_claim_alert(
          'sentinel:incident:fixture-b:stale',
          '2026-08-31T01:00:00Z'::timestamptz,
          5
      );
    IF durable_claim IS DISTINCT FROM false THEN
        RAISE EXCEPTION 'delivered sentinel alert was claimable again';
    END IF;
    SELECT claimed INTO reclaimed
      FROM panewire.panewire_sentinel_claim_alert(
          'sentinel:incident:fixture-b:checks_fail',
          '2026-08-31T01:00:00Z'::timestamptz,
          5
      );
    IF reclaimed IS DISTINCT FROM true THEN
        RAISE EXCEPTION 'expired sentinel claim was not reclaimable';
    END IF;
END;
$$;

RESET ROLE;
RESET request.jwt.claim.sub;
