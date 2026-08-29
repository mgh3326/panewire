-- LOCAL POSTGRESQL VALIDATION ONLY -- requires local-auth-stub.sql and schema.sql.
-- This proves the auth.uid() stand-in supports the intended source/destination
-- checks without printing payload bytes or credentials.

INSERT INTO panewire.machine_registry (machine_id, auth_user_id)
VALUES
    ('fixture-a', 'aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa'),
    ('fixture-b', 'bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb');

SET ROLE authenticated;
SET request.jwt.claim.sub = 'aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa';

DO $$
DECLARE
    accepted integer;
BEGIN
    SELECT count(*) INTO accepted
      FROM panewire.panewire_publish(
        '{
          "schema_version": 2,
          "message_id": "fixture-message",
          "delivery_id": "fixture-delivery",
          "message_kind": "inbox.delivery",
          "source": {"machine_id": "fixture-a"},
          "destination": {"machine_id": "fixture-b", "inbox_namespace": "smoke", "logical_path": "request.md"},
          "expect": {"machine_id": "fixture-b"},
          "payload": {"mode": "inline", "content_type": "text/markdown", "size_bytes": 5, "sha256": "0000000000000000000000000000000000000000000000000000000000000000", "classification": "public"},
          "created_at": "2026-08-29T12:00:00Z",
          "expires_at": "2026-08-30T12:00:00Z"
        }'::jsonb,
        'aGVsbG8='
      );
    IF accepted <> 1 THEN
        RAISE EXCEPTION 'local publish receipt was not singular';
    END IF;
    IF (SELECT count(*) FROM panewire.message_queue) <> 0 THEN
        RAISE EXCEPTION 'sender saw a foreign destination row through RLS';
    END IF;
    BEGIN
        PERFORM panewire.panewire_claim('fixture-b', 30, 1);
        RAISE EXCEPTION 'foreign claim unexpectedly succeeded';
    EXCEPTION WHEN insufficient_privilege THEN
        NULL;
    END;
END;
$$;

SET request.jwt.claim.sub = 'bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb';

DO $$
DECLARE
    claim_token uuid;
    body_is_erased boolean;
BEGIN
    SELECT token::uuid INTO claim_token
      FROM panewire.panewire_claim('fixture-b', 30, 1);
    IF claim_token IS NULL THEN
        RAISE EXCEPTION 'receiver did not claim its destination row';
    END IF;
    PERFORM payload_b64 FROM panewire.panewire_fetch_payload(claim_token);
    PERFORM panewire.panewire_ack(claim_token, 'accepted');
    SELECT body_erased INTO body_is_erased
      FROM panewire.panewire_message_status('fixture-delivery');
    IF body_is_erased IS DISTINCT FROM true THEN
        RAISE EXCEPTION 'ack did not erase transport body';
    END IF;
END;
$$;

RESET ROLE;
RESET request.jwt.claim.sub;
SET ROLE anon;
SELECT count(*) AS anon_visible_rows FROM panewire.message_queue;
RESET ROLE;
