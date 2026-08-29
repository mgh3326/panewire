-- Panewire stage 2a transport schema.
--
-- Execute exactly one of these operator-owned options:
--   1. Supabase Dashboard -> SQL Editor: paste this file and run it as the
--      project owner.  Add `panewire` to API > Exposed schemas before clients
--      use its RPCs.
--   2. psql "$DATABASE_URL" -v ON_ERROR_STOP=1 -f provision/schema.sql
--
-- Do not configure `postgres_changes` for panewire.message_queue: it would
-- send a whole row, including inline_body, to subscribers.  Use a private
-- Supabase Realtime Broadcast channel whose payload is only
-- `{ "message_id": "..." }`; claim polling remains the source of truth.
--
-- Post-run verification (contains no payload or credentials):
--   SELECT n.nspname, c.relname
--     FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
--    WHERE n.nspname = 'panewire' AND c.relkind = 'r'
--    ORDER BY c.relname;
--   SELECT schemaname, tablename, policyname, cmd
--     FROM pg_policies WHERE schemaname = 'panewire'
--    ORDER BY tablename, policyname;
--   SELECT n.nspname, p.proname, pg_get_function_identity_arguments(p.oid)
--     FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace
--    WHERE n.nspname = 'panewire' AND p.proname LIKE 'panewire_%'
--    ORDER BY p.proname;

BEGIN;

CREATE SCHEMA IF NOT EXISTS panewire;

-- `auth.uid()` is supplied by Supabase Auth.  Local PostgreSQL validation
-- installs a deliberately separate stub from provision/local-auth-stub.sql;
-- do not run that helper against a Supabase project.
CREATE TABLE IF NOT EXISTS panewire.machine_registry (
    machine_id   text PRIMARY KEY CHECK (machine_id <> ''),
    auth_user_id uuid NOT NULL UNIQUE,
    state        text NOT NULL DEFAULT 'active'
                 CHECK (state IN ('active', 'revoked')),
    created_at   timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at   timestamptz NOT NULL DEFAULT clock_timestamp(),
    revoked_at   timestamptz,
    CONSTRAINT panewire_machine_registry_revocation_shape CHECK (
        (state = 'active' AND revoked_at IS NULL) OR
        (state = 'revoked' AND revoked_at IS NOT NULL)
    )
);

-- The queue deliberately retains envelope metadata after acknowledgement but
-- removes inline_body.  SQLite remains metadata-only; this bounded body exists
-- solely in the remote transport until the receiver acks it.
CREATE TABLE IF NOT EXISTS panewire.message_queue (
    delivery_id                  text CONSTRAINT panewire_message_queue_pkey PRIMARY KEY CHECK (delivery_id <> ''),
    message_id                   text NOT NULL CHECK (message_id <> ''),
    schema_version               integer NOT NULL,
    message_kind                 text NOT NULL
                               CHECK (message_kind IN ('inbox.delivery', 'workflow.completion')),
    source_machine_id            text NOT NULL REFERENCES panewire.machine_registry(machine_id),
    source_instance_id           text,
    destination_machine_id       text NOT NULL REFERENCES panewire.machine_registry(machine_id),
    inbox_namespace              text NOT NULL CHECK (inbox_namespace <> ''),
    logical_path                 text NOT NULL CHECK (logical_path <> ''),
    expect_machine_id            text NOT NULL CHECK (expect_machine_id <> ''),
    expect_pane                  jsonb NOT NULL DEFAULT '{}'::jsonb,
    payload_mode                 text NOT NULL CHECK (payload_mode = 'inline'),
    content_type                 text NOT NULL CHECK (content_type <> ''),
    payload_size_bytes           bigint NOT NULL CHECK (payload_size_bytes BETWEEN 0 AND 196608),
    payload_sha256               text NOT NULL CHECK (payload_sha256 <> ''),
    classification               text NOT NULL
                               CHECK (classification IN ('public', 'personal_non_company')),
    policy_version               text NOT NULL CHECK (policy_version <> ''),
    created_at                   timestamptz NOT NULL,
    expires_at                   timestamptz NOT NULL,
    correlation_id               text,
    causation_id                 text,
    reply_destination_machine_id text,
    reply_correlation_id         text,
    reply_requested              boolean NOT NULL DEFAULT false,
    spawn_requested              boolean NOT NULL DEFAULT false,
    envelope                     jsonb NOT NULL CHECK (jsonb_typeof(envelope) = 'object'),
    inline_body                  bytea,
    state                        text NOT NULL DEFAULT 'ready'
                               CHECK (state IN ('ready', 'claimed', 'acked', 'rejected')),
    claim_token                  uuid,
    claimant_machine_id          text REFERENCES panewire.machine_registry(machine_id),
    claimed_at                   timestamptz,
    visibility_deadline          timestamptz,
    acked_at                     timestamptz,
    ack_disposition              text CHECK (ack_disposition IN ('accepted', 'terminal_reject')),
    accepted_at                  timestamptz NOT NULL DEFAULT clock_timestamp(),
    CONSTRAINT panewire_message_queue_time_shape CHECK (expires_at > created_at),
    CONSTRAINT panewire_message_queue_body_size CHECK (
        inline_body IS NULL OR octet_length(inline_body) = payload_size_bytes
    ),
    CONSTRAINT panewire_message_queue_body_lifecycle CHECK (
        (state IN ('ready', 'claimed') AND inline_body IS NOT NULL) OR
        (state IN ('acked', 'rejected') AND inline_body IS NULL)
    ),
    CONSTRAINT panewire_message_queue_completion_shape CHECK (
        (message_kind = 'inbox.delivery') OR
        (correlation_id IS NOT NULL AND causation_id IS NOT NULL)
    ),
    UNIQUE (message_id, destination_machine_id)
);

CREATE INDEX IF NOT EXISTS panewire_message_queue_claim_idx
    ON panewire.message_queue (destination_machine_id, state, visibility_deadline, created_at)
    WHERE state IN ('ready', 'claimed');
CREATE INDEX IF NOT EXISTS panewire_message_queue_expiry_idx
    ON panewire.message_queue (expires_at)
    WHERE state IN ('ready', 'claimed');
CREATE INDEX IF NOT EXISTS panewire_message_queue_source_idx
    ON panewire.message_queue (source_machine_id, created_at DESC);

ALTER TABLE panewire.machine_registry ENABLE ROW LEVEL SECURITY;
ALTER TABLE panewire.message_queue ENABLE ROW LEVEL SECURITY;

-- This SECURITY DEFINER helper is the only machine-registry read available to
-- clients.  It maps one authenticated Supabase Auth user to one active,
-- stable machine ID and never exposes other registry rows.
CREATE OR REPLACE FUNCTION panewire.current_machine_id()
RETURNS text
LANGUAGE sql
STABLE
SECURITY DEFINER
SET search_path = panewire, pg_temp
AS $$
    SELECT mr.machine_id
      FROM panewire.machine_registry AS mr
     WHERE mr.auth_user_id = auth.uid()
       AND mr.state = 'active'
     LIMIT 1
$$;

-- Publish is an RPC rather than a writable table endpoint so one authenticated
-- user cannot forge another machine's source identity.  The unique delivery ID
-- makes retries idempotent; a conflicting reuse of that ID is rejected.
CREATE OR REPLACE FUNCTION panewire.panewire_publish(
    p_envelope jsonb,
    p_payload_b64 text
)
RETURNS TABLE (
    message_id text,
    delivery_id text,
    accepted_at timestamptz,
    duplicate boolean
)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, panewire, pg_temp
AS $$
DECLARE
    v_machine text;
    v_body bytea;
    v_size bigint;
    v_existing panewire.message_queue%ROWTYPE;
BEGIN
    v_machine := panewire.current_machine_id();
    IF v_machine IS NULL THEN
        RAISE EXCEPTION 'authenticated machine registry entry is required'
            USING ERRCODE = '42501';
    END IF;
    IF jsonb_typeof(p_envelope) IS DISTINCT FROM 'object' THEN
        RAISE EXCEPTION 'publish envelope must be an object' USING ERRCODE = '22023';
    END IF;
    IF p_envelope #>> '{source,machine_id}' IS DISTINCT FROM v_machine THEN
        RAISE EXCEPTION 'source machine does not match authenticated machine'
            USING ERRCODE = '42501';
    END IF;
    IF p_envelope #>> '{expect,machine_id}' IS DISTINCT FROM p_envelope #>> '{destination,machine_id}' THEN
        RAISE EXCEPTION 'expected machine must equal destination machine'
            USING ERRCODE = '22023';
    END IF;

    v_size := (p_envelope #>> '{payload,size_bytes}')::bigint;
    v_body := decode(COALESCE(p_payload_b64, ''), 'base64');
    IF octet_length(v_body) <> v_size THEN
        RAISE EXCEPTION 'inline payload length differs from envelope declaration'
            USING ERRCODE = '22023';
    END IF;

    INSERT INTO panewire.message_queue (
        delivery_id, message_id, schema_version, message_kind,
        source_machine_id, source_instance_id, destination_machine_id,
        inbox_namespace, logical_path, expect_machine_id, expect_pane,
        payload_mode, content_type, payload_size_bytes, payload_sha256,
        classification, policy_version, created_at, expires_at,
        correlation_id, causation_id, reply_destination_machine_id,
        reply_correlation_id, reply_requested, spawn_requested,
        envelope, inline_body
    ) VALUES (
        p_envelope ->> 'delivery_id',
        p_envelope ->> 'message_id',
        (p_envelope ->> 'schema_version')::integer,
        p_envelope ->> 'message_kind',
        p_envelope #>> '{source,machine_id}',
        NULLIF(p_envelope #>> '{source,instance_id}', ''),
        p_envelope #>> '{destination,machine_id}',
        p_envelope #>> '{destination,inbox_namespace}',
        p_envelope #>> '{destination,logical_path}',
        p_envelope #>> '{expect,machine_id}',
        COALESCE(p_envelope #> '{expect,pane}', '{}'::jsonb),
        p_envelope #>> '{payload,mode}',
        p_envelope #>> '{payload,content_type}',
        v_size,
        p_envelope #>> '{payload,sha256}',
        p_envelope #>> '{payload,classification}',
        COALESCE(NULLIF(p_envelope ->> 'policy_version', ''), 'stage2-allowlist-v1'),
        (p_envelope ->> 'created_at')::timestamptz,
        (p_envelope ->> 'expires_at')::timestamptz,
        NULLIF(p_envelope ->> 'correlation_id', ''),
        NULLIF(p_envelope ->> 'causation_id', ''),
        NULLIF(p_envelope #>> '{reply,destination_machine_id}', ''),
        NULLIF(p_envelope #>> '{reply,correlation_id}', ''),
        COALESCE(NULLIF(p_envelope #>> '{reply,requested}', '')::boolean, false),
        COALESCE(NULLIF(p_envelope #>> '{spawn,requested}', '')::boolean, false),
        p_envelope,
        v_body
    )
    ON CONFLICT ON CONSTRAINT panewire_message_queue_pkey DO NOTHING
    RETURNING * INTO v_existing;

    IF FOUND THEN
        RETURN QUERY SELECT v_existing.message_id, v_existing.delivery_id,
                            v_existing.accepted_at, false;
        RETURN;
    END IF;

    SELECT * INTO v_existing
      FROM panewire.message_queue AS q
     WHERE q.delivery_id = p_envelope ->> 'delivery_id';
    IF NOT FOUND THEN
        RAISE EXCEPTION 'idempotent publish row was not found' USING ERRCODE = '40001';
    END IF;
    IF v_existing.message_id IS DISTINCT FROM p_envelope ->> 'message_id'
       OR v_existing.source_machine_id IS DISTINCT FROM v_machine
       OR v_existing.destination_machine_id IS DISTINCT FROM p_envelope #>> '{destination,machine_id}'
       OR v_existing.payload_sha256 IS DISTINCT FROM p_envelope #>> '{payload,sha256}'
       OR v_existing.payload_size_bytes IS DISTINCT FROM v_size THEN
        RAISE EXCEPTION 'delivery ID conflicts with immutable transport metadata'
            USING ERRCODE = '23505';
    END IF;
    RETURN QUERY SELECT v_existing.message_id, v_existing.delivery_id,
                        v_existing.accepted_at, true;
END;
$$;

-- Claim atomically changes visibility and only accepts the destination obtained
-- from auth.uid() -> machine_registry.  A supplied different destination is a
-- permission error, not an empty result, so caller mistakes are observable.
CREATE OR REPLACE FUNCTION panewire.panewire_claim(
    p_destination_machine_id text,
    p_visibility_seconds integer DEFAULT 30,
    p_limit integer DEFAULT 32
)
RETURNS TABLE (
    token text,
    destination_machine_id text,
    visibility_deadline timestamptz,
    envelope jsonb
)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, panewire, pg_temp
AS $$
DECLARE
    v_machine text;
BEGIN
    v_machine := panewire.current_machine_id();
    IF v_machine IS NULL OR p_destination_machine_id IS DISTINCT FROM v_machine THEN
        RAISE EXCEPTION 'claim destination does not match authenticated machine'
            USING ERRCODE = '42501';
    END IF;
    IF p_visibility_seconds NOT BETWEEN 5 AND 600 OR p_limit NOT BETWEEN 1 AND 64 THEN
        RAISE EXCEPTION 'claim bounds are outside the allowed range' USING ERRCODE = '22023';
    END IF;

    RETURN QUERY
    WITH candidates AS (
        SELECT q.delivery_id
          FROM panewire.message_queue AS q
         WHERE q.destination_machine_id = v_machine
           AND q.expires_at > clock_timestamp()
           AND (q.state = 'ready' OR (q.state = 'claimed' AND q.visibility_deadline <= clock_timestamp()))
         ORDER BY q.created_at, q.delivery_id
         FOR UPDATE SKIP LOCKED
         LIMIT p_limit
    ), claimed AS (
        UPDATE panewire.message_queue AS q
           SET state = 'claimed',
               claim_token = pg_catalog.gen_random_uuid(),
               claimant_machine_id = v_machine,
               claimed_at = clock_timestamp(),
               visibility_deadline = clock_timestamp() + make_interval(secs => p_visibility_seconds)
          FROM candidates AS c
         WHERE q.delivery_id = c.delivery_id
        RETURNING q.claim_token, q.destination_machine_id, q.visibility_deadline, q.envelope
    )
    SELECT c.claim_token::text, c.destination_machine_id, c.visibility_deadline, c.envelope
      FROM claimed AS c;
END;
$$;

CREATE OR REPLACE FUNCTION panewire.panewire_fetch_payload(p_token uuid)
RETURNS TABLE (payload_b64 text)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, panewire, pg_temp
AS $$
DECLARE
    v_machine text;
BEGIN
    v_machine := panewire.current_machine_id();
    IF v_machine IS NULL THEN
        RAISE EXCEPTION 'authenticated machine registry entry is required'
            USING ERRCODE = '42501';
    END IF;
    RETURN QUERY
    SELECT encode(q.inline_body, 'base64')
      FROM panewire.message_queue AS q
     WHERE q.claim_token = p_token
       AND q.claimant_machine_id = v_machine
       AND q.state = 'claimed'
       AND q.visibility_deadline > clock_timestamp();
    IF NOT FOUND THEN
        RAISE EXCEPTION 'claim token is not active for authenticated machine'
            USING ERRCODE = '42501';
    END IF;
END;
$$;

-- Ack is the only transport terminal transition.  It erases the body in the
-- same UPDATE that records the terminal disposition.
CREATE OR REPLACE FUNCTION panewire.panewire_ack(
    p_token uuid,
    p_disposition text
)
RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, panewire, pg_temp
AS $$
DECLARE
    v_machine text;
BEGIN
    v_machine := panewire.current_machine_id();
    IF v_machine IS NULL THEN
        RAISE EXCEPTION 'authenticated machine registry entry is required'
            USING ERRCODE = '42501';
    END IF;
    IF p_disposition NOT IN ('accepted', 'terminal_reject') THEN
        RAISE EXCEPTION 'invalid acknowledgement disposition' USING ERRCODE = '22023';
    END IF;
    UPDATE panewire.message_queue AS q
       SET state = CASE WHEN p_disposition = 'accepted' THEN 'acked' ELSE 'rejected' END,
           inline_body = NULL,
           claim_token = NULL,
           visibility_deadline = NULL,
           acked_at = clock_timestamp(),
           ack_disposition = p_disposition
     WHERE q.claim_token = p_token
       AND q.claimant_machine_id = v_machine
       AND q.state = 'claimed'
       AND q.visibility_deadline > clock_timestamp();
    IF NOT FOUND THEN
        RAISE EXCEPTION 'claim token is not active for authenticated machine'
            USING ERRCODE = '42501';
    END IF;
END;
$$;

-- This inspection RPC returns only lifecycle metadata.  It exists for an
-- operator smoke check of body erasure; it never returns inline_body itself.
CREATE OR REPLACE FUNCTION panewire.panewire_message_status(p_delivery_id text)
RETURNS TABLE (
    state text,
    body_erased boolean,
    acked_at timestamptz
)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, panewire, pg_temp
AS $$
DECLARE
    v_machine text;
BEGIN
    v_machine := panewire.current_machine_id();
    IF v_machine IS NULL THEN
        RAISE EXCEPTION 'authenticated machine registry entry is required'
            USING ERRCODE = '42501';
    END IF;
    RETURN QUERY
    SELECT q.state, q.inline_body IS NULL, q.acked_at
      FROM panewire.message_queue AS q
     WHERE q.delivery_id = p_delivery_id
       AND q.destination_machine_id = v_machine;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'delivery is not visible to authenticated machine'
            USING ERRCODE = '42501';
    END IF;
END;
$$;

-- Registry rows are administration-only.  Direct queue reads are RLS-limited
-- to the receiving machine; clients use the RPCs for all mutations.
DROP POLICY IF EXISTS panewire_queue_sender_insert ON panewire.message_queue;
DROP POLICY IF EXISTS panewire_queue_receiver_select ON panewire.message_queue;
CREATE POLICY panewire_queue_sender_insert
    ON panewire.message_queue
    FOR INSERT TO authenticated
    WITH CHECK (
        source_machine_id = panewire.current_machine_id()
        AND state = 'ready'
        AND inline_body IS NOT NULL
        AND claim_token IS NULL
        AND claimant_machine_id IS NULL
        AND acked_at IS NULL
        AND ack_disposition IS NULL
    );
CREATE POLICY panewire_queue_receiver_select
    ON panewire.message_queue
    FOR SELECT TO anon, authenticated
    USING (destination_machine_id = panewire.current_machine_id());

REVOKE ALL ON SCHEMA panewire FROM PUBLIC;
GRANT USAGE ON SCHEMA panewire TO anon, authenticated, service_role;
REVOKE ALL ON TABLE panewire.machine_registry FROM PUBLIC, anon, authenticated;
REVOKE ALL ON TABLE panewire.message_queue FROM PUBLIC, anon, authenticated;
-- `anon` intentionally receives SELECT privilege but no matching RLS policy,
-- so a publishable key alone observes zero queue rows rather than row data.
GRANT SELECT ON panewire.message_queue TO anon, authenticated;
GRANT INSERT ON panewire.message_queue TO authenticated;
GRANT ALL PRIVILEGES ON TABLE panewire.machine_registry, panewire.message_queue TO service_role;

REVOKE ALL ON FUNCTION panewire.current_machine_id() FROM PUBLIC;
REVOKE ALL ON FUNCTION panewire.panewire_publish(jsonb, text) FROM PUBLIC;
REVOKE ALL ON FUNCTION panewire.panewire_claim(text, integer, integer) FROM PUBLIC;
REVOKE ALL ON FUNCTION panewire.panewire_fetch_payload(uuid) FROM PUBLIC;
REVOKE ALL ON FUNCTION panewire.panewire_ack(uuid, text) FROM PUBLIC;
REVOKE ALL ON FUNCTION panewire.panewire_message_status(text) FROM PUBLIC;
-- The anonymous role needs this helper only because the SELECT RLS policy
-- evaluates it.  auth.uid() is NULL without a user JWT, so it returns no
-- machine ID and the policy still yields zero rows.
GRANT EXECUTE ON FUNCTION panewire.current_machine_id() TO anon, authenticated, service_role;
GRANT EXECUTE ON FUNCTION panewire.panewire_publish(jsonb, text) TO authenticated, service_role;
GRANT EXECUTE ON FUNCTION panewire.panewire_claim(text, integer, integer) TO authenticated, service_role;
GRANT EXECUTE ON FUNCTION panewire.panewire_fetch_payload(uuid) TO authenticated, service_role;
GRANT EXECUTE ON FUNCTION panewire.panewire_ack(uuid, text) TO authenticated, service_role;
GRANT EXECUTE ON FUNCTION panewire.panewire_message_status(text) TO authenticated, service_role;

COMMIT;
