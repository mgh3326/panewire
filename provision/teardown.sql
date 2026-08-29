-- Panewire stage 2a transport teardown.
--
-- Run only against the intended project/database:
--   psql "$DATABASE_URL" -v ON_ERROR_STOP=1 -f provision/teardown.sql
--
-- This is an explicit list and intentionally does not use DROP SCHEMA ...
-- CASCADE.  If an unexpected object remains in the panewire namespace, the
-- final DROP SCHEMA fails rather than deleting it incidentally.

BEGIN;

DO $$
BEGIN
    IF to_regclass('panewire.message_queue') IS NOT NULL THEN
        EXECUTE 'DROP POLICY IF EXISTS panewire_queue_sender_insert ON panewire.message_queue';
        EXECUTE 'DROP POLICY IF EXISTS panewire_queue_receiver_select ON panewire.message_queue';
    END IF;
END;
$$;

DROP FUNCTION IF EXISTS panewire.panewire_message_status(text);
DROP FUNCTION IF EXISTS panewire.panewire_ack(uuid, text);
DROP FUNCTION IF EXISTS panewire.panewire_fetch_payload(uuid);
DROP FUNCTION IF EXISTS panewire.panewire_claim(text, integer, integer);
DROP FUNCTION IF EXISTS panewire.panewire_publish(jsonb, text);
DROP FUNCTION IF EXISTS panewire.current_machine_id();
DROP TABLE IF EXISTS panewire.message_queue;
DROP TABLE IF EXISTS panewire.machine_registry;
DROP SCHEMA IF EXISTS panewire;

COMMIT;
