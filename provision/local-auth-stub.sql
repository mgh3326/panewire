-- LOCAL POSTGRESQL VALIDATION ONLY -- never run this file in Supabase.
--
-- schema.sql intentionally calls Supabase's auth.uid() and grants Supabase
-- roles.  This isolated helper supplies the smallest local stand-ins needed to
-- parse and exercise the idempotent DDL: auth schema, auth.uid(), and the
-- anon/authenticated/service_role NOLOGIN roles.  It does not emulate JWT
-- verification, GoTrue, PostgREST, Realtime, or service_role's Supabase
-- BYPASSRLS attribute.

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'anon') THEN
        CREATE ROLE anon NOLOGIN;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'authenticated') THEN
        CREATE ROLE authenticated NOLOGIN;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'service_role') THEN
        CREATE ROLE service_role NOLOGIN;
    END IF;
END;
$$;

CREATE SCHEMA IF NOT EXISTS auth;
CREATE OR REPLACE FUNCTION auth.uid()
RETURNS uuid
LANGUAGE sql
STABLE
AS $$
    SELECT NULLIF(current_setting('request.jwt.claim.sub', true), '')::uuid
$$;
