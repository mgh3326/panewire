-- Run once as a PostgreSQL cluster administrator. Supply the password without
-- placing it in this repository, for example:
-- psql -v context_password='...' -f deploy/sql/panewire_context.sql postgres
CREATE ROLE panewire LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT
  PASSWORD :'context_password';
CREATE DATABASE panewire OWNER panewire;
\connect panewire
REVOKE ALL ON DATABASE panewire FROM PUBLIC;
GRANT CONNECT ON DATABASE panewire TO panewire;
REVOKE CREATE ON SCHEMA public FROM PUBLIC;
GRANT USAGE, CREATE ON SCHEMA public TO panewire;
CREATE EXTENSION IF NOT EXISTS pg_trgm;
