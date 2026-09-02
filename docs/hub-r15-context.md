# R15 hub context store

The hub context database is the durable, cross-machine record for checkpoints,
handoffs, decisions, open questions, next actions, and agent memory. It is not
a replacement for Git: use it for session continuity and leave code and review
artifacts in their normal repositories.

## Session contract

Long-running sessions create a checkpoint at least every 30 minutes, before a
risky step, and before ending:

```sh
panewire ctx checkpoint --hub-url https://hub.example --hub-token-env /safe/hub.env \
  --session my-session --kind checkpoint --title 'before migration' --file note.md
```

The first action in a resumed or new session is:

```sh
panewire ctx recent --hub-url https://hub.example --hub-token-env /safe/hub.env \
  --session my-session --limit 3
panewire memory pull --hub-url https://hub.example --hub-token-env /safe/hub.env \
  --agent my-agent --dir ./memory --apply
```

Include this sentence in a worker brief: **접수 전 `panewire ctx recent --session
<발신 세션>`**. Add the normal hub URL and token-env flags supplied by the
operator.

`memory push` and `memory pull` are dry-runs unless `--apply` is supplied.
Push parses `name`, `description`, and `metadata.type` from Markdown
frontmatter. Pull preserves database-only memories, materializes each memory
file, and regenerates `MEMORY.md` as `- [name](file) — description`.

## Authentication and API

Use the existing mode-0600 credential file format:

```text
HUB_MACHINE_ID=machine-a
HUB_TOKEN=replace-with-machine-token
```

| Credential | Read checkpoints/memory | Create/update context | Delete memory |
| --- | --- | --- | --- |
| Machine token | Yes | Yes, stamped with its machine ID | Yes |
| Operator token | Yes | Yes, stamped `operator` | Yes |

The available endpoints are `POST`/`GET /v1/context/checkpoints`, metadata-only
`GET /v1/context/memory/{agent}`, and `PUT`/`GET`/`DELETE
/v1/context/memory/{agent}/{name}`. Bodies and memory content are limited to
64 KiB. A session keeps its newest 500 checkpoints and an agent keeps its
newest 2,000 memory entries.

Content that resembles a credential is rejected before it reaches PostgreSQL.
The response uses `error=secret_like_content` and identifies only a pattern
and never reflects a value.

## Search and documents

The hub creates PostgreSQL `simple` full-text and `pg_trgm` GIN indexes for
checkpoints, memory, and documents. Full-text search covers ordinary terms;
trigram matching also makes Korean partial matches useful. Search through
`GET /v1/context/search?q=&session=&kind=&limit=&scope=docs|ctx|all` or:

```sh
panewire ctx search 'handoff query' --scope all --session my-session \
  --hub-url https://hub.example --hub-token-env /safe/hub.env
```

Results include timestamp, session, kind, title, and a body snippet. Context
search includes checkpoints and memory; `docs` searches only documents.

Documents use inbox-relative keys such as `jobs/example/brief.md` and are
stored in the same `panewire` PostgreSQL database, without joins to another
database:

```sh
panewire doc put --key jobs/example/brief.md --file brief.md --kind brief \
  --session example --job example --hub-url https://hub.example --hub-token-env /safe/hub.env
panewire doc get jobs/example/brief.md --hub-url https://hub.example --hub-token-env /safe/hub.env
panewire doc list --prefix jobs/ --hub-url https://hub.example --hub-token-env /safe/hub.env
```

Bodies are limited to 512 KiB, retain a SHA-256 digest, and use the same secret
guard. `doc import` defaults to a disconnected dry run; it recursively matches
`**/*.md`, reports file and kind counts, and prints only rejected keys. With
`--apply`, equal SHA-256 records are skipped, making repeated imports
idempotent. It classifies `jobs/<job>/brief.md` as `brief`, `report.md` as
`report`, `answer-*.md` as `answer`, and other Markdown as `note`.

## Network and operations

The normal hub remains loopback-only. `--listen-tailnet` is optional and
accepts only a `100.64.0.0/10:PORT` address; wildcard and public addresses are
rejected. Keep Cloudflare `/ui` policy unchanged.

Create the separate `panewire` database and its non-superuser application role
with `deploy/sql/panewire_context.sql`; the password is passed to `psql`, never
stored in the SQL file. Set the mode-0600 `/etc/panewire/context-db.env` to
`PANEWIRE_CONTEXT_DB_URL=postgres://…` and install
`deploy/systemd/panewire-hub.service` plus
`panewire-context-backup.timer`. Hub startup applies its versioned PostgreSQL
migrations.

The bootstrap SQL creates `pg_trgm`; startup also executes `CREATE EXTENSION
IF NOT EXISTS pg_trgm` and fails clearly when the application role lacks that
database permission. CI runs the same migration against PostgreSQL 17.

The timer runs `pg_dump -Fc` daily and retains 14 days. It requires `pg_dump`
on the NCP host (or a wrapper that invokes the approved `at-standby` image).
To restore, stop the hub, retain the failed database, then have an operator run
`pg_restore -d panewire context-YYYYMMDD.dump` and start the hub. Do not expose
the database listener beyond loopback.
