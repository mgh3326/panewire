# Quota commands

`panewire quota` is the quota entry point. Pacelever is an internal dependency
scheduled for removal; callers should use panewire's interface.

- `panewire quota status [--json]` reads the local host's cached readings.
- `panewire quota refresh POOL` measures the named pool now, with a 60-second bound. `claude` and `codex` are measured by panewire itself ([claude](#native-measurement-claude), [codex](#native-measurement-codex)), and so is `devin` once the host's `spawn.json` lists it in `quota_native` ([devin](#native-measurement-devin), [shadow and native](#shadow-and-native)); every other pool, and `devin` until then, runs the dependency.
- `panewire quota observe KIND ...` forwards the observation kind and all remaining arguments, including `devin-free-limit` options, and returns the dependency's exit code.

Status and version calls each have a 30-second bound; gate calls have a
30-second bound and catalog calls a 15-second bound. A timeout reports
`<what>: timed out after <duration>`. Warnings from the dependency are omitted.

## Status JSON v1

`status --json` emits an object with `schema` set to
`panewire.quota.status.v1`, `generated_at` (the source timestamp), and a
`hosts` array. Each host contains `host`, `pacelever_version`, and `readings`.
Each reading has `pool`, `account` (empty if unknown), `source`, `age_seconds`
(number or null), `stale`, and `buckets`. Each bucket contains `window`,
`used_pct` (number or null), optional `resets_at`, and `scope` with `kind` and
optional `name`. Age is measured by the source, in seconds. A refresh that
returns a JSON reading emits the same reading shape; text responses pass through.

## Native measurement: claude

panewire measures the `claude` pool in its own process (`internal/quota/claude`,
`internal/quota/shared`); the node daemon, the `quota.refresh` node command,
placement's on-demand refresh and `panewire quota refresh claude` all run the
same code. The shared store flow below is shared with
[codex](#native-measurement-codex). Other pools still run pacelever until their
own change.

- **API first.** `GET /api/oauth/usage` on Anthropic's API host with the
  OAuth bearer token and `anthropic-beta: oauth-2025-04-20`; 20 s timeout,
  redirects refused, 2 MiB body cap. `five_hour` is the `5h` and `seven_day`
  the `7d` account bucket; each `limits[].scope.model.display_name` is a `7d`
  model bucket.
- **One CLI fallback.** If the API fails, `claude -p /usage --allowed-tools ""`
  runs once in a private temporary directory (15 s, its own process group
  killed on timeout, stderr discarded) and its panel is parsed: "current
  session" is `5h`, "current week (all models)" `7d`, other weeks model
  buckets; `% used` and `% left` both read. If the CLI fails too, the API's
  failure is the result. The CLI gets the inherited environment plus
  `CLAUDE_CONFIG_DIR` when an account is selected; never the token.
- **Error kinds** are pacelever's names (`auth` for 401/403, `rate_limited` for
  429 with `Retry-After` seconds or date capped at a day and a 300 s default,
  `http`, `network`, `parse_error`, `probe_error`, `credentials`,
  `token_expired`, and the shared-store kinds `account_mismatch`,
  `account_unknown`, `probe_failed`, `shared_*`). The node's login rule is
  unchanged: Login is missing only for `auth`, `auth_failed`, `credentials`
  and `token_expired`.
- **Credentials** are located, not copied: `<dir>/.credentials.json`
  (`claudeAiOauth.accessToken`, `expiresAt` in ms) where `dir` is
  `CLAUDE_SECURESTORAGE_CONFIG_DIR`, else `CLAUDE_CONFIG_DIR`, else
  `~/.claude`; on macOS also the keychain item `Claude Code-credentials`
  (suffixed with `-<hex(sha256(NFC(dir)))[:4]>` when a config dir is set). The
  unexpired login, then the later expiry, wins. The token is read into memory
  for the request and never logged, written, put in an error or on a command
  line; errors carry kinds, not response bodies.
- **Account key** `claude:<fp>`, `fp = hex(sha256("claude-account|" +
  oauthAccount.accountUuid))[:16]`, from `~/.claude.json` (or
  `$CLAUDE_CONFIG_DIR/.claude.json`), only when the login is shareable (the
  secure-storage dir is not steered away from the config dir).
- **Shared store flow.** The store is the `handoffkeep_env` named in the
  node's `spawn.json`. A fresh reading (valid as of the later of `measured_at`
  and `confirmed_at`, within 900 s, every window still running) is returned
  without measuring. Otherwise the node takes the account's lease (120 s),
  re-reads, measures, publishes `{kind: "auto", measured_at, buckets}` and
  releases. Another holder's live lease is polled (1 s, at most two lease
  windows) and an expired one taken over once; a measurement for another
  account refuses to publish (`account_mismatch`). A 429 backs the account off
  in memory for its `Retry-After`. Presence (`PUT /v1/quota/presence`) is
  announced at most every 10 minutes per account, under the node's machine id.
  With no `handoffkeep_env` the node measures directly and publishes to no
  store; with one it cannot use, the refresh fails `shared_unavailable`.

## Native measurement: codex

panewire measures the `codex` pool in its own process (`internal/quota/codex`)
through the same four paths and the same shared store flow as claude.

- **App-server first.** `codex -s read-only -a never app-server` runs with its
  working directory and `CODEX_HOME` set to the codex home, in its own
  process group, for at most 15 s. panewire sends `initialize`, the
  `initialized` notification, `account/rateLimits/read` and `account/read`.
  `rateLimits.primary` and `.secondary` are account buckets and each
  `rateLimitsByLimitId` entry except `codex` (sorted by id) is a model bucket
  named by its `limitName` (else `limitId`, else the id) with a leading
  `codex_` (capitalised) or `codex ` removed. A `free` plan with no windows is
  one `free` bucket at 0%. The process is always killed (the whole group) and
  reaped within 2 s: on success, failure, timeout and cancellation.
- **Wham usage API fallback.** Any app-server failure (no binary, a start or
  exchange error, an empty non-free plan, no measured window) falls back
  silently to `GET /backend-api/wham/usage` on ChatGPT's backend host with the
  access token as `Authorization: Bearer` and `ChatGPT-Account-Id:
  <account_id>`; 20 s timeout, redirects refused, 2 MiB body cap. `rate_limit.primary_window` and
  `.secondary_window` are account buckets, each `additional_rate_limits[]` a
  model bucket named by `limit_name`, and `credits` a `credit` bucket measured
  against a fixed 1000. The `x-codex-primary-used-percent` and
  `x-codex-secondary-used-percent` headers override the body's percentages and
  `x-codex-credits-balance` the balance. A reset is `reset_at` (epoch seconds)
  or now + `reset_after_seconds`.
- **Window labels** come from the window length: 28 days or more is `30d`, 6
  days `7d`, 20 hours `1d`, 4 to 6 hours `5h`, else `<n>s`. The app-server's
  `windowDurationMins` is converted to seconds first.
- **Error kinds** are the claude section's, with two differences: a 429 without
  `Retry-After` carries no default wait (the shared flow backs the account off
  300 s itself), and the app-server's own failures are never reported, only the
  API's.
- **Credentials** are located, not copied: `<codex home>/auth.json`
  (`tokens.access_token`, `tokens.account_id`), where the codex home is the
  account selector, else `CODEX_HOME`, else `~/.codex`. Without a token nothing
  is spawned or sent and the failure is `credentials`. The access token is read
  into memory for the one HTTP request; the app-server reads its own login. It
  is never logged, written, put in an error or on a command line. The
  app-server inherits the node's environment with `CODEX_HOME` set.
- **Binary** resolution: `PACELEVER_CODEX_BIN` (kept so an existing override
  works), then the configured path, then `codex` on `PATH`, then on macOS
  `/Applications/Codex.app/Contents/Resources/codex` and
  `~/Applications/Codex.app/Contents/Resources/codex`.
- **Account key** `codex:<fp>`, `fp = hex(sha256("codex-account|" +
  tokens.account_id))[:16]`. The email the app-server reports is not used. A
  login without an `account_id` has no key (`account_unknown`, and a host with
  no login at all is `credentials`); without a configured store the node still
  measures it directly.
- **Shared store flow** as for claude, except that a reading stays fresh for
  300 s, the default for every pool but claude.

## Native measurement: devin

panewire measures the `devin` pool in its own process (`internal/quota/devin`)
with pacelever's sources, buckets, error kinds and account fingerprint, and the
shared store flow below. It ships in [shadow](#shadow-and-native); the switch
makes it the pool's only measurement.

- **Only a host with a devin login measures.** The login is a non-blank
  `windsurf_api_key` in `<XDG data>/devin/credentials.toml` (`~/.local/share`
  by default). The key only proves a login exists; it is never read into a
  request, hashed or logged. Any other host answers `credentials` ("no login
  here") before any store call: it takes no lease, runs no probe and sends no
  request. The shared reading is read through placement and the quota gate as
  before.
- **Web source first.** With `DEVIN_BEARER_TOKEN` in the environment and
  `[pools.devin] org_id` in pacelever's `config.toml` (`PACELEVER_CONFIG`, else
  `<XDG config>/pacelever/config.toml`; the id must look like `org-…`),
  `GET /api/<org>/billing/quota/usage` on devin's web app host with the bearer
  token and `x-cog-org-id`; 20 s timeout, redirects refused, 2 MiB cap. `1d` (omitted
  when `hide_daily_quota`) and `7d` account buckets read from the flat
  `*_percentage` / `*_reset_at` fields or the nested `quota_usage` object
  (`used_percent`, `remaining_percent`, or `used`/`limit`/`remaining`); a value
  below 1 is a fraction, a `remaining_percent` of exactly 1 is 100% left. The
  plan name is capitalised. A 401/403 is `auth_failed`, a 429 `rate_limited`
  (with `Retry-After`, no default wait), another non-200 `http`, no response
  `network`, an unreadable body or no window `parse_error`. Any web failure
  falls through to the probe without being reported, as pacelever's does.
- **The `/usage` probe.** `devin --respect-workspace-trust false` runs on a
  200×50 pseudo-terminal in a private empty git repository (`devin` from
  `PACELEVER_DEVIN_BIN`, `[pools.devin] bin`, or `PATH`), at most 35 s. The
  probe waits for an input prompt, types `/usage`, and, after the input has
  settled (500 ms), Enter in a separate write; it parses the panel once the
  screen has been quiet 250 ms. **The prompt is any of `❯`, `❭` (U+276D, devin's
  own), `›`, `> ` or `Shift+Tab`** — without U+276D the probe never types and ends
  at its timeout (pacelever#57). A line naming weekly/7-day, 5h/5-hour, daily
  or monthly with a percentage is a `7d`/`5h`/`1d`/`30d` bucket (`% used`,
  `% left` or `% remaining`; a reset of "resets in 6d 2h" is now + that); a panel
  with none falls back to the plan line's "NN% remaining". A trust prompt, a
  timeout, an early exit or a missing binary is `probe_error` with the stage
  reached; an unreadable panel is `parse_error`. The TUI's whole process group is
  killed and reaped on every exit path. pacelever's `devin models list` Free tags
  are not part of the shared reading and are not measured.
- **Account key** `devin:<fp>`, `fp = hex(sha256("devin-account|" + org))[:16]`,
  `org` = `devin.org_id` in `<XDG config>/devin/config.json`, only with a login.
  A web reading carries the organization pacelever's config names, so a web
  answer for another organization than the login's is `account_mismatch` and not
  published; a login with no organization is `account_unknown`.
- **Free tier.** devin's SWE-2 models have no used percentage, so a reading also
  carries `free_bucket` (`available`, `exhausted` with `resets_at`, or
  `unknown`), which pacelever's gate requires of any devin reading. It comes
  from the host's own free-limit record (`devin-free-limit.json` in pacelever's
  cache directory, written by `panewire quota observe devin-free-limit`): an
  exceed that still holds is `exhausted` until its reset, a record with no
  readable reset holds for an hour after its observation, one with no readable
  time is `unknown`, none is `available`. Merging keeps another host's later
  hold, and a locally recorded exceed is published even over a fresh reading.
  The free state does not depend on the paid probe: a failed measurement still
  publishes `{buckets: [], free_bucket}` and the refresh succeeds with no paid
  window (pacelever's rule); only a 429 is remembered, backing the paid probe off
  for its `Retry-After` (300 s default) while the free state keeps publishing.
- **Shared store flow** as for claude, with a 300 s freshness window, except
  that **the wait for another host's measurement is bounded at 15 s** (not two
  lease windows): past it the refresh ends `shared_pending` instead of holding a
  spawn path.

## Shadow and native

A pool that can be measured natively but still spawns through pacelever (today
only `devin`) has a switch in the node's `spawn.json`:

```json
{"quota_native": ["devin"]}
```

- **shadow (default; key absent, unreadable or not listing the pool).**
  Everything that ran pacelever for the pool still does: the fleet-state loop,
  the `quota.refresh` node command, placement's refresh and `panewire quota
  refresh devin` (which runs `pacelever refresh devin`). The node also measures
  the pool itself, without touching the store (no lease, no publication, no
  presence — pacelever owns the shared reading), and writes one line per
  comparison: `msg="quota shadow compare" pool=devin verdict=… account=…
  native_buckets=… pacelever_buckets=… diffs=…`. `verdict` is `match` (same
  account; same buckets, used percentages within 1 point, resets within 5
  minutes), `differ` (with `diffs` naming each `used:`, `reset:`,
  `native_missing:`, `pacelever_missing:` or `account`), `native_error`,
  `pacelever_error` or `both_error`, with each side's error kind. A host without
  a login logs `quota shadow skipped reason=no_login` at debug. The comparison
  runs in the background, one per pool at a time, so it never delays a spawn.
  Only the node daemon compares (the loop and `quota.refresh`); the CLI in shadow
  is pacelever alone.
- **native (`quota_native` lists the pool).** The same four paths run the
  native measurement and its store flow and run no pacelever for the pool. The
  file is read on every refresh: flipping it needs no restart, and removing the
  line returns to shadow. `internal/fleet` sends the refresh of a switched pool
  through `panewire quota refresh <pool>` on the host that holds the account
  (locally or over ssh), and that host's own `spawn.json` decides; a host running
  an older panewire answers with pacelever.

The desk flips the switch after reading the comparison lines: a few hours of
loop rounds and the spawns made meanwhile.

## Artificial Analysis annotations

`panewire quota aa-sync --out catalog.json` reads the current catalog through
pacelever, fetches the free Data API, and writes a catalog artifact for review
and a later operator-controlled catalog push. `--dry-run` reports the result
without writing the artifact. The catalog must be fresh and server-sourced when
using `--out`. Only explicit model mappings are annotated; unmapped or ambiguous
rows are reported. Annotations are model priors, not grade evidence.

Set `ARTIFICIAL_ANALYSIS_API_KEY` in the environment, or place it in the optional
mode-0600 `panewire/aa.env` under the user configuration directory. Data is
provided by [Artificial Analysis](https://artificialanalysis.ai/); see its
[Data API reference](https://artificialanalysis.ai/data-api/docs/reference).
