# Quota commands

`panewire quota` is the quota entry point. Pacelever is an internal dependency
scheduled for removal; callers should use panewire's interface.

- `panewire quota status [--json]` reads the local host's cached readings. On a
  host with no `pacelever` on `PATH` it does not fail: it reports, for each pool
  this host measures itself, the shared store's stored reading of the host's
  account (read only: no measurement, lease or publication), sets
  `"pacelever": "absent"` on the host, and prints
  `quota status: pacelever absent; native readings only` on stderr. A pool whose
  reading cannot be read is listed with `source` `unavailable`.
- `panewire quota refresh POOL` measures the named pool now, with a 60-second bound. `claude` and `codex` are measured by panewire itself ([claude](#native-measurement-claude), [codex](#native-measurement-codex)), and so are `commandcode`, `devin` and `agy` once their switch is flipped ([commandcode](#native-measurement-commandcode), [devin](#native-measurement-devin), [agy](#native-measurement-agy)); every other pool, and those two until then, runs the dependency.
- `panewire quota observe KIND ...` forwards the observation kind and all remaining arguments, including `devin-free-limit` options, and returns the dependency's exit code.

Status and version calls each have a 30-second bound; gate calls have a
30-second bound. A timeout reports `<what>: timed out after <duration>`.
Warnings from the dependency are omitted. The catalog read is not a
dependency call; it has an 8-second bound ([catalog.md](catalog.md)).

## Status JSON v1

`status --json` emits an object with `schema` set to
`panewire.quota.status.v1`, `generated_at` (the source timestamp), and a
`hosts` array. Each host contains `host`, `pacelever_version`, and `readings`,
plus `pacelever: "absent"` when the readings are panewire's own.
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
[codex](#native-measurement-codex) and
[commandcode](#native-measurement-commandcode). Other pools still run pacelever
until their own change.

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
  announced at most every 10 minutes per account, under the node's machine id,
  as soon as the store answers the first read — before any lease or
  measurement — so a host whose measurement fails, is rate-limited or waits on
  another host's lease is still known to placement as holding the account (a
  login with no readable shared reading stays `unknown`, never absent).
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

## Native measurement: commandcode

panewire measures the `commandcode` pool in its own process
(`internal/quota/commandcode`) through the same shared store flow as claude and
codex. Unlike them it ships behind a switch, in [shadow](#shadow-and-native)
first.

- **Requests.** `GET` on the commandcode API host: `/alpha/whoami?limits=1`
  (who the key is), `/alpha/billing/credits` (scoped with `?orgId=` when whoami
  names an org), `/alpha/billing/subscriptions` and `/alpha/usage/summary`. Each
  sends `Authorization: Bearer <key>` and `User-Agent: cmd/1.88.1` (the API's
  edge rejects generic clients; this is pacelever's constant), 20 s timeout,
  redirects refused, 2 MiB body cap. Responses may carry their object under
  `data` or at the top level.
- **5h and 7d** are `windowLimits.fiveHour` and `.weekly` of the credits
  response: `used / cap`, clamped to 100 when past the cap. A window without a
  positive `cap` or a numeric `used >= 0` is left out; a malformed value never
  reads as an unused window. `resetAt` is epoch seconds, milliseconds or
  microseconds, RFC 3339 or an HTTP date.
- **30d** is the share of this billing period's plan credits already spent:
  `totalMonthlyCredits / (totalMonthlyCredits + credits.monthlyCredits)`, with
  `totalMonthlyCredits` from the usage summary (its `periodBasis` must be
  `billing-period` when it says one) and `monthlyCredits` (the plan credits
  left, USD, floored at 0) from the credits response. The reset is the
  subscription's `data.currentPeriodEnd`. Purchased and free credits are never
  read: they are outside the plan's allowance, so neither the numerator nor the
  denominator includes them. There is no plan table, so a price change needs no
  release.
- **A missing 30d is a missing window.** If the usage summary or the credits
  `monthlyCredits` cannot be read (a failed or malformed response, a non-numeric
  or negative total, another `periodBasis`, a zero total) the reading has the
  5h and 7d buckets and no 30d bucket; nothing is estimated. If only the
  subscription is unreadable, 30d keeps its value and has no reset (and the
  reading has no plan name). Refusing a spawn for want of a 30d window is the
  gate's rule, not this measurement's. A deadline that ends during a secondary
  request fails the whole measurement as `timeout` rather than publishing a
  partial reading.
- **Error kinds** follow pacelever's for this pool: no key `credentials`; a
  401/403 `auth_failed`, except a 403 whose body is not JSON, which is
  `edge_rejected`; 429 `rate_limited` (with `Retry-After`; without it the shared
  flow backs the account off 300 s itself); a deadline `timeout`; a failed
  request `network`; any other status `http`; unreadable JSON or no usable
  window `parse_error`. A body saying `success: false` is `http`. Only the credits
  and whoami requests decide the kind; a secondary request's failure never
  does.
- **Credentials** are located, not copied: the key is
  `COMMAND_CODE_API_KEY`, else `COMMANDCODE_API_KEY`, else `apiKey` of
  `~/.commandcode/auth.json` — pacelever's sources, no others. It is read into
  memory for the requests of one measurement, and never logged, written, put in
  an error or on a command line.
- **Account key** `commandcode:<fp>`, `fp = hex(sha256("commandcode-account|" +
  id))[:16]`, where `id` is whoami's `user.id`, else `org.id`: pacelever's key
  for the same login. The fingerprint is read before each refresh, which costs
  one whoami request the first time a key is seen and none after (the node
  remembers it). A login whose whoami names no id has no key (`account_unknown`
  with a store; the node still measures it directly without one).
- **Shared store flow** as for codex: a reading stays fresh for 300 s.

## Native measurement: devin

panewire measures the `devin` pool in its own process (`internal/quota/devin`)
with pacelever's sources, buckets, error kinds and account fingerprint, and the
shared store flow. It has a switch and ships in [shadow](#shadow-and-native).

- **Only a host with a devin login measures.** The login is a non-blank
  `windsurf_api_key` in `<XDG data>/devin/credentials.toml` (`~/.local/share`
  by default). The key only proves a login exists; it is never read into a
  request, hashed or logged. Any other host answers `credentials` ("no login
  here") before any store call: it takes no lease, runs no probe and sends no
  request. It has no account key, so it cannot read the shared reading itself;
  placement and the quota gate read it from the store as before.
- **Web source first.** With `DEVIN_BEARER_TOKEN` in the environment and
  `[pools.devin] org_id` in pacelever's `config.toml` (`PACELEVER_CONFIG`, else
  `<XDG config>/pacelever/config.toml`; the id must look like `org-…`),
  `GET /api/<org>/billing/quota/usage` on devin's web app host with the bearer
  token and `x-cog-org-id`; 20 s timeout, redirects refused, 2 MiB cap, and a
  body that is one JSON object and nothing after it. `1d` (omitted when
  `hide_daily_quota`) and `7d` account buckets read from the flat
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
  time is `unknown`, none is `available`. A record file that is present but unreadable or
  malformed is `unknown` too; only an absent file is `available`. Merging keeps another host's later
  hold, and a locally recorded exceed is published even over a fresh reading.
  The free state does not depend on the paid probe: a failed measurement still
  publishes `{buckets: [], free_bucket}` and the refresh succeeds with no paid
  window (pacelever's rule); only a 429 is remembered, backing the paid probe off
  for its `Retry-After` (300 s default) while the free state keeps publishing.
- **Store requirement.** Publishing a devin reading sends `free_bucket`, which
  the store accepts since handoffkeep `c8cf75f4` ("Accept additive free_bucket on
  shared quota observations", hk 2334, #96). Do not flip devin native on a host
  whose hub store predates it: the publish would fail `shared_publish_failed`
  (pacelever's devin publication needs the same build).
- **Shared store flow** as for codex: a reading stays fresh for 300 s, except
  that **the wait for another host's measurement is bounded at 15 s** (not two
  lease windows): past it the refresh ends `shared_pending` instead of holding a
  spawn path.

## Native measurement: agy

panewire measures the `agy` (Antigravity) pool in its own process
(`internal/quota/agy`) with pacelever's sources, buckets, error kinds and
shared-store flow. It has a switch and ships in [shadow](#shadow-and-native).

- **agy meters per group, and the groups have separate quotas.** The `gemini`
  group covers the Gemini models (`agy-flash`, `agy-flash38-*`); the `3p` group
  covers the third-party models, Claude and GPT (`agy-sonnet46`, `agy-opus46`,
  `agy-oss120`). A reading has only group buckets — `scope` `group`, the group
  name in `model` (`gemini`, `3p`, or `unknown` for an identity that maps to
  neither) — and never an account-wide bucket. Buckets are the worst reading of
  a group and window, ordered gemini, 3p, unknown and by window (`5h`, `7d`,
  `30d`, `1d`, `unknown`).
- **Sources, in order.** (1) The running Antigravity app's local server: the
  process whose command line names `language_server*` or `agy` and carries an
  antigravity marker, its `--csrf_token` and `--extension_server_port`, asked
  over Connect-RPC on `127.0.0.1` only (its TLS ports from `lsof` first, then
  the plain extension port; the CSRF token never leaves loopback) at
  `RetrieveUserQuotaSummary`, `GetUserStatus` and `GetCommandModelConfigs`.
  It needs no credential. Any failure falls through to (2). (2) The cloud:
  `POST /v1internal:retrieveUserQuotaSummary` at the daily Cloud Code
  endpoint, then the production one (pacelever's hosts, in that order), with
  the CLI's OAuth token, merged with the per-model list
  (`fetchAvailableModels`, best effort) that supplies a group the summary omits.
  A refused token is re-armed once with `agy models` under the lock file
  pacelever uses (`<cache>/pacelever/agy-wake.lock`, so the two never wake the
  CLI at once) and retried; a token another holder just refreshed is reused. On
  macOS the keychain item the CLI keeps (`security find-generic-password -s
  gemini -a antigravity`) is the login when the file has none, and the fallback
  when the file's token stays refused.
- **Credentials** are located, not copied: the token is `token.access_token` of
  `~/.gemini/antigravity-cli/antigravity-oauth-token`, else of
  `$XDG_CONFIG_HOME/antigravity-cli/antigravity-oauth-token`. It is read into
  memory for one measurement and never logged, written, put in an error or on a
  command line. `PACELEVER_AGY_BIN` names the agy binary, else `agy` on `PATH`.
- **Error kinds** follow pacelever's: no login `credentials`; a refused login
  `auth_failed`; 429 `rate_limited` (with `Retry-After`); a 5xx a retryable
  `network` failure; any other status `http`; a request that got no answer
  `network`; an answer with no usable window `parse_error`.
- **Account key** `agy:<fp>` is **configured, not derived** — agy has no login to
  hash, and pacelever takes it from config too. This host's `[quota.accounts]`
  `agy = "<16-hex fingerprint>"` names the account, and `[quota].measurable_accounts`
  must list `agy:<fp>`: the host measures agy, announces presence and publishes
  under that key only then. A host with neither does not measure agy, claims no
  presence, answers `credentials` (as a host with no login) and logs one line
  (`quota agy: this host has no measurable agy account …`); it never guesses a
  key. Both keys are read at each use, so a change needs no restart.
- **Shared store flow** as for codex: a reading stays fresh for 300 s.
- **Placement.** Each agy catalog rung names the group it draws from — the
  catalog row's `group` when pacelever's catalog names one, else the group of its
  model id by pacelever's rule (`gemini*` → gemini; `3p-*`, or a name containing
  `claude`, `gpt` or `oss` → 3p). Placement ranks and blocks a rung on its own
  group's windows only: with gemini exhausted `agy-flash*` is `window_exhausted`
  and `agy-sonnet46` is still offered, and the reverse. A rung whose group the
  reading has no window for is `quota_unknown` (no other group's window stands in
  for it). A rung that maps to no group keeps the conservative rule: the
  strictest group blocks it. The admission gate (`pacelever gate`) already
  matches a row to its own group.

### Shadow and native

`commandcode`, `devin` and `agy` are native only when the host's fleet config
(`~/.config/fleet/hosts.toml`, or `FLEET_HOSTS_CONFIG`) lists them:

```toml
[quota]
native_pools = ["commandcode", "devin", "agy"]
measurable_accounts = ["agy:0123456789abcdef"]   # agy: the account this host measures

[quota.accounts]
agy = "0123456789abcdef"                       # agy: its fingerprint (configured, not derived)
```

The file is read at each use, so flipping the switch needs no restart. A
missing, unreadable or invalid file leaves the pool in shadow; `native_pools`
lists only pools that have a switch (today `commandcode`, `devin` and `agy`) and any other entry is
a config error. `claude` and `codex` are always native and need no entry.

- **Shadow (the default).** Spawns, placement refresh, the `quota.refresh` node
  command and `panewire quota refresh commandcode` still run pacelever for the
  pool, and the fleet-state loop still publishes pacelever's reading. Beside each
  pacelever refresh the node measures commandcode itself — once, directly, with
  no store lease, no publication and no presence, so no other host's reading
  changes — and logs one line (the node's warning log). The measurement runs in the
  background with a 25 s bound, so it never delays or fails the refresh it
  rides on, and a pool already being measured in shadow is skipped, not queued:

  ```
  quota shadow commandcode: match=false account=same pacelever=[5h=0.0%@2026-11-06T09:00:00Z 7d=1.3%@2026-11-12T00:00:00Z] native=[5h=0.0%@2026-11-06T09:00:00Z 7d=1.3%@2026-11-12T00:00:00Z 30d=9.3%@2026-11-27T07:38:00Z] diff=30d: native only
  ```

  `match` is true when the account key and every window agree: used percentages
  within 1 point and resets within 60 s, a window on one side only is a
  difference. A failed side prints `error <kind>` in place of its windows, and
  `diff` says which side failed. pacelever cannot read 30d (its response has no
  `planId`), so a healthy shadow line reads `diff=30d: native only`: that is the
  expected difference, not a fault. The desk reads the lines and flips the switch.
  For `devin` the line is `quota shadow devin: …` and healthy means `match=true`;
  pacelever's windowless per-model Free tag is not a window and is left out of the
  comparison. A host without a devin login reads `native=error credentials` (the
  node starts no probe and sends no request there). For `agy` the line is
  `quota shadow agy: …` and a window is told apart by its group
  (`5h/gemini=20.0%@…  5h/3p=50.0%@…  7d/3p=60.0%@…`); healthy means
  `match=true`. A host with no measurable agy account reads
  `native=error credentials`.
- **Native.** The fleet-state loop, `quota.refresh`, placement refresh and
  `panewire quota refresh commandcode` use the node's measurement and the shared
  store as for codex, and no spawn-side step runs `pacelever refresh` for the
  pool (an ssh refresh of a remote account runs `panewire quota refresh
  commandcode`, which follows the target's own switch). The `pacelever gate`
  call is not part of this switch; [Admission](#admission) has its own.

### Artificial Analysis annotations

`panewire catalog aa-sync --out catalog.json` reads the catalog from
handoffkeep, fetches the free Data API, and writes a catalog artifact for
review and a later operator-controlled `panewire catalog push`. See
[the catalog command contract](catalog.md). `--dry-run` reports the result
without writing the artifact. The catalog must be fresh and server-sourced
when using `--out`. Only explicit model mappings are annotated; unmapped or
ambiguous rows are reported. Annotations are model priors, not grade evidence.

Set `ARTIFICIAL_ANALYSIS_API_KEY` in the environment, or place it in the optional
mode-0600 `panewire/aa.env` under the user configuration directory. Data is
provided by [Artificial Analysis](https://artificialanalysis.ai/); see its
[Data API reference](https://artificialanalysis.ai/data-api/docs/reference).

## Launch

Before admission, every spawn, delegate, placement and plan resolves the
launch rung: the profile (its canon), effort, model id, grade, gate, pool and
family a request runs. Until hk 3548 only `pacelever policy launch PROFILE
--json` answered it, from pacelever's own catalog cache (one-hour TTL), so a
row added in handoffkeep was refused until that cache expired (2026-11-08:
`omp-dsflash41@max`, `e6_arm_required: unplaced profile`, expired by hand on
two hosts). panewire now has its own lookup
(`internal/fleet/launch_native.go`), a rule-for-rule port of pacelever
v0.3.28's `policy launch` (catalog.go `resolveLaunch`, `resolveRow`,
`roleCheck`, `defaultEffort`; bench.go `decodeCatalog`, `ReadCatalog`). It
reads:

- **The bench catalog** through panewire's catalog client and its freshness
  rule ([catalog.md](catalog.md#cache)): a cache entry younger than the TTL,
  else handoffkeep, else a cache entry, stale past the stale maximum. The TTL
  and the stale maximum are pacelever's `[bench] catalog_ttl_s` and
  `catalog_stale_max_s` in its `config.toml`, as pacelever's policy launch
  reads them (bench.go `ReadRows`); unset, or zero or less, they are one hour
  and one day, pacelever's defaults and panewire's own. When a cached catalog
  refuses a request, the lookup reads handoffkeep once more before it
  refuses, so a new row is found by the next lookup; that read replaces the
  cache the selection also reads (see [Shadow and native
  launch](#shadow-and-native-launch)). Without any catalog it judges the
  bundled snapshot as stale, as pacelever does.
- **pacelever's bundled snapshot** (`internal/fleet/catalog-snapshot.json`,
  pacelever v0.3.28's `catalog_snapshot.json` byte for byte) for what the
  handoffkeep rows do not carry: each rung's family; a rung the snapshot holds
  unplaced at the same grade stays unplaced (it needs an exact E6 marker); the
  effort a profile defaults to; the alias `codex-max` → `codex-sol`; and the
  rows of a profile handoffkeep does not list.
- **pacelever's `config.toml`**, loaded with every rule pacelever v0.3.28's
  `LoadConfig` applies before `policy launch` resolves anything
  (`internal/fleet/pacelever_config.go`): the TOML types of `[quota]`,
  `[profiles]`, `[bench]`, `[settings]` and each known pool; the shared quota
  TTL ranges, `host_id`, per-pool fresh TTLs, account fingerprints and
  measurable account keys; and each known pool's class, exploration, cutoff,
  in-flight reserve and `credit_max_age_s`. A file pacelever refuses exits 2.
  The pool policy in it, read as [Admission](#admission) reads it, gives
  subscription. The gate and the credit pool views keep their narrower read.
- **The E6 marker** from the request, else `PACELEVER_E6_ARM`.

It answers as `pacelever policy launch` does through panewire: a refusal exits 3
with pacelever's reason (`quota catalog: e6_arm_required: …`; the pacelever path
reports `quota catalog: dependency failed`, since pacelever prints its reason
only on stderr), a missing profile or an unreadable pool policy exits 2, and a
stale catalog is refused `quota catalog is stale` (exit 70) unless the rung's
own rule refuses first. The rules, each with its test in
`internal/fleet/launch_native_test.go`:

| Rule (pacelever v0.3.28) | Refusal |
|---|---|
| A pi- profile or `cmd-dsflash41` needs an explicit effort, one of its placed efforts | `explicit --effort required for credit profile`, `credit effort is not placed` |
| An effort outside the effort ladder must be a catalog rung | `unknown effort rung` |
| `codex-med`, `builder-terra-*` and any profile with a `gpt-5.6-terra` row | `model_refused: retired model identity` |
| A profile with no live row | `profile "P" is unknown or retired` |
| An unrated or unplaced rung needs the exact `<canon>@<effort>` marker, except for the lead, consult, builder and counsel roles on an unrated rung | `e6_arm_required: exact measurement marker required`, `e6_arm_required: unplaced profile requires exact measurement marker`, `e6_arm_required` |
| A requested rung that is retired | `rung is retired in the catalog` |
| A credit or `codex-terra61` request for a rung that is not placed | `e6_arm_required: rung is not placed` |
| Seat roles: a credit rung seats a worker or tester, or a builder on a placed default rung; `fable` and `codex-astra` never a worker or captain, and a builder, tester or reviewer only for a round extension; `codex-astra` only for a director, architect, operator-request or round-extension purpose; any other `astra` profile is removed | `role_restricted: …` |
| An `--operator-request` must be `hk:task/N` or `hk:doc/…` | `operator_request_ref_invalid` |
| An unsubscribed profile or pool | `profile is unsubscribed` |
| A `consult_only` rung needs `--operator-request` (not `codex-astra`) | `profile is consult_only; pass --operator-request` |
| On a stale catalog, an escalation rung needs `--operator-request` | `catalog is stale; non-default gate requires --operator-request` |

An effortless request resolves to the profile's default effort, else the best
graded ordinary rung at the lowest effort; a requested effort with no eligible
rung of its own falls back to that rung and keeps the requested effort, as in
pacelever. The launch rule for a credit rung is pacelever's (`pi-` profiles
and `cmd-dsflash41`); the gate's stricter one ([Command Code
seats](#command-code-seats)) still refuses at admission. pacelever's
exploration does not apply: panewire passes no `--task`, so pacelever keeps
its baseline answer.

### Shadow and native launch

`[quota].launch` in the host's fleet config picks who resolves the rung:

```toml
[quota]
launch = "native"   # or "shadow", the default
```

- **Shadow (the default).** `pacelever policy launch` resolves it, and the
  spawn gets exactly its answer, as before. After pacelever answers, panewire
  resolves the same request offline and records one fleet event. The
  comparison is side-effect free:

  - it reads only local state: pacelever's `config.toml` and panewire's
    catalog cache, read only (and the handoffkeep endpoint from the
    svc-panewire credential, environment or `~/.config/panewire/hk-svc.env`,
    only to name the cache file);
  - it never asks handoffkeep, never refreshes after a refusal, and never
    writes, renames or touches a cache file, so it cannot change what the
    selection reads;
  - it never holds up the spawn: the lookup returns pacelever's answer at
    once, and the comparison — its local reads and its event append — runs
    in a background goroutine with no context of the caller's, so it cannot
    spend the deadline the next gate call needs;
  - the event is a best-effort fleet event append, to the local state
    directory only (no network).

  **One comparison at a time.** A process runs at most one shadow comparison,
  launch or [credit pool](#credit-pool-views), at a time. While one runs, a
  new one is skipped — no goroutine starts — and counted; the next event a
  comparison records starts its detail with `skipped_busy=N`. The slot is
  freed only when the running comparison really returns, so a hung
  filesystem holds one goroutine, not one per spawn. A comparison whose
  local reads take more than 2 s records `outcome=skipped
  reason=shadow_timeout` instead of its answer when they do return.

  **Sampling, not a record of every spawn.** Nothing waits for a comparison
  at process exit. A spawn admitted in a short-lived `panewire` process — a
  `spawn --dry-run`, `plan`, `resolve`, `delegate --headless`, or the legacy
  ssh `_spawn-stdin` — may exit before its comparison ends, and that
  comparison is lost. A real spawn submitted with `panewire spawn` or `work`
  is admitted in the hub server (placement: `launchPool` and the pool views)
  and then in the node daemon (`ExecuteNode`: `admitLaunch` and the pool
  views), both long-lived, so their comparisons complete; the losses are
  dry runs and the CLI-only commands above, plus comparisons skipped as busy.

  The cache counts as fresh under pacelever's `[bench] catalog_ttl_s` in its
  `config.toml` (default one hour), read in the same local config read the
  comparison makes, so the shadow and native mode agree on which cache is
  fresh. When the cache holds no fresh entry for the endpoint, nothing is
  compared: `outcome=skipped reason=no_fresh_cache`. A cache entry whose
  `fetched_at` is missing or unreadable is never fresh, whatever the TTL.
  Otherwise:

  ```
  kind=quota_launch_shadow outcome=disagree reason=commandcode
  detail=profile=omp-dsflash41 effort=max role=worker purpose= pacelever{exit=3 profile= model= effort= grade= gate= pool= family= stale=false reason="e6_arm_required: unplaced profile requires exact measurement marker"} native{exit=0 profile=omp-dsflash41 model=deepseek/deepseek-v4.1-flash effort=max grade=A+ gate=default pool=commandcode family= stale=false reason=""} diff=exit,profile,model_id,effort,grade,gate,pool
  ```

  `agree` means the same exit status, launch fields and reason;
  `reason_differs` the same status and fields in other words; `disagree`
  anything else, and `diff` lists the fields that differ. pacelever's reason is
  the `error:` line it prints on stderr. The native answer never changes the
  spawn's outcome. Expected differences: pacelever's own catalog cache is not
  panewire's, so a row changed in the last hour can differ until both caches
  read it, and the shadow never refreshes to close that gap; and pacelever
  with no catalog backend configured serves its snapshot as fresh where
  panewire refuses it as stale.
- **Native.** panewire's lookup decides and `pacelever policy launch` is never
  run. panewire-design flips it on a host after the shadow events agree; it is
  host config, independent of `[quota].gate`. Only native mode reads
  handoffkeep for the lookup, and only native mode refreshes after a cached
  refusal. That refresh replaces panewire's shared catalog cache, which the
  selection (`CLIQuota.Catalog`) also reads, so on a native host the
  selection can see a newer catalog before its own TTL expires. This is
  accepted: native is a deliberate host switch, and the refreshed catalog is
  handoffkeep's current one.

### Credit pool views

Selection ranks a credit pool, and admission refuses an excluded one, from the
pool's view: its class, the used share of its current operator credit entry
and its balance. The view follows `[quota].launch`:

- **Shadow (the default).** The view is pacelever's status document, read by
  one `pacelever --json` run, and the caller gets exactly that, as before.
  After it is read, when the shared store serves every credit pool (see
  Native), panewire reads each credit pool's class offline from pacelever's
  `config.toml` and records one fleet event per read:

  ```
  kind=quota_pools_shadow outcome=disagree reason=credit
  detail=anthropic-api pacelever{class=spend} native{class=exclude} openai-api pacelever{class=spend} native{class=spend} diff=anthropic-api:class
  ```

  The comparison is offline and side-effect free, as the launch shadow is: it
  reads only the pool policy files, never the shared store, in the background
  after the views are returned, one comparison per process at a time, and
  may be lost when a short-lived process exits ([Shadow and native
  launch](#shadow-and-native-launch); `outcome=skipped reason=shadow_timeout`
  when its reads take more than 2 s). So only the class is
  compared: the used share lives only in the shared store, and the balance is
  the same store's on both paths. `agree` means every credit pool has the same
  class; `disagree` anything else, with `diff` listing the differing
  `pool:class` pairs, or `error` when either side failed to read. The native
  view never reaches selection or admission.
- **Native.** When every credit pool of the pool policy (`credit_pools`) is a
  hand-recorded API pool (`anthropic-api`, `openai-api`) and the shared store
  is wired, panewire builds the view itself: the class from pacelever's
  `config.toml` as [Admission](#admission) reads it, the entry and balance
  from the shared store for the account `[quota.accounts]` (else pacelever's
  own `[quota.accounts]`) names. `pacelever --json` is not started. Otherwise
  — a credit pool the store does not serve, or no shared reader — the
  `pacelever --json` status read runs as in shadow.

panewire-design switches a host to native after its shadow events agree.

## Admission

Every spawn, delegate, placement and selection asks one admission question per
candidate: may this profile, effort and role run on this account now? Until
hk 3248 that answer came from `pacelever gate`. panewire now computes the same
verdict itself (`internal/fleet/admission_verdict.go`, a rule-for-rule port of
pacelever v0.3.28's gate) from three inputs:

- **The account's stored reading** in the shared quota store
  (`/v1/quota/accounts/<key>/latest`): bucket windows with scope, used
  percentage and reset, the plan, devin's free-bucket state, and the
  operator-entered manual windows. A request with an account key reads that
  account; a local request reads the account this host holds (the fleet
  config's `[quota.accounts]`, this host's presence records, then pacelever's
  own `[quota.accounts]`).
- **The bench catalog** from handoffkeep (`GET /v1/bench/catalog`), validated
  and cached by panewire as [catalog.md](catalog.md) describes; an unavailable
  or stale catalog refuses (exit 2). No pacelever process runs. The launch rung
  the verdict is asked about comes from the launch lookup ([Launch](#launch)),
  which has its own switch.
- **The pool policy** in pacelever's `config.toml` (`PACELEVER_CONFIG`, else
  `~/.config/pacelever/config.toml`): class and its `until` date, cutoff,
  subscription, `fusion_billing_fp`. A missing file is the default policy; an
  invalid one refuses (exit 2).

The verdict keeps pacelever's exit statuses (0 admit, 2 invalid, 3 blocked,
4 unmeasurable, 5 role denied, 6 unsubscribed) and prints the same
`scopefuel.gate.v1` receipt, with `"judged_by": "panewire"`, so every caller
reads it as before. Like pacelever's gate it measures a stale reading of an
account this host holds once (`panewire quota refresh <pool>`) and reads again;
in a pool panewire does not measure, a stale reading is unmeasurable. On a refusal of
a graded rung, `alternatives` lists the same-grade rungs the stored readings of
the other pools this host holds would admit, read with no lease and no
measurement (hk 3211).

Rules beyond pacelever's: a commandcode reading without its `30d` window is
unmeasurable (`commandcode_required_window_missing:30d`,
decision/2026-11-06/commandcode-30d-required-in-panewire-gate). It needs the
node's native commandcode measurement, the only one that publishes `30d`. And
a credit rung seats only a worker or tester (operator, 2026-11-07 for
cmd-dsflash41; hk 3532 extends it to every commandcode-pool rung); pacelever
also seats a builder on a placed default rung.

### Command Code seats

A rung is a credit rung when its catalog row's pool is `commandcode`
(`admissionCredit` in `internal/fleet/admission_verdict.go`). The pool decides,
not the profile name: a credit rung needs an explicit effort, ranks as credit
among alternatives, and seats only a worker or tester. The profiles that draw on
the pool:

| Profile | Harness | How it reaches Command Code | Seat roles | Notes |
|---|---|---|---|---|
| `cmd-dsflash41` | cmd | the cmd harness's Command Code provider | worker, tester | Operator 2026-11-07 (hk 3248): no builder. |
| `omp-dsflash41` | omp | omp's `commandcode` provider, gated by the host's `omp_commandcode` capability | worker, tester, reviewer | The policy seat lists reviewer, but admission refuses it on a credit rung (see below). |

Both profiles share one Command Code account, its 30-day window
(`commandcode_required_window_missing:30d`), and any
`quota.pool_max_concurrent.commandcode` cap (see [config](config.md)). A spawn
on either profile counts against the same account reading.

Admission refuses the reviewer role for omp-dsflash41 today, because the credit
rule admits only a worker or tester. The policy seat still lists reviewer, so a
reviewer launch passes the seat check but is refused at admission. Whether to
lift that for omp-dsflash41 is the operator's decision; it is not changed here.

Where panewire reads less than pacelever's local gate, it refuses or omits
rather than guesses:

- no local probe: a reading is the stored one, measured by the node;
- no host-local manual store, free-limit file, usage-limit hold or inflight
  count (a configured `inflight_reserve_pct` reads `inflight=unknown(local)`);
  the shared store carries the free-bucket state and manual windows instead.
  A local request falls back to the held account's stored manual windows when
  its automatic reading is unusable; a named account (`--account-key`) does not,
  as in pacelever;
- the catalog JSON has no `placed` flag or effort defaults: a graded rung is
  placed, and an effortless rung's reason names no default effort.

### Shadow and native gate

`[quota].gate` in the host's fleet config picks who decides:

```toml
[quota]
gate = "native"   # or "shadow", the default
```

- **Shadow (the default).** `pacelever gate` decides. Beside each call panewire
  computes its own verdict within 10 s — reading only: no measurement, no
  alternatives — and records one fleet event:

  ```
  kind=quota_gate_shadow outcome=disagree reason=claude
  detail=profile=opus effort=high role=worker account=claude:… pacelever{exit=0 pool=claude basis= reason="…"} native{exit=3 pool=claude basis= reason="quota exhausted window=5h cutoff=99"}
  ```

  `agree` means the same exit status, pool and gate basis and the same reason;
  `reason_differs` the same decision in other words; `disagree` anything else.
  A commandcode spawn on a host whose commandcode readings pacelever publishes
  reads `disagree` (no `30d`): that is the expected difference until the pool
  is native. The desk reads the events and flips the switch.
- **Native.** panewire's verdict decides: no `pacelever gate` and no
  `pacelever refresh` run in the gate, and the catalog comes from handoffkeep
  ([catalog.md](catalog.md)). `pacelever policy launch` follows its own switch
  ([Launch](#launch)); the status read of a pool the shared store does not
  serve and the selector's refresh of a pool panewire does not measure stay
  until hk 3250.

The golden verdicts in `internal/fleet/testdata/admission_golden/verdicts.json`
are pacelever's own: `generator/regen.sh` runs its gate over the scenarios in a
throwaway clone. A scenario where panewire differs on purpose records the
native verdict and why.
