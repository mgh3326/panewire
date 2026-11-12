# 쿼터 명령

`panewire quota`가 쿼터 인터페이스입니다. 판정, launch lookup, 측정은 panewire의 native 구현만 사용합니다.

- `panewire quota status [--json]`은 측정 가능한 native 풀의 저장된 값을 읽습니다. 측정이나 게시를 실행하지 않습니다. 이 host가 presence를 게시하는 credit pool도 각각 한 줄로 보여 줍니다 ([Credit pool presence](#credit-pool-presence)).
- `panewire quota refresh POOL`은 `claude`, `codex`, `commandcode`, `devin`, `agy`만 측정합니다. 다른 풀은 측정 소스가 없다는 이유와 함께 exit 5(`ExitConditionInvalid`)로 거부합니다.
- `[quota].gate`, `[quota].launch`, `[quota].native_pools`, `PANEWIRE_QUOTA_BIN`은 폐기되었습니다. 설정 파일에 키가 있으면 migration 경고를 출력합니다. `gate`와 `launch`는 지정할 경우 `"native"`만 허용하며, `native_pools`는 무시됩니다. 환경 변수 `PANEWIRE_QUOTA_BIN`도 무시됩니다.
- operator credit pools (`anthropic-api`, `openai-api`)의 수동 기록은 shared store에서 계속 읽습니다.
- `panewire quota observe devin-free-limit`은 Devin SWE-2 free-tier limit을 호환 cache에 기록하거나 지웁니다. `--reset <RFC3339>`을 지정하고, reset을 모르면 `--unparsed`로 1시간 보수 hold를 기록합니다. `--unparsed`는 아직 유효한 기존 hold를 단축하지 않습니다. `--reset`은 운영자가 기존 hold를 명시적으로 수정할 때 사용합니다. `--clear`는 local 기록을 지우며 손상된 cache 파일도 제거합니다. 손상된 파일을 account 단위로 clear할 수는 없으므로 이 경우 `--account-key` 없이 `--clear`를 실행합니다.

## Status JSON v2

`status --json`은 `schema`가 `panewire.quota.status.v2`인 객체를 출력하며 `generated_at`과 `hosts`를 포함합니다. 각 host에는 `host`와 `readings`가 있습니다. pacelever 관련 필드는 없습니다. reading에는 `pool`, `account`, `source`, `age_seconds`, `stale`, `buckets`가 있습니다. 이 host가 presence를 게시하는 credit pool의 reading에는 `class`, `credit`, `presence`도 있습니다 ([Credit pool presence](#credit-pool-presence)).

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
- **Binary** resolution: `PANEWIRE_CODEX_BIN` (`PACELEVER_CODEX_BIN` fallback),
  then the configured path, then `codex` on `PATH`, then on macOS
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

`commandcode` pool은 panewire 프로세스(`internal/quota/commandcode`)에서 측정하며
claude, codex와 같은 shared store 흐름을 사용합니다. 모든 호스트에서 항상 native로
측정합니다.

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
  microseconds, RFC 3339 or an HTTP date. The vendor's 7d limit is a rolling
  window whose `resetAt` reads relative to the reading time (exactly 5 days after
  the reading time), not a fixed calendar reset boundary; it is marked
  unpaceable (no `ResetsAt`) so `projectAtReset` does not turn a moving reset into
  an artificial pace (hk 4330). 5h keeps its `resetAt`, and 30d carries the
  subscription `currentPeriodEnd`.
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

`devin` pool은 panewire 프로세스(`internal/quota/devin`)에서 측정합니다. 기존
source, bucket, error kind, account fingerprint 및 shared store 형식을 유지하며,
모든 호스트에서 항상 native로 측정합니다.

- **Only a host with a devin login measures.** The login is a non-blank
  `windsurf_api_key` in `<XDG data>/devin/credentials.toml` (`~/.local/share`
  by default). The key only proves a login exists; it is never read into a
  request, hashed or logged. Any other host answers `credentials` ("no login
  here") before any store call: it takes no lease, runs no probe and sends no
  request. It has no account key, so it cannot read the shared reading itself;
  placement and the quota gate read it from the store as before.
- **Web source first.** With `DEVIN_BEARER_TOKEN` in the environment and
  `[pools.devin] org_id` in panewire's `config.toml` (`PANEWIRE_CONFIG`, else
  `<XDG config>/panewire/config.toml`; legacy pacelever paths are read when
  the panewire file is absent; the id must look like `org-…`),
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
  `PANEWIRE_DEVIN_BIN` (`PACELEVER_DEVIN_BIN` fallback), `[pools.devin] bin`,
  or `PATH`), at most 35 s. The
  probe waits for an input prompt, types `/usage`, and, after the input has
  settled (500 ms), Enter in a separate write; it parses the panel once the
  screen has been quiet 250 ms. **The prompt is any of `❯`, `❭` (U+276D, devin's
  own), `›`, `> ` or `Shift+Tab`** — without U+276D the probe never types and ends
  at its timeout (pacelever#57). A line naming weekly/7-day, 5h/5-hour, daily
  or monthly with a percentage is a `7d`/`5h`/`1d`/`30d` bucket (`% used`,
  `% left` or `% remaining`); an absolute reset date (such as `resets Nov 15, 8:00 AM (UTC)`)
  is parsed directly as a stable UTC timestamp, while a duration like "resets in 6d 2h"
  is now + that (hk 4330); a panel with none falls back to the plan line's "NN% remaining". A trust prompt, a
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
  from the host's own free-limit record (`devin-free-limit.json` in the
  pacelever-compatible cache directory, written by panewire's native
  `panewire quota observe devin-free-limit` command): an
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

`agy` (Antigravity) pool은 panewire 프로세스(`internal/quota/agy`)에서 측정합니다.
기존 source, bucket, error kind 및 shared store 형식을 유지하며, 모든 호스트에서
항상 native로 측정합니다.

- **agy meters per group, and the groups have separate quotas.** The `gemini`
  group covers the Gemini models (`agy-flash38-*`); the `3p` group
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
  A refused token is re-armed once with `agy models` under the panewire lock
  file (`<cache>/panewire/agy-wake.lock`; the legacy cache path is used during
  migration), so concurrent refreshes never wake the CLI at once. It is
  retried; a token another holder just refreshed is reused. The
  OS keyring item the CLI keeps is the login when the file has none, and the
  fallback when the file's token stays refused: on macOS the keychain item
  (`security find-generic-password -s gemini -a antigravity`), on Linux the
  secret service item with attributes `service=gemini` and `username=antigravity`
  (the item go-keyring writes), read over the session bus with no prompt and a
  10 s bound; a locked item, a missing session bus or no answer is no login.
- **Credentials** are located, not copied: the token is `token.access_token` of
  `~/.gemini/antigravity-cli/antigravity-oauth-token`, else of
  `$XDG_CONFIG_HOME/antigravity-cli/antigravity-oauth-token`. It is read into
  memory for one measurement and never logged, written, put in an error or on a
  command line. `PANEWIRE_AGY_BIN` names the agy binary (`PACELEVER_AGY_BIN`
  fallback), else `agy` on `PATH`.
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
  group's windows only: with gemini exhausted `agy-flash38-*` is `window_exhausted`
  and `agy-sonnet46` is still offered, and the reverse. A rung whose group the
  reading has no window for is `quota_unknown` (no other group's window stands in
  for it). A rung that maps to no group keeps the conservative rule: the
  strictest group blocks it. The admission gate (`pacelever gate`) already
  matches a row to its own group.

### Native 측정 풀

`claude`, `codex`, `commandcode`, `devin`, `agy`는 모든 호스트에서 native로 측정합니다. `[quota].native_pools`는 폐기된 키이며 무시됩니다. 다른 풀(`grok`, `kimi`, `kiro`, `clinepass`, `upstage` 등)은 자동 측정하지 않습니다. quota refresh, node quota loop, `quota.refresh` op, 선택 시 refresh는 해당 풀을 측정하지 않고 소스 없음으로 보고합니다.

`[quota].accounts`와 `measurable_accounts`는 계정 식별 및 host 측정 허용 범위에 계속 사용됩니다.

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
- **The embedded catalog snapshot** (`internal/fleet/catalog-snapshot.json`,
  pacelever v0.3.28's `catalog_snapshot.json` byte for byte) for what the
  handoffkeep rows do not carry: each rung's family; a rung the snapshot holds
  unplaced at the same grade stays unplaced (it needs an exact E6 marker); the
  effort a profile defaults to; the alias `codex-max` → `codex-sol`; and the
  rows of a profile handoffkeep does not list.
- **The panewire config file**, loaded with the validation rules ported from
  pacelever v0.3.28's `LoadConfig` before native launch resolves anything
  (`internal/fleet/launch_config.go`): the TOML types of `[quota]`,
  `[profiles]`, `[bench]`, `[settings]` and each known pool; the shared quota
  TTL ranges, `host_id`, per-pool fresh TTLs, account fingerprints and
  measurable account keys; and each known pool's class, exploration, cutoff,
  in-flight reserve and `credit_max_age_s`. A file pacelever refuses exits 2.
  The pool policy in it, read as [Admission](#admission) reads it, gives
  subscription. The gate and the credit pool views keep their narrower read.
- **The E6 marker** from the request, else `PANEWIRE_E6_ARM` (with
  `PACELEVER_E6_ARM` fallback). For a rung whose catalog effort is empty the
  marker is `<canon>@`; [config](config.md) lists the forms.

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
| Seat roles: a credit rung seats a worker or tester, or a builder on a placed default rung; `fable` and `codex-astra` have no role restriction of their own (their rung's gate decides, as for any profile); any other `astra` profile is removed | `role_restricted: …` |
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

### Native launch lookup

`[quota].launch`는 폐기된 설정 키입니다. 설정에 남아 있어도 경고 후 native lookup을 사용하며, 지정한 값이 `"native"`가 아니면 설정 오류입니다. panewire의 catalog lookup이 launch 결과를 결정합니다. `quota_launch_shadow` 이벤트와 shadow 비교 코드는 제거되었습니다.

### Credit pool views

Selection과 admission은 pool policy 및 shared store에서 credit pool class, manual entry, balance를 읽습니다. shared store를 쓸 수 없으면 status를 조회한 것으로 추측하지 않고 오류를 반환합니다. `quota_pools_shadow` 비교는 제거되었습니다. `anthropic-api`와 `openai-api`의 operator manual entries는 유지됩니다.

### Credit pool presence

A credit pool (`anthropic-api`, `openai-api`) has no login to derive an account
from and nothing to measure: the operator records its balance by hand and the
`omp` harness spends it. Placement binds a pool's rungs to a host only through
that host's presence record for the pool's account, so a credit pool is
placeable on a host exactly when the node publishes presence for it. The node's
quota loop (the one that runs with `--state-publish`, [fleet-state.md](fleet-state.md#node-publisher))
publishes it once per presence interval (10 minutes) when all three hold — this
is pacelever's `maybeCreditPresence`, the only publisher these pools had before
pacelever retired (hk 3998):

- `omp` is installed on the host. The node looks on `PATH` and in the fleet's
  install directories, because a daemon's `PATH` can be narrower than an
  operator shell's.
- The operator's pool class is not `exclude`: `[pools.<pool>]` in panewire's
  `config.toml` (`PANEWIRE_CONFIG`, else `~/.config/panewire/config.toml`,
  with legacy pacelever paths read during migration). An
  `until` date that has passed lifts the exclusion, as it does for the gate.
- The shared store holds an unexpired manual `window=credit` entry for the
  pool's account (`manual` in `/v1/quota/accounts/<key>/latest`).

The account key is `<pool>:<fingerprint>`: panewire's `[quota].accounts` entry
for the pool when this host has one (the key `panewire manual set` writes),
else pacelever's `[quota.accounts]`, so the key the pacelever era published
still names the same account. The host id is the one placement reads the local
host's records under: `[local].presence_id`, else the fleet machine id. A host
without the harness, an excluded pool, a missing or expired entry and an
unreadable store publish nothing; the publisher never invents a key or a
balance.

Record the credit with the remaining balance, the grant total and the expiry
(the window defaults to `credit` for a credit pool):

```sh
panewire manual set --pool anthropic-api --credit-balance 180 --credit-total 200 --expires 2026-11-16
```

`--credit-balance` is the remaining dollars, `--credit-total` the grant total
and `--expires` the date the credit expires (`YYYY-MM-DD` or RFC 3339). The
entry is stored under the pool's `[quota].accounts` key in the shared store, so
every host reads the same entry; a pool with no fingerprint there refuses with
`shared account fingerprint required`. The same command with
`--pool openai-api` records the other pool.

`panewire quota status` lists a line for each credit pool on a host that
publishes it (the harness is installed), with the class, the manual entry's
state (`live`, `expired`, `absent`, or `unknown` when the store could not be
read) and this host's presence (`present`, `absent` or `unknown`), so an
operator can see why a credit rung is or is not placeable there. The presence
record carries the id placement reads the local host under:
`[local].presence_id`, else the fleet machine id.

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
- **The pool policy** in panewire's `config.toml` (`PANEWIRE_CONFIG`, else
  `~/.config/panewire/config.toml`, with legacy pacelever paths read during
  migration): class and its `until` date, cutoff,
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
| `omp-musespark13` | omp | `commandcode/meta/muse-spark-1.3`, gated by `omp_commandcode` | worker, tester, reviewer | low, high, max; unrated until routing-policy grades it. |
| `omp-argon4` | omp | `commandcode/google/gemini-4-argon`, gated by `omp_commandcode` | worker, tester, reviewer | off only; no thinking levels. |
| `omp-mimo26pro` | omp | `commandcode/xiaomi/mimo-v2.6-pro`, gated by `omp_commandcode` | worker, tester, reviewer | off only; no thinking levels. |
| `omp-cc-opus55` | omp | `commandcode/claude-opus-5-5`, gated by `omp_commandcode` | worker, tester, reviewer | low, high, max; separate from `omp-opus`. |
| `omp-cc-sonnet55` | omp | `commandcode/claude-sonnet-5-5`, gated by `omp_commandcode` | worker, tester, reviewer | low, high, max; separate from `omp-sonnet`. |
| `omp-cc-sol61` | omp | `commandcode/gpt-6.1-sol`, gated by `omp_commandcode` | worker, tester, reviewer | off only; no thinking levels; separate from `omp-sol`. |

A seat whose model has no controllable thinking levels carries `no_thinking` in
the seat policy (`internal/fleet/policy.json`, hk 3974). Admission admits its
`off` rung only from that property, the launcher passes `--model <id>` without
an effort, and the landing proof accepts the footer without an effort glyph.
The property belongs to the seat, not to a catalog row: the row keeps the
rung's profile, effort, model id, pool and grade, and a row that still carries
a `no_thinking` field is ignored. `omp-dsflash41@off` and any
other credit rung at `off` whose seat lacks the property are refused
(`credit effort is not placed`).

All seven omp profiles and cmd-dsflash41 share one Command Code account, its 30-day window
(`commandcode_required_window_missing:30d`), and any
`quota.pool_max_concurrent.commandcode` cap (see [config](config.md)). A spawn
on any of these profiles counts against the same account reading.

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

### Native admission gate

`[quota].gate`는 폐기된 설정 키입니다. 설정에 남아 있어도 경고 후 panewire native verdict를 사용하며, 지정한 값이 `"native"`가 아니면 설정 오류입니다. `pacelever gate`, `quota_gate_shadow` 이벤트 및 shadow 비교 코드는 제거되었습니다. native verdict는 기존 shared quota store와 handoffkeep catalog를 사용합니다.

Golden verdicts in `internal/fleet/testdata/admission_golden/verdicts.json`은 이전 구현과의 보존 비교 자료입니다. 현재 production 경로에서는 외부 quota CLI를 실행하지 않습니다.
