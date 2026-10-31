# R26 hub-directed spawn

The hub keeps request status only in memory, just like presence. It forwards
a document key without fetching or storing brief text; the target node fetches
the document with its own handoffkeep client.

> A. hub — `POST /v1/spawn` (operator token)
>
> Request:
>
> ```json
> {"request_id":"<uuid4>","machine":"machine-a","cwd_key":"repo-a",
>  "brief":{"inline":"<≤65536 bytes UTF-8>"},
>  "args":["-m","codex-terra","-w","worker","-l","label-a","--t","T1","--job","job-a","--owner","lane-a"],
>  "wait_seconds":120}
> ```
>
> - 검증: `request_id` uuid, `machine` `^[a-z0-9-]{1,32}$`, `cwd_key`
>   `^[a-z0-9._-]{1,64}$`, inline/doc 중 정확히 하나(빈 값은 400), 선택적 `checkout`은
>   `worktree.prepare` 문법, `args`는 **runtug 플래그 allowlist**만
>   (`-m -w -l --t --effort --job --owner --report --role --lane --parent
>   -L --task --parent-job --builder-family --builder-job --tags --urgency`), 각 값
>   `^[A-Za-z0-9._:/@-]{1,200}$` 단 `--tags`만 쉼표 목록(최대 16개) 허용
>   (공백·셸 메타문자 거부), `--role`, `--builder-family`, `--parent-job`, `--builder-job` 값은 유효성
>   검사를 거치며, `wait_seconds`는 1..300.
> - `--report-relative`는 **노드의** jobs root 기준 상대 경로다. 절대 경로, 끝 `/`,
>   빈 세그먼트·`.`·`..` 세그먼트는 400으로 거부하며, 노드는 다른 플래그처럼
>   `runtug spawn`에 그대로 전달한다.
> - `checkout`은 노드에서 `worktree.prepare`를 먼저 실행한다. 성공하면
>   준비된 worktree에서 spawn하고, 거부되면 `rc:81` 및 준비 outcome을
>   반환하며 spawn을 호출하지 않는다.
> - 처리: 대상 노드가 connected **and** accepting_effective 아니면 503
>   `{"error":"node_unavailable","state":...}`. `request_id` 중복이면
>   409 + 기존 결과. 노드에 `job.spawn` 전송 후 `wait_seconds`까지
>   `job.spawn.result` 대기.
> - 응답: 200 `{"request_id","machine","rc":0,"job_id":"…","pane":"…","stdout_tail":"<≤4KiB>","completed_at"}`
>   / 노드 응답 없으면 504 `{"request_id","status":"pending"}`(노드는 계속
>   실행; `GET /v1/spawn/{request_id}`로 최대 1h 조회 — hub 인메모리,
>   presence와 같은 비영속). 노드 절단 시 `{"status":"lost"}`.
> - 브로드캐스트 `/v1/events`: `spawn.requested`·`spawn.result`{request_id,
>   machine, rc} (브리프 본문·args 값 미포함).
>
> B. 노드 — `job.spawn` 수신
>
> - 노드 설정 `~/.config/panewire/spawn.json`:
>   `{"cwd_map":{"repo-a":"/abs/path"},"workspace":"worker","herdr_session":"worker","enabled":true,"work_root":"/abs/work","handoffkeep_env":"/path/to/node-hk.env","brief_doc_prefix":"briefs/"}`.
>   `enabled` 기본 false(파일 부재 = 거부 `spawn_disabled`), `cwd_key` 미등록
>   = 거부 `cwd_unmapped` — 거부도 `job.spawn.result{rc:2, error}`로 회신.
> - 실행: 브리프를 `mktemp`(0600)에 쓰고 `env HERDR_SESSION=<s> runtug spawn
>   -c <cwd> -p <tmp> --host local -w <workspace> <args...>
>   --override-reason hub-directed`를 **argv로**(셸
>   문자열 금지), 타임아웃 `wait_seconds+60`, 종료 후 tmp 삭제.
>   `stdout_tail` 4KiB. 결과 메시지 `job.spawn.result{request_id, rc, job_id,
>   pane, stdout_tail}` — job_id/pane은 runtug OK 라인 파싱.
> - 동시성: 노드당 동시 spawn 실행 1(큐잉), 같은 request_id 재수신은 무시(멱등).

## A. hub — `POST /v1/spawn` (operator token)

Request:

```json
{"request_id":"<uuid4>","machine":"machine-a","cwd_key":"repo-a",
 "brief":{"inline":"<≤65536 bytes UTF-8>"},
 "args":["-m","codex-terra","-w","worker","-l","label-a","--t","T1","--job","job-a","--owner","lane-a"],
 "wait_seconds":120}
```

- Validation: `request_id` uuid, `machine` `^[a-z0-9-]{1,32}$`, `cwd_key`
  `^[a-z0-9._-]{1,64}$`, exactly one nonempty `brief.inline` or `brief.doc`, optional `checkout`
  following the `worktree.prepare` grammar, and `args` accepts **only runtug
  flags** (`-m -w -l --t --effort --job --owner --report --report-relative --role --lane
  --parent -L --task --parent-job --builder-family --builder-job --tags
  --urgency`), each value
  `^[A-Za-z0-9._:/@-]{1,200}$` — except `--tags`, a comma list of up to 16
  entries, all within the 200-char bound — (spaces and shell metacharacters
  rejected),
  and `wait_seconds` is 1..300. `--role` accepts `worker`, `builder`,
  `reviewer`, or `tester`; `--builder-family` accepts `anthropic`, `openai`,
  `google`, `xai`, `deepseek`, `meta`, `mistral`, or `other`; job values use
  `^[A-Za-z0-9._-]{1,128}$`.
- `--report-relative` is a report path relative to the **node's** jobs root
  (the caller cannot know the node's home). Absolute paths, a trailing slash
  and empty, `.` and `..` segments are refused with 400. The node passes it
  through to `runtug spawn` verbatim like the other flags. A hub without this
  flag answers 400 `invalid_request` for the whole request.
- `brief.doc` is the hk document key (for example,
  `briefs/<job>/brief.md`); the node fetches `GET /v1/documents/<key>` with
  its own handoffkeep credential. Keys must be under the node's configured
  `brief_doc_prefix` (default `briefs/`), and empty, `.` and `..` path segments
  are refused. Inline callers keep the existing request and response shapes.
- A `checkout` runs node `worktree.prepare` first. The node spawns in the
  prepared worktree. It accepts `{origin, mode, branch, sha, fetch, dir}` and
  follows the same mode-specific rules as `worktree.prepare`. A refusal
  returns `rc:81` with
  `error:"checkout_refused:<outcome>"` and does not invoke runtug.
- Processing: if the target node is not connected **or** not
  `accepting_effective`, return 503
  `{"error":"node_unavailable","state":...}`. A duplicate `request_id`
  returns 409 with its existing result. Send `job.spawn` to the node and wait
  up to `wait_seconds` for `job.spawn.result`.
- Nodes advertise `spawn_v2` in their hello. Requests with `checkout` or
  `brief.doc` to a connected node without that capability return 503 with
  `error:"spawn_capability_required"` immediately.
- Response: 200
  `{"request_id","machine","rc":0,"job_id":"…","pane":"…","stdout_tail":"<≤4KiB>","completed_at"}`.
  If the node has not replied, return 504
  `{"request_id","status":"pending"}` (the node keeps running; query
  `GET /v1/spawn/{request_id}` for up to one hour — hub memory only, like
  presence). A disconnected node reads as `{"status":"lost"}`.
- Broadcast `/v1/events`: `spawn.requested` and
  `spawn.result` `{request_id, machine, rc}` (no brief body or argument
  values).

### Rollout order and rollback (`spawn_v2` hello)

The hub's hello parser accepts only a fixed set of fields. A hub from before
`spawn_v2` therefore drops a hello that carries `capabilities`: it registers no
node and never answers that node's ping. The node must not depend on the hub
being upgraded first, so it probes its own registration:

1. The node sends `hello` with `capabilities:["spawn_v2"]`, immediately followed
   by a `ping`. A hub answers a ping with `pong` only for a node whose hello it
   accepted, so the `pong` is the hello's acknowledgement. No extra wait is
   added: the node keeps serving while it waits.
2. If no `pong` arrives within 5 seconds, the node sends a second `hello`
   without `capabilities` (same `machine_id`, `version`, `accepting`), repeats
   its heartbeat and pending job-completion state, and pings again. That
   registers it on an old hub. The node then behaves as a node without
   `spawn_v2` for the rest of that connection.
3. The retry happens once per connection. If the capability-free hello is not
   acknowledged either, the node logs a warning and keeps the session as it did
   before; the cause is not the capabilities field.
4. The next reconnect starts at step 1 again, so a node picks `spawn_v2` back up
   as soon as it reconnects to a hub that understands it.

| Order | Result |
|---|---|
| Hub upgraded first, then nodes | The hello with `capabilities` is accepted; one hello, no fallback. |
| Node upgraded first (old hub) | Registers via the fallback hello after about 5 seconds; An old hub has no `spawn_v2` handling, so `checkout` and `brief.doc` spawns are unavailable until the hub is upgraded. |
| Hub rolled back after nodes upgraded | Connected nodes reconnect, fall back as above, and stay on the fleet without `spawn_v2`. Upgrading the hub again and reconnecting the nodes restores the capability. |

Limits: the fallback relies on the old hub dropping the hello and leaving the
connection open, as the current hub does. Events a node emits during the roughly
5 second window before the fallback hello are written to a hub that has not
registered it and are dropped by that hub; heartbeat and job-completion state
are repeated immediately after the fallback hello. The recommended order is
still hub first, then nodes.

## B. node — `job.spawn` reception

- Node configuration `~/.config/panewire/spawn.json` — optional
  `runtug_path` pins the runtug binary where the daemon's minimal PATH
  (launchd/systemd) does not find it. A set `runtug_path` is authoritative:
  a non-executable value rejects with `runtug_missing` and no fallback runs.
  When it is unset, the order is `~/.local/bin/runtug`, then PATH
  (`runtug_missing` when none):

  ```json
  {"cwd_map":{"repo-a":"/abs/path"},"workspace":"worker","herdr_session":"worker","enabled":true,"work_root":"/abs/work","handoffkeep_env":"/path/to/node-hk.env","brief_doc_prefix":"briefs/"}
  ```

  `work_root` is required for checkouts. `brief_doc_prefix` controls which hk
  document keys the node may fetch; it defaults to `briefs/`. Nodes advertise
  `spawn_v2` in hello when they support checkout and document briefs. The hub
  refuses those requests to older nodes without waiting for a timeout.
  `enabled` defaults to false (no file rejects with `spawn_disabled`), and an
  unmapped `cwd_key` rejects with `cwd_unmapped`. Rejections also reply as
  `job.spawn.result{rc:2, error}`. The optional `handoffkeep_env` path names
  a mode-0600 env file containing this node’s `HANDOFFKEEP_URL` and
  `HANDOFFKEEP_TOKEN`; the node uses it to fetch `brief.doc`. The hub never
  receives the document text.
- A checkout runs `worktree.prepare` first; on success its returned path is
  used as spawn `cwd`. Any refusal returns `rc:81` and its outcome without
  invoking runtug.
- Execution: write the brief to a `mktemp` file (0600), then execute
  `env HERDR_SESSION=<s> runtug spawn -c <cwd> -p <tmp> --host local -w <workspace> <args...> --override-reason hub-directed`
  as argv (never a shell string). The timeout is `wait_seconds+60`; remove the
  temporary file afterwards. `stdout_tail` is at most 4 KiB.
- Result message:
  `job.spawn.result{request_id, rc, job_id, pane, stdout_tail}`. Parse
  `job_id` and `pane` from the same `runtug` OK-line rule:

  ```text
  OK pane=<pane> model=<m> label=<l> status=<s> landed=<y|n> job=<job-id> quota_record=<...>
  ```

  From a line beginning `^OK pane=`, split on spaces and take `pane=` and
  `job=` key/value fields. If there is no OK line, both values are empty.
- Concurrency: one spawn executes at a time per node (queued); a repeated
  `request_id` is ignored.

## Boundaries

The injected configuration path is available through `HubClientConfig` for
tests. The production default is the path above; no normal test reads it.
`spawn.json` and the node handoffkeep env file are operator-provisioned and
are not deployed by this change.
