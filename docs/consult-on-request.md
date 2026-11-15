# Consult on request — the hub picks the seat, runs it and returns the stored answer

Operator decision 250 (candidate G of
`hk:doc design/2026-11-13/hub-owned-responsibilities`, hk 4552 phase 1). The
round-3 rule used to leave the consult to the author; a tool that sees the
review round itself asks the hub instead. The hub resolves the seat, runs one
ordinary hub spawn, waits for the stored report and records the answer
document. The first caller is mergewake (mergewake hk 4551).

## Request

```
panewire consult request --task N --question hk:doc/KEY \
  [--author-job JOB | --author-family F] [--pr REPO#N --head SHA] \
  [--requester LANE|TOOL] [--json]
```

The CLI posts to `POST /v1/consults`:

```json
{"task_id": "4552", "requester": "mergewake/owner/repo#12",
 "question_doc": "hk:doc/design/2026-11-13/question-4552",
 "author_job": "hk4552-author", "pr": "owner/repo#12", "head": "0123…"}
```

- `requester` is the lane or tool id the answer belongs to. It defaults to
  `PANEWIRE_LANE`, else the credential's machine id. A lane requester gets one
  `ready` lane event; a tool requester polls.
- `question_doc` is the question document. The hub fetches its body and puts
  it after a fixed consult preamble.
- `author_job` names the author's job; its hk record gives the family and its
  stored spawn submission gives the repository and checkout evidence. When it
  is omitted, `author_family` must name the family and the task's bound job
  supplies the repository evidence.
- `pr` and `head` pin the checkout to the PR head. Without them the author
  job's stored checkout names the commit and ref.
- No profile pin is accepted. The caller cannot choose the seat.

### Idempotency

The key is `(task, repo#pr, head)`. A queued, running or answered consult is
returned unchanged (the POST answers `200` instead of `201`). A `refused` or
`failed` row is a retry: the same POST re-plans and replaces the attempt under
the same id, so a transient quota, hk or spawn failure is not permanent. A new
head is a new consult, which matches the review round count. The question
document may change between evaluations; it is not part of the key.

## Seat

The hub uses the consult floor (`hk:doc policy/model-families` §Consult):

- the task's title grade + one rung, capped at SS; a task without a grade asks
  at S+; a design-session lane asks at SS;
- a family other than the author's;
- a consult-eligible catalog row through the ordinary quota gate.

No seat is state `refused` with the reason (quota, no row, unknown author
family); nothing is spawned.

## Run

One spawn through the ordinary durable hub spawn path:

- `--role consult`, `--task N`, `--owner` the requester lane (or
  `operator-desk` for a tool), the resolved `-m`/`--effort` with an override
  reason and `--containment hk:N`;
- a fresh detached checkout of the PR head on the chosen host;
- the brief is the fixed preamble plus the question document; it changes no
  code.

A consult shares its task with the author's job: a consult seat takes no
task/role reservation (hk 4568).

## States

| State | Meaning |
| --- | --- |
| `queued` | accepted; the spawn operation is not placed yet |
| `running` | the spawn placed; the job runs |
| `answered` | the job completed, its report is stored and carries the answer shape; `answer_doc` names it |
| `refused` | no seat, no repository evidence or no author family; nothing was spawned |
| `failed` | the job or its answer did not arrive: spawn failed twice, the job was lost, the time limit passed, or the report lacks a section. The job's pane is closed and the job marked lost when the node still lists it |
| `cancelled` | reserved for an operator cancel; no route in phase 1 |

A lost or failed job gets one respawn on the next eligible seat (the failed
seat profile is excluded, the family and floor rules still apply), then
`failed`. A report that completes but lacks a section fails without a respawn.
A queued row whose spawn operation is missing (a crash between the row and the
claim) is started by the coordinator on its next pass.

The time limit is the hub's `--consult-timeout` (default 2 h).

## Answer shape

The consult report must carry these sections; the hub checks them before it
records `answer_doc`.

- `## Verdict` — one line: `apply`, `cut`, `split` or `escalate`
- `## Root cause` — prose
- `## Design` — the one design that closes the findings
- `## Findings` — a table with one row per finding id:
  `| finding id | action (fix/dismiss/defer) | reason |`

`answer_doc` is the hk document key of the stored report, in the receipt model
of `panewire done` (`reports/<job>/sha256/<digest>.md`); mergewake passes the
key through instead of reading the document.

## Read

```
GET /v1/consults/{id}
```

```json
{"id": "…", "state": "answered",
 "seat": {"profile": "codex-astra", "family": "openai", "grade": "SS"},
 "job": "consult-4552-1a2b3c4d", "answer_doc": "reports/consult-…/sha256/….md",
 "reason": "", "request_id": "…", "requester": "mergewake/owner/repo#12",
 "created_at": "2026-11-15T01:02:03Z", "answered_at": "2026-11-15T01:19:00Z"}
```

`seat` is absent when the consult was refused before a seat was chosen. A
terminal row keeps its state across a hub restart; the rows are durable.

## Auth

The consult routes admit:

- the operator credential;
- the calling node's own hub credential (`X-Panewire-Machine-ID` plus its
  `HUB_TOKEN_<machine>`), which is what mergewake uses on oci;
- a service credential holding the `consult` scope.

A node credential is admitted only on the hub's `--pr-status-host` (default
`oci`, the mergewake host); every other node gets `403`. That is the same
machine restriction `pr.status` already uses: a consult spends a paid seat and
can put an arbitrary hk document into a brief, so no other node may drive it.

They refuse an unauthenticated caller with `401`, and a service whose scopes
do not include `consult` with `403 {"error":"scope_forbidden"}`. The `consult`
scope grants only these two routes; it is not a node credential.

## Exit status (CLI)

| rc | meaning |
| --- | --- |
| 0 | the hub answered with a consult row (`CONSULT id=… state=… seat=… job=…`) |
| 2 | the request was malformed or the credential is not admitted |
| 1 | the hub or its store is unavailable |

`--json` prints the hub's row unchanged.
