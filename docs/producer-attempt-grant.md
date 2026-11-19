# Producer calls carry an attempt grant bound at launch (hk 5310)

A job's grant epoch fences hk writes by owner (hk 5067, #703). A producer call
(`panewire job done|escalate|joined`, `panewire emit` for a `job.*` kind) used
to read the job's *current* local grant, so it had no attempt identity of its
own. After A/1 → B/2 → A/3 on one node, a live A/1 worker's `job done` was
recorded as A/3 and accepted. This page is the rule that closes that gap.

## The binding

The node binds the attempt when it launches the worker pane
(`internal/fleet/spawn.go`, next to `ARBITER_JOB`):

| Variable | Value |
|---|---|
| `ARBITER_JOB` | the job the pane was launched for (already set) |
| `ARBITER_JOB_EPOCH` | that job's grant epoch on this node at launch |

Nothing rewrites them, so they are fixed for the pane's whole process tree
even after the hub regrants the job. A legacy-only job id (outside the hub's
id pattern) cannot hold a grant and gets no `ARBITER_JOB_EPOCH`.

The binding is the attempt for **its own job only**: a command about another
job id sees no identity.

## The check

A producer call presents an attempt, from the bound `ARBITER_JOB_EPOCH` or from
an explicit `--epoch N`, and `internal/jobowner.ProducerEpoch` compares it to
the node's current grant before anything is written:

| Presented | Current grant | Result |
|---|---|---|
| bound or `--epoch N` | equal | accepted; the record carries N |
| bound or `--epoch N` | newer than N | refused: `stale attempt` |
| bound or `--epoch N` | older than N | refused: `attempt epoch ahead of the node's grant` (retry once the node has persisted the grant) |
| nothing | 1 | accepted as epoch 1 |
| nothing | above 1 | refused: `attempt identity required` |
| anything, including nothing at 1 | the hub revoked the grant | refused: `attempt revoked` until a newer grant supersedes the revocation |
| `--epoch N` with a different bound epoch | any | refused: `--epoch conflicts with the epoch bound at launch` |
| `ARBITER_JOB_EPOCH` set but not a positive integer | any | refused: `malformed attempt binding` |

A revocation matters because A/1 → B/2 only appends a `job.revoked` marker and
leaves A's grant file at 1: the bound epoch still equals the file, so the
revocation (a hub marker above the grant) is checked as well. A later A/3 grant
supersedes it.

A refusal exits 2, names the reason and the two epochs on stderr, and writes no
event file and sends no socket push. `--epoch` cannot lift a bound worker to a
newer grant: a bound call that also passes a different `--epoch` is refused.

An operator or parent call has no launch binding. Above epoch 1 it passes the
epoch it means to act under, for example `panewire job done JOB --report R
--epoch 3`; the same check applies.

## Where it is enforced

- **The CLI** (`producer_attempt.go`, used by `job.go` and `emit.go`): the
  check runs first, so a refused call leaves no event file.
- **The node daemon** (`emitRelayEvent`): a `job.*` push goes through the same
  `ProducerEpoch` rule as the CLI, so for a client that talks to the socket
  without the CLI the submitted epoch must equal the persisted grant, and a
  revoked grant is refused. A future epoch the node has not persisted (the
  revocation's own epoch, or a regrant not yet landed) is refused, not passed.
  A daemon with no configured inbox namespace has no authoritative grant, so it
  refuses a `job.*` push instead of trusting the request's own root.
- **The hub** (`observeJobCompletion` and the owner assignment fence): a
  completion whose epoch differs from the job's current epoch is not accepted
  and does not close the job. The A/1 worker's call carries epoch 1, so the hub
  fences it where before it arrived as epoch 3 and passed.

The fleet's own `panewire done|escalate|restore` (operator lifecycle commands)
capture the epoch themselves and are unchanged.

## Rollout

Nothing deploys on its own; the check takes effect per node when that node's
`panewire` binary is replaced.

- A worker already running without `ARBITER_JOB_EPOCH` keeps working while its
  job's grant is epoch 1: no identity is accepted at epoch 1.
- After a reassignment raises the grant above 1, an old worker without the
  variable is refused `attempt identity required`. That is intended: such a
  worker cannot be told apart from the superseded attempt. Its work is finished
  by an operator or parent call with an explicit `--epoch`, or by a respawn,
  which binds the new grant.
- A worker launched by the new binary under grant N carries N from then on. If
  the hub later regrants the job to it again (N → M), the worker is stale too
  until it is relaunched; its output is recoverable by an operator call with
  `--epoch M`.
- A worker launched by the new binary but running under an old `panewire`
  keeps today's behaviour (it ignores the variable) until that binary is
  replaced.

Tests: `producer_attempt_t5310_test.go`, `job_owner_attempt_t5067_test.go`,
`internal/jobowner` (`TestProducerEpoch`), `internal/fleet`
(`TestSpawnPaneEnvironmentBindsTheGrantEpoch_t5310`).
