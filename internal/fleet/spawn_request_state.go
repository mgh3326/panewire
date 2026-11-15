package fleet

import (
	"fmt"
	"io"
	"strings"
)

// spawnStateKind is how a client treats one request state a hub reports in
// GET /v1/spawns/<id>. The design (hk design/2026-11-13/hub-owned-spawn-queue
// §2) names the state set; today's hub reports only the legacy names, and a
// client learns every name before a hub emits it (hk 4659, decision 254).
type spawnStateKind int

const (
	// spawnUnknown is a state this client has no entry for, such as one a
	// newer hub adds. The wait goes on; it never fails and never succeeds.
	spawnUnknown spawnStateKind = iota
	// spawnActive keeps waiting without a word.
	spawnActive
	// spawnParked keeps waiting and says why, and when the hub looks again.
	spawnParked
	// spawnLanded ends the request phase: the pane is up.
	spawnLanded
	// spawnRefused is a refusal; the exit code is the hub's recorded rc.
	spawnRefused
	// spawnEnded is a request that ended without landing and without a
	// refusal: given_up (its time limit passed) or cancelled.
	spawnEnded
	// spawnUnresolved is deadline_exceeded: the node may still run the job.
	spawnUnresolved
)

// spawnRequestStates is every state name the clients know. Today's hub
// emits accepted, dispatching, succeeded, failed, retryable, reconciling and
// deadline_exceeded (TestHubSpawnExternalStatesUnchanged keeps it so); the
// rest are the design's names.
var spawnRequestStates = map[string]spawnStateKind{
	// today
	"accepted":          spawnActive,
	"dispatching":       spawnActive,
	"reconciling":       spawnActive,
	"succeeded":         spawnLanded,
	"failed":            spawnRefused,
	"retryable":         spawnRefused,
	"deadline_exceeded": spawnUnresolved,
	// design §2
	"waiting":   spawnActive,
	"pending":   spawnActive,
	"parked":    spawnParked,
	"bound":     spawnActive,
	"launching": spawnActive,
	"landed":    spawnLanded,
	"refused":   spawnRefused,
	"given_up":  spawnEnded,
	"cancelled": spawnEnded,
}

func classifySpawnState(state string) spawnStateKind {
	return spawnRequestStates[state]
}

// spawnNeverLanded reports a terminal state in which no pane was started for
// the request, so a submitter-side job directory the hub never held a record
// for may go (job_dir_ttl.go). Today only failed qualifies on the wire; the
// others are the design's terminal states that end without a pane.
func spawnNeverLanded(state string) bool {
	switch state {
	case "failed", "refused", "given_up", "cancelled":
		return true
	}
	return false
}

// spawnWaitNotices keeps WaitSpawn from repeating itself: a parked or waiting
// line only when it changes, an unknown state once per name.
type spawnWaitNotices struct {
	parkedLine  string
	waitingLine string
	unknownSet  map[string]bool
}

func (n *spawnWaitNotices) parked(out io.Writer, request, reason, nextCheckAt string) {
	line := fmt.Sprintf("wait request=%s state=parked reason=%s next_check_at=%s", request, firstNonempty(reason, "none"), firstNonempty(nextCheckAt, "none"))
	if out == nil || line == n.parkedLine {
		return
	}
	n.parkedLine = line
	fmt.Fprintln(out, line)
}

// waiting says once what a queued request waits for and when its time limit
// ends; a change in either says it again.
func (n *spawnWaitNotices) waiting(out io.Writer, request, expiresAt string, reasons []string) {
	line := fmt.Sprintf("wait request=%s state=waiting expires_at=%s waiting_for=%s", request, firstNonempty(expiresAt, "none"), firstNonempty(strings.Join(reasons, ","), "none"))
	if out == nil || line == n.waitingLine {
		return
	}
	n.waitingLine = line
	fmt.Fprintln(out, line)
}

func (n *spawnWaitNotices) unknown(out io.Writer, state string) {
	if out == nil || n.unknownSet[state] {
		return
	}
	if n.unknownSet == nil {
		n.unknownSet = map[string]bool{}
	}
	n.unknownSet[state] = true
	fmt.Fprintf(out, "unknown state %s: still waiting\n", state)
}

// SpawnRequestStateKnown reports whether the clients have an entry for state;
// the hub's tests use it to keep every state the hub emits one the clients
// already read.
func SpawnRequestStateKnown(state string) bool {
	_, ok := spawnRequestStates[state]
	return ok
}
