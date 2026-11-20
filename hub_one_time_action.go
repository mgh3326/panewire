package panewire

import (
	"strings"
	"time"

	"github.com/mgh3326/panewire/internal/fleet"
)

// A first-run dialog or a missing login is a one-time human action on the
// host: nothing here answers a dialog. The operator hears it once per
// (host, kind, need), as a Sentry "ACTION NEEDED" event that the Discord rule
// posts; no lane or inbox gets it. The told state is durable in the hub store
// (hub_one_time_action_store.go), so a restart does not tell it again. A need
// clears only after a readiness report shows it gone and it stays gone for one
// full report cycle, so a host whose report flaps is not told on every flap.
// The refusal and readiness handlers only hand observations to the notice
// worker (hub_one_time_notice.go), which owns the told state and every send.
// The operator wait that goes with a standing need is a separate obligation
// with its own worker (hub_one_time_wait.go). docs/placement.md, "Readiness".

const (
	// oneTimeLoginMissing is the need of a harness whose login is missing.
	oneTimeLoginMissing = "login_missing"
	// oneTimeActionClearCycle is one full readiness report cycle: a need the
	// node reports gone must stay gone this long before it is forgotten.
	oneTimeActionClearCycle = nodeReadinessInterval
)

type oneTimeActionKey struct{ machine, kind, need string }

// oneTimeActionState is one need's row. The notice episode is ToldAt (when
// the Sentry notice was sent; zero once a recurrence resets the episode),
// GoneSince (when a report first showed the need gone; zero while it shows)
// and ConfirmedAt (set once anti-flap clearing confirmed the need gone for a
// full cycle). The wait is a separate obligation: AnchorRef is where the anchor
// walk last reached, WaitTaskID is the anchor the wait is on (written before
// the wait write, so an ambiguous write can be resolved), and WaitID is the
// confirmed operator wait. A row lives while its episode is told or it holds a
// wait; see oneTimeRowKept.
type oneTimeActionState struct {
	ToldAt      time.Time
	GoneSince   time.Time
	ConfirmedAt time.Time
	AnchorRef   string
	WaitTaskID  int64
	WaitID      int64
	// SightedAt is the latest sighting of a login need, kept in memory only. A
	// report must be newer than it to count toward the clear, so a report
	// computed before the refusal cannot clear an untold need (hk 5686 review).
	SightedAt time.Time
}

// clearBoundary is the time a report must be newer than to count toward a
// clear: the notice's delivery or the latest sighting, whichever is later.
func (s oneTimeActionState) clearBoundary() time.Time {
	if s.SightedAt.After(s.ToldAt) {
		return s.SightedAt
	}
	return s.ToldAt
}

// oneTimeRowKept reports whether a row still has something to remember. A told
// episode the clear has not yet confirmed needs its row; so does a wait that
// is not known to be closed, even after its episode ended. A login need is
// remembered from its first sighting, told or not: placement reads it
// (standingLoginNeeds), and a failed notice only leaves ToldAt zero to retry.
func oneTimeRowKept(key oneTimeActionKey, state oneTimeActionState) bool {
	if state.holdsWait() {
		return true
	}
	if key.need == oneTimeLoginMissing {
		return state.ConfirmedAt.IsZero()
	}
	return !state.ToldAt.IsZero() && state.ConfirmedAt.IsZero()
}

// holdsWait reports whether the row names any handoffkeep wait or anchor
// write, confirmed or not.
func (s oneTimeActionState) holdsWait() bool {
	return s.WaitTaskID != 0 || s.WaitID != 0
}

// oneTimeSighting is one need seen on a host at a time, with the action the
// person takes for it.
type oneTimeSighting struct {
	key    oneTimeActionKey
	action string
	at     time.Time
}

// oneTimeActionFor is the action a person takes on the host for a need. The
// text names no screen: it is built from the closed dialog vocabulary only.
func oneTimeActionFor(kind, need string) string {
	switch need {
	case fleet.DialogCodexHooksReview:
		return `run codex once and choose "Trust all and continue"`
	case fleet.DialogCodexTrust, fleet.DialogClaudeTrust:
		return "trust the workspace"
	case fleet.DialogOmpSetupWizard:
		return "run omp once and finish its setup"
	case oneTimeLoginMissing:
		return "log in"
	}
	return ""
}

// oneTimeSightingFor is the sighting of need on machine, or false when the need
// is not one the operator is told about.
func oneTimeSightingFor(machine, kind, need string, at time.Time) (oneTimeSighting, bool) {
	action := oneTimeActionFor(kind, need)
	if action == "" || machine == "" || kind == "" {
		return oneTimeSighting{}, false
	}
	return oneTimeSighting{key: oneTimeActionKey{machine, kind, need}, action: action, at: at}, true
}

// noteOneTimeAction hands a sighting of a need to the notice worker, which tells
// the operator the first time it is seen for (machine, kind, need). It returns
// at once: no HTTP or storage happens on the caller's path.
func (h *HubServer) noteOneTimeAction(machine, kind, need string, at time.Time) {
	h.enqueueOneTimeObservation(oneTimeObservation{machine: machine, kind: kind, need: need, at: at})
}

// noteOneTimeActionRefusal reads a failed node result the spawn engine made
// before it injected the brief. The dialog comes from the engine's markers and
// the login from its refusal code, never from a screen.
func (h *HubServer) noteOneTimeActionRefusal(machine, profile string, result hubSpawnResult) {
	if result.RC == 0 {
		return
	}
	kind := fleet.ProfileKind(profile)
	need := classifyLaunchDialog(kind, result.StdoutTail)
	// Only the verified login-screen refusal is login evidence. An rc 88 for an
	// account the host cannot confirm is not, and its notice would say "log in".
	if need == "" && spawnFailureCode(result) == "auth_unavailable" && strings.Contains(result.StdoutTail, loginScreenAuthMessage) {
		need = oneTimeLoginMissing
	}
	if need != "" {
		h.noteOneTimeAction(machine, kind, need, h.now().UTC())
	}
}

// noteReadinessActions hands a node's readiness report to the notice worker.
// The worker tells each dialog or missing login the report shows once, and
// forgets a told need only after the report has kept showing it gone for a full
// cycle. A report older than the notice cannot clear it: it may predate the
// launch that raised the need.
func (h *HubServer) noteReadinessActions(machine string, report *fleet.Readiness, received time.Time) {
	if report == nil {
		return
	}
	h.enqueueOneTimeObservation(oneTimeObservation{machine: machine, report: report, at: received})
}

// clearOneTimeActions advances the clear of every told need of machine that
// report reads. A report that shows the need cancels a pending clear. A report
// that shows it gone starts the clear countdown, and a later gone report at
// least one cycle after that confirms the clear. A confirmed clear with no wait
// to close drops the row here; one with a wait stays until the wait worker has
// closed it (hub_one_time_wait.go). A recurrence after a confirmed clear starts
// a new untold episode and keeps any wait it holds. It returns the needs whose
// clear was just confirmed, so the wait worker can close their waits.
func (h *HubServer) clearOneTimeActions(machine string, report *fleet.Readiness) []oneTimeActionKey {
	h.oneTimeMu.Lock()
	defer h.oneTimeMu.Unlock()
	var confirmed []oneTimeActionKey
	for key, state := range h.oneTimeActions {
		// A kind the report does not cover (kiro, opencode) says nothing about the
		// need, so it can neither show it gone nor clear it.
		harness, covered := report.Harnesses[key.kind]
		if key.machine != machine || !covered || !report.CheckedAt.After(state.clearBoundary()) {
			continue
		}
		// An unmeasured login (the probe failed or was rate limited) says nothing
		// about the need: it is neither gone nor cleared.
		if key.need == oneTimeLoginMissing && harness.Login == fleet.ReadinessLoginUnknown {
			continue
		}
		next := state
		shows := readinessShowsNeed(harness, key.need)
		switch {
		case shows && !state.ConfirmedAt.IsZero():
			// The need returned after its clear was confirmed: a new untold episode.
			// A wait still held stays; the new episode re-asserts it on the same
			// anchor, which cancels the pending close.
			next.ToldAt = time.Time{}
			next.GoneSince = time.Time{}
			next.ConfirmedAt = time.Time{}
		case shows && !state.GoneSince.IsZero():
			next.GoneSince = time.Time{}
		case shows:
			continue
		case !state.ConfirmedAt.IsZero():
			// A confirmed clear whose wait close has not landed yet: every report that
			// shows the need gone asks the wait worker again.
			if state.holdsWait() {
				confirmed = append(confirmed, key)
			}
			continue
		case state.GoneSince.IsZero():
			next.GoneSince = report.CheckedAt
		case report.CheckedAt.Sub(state.GoneSince) >= oneTimeActionClearCycle && state.ConfirmedAt.IsZero():
			next.ConfirmedAt = report.CheckedAt
			confirmed = append(confirmed, key)
		default:
			continue
		}
		if err := h.saveOneTimeLocked(key, next); err != nil {
			h.logger.Warn("one-time action state could not be stored", "host", key.machine, "kind", key.kind, "error", err)
		}
	}
	return confirmed
}

func readinessShowsNeed(harness fleet.HarnessReadiness, need string) bool {
	if need == oneTimeLoginMissing {
		return harness.Login == fleet.ReadinessLoginMissing
	}
	return harness.Dialog == need
}
