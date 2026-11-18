package panewire

import (
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
// docs/placement.md, "Readiness".

const (
	// oneTimeLoginMissing is the need of a harness whose login is missing.
	oneTimeLoginMissing = "login_missing"
	// oneTimeActionClearCycle is one full readiness report cycle: a need the
	// node reports gone must stay gone this long before it is forgotten.
	oneTimeActionClearCycle = nodeReadinessInterval
)

type oneTimeActionKey struct{ machine, kind, need string }

// oneTimeActionState is one told need. ToldAt is when the notice was sent.
// GoneSince is when a report first showed the need gone, and zero while the
// need shows; it clears once it has stayed gone for oneTimeActionClearCycle.
type oneTimeActionState struct {
	ToldAt    time.Time
	GoneSince time.Time
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
	if need == "" && spawnFailureCode(result) == "auth_unavailable" {
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
// least one cycle after that forgets the need.
func (h *HubServer) clearOneTimeActions(machine string, report *fleet.Readiness) {
	h.oneTimeMu.Lock()
	defer h.oneTimeMu.Unlock()
	for key, state := range h.oneTimeActions {
		// A kind the report does not cover (kiro, opencode) says nothing about the
		// need, so it can neither show it gone nor clear it.
		harness, covered := report.Harnesses[key.kind]
		if key.machine != machine || !covered || !report.CheckedAt.After(state.ToldAt) {
			continue
		}
		// An unmeasured login (the probe failed or was rate limited) says nothing
		// about the need: it is neither gone nor cleared.
		if key.need == oneTimeLoginMissing && harness.Login == fleet.ReadinessLoginUnknown {
			continue
		}
		shows := readinessShowsNeed(harness, key.need)
		switch {
		case shows && !state.GoneSince.IsZero():
			state.GoneSince = time.Time{}
			h.oneTimeActions[key] = state
			h.putOneTimeActionLocked(key, state)
		case shows:
		case state.GoneSince.IsZero():
			state.GoneSince = report.CheckedAt
			h.oneTimeActions[key] = state
			h.putOneTimeActionLocked(key, state)
		case report.CheckedAt.Sub(state.GoneSince) >= oneTimeActionClearCycle:
			delete(h.oneTimeActions, key)
			h.deleteOneTimeActionLocked(key)
		}
	}
}

func readinessShowsNeed(harness fleet.HarnessReadiness, need string) bool {
	if need == oneTimeLoginMissing {
		return harness.Login == fleet.ReadinessLoginMissing
	}
	return harness.Dialog == need
}
