package panewire

import (
	"fmt"
	"strings"
	"time"

	"github.com/getsentry/sentry-go"
	"github.com/mgh3326/panewire/internal/fleet"
)

// A first-run dialog or a missing login is a one-time human action on the
// host: nothing here answers a dialog. The hub tells the desk once per
// (host, kind, need) and keeps the notice until the node's readiness for that
// pair stops showing the need, so a host that stays blocked is not announced
// on every refused spawn. docs/placement.md, "Readiness".

const (
	oneTimeActionLane = "operator-desk"
	// oneTimeLoginMissing is the need of a harness whose login is missing.
	oneTimeLoginMissing = "login_missing"
)

type oneTimeActionKey struct{ machine, kind, need string }

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

// noteOneTimeAction tells the desk about a need the first time it is seen for
// (machine, kind). Later reports of the same need send nothing until
// clearOneTimeActionsLocked forgets it.
func (h *HubServer) noteOneTimeAction(machine, kind, need string, at time.Time) {
	action := oneTimeActionFor(kind, need)
	if action == "" || machine == "" || kind == "" {
		return
	}
	key := oneTimeActionKey{machine, kind, need}
	h.mu.Lock()
	if _, told := h.oneTimeActions[key]; told {
		h.mu.Unlock()
		return
	}
	if h.oneTimeActions == nil {
		h.oneTimeActions = map[oneTimeActionKey]time.Time{}
	}
	h.oneTimeActions[key] = at
	h.mu.Unlock()
	text := fmt.Sprintf("[%s] %s needs a one-time action: %s", machine, kind, action)
	eventID := fmt.Sprintf("one-time-action:%s:%s:%s:%d", machine, kind, need, at.UnixMilli())
	event := hubJobEventPayload{
		JobID:      laneEventTransportID(oneTimeActionLane, eventID),
		Epoch:      1,
		OwnerLane:  oneTimeActionLane,
		EventID:    eventID,
		Text:       text,
		Host:       "hub",
		Label:      "one-time-action",
		NotifyType: "automation-failed",
		Reason:     "one_time_action:" + need,
	}
	h.relayLaneEvent(event, nil)
	h.logger.Warn("host needs a one-time action", "host", machine, "kind", kind, "need", need)
	if client := h.spawnSentryClient(); client != nil {
		hub := sentry.NewHub(client, sentry.NewScope())
		// The title and fingerprint carry the action without its quotes, so one
		// issue is one host and action and the Discord rule posts it once.
		plain := strings.ReplaceAll(action, `"`, "")
		hub.CaptureEvent(&sentry.Event{Level: sentry.LevelWarning, Message: fmt.Sprintf("ACTION NEEDED: %s %s — %s", machine, kind, plain), Fingerprint: []string{"action-needed", machine, kind, plain}, Tags: map[string]string{"host": machine, "kind": kind, "need": need}})
	}
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

// noteReadinessActions applies a node's readiness report: each dialog or
// missing login it shows is told once, and a told need the report no longer
// shows is forgotten so its return is told again. A report older than the
// notice cannot forget it: it may predate the launch that raised the need.
func (h *HubServer) noteReadinessActions(machine string, report *fleet.Readiness, received time.Time) {
	if report == nil {
		return
	}
	h.mu.Lock()
	for key, at := range h.oneTimeActions {
		if key.machine != machine || !report.CheckedAt.After(at) {
			continue
		}
		if !readinessShowsNeed(report.Harnesses[key.kind], key.need) {
			delete(h.oneTimeActions, key)
		}
	}
	h.mu.Unlock()
	for _, kind := range fleet.ReadinessKinds {
		harness, ok := report.Harnesses[kind]
		if !ok {
			continue
		}
		if harness.Dialog != "" {
			h.noteOneTimeAction(machine, kind, harness.Dialog, received)
		}
		if kind != "cmd" && harness.Login == fleet.ReadinessLoginMissing {
			h.noteOneTimeAction(machine, kind, oneTimeLoginMissing, received)
		}
	}
}

func readinessShowsNeed(harness fleet.HarnessReadiness, need string) bool {
	if need == oneTimeLoginMissing {
		return harness.Login == fleet.ReadinessLoginMissing
	}
	return harness.Dialog == need
}
