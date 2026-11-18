package panewire

import (
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/getsentry/sentry-go"
	"github.com/mgh3326/panewire/internal/fleet"
)

// A first-run dialog or a missing login is a one-time human action on the
// host: nothing here answers a dialog. The operator hears it once per
// (host, kind, need), as a Sentry "ACTION NEEDED" event that the Discord rule
// posts; no lane or inbox gets it. The told state is durable in the hub store
// (hub_one_time_action_store.go), so a restart does not tell it again. A need
// clears only after a readiness report shows it gone and it stays gone for one
// full report cycle, so a host whose report flaps is not told on every flap.
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

// oneTimeSentryFlushTimeout bounds the wait for the transport to accept the
// notice before the need is recorded as told.
const oneTimeSentryFlushTimeout = 2 * time.Second

// errOneTimeSentryUnconfigured says no Sentry DSN is set, so nothing is sent.
var errOneTimeSentryUnconfigured = errors.New("sentry is not configured")

// noteOneTimeAction tells the operator about a need the first time it is seen
// for (machine, kind, need). A later sighting while the need is told sends
// nothing; it only restarts the clear countdown if the need had gone. A need is
// recorded as told only once the Sentry transport has accepted and flushed its
// notice; until then the next report tries again. oneTimeMu is held across the
// send, so two sightings cannot both tell one need.
func (h *HubServer) noteOneTimeAction(machine, kind, need string, at time.Time) {
	action := oneTimeActionFor(kind, need)
	if action == "" || machine == "" || kind == "" {
		return
	}
	key := oneTimeActionKey{machine, kind, need}
	h.oneTimeMu.Lock()
	defer h.oneTimeMu.Unlock()
	if state, told := h.oneTimeActions[key]; told {
		if !state.GoneSince.IsZero() {
			state.GoneSince = time.Time{}
			h.oneTimeActions[key] = state
			h.putOneTimeActionLocked(key, state)
		}
		return
	}
	if err := h.sendOneTimeAction(machine, kind, need, action); err != nil {
		if errors.Is(err, errOneTimeSentryUnconfigured) {
			h.logger.Debug("one-time action not told: no Sentry DSN", "host", machine, "kind", kind, "need", need)
		} else {
			h.logger.Warn("one-time action not told; the next readiness report tries again", "host", machine, "kind", kind, "need", need, "error", err)
		}
		return
	}
	if h.oneTimeActions == nil {
		h.oneTimeActions = map[oneTimeActionKey]oneTimeActionState{}
	}
	state := oneTimeActionState{ToldAt: at}
	h.oneTimeActions[key] = state
	h.putOneTimeActionLocked(key, state)
	h.logger.Warn("host needs a one-time action", "host", machine, "kind", kind, "need", need)
}

// oneTimeNotifier sends one ACTION NEEDED event and answers whether Sentry
// accepted it: a 2xx for that event, not a drained queue.
type oneTimeNotifier func(event *sentry.Event) (accepted bool, err error)

// newOneTimeNotifier returns a notifier for the panewire DSN, or nil when no DSN
// is configured. The DSN is the panewire one, never the SDK's SENTRY_DSN
// fallback. Each send builds its own Sentry client and closes it, so its
// delivery counts cover only that event: a late answer to a send that timed out
// cannot be credited to the next one, and no transport worker outlives a send.
func newOneTimeNotifier(dsn string) oneTimeNotifier {
	dsn = strings.TrimSpace(dsn)
	if dsn == "" {
		return nil
	}
	return func(event *sentry.Event) (bool, error) {
		delivery := &crashDelivery{base: http.DefaultTransport}
		options := spawnSentryOptions(dsn)
		options.HTTPTransport = delivery
		client, err := sentry.NewClient(options)
		if err != nil {
			return false, err
		}
		defer client.Close()
		if client.CaptureEvent(event, nil, sentry.NewScope()) == nil {
			return false, errors.New("sentry dropped the action-needed event")
		}
		if !client.Flush(oneTimeSentryFlushTimeout) {
			return false, errors.New("sentry did not flush the action-needed event in time")
		}
		accepted, rejected := delivery.take()
		return accepted > 0 && rejected == 0, nil
	}
}

// sendOneTimeAction hands the ACTION NEEDED event to the notifier. It answers
// an error when reporting is not configured or Sentry did not accept the event.
func (h *HubServer) sendOneTimeAction(machine, kind, need, action string) error {
	notify := h.oneTimeNotify
	if notify == nil {
		notify = newOneTimeNotifier(h.oneTimeDSN)
	}
	if notify == nil {
		return errOneTimeSentryUnconfigured
	}
	// The title and fingerprint carry the action without its quotes, so one
	// issue is one host and action and the Discord rule posts it once.
	plain := strings.ReplaceAll(action, `"`, "")
	event := &sentry.Event{Level: sentry.LevelWarning, Message: fmt.Sprintf("ACTION NEEDED: %s %s — %s", machine, kind, plain), Fingerprint: []string{"action-needed", machine, kind, plain}, Tags: map[string]string{"host": machine, "kind": kind, "need": need}}
	accepted, err := notify(event)
	if err != nil {
		return err
	}
	if !accepted {
		return errors.New("sentry did not accept the action-needed event")
	}
	return nil
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
// missing login it shows is told once, and a told need the report keeps
// showing as gone is forgotten only after it has stayed gone for a full cycle.
// A report older than the notice cannot clear it: it may predate the launch
// that raised the need.
func (h *HubServer) noteReadinessActions(machine string, report *fleet.Readiness, received time.Time) {
	if report == nil {
		return
	}
	h.clearOneTimeActions(machine, report)
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
