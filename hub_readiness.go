package panewire

import (
	"bytes"
	"encoding/json"
	"regexp"
	"slices"
	"strings"
	"time"

	"github.com/mgh3326/panewire/internal/fleet"
)

// Readiness: a node reports, per harness kind and per repository, whether it
// can launch work (fleet.Readiness). The hub keeps the last report and
// publishes it as the `readiness` fleet-state value of the host; placement
// (the spawn engine) drops a (host, harness) pair that is not ready before it
// ranks hosts. The hub also owns the launch-failure backoff: it sees every
// node answer to a spawn, so it counts consecutive failures per (host, kind).
// docs/placement.md, "Readiness".

const (
	// hubFeatureReadiness: an older hub rejects a heartbeat with an unknown
	// field, so a node sends readiness only to a hub that advertised it.
	hubFeatureReadiness = "readiness"

	fleetStateKindReadiness = "readiness"

	// Defaults of the launch-failure backoff; placement.json overrides them
	// with launch_failure_threshold and launch_backoff_minutes.
	placementDefaultLaunchFailures = 2
	placementDefaultLaunchBackoff  = 30 * time.Minute
	maxPlacementLaunchFailures     = 100
	maxPlacementLaunchBackoffMin   = 24 * 60

	// hubReadinessMaxBytes bounds the readiness object a heartbeat carries.
	hubReadinessMaxBytes = 32 << 10
	// hubReadinessExpiry is how long a stored report stays valid without a
	// resend. The node resends every 45 s, so it is several resend intervals.
	hubReadinessExpiry = 5 * time.Minute
)

// launchFailureThreshold is the consecutive failures of one (host, kind) that
// start the backoff.
func (p PlacementPolicy) launchFailureThreshold() int {
	if p.LaunchFailureThreshold > 0 {
		return p.LaunchFailureThreshold
	}
	return placementDefaultLaunchFailures
}

func (p PlacementPolicy) launchBackoff() time.Duration {
	if p.LaunchBackoffMinutes > 0 {
		return time.Duration(p.LaunchBackoffMinutes) * time.Minute
	}
	return placementDefaultLaunchBackoff
}

type launchKey struct{ machine, kind string }

type launchBackoff struct {
	failures int
	// last is the time of the latest failure; a count older than the backoff
	// window does not carry into a new failure.
	last  time.Time
	until time.Time
}

// decodeHubReadiness reads the heartbeat's readiness object leniently, like
// host_signals: a bad or oversized object costs the report, never the node's
// liveness. Unknown fields are dropped, and the result carries only closed
// data (names, versions, booleans).
func decodeHubReadiness(raw json.RawMessage) *fleet.Readiness {
	if len(raw) == 0 || len(raw) > hubReadinessMaxBytes {
		return nil
	}
	var r fleet.Readiness
	if json.NewDecoder(bytes.NewReader(raw)).Decode(&r) != nil || r.Harnesses == nil {
		return nil
	}
	return sanitizeReadiness(r)
}

// Version fields carry only what the node's own probes produce: dotted
// numbers, and for panewire itself "dev" or a describe suffix. A token that
// merely looks like a name (a credential-shaped string) is not a version.
var (
	readinessVersionPattern = regexp.MustCompile(`^[0-9]+(?:\.[0-9]+){1,4}$`)
	// readinessPanewirePattern is the forms a build stamps (dist-version.sh:
	// git describe --always --dirty, a release tag, panewire-rN, dev).
	readinessPanewirePattern = regexp.MustCompile(`^(?:dev|panewire-r[0-9]+|v?[0-9]+(?:\.[0-9]+){1,3}|[0-9a-f]{7,40})(?:-[0-9]+-g[0-9a-f]{7,40})?(?:-dirty)?$`)
)

func readinessVersionOK(s string) bool {
	return s == "" || readinessVersionPattern.MatchString(s)
}

// sanitizeReadiness keeps only values that fit the closed vocabulary, so a
// node cannot put free text (a path, a screen, a secret) into the fleet state.
func sanitizeReadiness(in fleet.Readiness) *fleet.Readiness {
	out := fleet.Readiness{CheckedAt: in.CheckedAt.UTC(), Brief: in.Brief, Harnesses: map[string]fleet.HarnessReadiness{}, Toolchains: map[string]string{}, Repos: []string{}}
	if in.PanewireVersion == "" || readinessPanewirePattern.MatchString(in.PanewireVersion) {
		out.PanewireVersion = in.PanewireVersion
	}
	for _, kind := range fleet.ReadinessKinds {
		h, ok := in.Harnesses[kind]
		if !ok {
			continue
		}
		clean := fleet.HarnessReadiness{Installed: h.Installed, HerdrIntegration: h.HerdrIntegration, Login: fleet.ReadinessLoginUnknown}
		if readinessVersionOK(h.Version) {
			clean.Version = h.Version
		}
		switch h.Login {
		case fleet.ReadinessLoginPresent, fleet.ReadinessLoginMissing:
			clean.Login = h.Login
		}
		if fleetStateAccountKeyPattern.MatchString(h.Account) {
			clean.Account = h.Account
		}
		switch h.Dialog {
		case "", fleet.DialogCodexHooksReview, fleet.DialogCodexTrust, fleet.DialogOmpSetupWizard, fleet.DialogClaudeTrust:
			clean.Dialog = h.Dialog
		}
		out.Harnesses[kind] = clean
	}
	for _, repo := range in.Repos {
		if hubSpawnCWDKeyPattern.MatchString(repo) && len(out.Repos) < 256 {
			out.Repos = append(out.Repos, repo)
		}
	}
	for tool, version := range in.Toolchains {
		if (tool == "go" || tool == "node" || tool == "bun") && readinessVersionOK(version) {
			out.Toolchains[tool] = version
		}
	}
	switch in.Config.Source {
	case fleet.ReadinessConfigDefault, fleet.ReadinessConfigFile:
		out.Config.Source = in.Config.Source
	}
	// Workspace and session names are free text in spawn.json, so only the
	// built-in defaults travel by name; any other value reads "custom".
	out.Config.Workspace = fleet.ReadinessConfigName(in.Config.Workspace, fleet.ReadinessDefaultWorkspace)
	out.Config.HerdrSession = fleet.ReadinessConfigName(in.Config.HerdrSession, fleet.ReadinessDefaultSession)
	out.Config.WorkRoot = in.Config.WorkRoot
	return &out
}

// observeReadinessLocked records a heartbeat's readiness report. Callers hold
// h.mu.
func (h *HubServer) observeReadinessLocked(record *hubNodeRecord, readiness *fleet.Readiness, received time.Time) {
	if readiness == nil {
		return
	}
	record.readiness, record.readinessAt = readiness, received
}

// publishFleetReadiness publishes one machine's readiness value: the node's
// last report (or readiness_unknown when it never sent one) and the hub's
// launch backoff. An identical value is not republished within the load value's
// republish bound, which keeps measured_at fresh for consumers. It carries the
// hub's standing login needs (standingLoginNeeds) the same way it carries the
// launch backoff.
func (h *HubServer) publishFleetReadiness(machine string, now time.Time) {
	// loginNeedMu covers the read of the login needs and the publish, so a
	// publish that read an older set cannot land after one that read a newer
	// set. Lock order: loginNeedMu, then h.mu. No caller holds h.mu here, and
	// the one-time I/O (oneTimeMu) is never taken on this path.
	h.loginNeedMu.Lock()
	defer h.loginNeedMu.Unlock()
	loginNeeds := slices.Clone(h.loginNeeds[machine])
	h.mu.Lock()
	record := h.nodes[machine]
	if record == nil {
		h.mu.Unlock()
		return
	}
	// A report the node stopped resending is not republished as fresh: past
	// the expiry the node reads as unknown, like one that never reported.
	report := record.readiness
	if report != nil && now.Sub(record.readinessAt) > hubReadinessExpiry {
		report = nil
	}
	state := fleet.ReadinessState{Unknown: report == nil, Readiness: report, LoginNeeds: loginNeeds}
	for key, entry := range h.launchBackoffs {
		if key.machine == machine && now.Before(entry.until) {
			if state.BackoffUntil == nil {
				state.BackoffUntil = map[string]time.Time{}
			}
			state.BackoffUntil[key.kind] = entry.until.UTC()
		}
	}
	h.mu.Unlock()
	h.stateStore().publish(FleetStateMessage{Host: machine, Kind: fleetStateKindReadiness, MeasuredAt: now, ReceivedAt: now, Readiness: &state}, fleetStateLoadRepublish)
}

// refreshLoginNeedsLocked rebuilds loginNeeds from the one-time rows: each
// machine's login_missing kinds whose clear is not yet confirmed, so the
// anti-flap clear of hub_one_time_action.go decides when one lifts. Placement
// skips the (host, kind) pair while it stands (fleet ReadinessState.LoginNeeds).
// The caller holds oneTimeMu. loginNeedMu is taken only for the map swap.
func (h *HubServer) refreshLoginNeedsLocked() {
	byMachine := map[string][]string{}
	for key, state := range h.oneTimeActions {
		// A kind the node does not report on (kiro, opencode) cannot clear by
		// report, so its need is not a placement skip (readinessShowsNeed).
		if key.need == oneTimeLoginMissing && state.ConfirmedAt.IsZero() && slices.Contains(fleet.ReadinessKinds, key.kind) {
			byMachine[key.machine] = append(byMachine[key.machine], key.kind)
		}
	}
	for machine := range byMachine {
		slices.Sort(byMachine[machine])
	}
	h.loginNeedMu.Lock()
	h.loginNeeds = byMachine
	h.loginNeedMu.Unlock()
}

// launchFailureReason reports whether a failed node answer shows that the host
// failed to launch the harness. Only these reasons count toward launch_backoff.
// A timeout, a late claim, the hub's own unknown or reconciling outcomes,
// request faults and checkout state do not, so they never bench a host. The
// table is in docs/placement.md, "Launch-failure backoff". A reason may carry
// the engine's code after a colon, as spawn_failed:<code>.
func launchFailureReason(reason string) bool {
	switch code, _, _ := strings.Cut(reason, ":"); code {
	case "spawn_failed", "spawn_disabled", "cwd_unmapped", "brief_doc_unavailable":
		return true
	default:
		return false
	}
}

// hostRefusalBackoff is how long a host-local refusal (nextHostRefusals) keeps
// its host out of the next placement checks: the launch-failure backoff of
// placement.json, the window a launch failure benches a host for. After it the
// host is tried again, and a repeat refusal re-arms it (hk 5214).
func (h *HubServer) hostRefusalBackoff() time.Duration {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.reloadPlacementPolicyLocked()
	policy := h.placementPolicy
	if !policy.valid() {
		policy = DefaultPlacementPolicy()
	}
	return policy.launchBackoff()
}

// observeLaunchOutcome counts one node answer to a spawn against its
// (machine, kind). The threshold-th consecutive launch failure excludes the
// pair for the policy's backoff; a success clears it. A failed answer that is
// not a launch failure (launchFailureReason) changes nothing.
func (h *HubServer) observeLaunchOutcome(machine, profile string, rc int, reason string, now time.Time) {
	kind := fleet.ProfileKind(profile)
	if kind == "" || machine == "" || rc != 0 && !launchFailureReason(reason) {
		return
	}
	h.mu.Lock()
	h.reloadPlacementPolicyLocked()
	policy := h.placementPolicy
	if !policy.valid() {
		policy = DefaultPlacementPolicy()
	}
	key := launchKey{machine, kind}
	changed := false
	if rc == 0 {
		if _, had := h.launchBackoffs[key]; had {
			delete(h.launchBackoffs, key)
			changed = true
		}
	} else {
		if h.launchBackoffs == nil {
			h.launchBackoffs = map[launchKey]*launchBackoff{}
		}
		entry := h.launchBackoffs[key]
		if entry == nil {
			entry = &launchBackoff{}
			h.launchBackoffs[key] = entry
		}
		if entry.failures > 0 && now.Sub(entry.last) > policy.launchBackoff() {
			entry.failures = 0
		}
		entry.failures++
		entry.last = now
		if entry.failures >= policy.launchFailureThreshold() {
			entry.until = now.Add(policy.launchBackoff())
			changed = true
			h.logger.Warn("launch backoff", "machine", machine, "kind", kind, "failures", entry.failures, "until", entry.until.UTC().Format(time.RFC3339), "reason", strings.ToLower(reason))
		}
	}
	h.mu.Unlock()
	if changed {
		h.publishFleetReadiness(machine, now)
	}
}
