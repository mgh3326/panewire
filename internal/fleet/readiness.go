package fleet

import (
	"context"
	"slices"
	"strconv"
	"strings"
	"time"
)

// Readiness is what a node reports about its ability to launch work, per
// harness kind and per repository (docs/placement.md, "Readiness"). It is
// closed data: names, versions and booleans. It never carries a credential, a
// path, a file's contents or screen text, so it can travel in a heartbeat and
// in the placement record.
type Readiness struct {
	PanewireVersion string                      `json:"panewire_version,omitempty"`
	CheckedAt       time.Time                   `json:"checked_at"`
	Harnesses       map[string]HarnessReadiness `json:"harnesses"`
	// Repos are the cwd_map keys the node can launch in.
	Repos []string `json:"repos"`
	// Brief is whether the node can fetch a brief document.
	Brief ReadinessBrief `json:"brief"`
	// Toolchains maps an installed toolchain (go, node, bun) to its version.
	Toolchains map[string]string `json:"toolchains"`
	Config     ReadinessConfig   `json:"config"`
}

// HarnessReadiness is one harness kind on one node.
type HarnessReadiness struct {
	Installed bool   `json:"installed"`
	Version   string `json:"version,omitempty"`
	// HerdrIntegration is whether herdr's integration for the harness is
	// current on the node.
	HerdrIntegration bool `json:"herdr_integration"`
	// Login is present, missing or unknown (the node does not measure the
	// harness's pool). Account is the pacelever account key when present.
	Login   string `json:"login"`
	Account string `json:"account,omitempty"`
	// Dialog is the first-run dialog the last launch stopped at, "" once a
	// later launch got past it. It is the last known state, not a live probe.
	Dialog string `json:"dialog,omitempty"`
}

type ReadinessBrief struct {
	HandoffkeepEnv bool `json:"handoffkeep_env"`
}

// ReadinessConfig is the node's effective spawn configuration: what it would
// run on with no spawn.json (Defaults) or with the file. Names only.
type ReadinessConfig struct {
	Source       string `json:"source"`
	Workspace    string `json:"workspace,omitempty"`
	HerdrSession string `json:"herdr_session,omitempty"`
	WorkRoot     bool   `json:"work_root"`
}

// ReadinessState is the fleet-state `readiness` value of one host: the node's
// report, if it has one, and the hub's launch-failure backoff.
type ReadinessState struct {
	// Unknown marks a node that never reported readiness (readiness_unknown):
	// the filter treats it as today and does not drop its pairs.
	Unknown      bool                 `json:"readiness_unknown,omitempty"`
	Readiness    *Readiness           `json:"readiness,omitempty"`
	BackoffUntil map[string]time.Time `json:"backoff_until,omitempty"`
}

// Outcome codes of the readiness filter. The first seven name a check; the
// last is the launch-failure backoff.
const (
	ReadyHarnessMissing    = "harness_missing"
	ReadyHarnessVersion    = "harness_version"
	ReadyDialogPending     = "dialog_pending"
	ReadyLoginMissing      = "login_missing"
	ReadyRepoUnmapped      = "repo_unmapped"
	ReadyBriefUnavailable  = "brief_unavailable"
	ReadyToolchainMissing  = "toolchain_missing"
	ReadyLaunchBackoff     = "launch_backoff"
	ReadinessLoginPresent  = "present"
	ReadinessLoginMissing  = "missing"
	ReadinessLoginUnknown  = "unknown"
	ReadinessConfigDefault = "defaults"
	ReadinessConfigFile    = "spawn.json"
)

// ReadinessKinds are the harness kinds a node reports on.
var ReadinessKinds = []string{"claude", "codex", "omp", "agy", "devin", "cmd"}

// First-run dialog markers a launch can stop at.
const (
	DialogCodexHooksReview = "codex_hooks_review"
	DialogCodexTrust       = "codex_directory_trust"
	DialogOmpSetupWizard   = "omp_setup_wizard"
	DialogClaudeTrust      = "claude_trust"
)

// ReadinessNeed is what one (host, harness) pair must have for this spawn.
type ReadinessNeed struct {
	Kind string
	// Repo is the cwd_map key the node must hold; "" skips the check.
	Repo string
	// Brief is set when the node fetches the brief document itself (a
	// hub-routed spawn).
	Brief bool
	// Toolchains are the toolchains the repo needs.
	Toolchains []string
	// MinVersion is the oldest acceptable harness version; "" accepts any.
	MinVersion string
}

// Drop answers the code of the first check the pair fails, or "". A host with
// no readiness report (Unknown) is never dropped by a readiness check; the
// hub-observed backoff still applies.
func (s ReadinessState) Drop(n ReadinessNeed, now time.Time) string {
	if until, ok := s.BackoffUntil[n.Kind]; ok && now.Before(until) {
		return ReadyLaunchBackoff
	}
	r := s.Readiness
	if s.Unknown || r == nil {
		return ""
	}
	if h, reported := r.Harnesses[n.Kind]; reported {
		switch {
		case !h.Installed:
			return ReadyHarnessMissing
		case n.MinVersion != "" && !versionAtLeast(h.Version, n.MinVersion):
			return ReadyHarnessVersion
		case h.Dialog != "":
			return ReadyDialogPending
		case n.Kind != "cmd" && h.Login == ReadinessLoginMissing:
			return ReadyLoginMissing
		}
	}
	switch {
	case n.Repo != "" && !slices.Contains(r.Repos, n.Repo):
		return ReadyRepoUnmapped
	case n.Brief && !r.Brief.HandoffkeepEnv:
		return ReadyBriefUnavailable
	}
	for _, tool := range n.Toolchains {
		if _, ok := r.Toolchains[tool]; !ok {
			return ReadyToolchainMissing
		}
	}
	return ""
}

// versionAtLeast compares dotted numeric versions ("0.166.0", "v2.1"). A
// version that does not parse is not at least anything.
func versionAtLeast(have, want string) bool {
	h, okH := parseVersion(have)
	w, okW := parseVersion(want)
	if !okH || !okW {
		return false
	}
	for i := 0; i < max(len(h), len(w)); i++ {
		var a, b int
		if i < len(h) {
			a = h[i]
		}
		if i < len(w) {
			b = w[i]
		}
		if a != b {
			return a > b
		}
	}
	return true
}

func parseVersion(v string) ([]int, bool) {
	v = strings.TrimPrefix(strings.TrimSpace(v), "v")
	if v == "" {
		return nil, false
	}
	var out []int
	for _, part := range strings.Split(v, ".") {
		// Keep the numeric prefix of each part, so "1-beta" reads as 1.
		end := 0
		for end < len(part) && part[end] >= '0' && part[end] <= '9' {
			end++
		}
		n, err := strconv.Atoi(part[:end])
		if err != nil {
			return nil, false
		}
		out = append(out, n)
	}
	return out, true
}

// ProfileKind answers the harness kind of a seat profile name from the
// built-in policy, or "". The hub uses it to attribute a launch outcome.
func ProfileKind(profile string) string {
	policy, err := LoadPolicy("")
	if err != nil {
		return ""
	}
	return policy.Profiles[profile].Kind
}

// readinessDrop is one (host, profile) pair the filter removed.
type readinessDrop struct {
	host, profile, code string
}

// readinessView is what one placement round knows about readiness: each
// host's state, and what this spawn needs of a host.
type readinessView struct {
	states     map[string]ReadinessState
	repo       string
	minVersion map[string]string
	toolchains []string
	now        time.Time
}

func (a *App) newReadinessView(ctx context.Context, cwd string) readinessView {
	v := readinessView{now: a.Now(), states: a.readinessStates(ctx)}
	if len(v.states) > 0 {
		v.repo = a.placementRepo(ctx, cwd)
		v.minVersion = harnessMinVersions(a.Config.Placement)
		v.toolchains = repoToolchains(a.Config.Placement, v.repo)
	}
	return v
}

// drop answers the code that removes the pair (host, kind), or "". A host
// with no state is unknown and is never dropped.
func (v readinessView) drop(host string, cfg map[string]any, kind string) string {
	state, reported := v.states[host]
	if !reported {
		return ""
	}
	return state.Drop(ReadinessNeed{Kind: kind, Repo: v.repo, Brief: str(cfg, "via") == "hub", Toolchains: v.toolchains, MinVersion: v.minVersion[kind]}, v.now)
}

// readinessDrop answers the code that removes the pair (host, seat), and
// records the drop once.
func (sp *selPresence) readinessDrop(h selHost, seat *Seat) string {
	code := sp.ready.drop(h.name, h.cfg, seat.Kind)
	if code == "" {
		return ""
	}
	d := readinessDrop{host: h.name, profile: seat.Canon, code: code}
	if !slices.Contains(sp.drops, d) {
		sp.drops = append(sp.drops, d)
	}
	return code
}

// firstDrop is the code of the first recorded drop of a seat's profile.
func (sp *selPresence) firstDrop(seat *Seat) string {
	for _, d := range sp.drops {
		if d.profile == seat.Canon {
			return d.code
		}
	}
	return ""
}

// dropsOn lists the drops recorded on one host.
func (sp *selPresence) dropsOn(host string) []readinessDrop {
	var out []readinessDrop
	for _, d := range sp.drops {
		if d.host == host {
			out = append(out, d)
		}
	}
	return out
}

// harnessMinVersions reads placement.harness_min_version: harness kind to the
// oldest acceptable version.
func harnessMinVersions(placement map[string]any) map[string]string {
	raw, _ := placement["harness_min_version"].(map[string]any)
	out := map[string]string{}
	for kind, v := range raw {
		if s, ok := v.(string); ok && s != "" {
			out[kind] = s
		}
	}
	return out
}

// repoToolchains reads placement.repo_toolchains: repository key to the
// toolchains (go, node, bun) its work needs on the host.
func repoToolchains(placement map[string]any, repo string) []string {
	raw, _ := placement["repo_toolchains"].(map[string]any)
	return stringsValue(raw, repo)
}
