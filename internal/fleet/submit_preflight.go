package fleet

// Spawn input checks that fail at submit (hk 4270, decision 152).
//
// Four mistakes in a spawn's own input used to surface only after the hub had
// accepted the request: the engine refused it on the node and the hub told the
// design lane with a SPAWN_FAILED inbox event. Each is decided by the input and
// state the submitter can read, so `panewire spawn` refuses it in the caller's
// terminal, and the hub's submit endpoint refuses the two a non-CLI submitter
// could still send. The engine keeps every check as before.
//
//   - the agent name (the label): herdr takes a lowercase start, then
//     [a-z0-9_-], 1 to 32 characters (hk 4142);
//   - the task title's (grade X, Tn) marker and its scale against --t;
//   - the task claim: the task exists, is not parked or ended, and is not bound
//     to another job or claimant;
//   - host eligibility: a host that is not excluded, is the pinned host when
//     one is pinned, and declares every --tags capability.
//
// The task checks need hk. When hk cannot answer, the submit goes on and the
// engine decides on the node, as it did before.

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
)

var agentNamePattern = regexp.MustCompile(`^[a-z][a-z0-9_-]{0,31}$`)

// ValidAgentName reports whether herdr accepts label as an agent name.
func ValidAgentName(label string) bool { return agentNamePattern.MatchString(label) }

func agentNameRefusal(label string) error {
	return fail(exitUsage, "label %q is not a valid agent name: herdr takes a lowercase letter first, then lowercase letters, digits, - or _, at most 32 characters; pass a shorter -l LABEL", label)
}

// submitPreflight runs the checks of the caller's terminal: the label, then
// the task. The label is a property of the command and is always checked. The
// task checks read mutable state, so they are skipped when the job already has
// a saved submission: that is a replay of an accepted request, which must reach
// the hub's idempotent lookup even if the task has moved on since. A spawn that
// names no task (it inherits its parent job's) has no task to read.
func (a *App) submitPreflight(ctx context.Context, s Spawn) error {
	if !ValidAgentName(s.Label) {
		return agentNameRefusal(s.Label)
	}
	if s.Task == "" || s.ParentJob != "" || a.hasSubmissionReceipt(s.Job) {
		return nil
	}
	// Spawn translates the builder flags into the inherited-parent fields
	// taskTakesSpawn reads; do the same here so a review pass of a claimed
	// task is judged as the node judges it. Spawn validates the flags.
	if s.Role == "reviewer" && (s.AuthorFamily != "" || s.BuilderJob != "") {
		s.ParentFamily = s.AuthorFamily
		s.Inherited = s.BuilderJob
	}
	t, err := hkGetTask(ctx, s.Task)
	switch {
	case err == nil:
	case errors.Is(err, errHKNotFound) || strings.Contains(err.Error(), "not_found"):
		return fail(exitUsage, "task does not exist")
	default:
		return nil
	}
	if t.State == "" || !hkTaskIDIs(t, s.Task) {
		return nil
	}
	if e := taskTakesSpawn(t, s); e != nil {
		return e
	}
	// A consult needs no marker; the engine resolves it before the marker
	// check (routeFor). The policy table is not read here (a submit reads no hk
	// document of its own).
	if s.RoutingRole == "" {
		s.RoutingRole = s.Role
	}
	_, e := taskMarkerGrade(t, s, s.Bypass)
	return e
}

// hasSubmissionReceipt reports whether the job's submission receipt exists.
func (a *App) hasSubmissionReceipt(job string) bool {
	_, err := os.Lstat(filepath.Join(a.Jobs, job, "submission.json"))
	return err == nil
}

// SubmitHostRefusal refuses a request no configured host can take: one whose
// --tags no eligible host declares, or whose pinned host is excluded or lacks
// them. Hosts are the ones the placement reads (a.Config.Local and
// a.Config.Hosts), so the answer is the one placement would give on the node.
func (a *App) SubmitHostRefusal(s Spawn) error {
	required, err := parseTags(s.Tags)
	if err != nil {
		return err
	}
	excluded, err := parseHosts(s.ExcludeHosts)
	if err != nil {
		return err
	}
	pinned := ""
	if s.Host != "" && s.Host != "auto" {
		pinned = s.Host
	}
	machine := a.machine()
	if pinned == "local" {
		pinned = machine
	}
	if pinned != "" {
		// A pinned host the configuration does not list is still a host the
		// hub's placement may know through its nodes; its capabilities cannot
		// be judged here, so the node decides.
		if _, listed := a.hostConfig(pinned); !listed {
			if slices.Contains(excluded, pinned) {
				return fail(exitPlacement, "NO_ELIGIBLE_HOST: host %s is both pinned and excluded; correct --tags, --host or --exclude-host before submitting", pinned)
			}
			return nil
		}
	}
	var hosts []selHost
	add := func(name string, cfg map[string]any, local bool) {
		h := selHost{name: name, local: local, cfg: cfg}
		h.excluded = slices.Contains(excluded, name) || pinned != "" && pinned != name || local && !boolean(cfg, "spawn", true)
		hosts = append(hosts, h)
	}
	add(machine, a.Config.Local, true)
	for name, cfg := range a.Config.Hosts {
		if name != machine {
			add(name, cfg, false)
		}
	}
	for _, h := range hosts {
		if !h.excluded && len(missingCaps(h.cfg, required)) == 0 {
			return nil
		}
	}
	reason := "no host is eligible for this request"
	switch {
	case pinned != "" && slices.Contains(excluded, pinned):
		reason = "host " + pinned + " is both pinned and excluded"
	case pinned != "":
		if cfg, ok := a.hostConfig(pinned); ok {
			if missing := missingCaps(cfg, required); len(missing) > 0 {
				reason = "host " + pinned + " does not declare cap " + strings.Join(missing, ",")
			}
		}
	default:
		if gap := capsGap(hosts, required); gap != "" {
			reason = strings.TrimPrefix(gap, "; ")
		}
	}
	return fail(exitPlacement, "NO_ELIGIBLE_HOST: %s; correct --tags, --host or --exclude-host before submitting", reason)
}

func (a *App) hostConfig(name string) (map[string]any, bool) {
	if name == a.machine() {
		return a.Config.Local, true
	}
	cfg, ok := a.Config.Hosts[name]
	return cfg, ok
}
