package fleet

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"time"
)

// Fleet-wide find (hk 3707): `panewire find` resolves a pane on this host
// through herdr and, when the [hub] operator credential is configured, also
// asks the hub where the same name lives on every other host. The hub side
// reads three projections — the session snapshots nodes publish in
// /v1/nodes, the live job registry at /v1/jobs, and the lane registry at
// /v1/lanes — never another host's filesystem. Every match carries the
// machine id next to the pane id so the operator knows where to point
// panewire prompt. Hosts without an operator credential (every node) keep
// the local-only lookup.

const (
	findFleetTimeout   = 10 * time.Second
	findSnapshotMaxAge = 120 * time.Second
	findSnapshotOK     = "ok"
)

// findMatch is one located pane merged across every source that reported it.
// Via names the sources (local, session, job, lane); NameTier marks a match
// on a canonical identity — a herdr agent name, a session agent_name, a job
// id or a lane name — which outranks a display-label match exactly as the
// local name-over-label precedence does. Local means this host's herdr
// answered for the pane, the only kind with a cwd and a screen preview.
type findMatch struct {
	Machine  string
	PaneID   string
	Name     string
	CWD      string
	Status   string
	Via      map[string]bool
	NameTier bool
	Local    bool
	Stale    bool
}

func (m *findMatch) absorb(o findMatch) {
	if m.Name == "" {
		m.Name = o.Name
	}
	if m.CWD == "" {
		m.CWD = o.CWD
	}
	if m.Status == "" {
		m.Status = o.Status
	}
	m.NameTier = m.NameTier || o.NameTier
	m.Local = m.Local || o.Local
	m.Stale = m.Stale || o.Stale
	for via := range o.Via {
		m.Via[via] = true
	}
}

// mergeFindMatches dedupes candidates on (machine, pane_id), unions their
// sources, and applies the name-over-label precedence fleet-wide: when any
// canonical-identity match exists, display-label matches are dropped.
func mergeFindMatches(candidates []findMatch) []findMatch {
	byKey := make(map[string]*findMatch, len(candidates))
	for _, candidate := range candidates {
		if candidate.Machine == "" || candidate.PaneID == "" {
			continue
		}
		key := candidate.Machine + "\x00" + candidate.PaneID
		if merged, ok := byKey[key]; ok {
			merged.absorb(candidate)
			continue
		}
		copied := candidate
		if copied.Via == nil {
			copied.Via = map[string]bool{}
		}
		byKey[key] = &copied
	}
	nameTier := false
	for _, m := range byKey {
		if m.NameTier {
			nameTier = true
			break
		}
	}
	matches := make([]findMatch, 0, len(byKey))
	for _, m := range byKey {
		if nameTier && !m.NameTier {
			continue
		}
		matches = append(matches, *m)
	}
	sort.Slice(matches, func(i, j int) bool {
		if matches[i].Machine != matches[j].Machine {
			return matches[i].Machine < matches[j].Machine
		}
		return matches[i].PaneID < matches[j].PaneID
	})
	return matches
}

func (a *App) localFindMatch(ag Agent, nameTier bool) findMatch {
	return findMatch{
		Machine:  a.machine(),
		PaneID:   ag.Pane,
		Name:     ag.Name,
		CWD:      ag.CWD,
		Status:   ag.Status,
		Via:      map[string]bool{"local": true},
		NameTier: nameTier,
		Local:    true,
	}
}

// findFleetConfigured reports whether an operator hub credential is
// configured. Node hosts carry only a lanes/node credential, so they keep
// the local lookup without a warning on every find.
func (a *App) findFleetConfigured() bool {
	hub := a.Config.Hub
	return str(hub, "hub_url") != "" && (str(hub, "operator_token_env") != "" || str(hub, "hub_token_env") != "")
}

// fleetFind asks the hub for every pane the target names. Each projection
// fails independently: a hub that answers /v1/jobs but not /v1/nodes still
// contributes its job matches, and every failure is a warning on stderr so
// a partial fleet answer is never mistaken for a complete one.
func (a *App) fleetFind(ctx context.Context, target string) []findMatch {
	if !a.findFleetConfigured() {
		return nil
	}
	client, err := newHubHTTP(a.Config.Hub)
	if err != nil {
		a.warn("find fleet lookup skipped: %v", err)
		return nil
	}
	now := a.Now()
	var matches []findMatch
	if data, err := a.findFleetGet(ctx, client, "/v1/nodes"); err != nil {
		a.warn("find fleet sessions unavailable: %v", err)
	} else if found, err := findSessionMatches(data, target, now); err != nil {
		a.warn("find fleet sessions unavailable: %v", err)
	} else {
		matches = append(matches, found...)
	}
	if data, err := a.findFleetGet(ctx, client, "/v1/jobs"); err != nil {
		a.warn("find fleet jobs unavailable: %v", err)
	} else if found, err := findJobMatches(data, target); err != nil {
		a.warn("find fleet jobs unavailable: %v", err)
	} else {
		matches = append(matches, found...)
	}
	if data, err := a.findFleetGet(ctx, client, "/v1/lanes"); err != nil {
		a.warn("find fleet lanes unavailable: %v", err)
	} else if found, err := findLaneMatches(data, target); err != nil {
		a.warn("find fleet lanes unavailable: %v", err)
	} else {
		matches = append(matches, found...)
	}
	return matches
}

func (a *App) findFleetGet(ctx context.Context, client *hubHTTP, path string) ([]byte, error) {
	status, data, err := client.do(ctx, http.MethodGet, path, nil, findFleetTimeout)
	if err != nil {
		return nil, err
	}
	if status != http.StatusOK {
		return nil, fmt.Errorf("hub answered HTTP %d", status)
	}
	return data, nil
}

// findNodeWire is the slice of GET /v1/nodes find reads. The session
// snapshot keeps its raw sessions so an ok-status node with a malformed
// list is skipped instead of half-decoded.
type findNodeWire struct {
	MachineID       string            `json:"machine_id"`
	State           string            `json:"state"`
	SessionSnapshot *findSnapshotWire `json:"session_snapshot"`
}

type findSnapshotWire struct {
	Sessions       []findSessionWire `json:"sessions"`
	SnapshotStatus string            `json:"snapshot_status"`
	Truncated      bool              `json:"truncated"`
	ReceivedAt     time.Time         `json:"received_at"`
	Stale          bool              `json:"stale"`
}

type findSessionWire struct {
	PaneID       string `json:"pane_id"`
	Label        string `json:"label"`
	AgentName    string `json:"agent_name"`
	DisplayLabel string `json:"display_label"`
	Status       string `json:"status"`
}

// findSnapshotStale marks session matches whose observation the hub itself
// no longer vouches for: a disconnected node, a stale flag, a missing or
// future received_at, or an observation older than the snapshot budget.
// Truncation is not staleness — a cut list still reports real panes.
func findSnapshotStale(node findNodeWire, snapshot findSnapshotWire, now time.Time) bool {
	if node.State != "connected" || snapshot.Stale || snapshot.ReceivedAt.IsZero() || snapshot.ReceivedAt.After(now) {
		return true
	}
	return now.Sub(snapshot.ReceivedAt) > findSnapshotMaxAge
}

// findSessionMatches applies the local find precedence inside each session:
// agent_name is the canonical identity (name tier); label and display_label
// are display context (label tier).
func findSessionMatches(data []byte, target string, now time.Time) ([]findMatch, error) {
	var body struct {
		Nodes []findNodeWire `json:"nodes"`
	}
	if err := json.Unmarshal(data, &body); err != nil || body.Nodes == nil {
		return nil, fmt.Errorf("invalid nodes response")
	}
	var matches []findMatch
	for _, node := range body.Nodes {
		snapshot := node.SessionSnapshot
		if snapshot == nil || snapshot.SnapshotStatus != findSnapshotOK || node.MachineID == "" {
			continue
		}
		stale := findSnapshotStale(node, *snapshot, now)
		for _, session := range snapshot.Sessions {
			nameTier := session.AgentName == target
			if !nameTier && session.Label != target && session.DisplayLabel != target {
				continue
			}
			if session.PaneID == "" {
				continue
			}
			matches = append(matches, findMatch{
				Machine:  node.MachineID,
				PaneID:   session.PaneID,
				Name:     session.AgentName,
				Status:   session.Status,
				Via:      map[string]bool{"session": true},
				NameTier: nameTier,
				Stale:    stale,
			})
		}
	}
	return matches, nil
}

// findJobMatches maps an active or orphaned hub job id to its machine and
// pane. Completed jobs are already absent from /v1/jobs.
func findJobMatches(data []byte, target string) ([]findMatch, error) {
	var body struct {
		Jobs []struct {
			Machine string `json:"machine"`
			JobID   string `json:"job_id"`
			Pane    string `json:"pane"`
		} `json:"jobs"`
	}
	if err := json.Unmarshal(data, &body); err != nil || body.Jobs == nil {
		return nil, fmt.Errorf("invalid jobs response")
	}
	var matches []findMatch
	for _, job := range body.Jobs {
		if job.JobID != target || job.Pane == "" {
			continue
		}
		matches = append(matches, findMatch{
			Machine:  job.Machine,
			PaneID:   job.Pane,
			Via:      map[string]bool{"job": true},
			NameTier: true,
		})
	}
	return matches, nil
}

// findLaneMatches maps a registered lane name to its machine and pane.
func findLaneMatches(data []byte, target string) ([]findMatch, error) {
	var body struct {
		Lanes []struct {
			Lane    string `json:"lane"`
			Machine string `json:"machine"`
			Pane    string `json:"pane"`
		} `json:"lanes"`
	}
	if err := json.Unmarshal(data, &body); err != nil || body.Lanes == nil {
		return nil, fmt.Errorf("invalid lanes response")
	}
	var matches []findMatch
	for _, lane := range body.Lanes {
		if lane.Lane != target || lane.Pane == "" {
			continue
		}
		matches = append(matches, findMatch{
			Machine:  lane.Machine,
			PaneID:   lane.Pane,
			Via:      map[string]bool{"lane": true},
			NameTier: true,
		})
	}
	return matches, nil
}

func findViaString(via map[string]bool) string {
	names := make([]string, 0, len(via))
	for name := range via {
		names = append(names, name)
	}
	sort.Strings(names)
	return strings.Join(names, ",")
}
