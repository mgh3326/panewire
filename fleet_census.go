package panewire

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode"
)

// fleet-census answers "what panes are up, who spawned them, and are they
// finished" in one read-only pass (#501, design/2026-09-21/fleet-census-reap
// task D). It never writes: no pane, tab, lane, job-event, or queue mutation
// exists anywhere in this file, and no --apply-style flag is accepted.
//
// The pane classification vocabulary is closed at six values:
//
//	reapable           the pane's job would appear in `wrk reap` dry-run's
//	                   would-close set (verified against the ae1f544 rules:
//	                   revived-job refusal, newest spawn receipt taken as one
//	                   unit, tab-mismatch, single-pane tab confirmed by a fresh
//	                   tab list, malformed-record skip)
//	builder            the job's claim role accumulated builder|captain, which
//	                   wrk reap excludes unless --include-builders is passed
//	no-terminal-event  a job record exists but has no effective terminal event
//	                   (detail=reclaimed-after-terminal marks a job that was
//	                   claimed again after finishing)
//	terminal-held      a terminal event exists but a reap condition is unmet;
//	                   detail carries the same reason string wrk would print
//	no-job-record      no job's newest spawn receipt names this pane
//	unobservable       the inputs needed to judge this pane were not observable
//	                   (stale/untrusted snapshot, remote job registry absent)
const (
	fleetCensusClassReapable        = "reapable"
	fleetCensusClassBuilder         = "builder"
	fleetCensusClassNoTerminalEvent = "no-terminal-event"
	fleetCensusClassTerminalHeld    = "terminal-held"
	fleetCensusClassNoJobRecord     = "no-job-record"
	fleetCensusClassUnobservable    = "unobservable"
)

const (
	fleetCensusOutcomeOK      = "ok"
	fleetCensusOutcomePartial = "partial"
	fleetCensusOutcomeError   = "error"
)

// Job verdicts mirror `wrk reap` dry-run output. "would-close" is the set A-4
// pins; "skip" reasons reuse wrk's own strings where wrk prints one, and a
// census-only label where wrk drops the job silently.
const (
	fleetCensusVerdictWouldClose = "would-close"
	fleetCensusVerdictSkip       = "skip"
	fleetCensusVerdictSilent     = "not-candidate"
)

// Skip reasons that wrk prints verbatim, plus census labels for the skips wrk
// makes silently. The wrk-literal reasons must stay byte-identical: the
// equivalence fixture compares them.
const (
	fleetCensusReasonReclaimed   = "reclaimed-after-terminal"
	fleetCensusReasonMalformed   = "malformed-record"
	fleetCensusReasonTabUnknown  = "tab-count-unknown"
	fleetCensusReasonTabResolved = "tab-unresolved"
	// census-only labels for wrk's silent drops
	fleetCensusReasonNoTerminal  = "no-terminal-event"
	fleetCensusReasonBuilderRole = "builder-role"
	fleetCensusReasonWithinGrace = "within-grace"
	fleetCensusReasonReaped      = "already-reaped"
	fleetCensusReasonNoPane      = "no-spawn-pane"
)

// Non-local classification details.
const (
	fleetCensusDetailRemoteJobUnknown = "job-record-remote-unavailable"
	fleetCensusDetailStaleSnapshot    = "snapshot-stale"
	fleetCensusDetailPartialSnapshot  = "snapshot-truncated"
	fleetCensusDetailNoAgent          = "no-agent-on-pane"
)

const (
	fleetCensusDefaultGrace     = 10 * time.Minute
	fleetCensusDefaultJobsRel   = "work/herdr-inbox/jobs"
	fleetCensusDefaultHerdrSock = ".config/herdr/herdr.sock"
	fleetCensusDefaultNodeEnv   = ".config/panewire/hub-node.env"
)

var (
	fleetCensusTerminalKinds = map[string]bool{"job.completed": true, "job.joined": true, "job.revoked": true}
	fleetCensusReviveKinds   = map[string]bool{"job.claim": true, "job.reclaim": true, "job.spawned": true, "job.reprompted": true}
	fleetCensusGracePattern  = regexp.MustCompile(`^(\d+)([smhd]?)$`)
)

type fleetCensusOptions struct {
	jsonOut     bool
	grace       time.Duration
	jobsRoot    string
	herdrSocket string
	machineID   string
	nodeEnv     string
	hubURL      string
	hubTokenEnv string
	hubCFEnv    string
}

// parseFleetCensusArgs accepts only the closed flag set below — there is no
// apply or mutation flag to smuggle in. Unknown flags and positionals are
// usage errors.
func parseFleetCensusArgs(args []string) (fleetCensusOptions, error) {
	options := fleetCensusOptions{grace: fleetCensusDefaultGrace}
	seen := make(map[string]bool)
	for index := 0; index < len(args); index++ {
		argument := args[index]
		if !strings.HasPrefix(argument, "-") {
			return options, errors.New("fleet-census takes no positional arguments")
		}
		name, value, hasValue := strings.Cut(argument, "=")
		switch name {
		case "--json":
			if seen[name] {
				return options, errors.New("duplicate fleet-census flag")
			}
			seen[name] = true
			parsed := true
			if hasValue {
				parsedValue, err := strconv.ParseBool(value)
				if err != nil {
					return options, errors.New("invalid fleet-census flag value")
				}
				parsed = parsedValue
			}
			options.jsonOut = parsed
		case "--grace", "--jobs-root", "--herdr-socket", "--machine-id", "--node-env", "--hub-url", "--hub-token-env", "--hub-cf-env":
			if seen[name] {
				return options, errors.New("duplicate fleet-census flag")
			}
			seen[name] = true
			if !hasValue {
				if index+1 >= len(args) {
					return options, errors.New("fleet-census flag value is required")
				}
				index++
				value = args[index]
			}
			switch name {
			case "--grace":
				parsed, err := parseFleetCensusGrace(value)
				if err != nil {
					return options, errors.New("invalid --grace value")
				}
				options.grace = parsed
			case "--jobs-root":
				options.jobsRoot = value
			case "--herdr-socket":
				options.herdrSocket = value
			case "--machine-id":
				options.machineID = value
			case "--node-env":
				options.nodeEnv = value
			case "--hub-url":
				options.hubURL = value
			case "--hub-token-env":
				options.hubTokenEnv = value
			case "--hub-cf-env":
				options.hubCFEnv = value
			}
		default:
			return options, errors.New("unknown fleet-census flag")
		}
	}
	if (options.hubURL == "") != (options.hubTokenEnv == "") {
		return options, errors.New("fleet-census hub requires both --hub-url and --hub-token-env")
	}
	if options.hubURL == "" && options.hubCFEnv != "" {
		return options, errors.New("fleet-census --hub-cf-env requires --hub-url")
	}
	if options.machineID != "" && !machineIDPattern.MatchString(options.machineID) {
		return options, errors.New("invalid --machine-id")
	}
	return options, nil
}

// parseFleetCensusGrace mirrors wrk's parse_duration_s: digits plus an
// optional s/m/h/d suffix. Unknown formats fail rather than falling to zero.
func parseFleetCensusGrace(value string) (time.Duration, error) {
	match := fleetCensusGracePattern.FindStringSubmatch(strings.TrimSpace(value))
	if match == nil {
		return 0, errors.New("invalid duration")
	}
	amount, err := strconv.ParseInt(match[1], 10, 64)
	if err != nil {
		return 0, errors.New("invalid duration")
	}
	scale := map[string]time.Duration{"": time.Second, "s": time.Second, "m": time.Minute, "h": time.Hour, "d": 24 * time.Hour}[match[2]]
	return time.Duration(amount) * scale, nil
}

// fleetCensusPaneObs is one pane.list row: the full pane set including panes
// whose agent already exited (agent_status="unknown", no agent field).
type fleetCensusPaneObs struct {
	PaneID      string
	WorkspaceID string
	TabID       string
	Status      string
	HasAgent    bool
	AgentName   string
	Harness     string
}

// fleetCensusLocalView is one consistent local observation: live agents
// (agent.list, wrk's probe source), every pane (pane.list), and tab pane
// counts (tab.list, wrk's shared-tab gate). Tabs==nil means tab.list could
// not be read, which is exactly wrk's empty tab-count path: every tab verdict
// becomes unknown.
type fleetCensusLocalView struct {
	agents     map[string]fleetCensusPaneObs
	panes      []fleetCensusPaneObs
	paneListOK bool
	tabs       map[string]*int
	tabsOK     bool
}

// readFleetCensusLocal performs the three read-only herdr calls. agent.list
// failure means the local node is unobservable (wrk could not probe either).
// pane.list failure degrades the row set to live agents and is recorded;
// tab.list failure only degrades the shared-tab gate, same as wrk.
func readFleetCensusLocal(ctx context.Context, client *HerdrClient) (fleetCensusLocalView, error) {
	var view fleetCensusLocalView
	raw, err := client.Call(ctx, "agent.list", map[string]any{})
	if err != nil {
		return view, err
	}
	var listed struct {
		Agents []map[string]any `json:"agents"`
	}
	if json.Unmarshal(raw, &listed) != nil {
		return view, errors.New("invalid herdr agent list")
	}
	view.agents = make(map[string]fleetCensusPaneObs, len(listed.Agents))
	for _, entry := range listed.Agents {
		pane := identityFromMap(entry)
		if pane.PaneID == "" {
			continue
		}
		view.agents[pane.PaneID] = fleetCensusPaneObs{
			PaneID: pane.PaneID, WorkspaceID: pane.WorkspaceID, TabID: pane.TabID,
			Status: pane.Status, HasAgent: true, AgentName: pane.Name, Harness: pane.Harness,
		}
	}
	if rawPanes, err := client.Call(ctx, "pane.list", map[string]any{}); err == nil {
		var paneList struct {
			Panes []map[string]any `json:"panes"`
		}
		if json.Unmarshal(rawPanes, &paneList) == nil {
			view.paneListOK = true
			for _, entry := range paneList.Panes {
				pane := identityFromMap(entry)
				if pane.PaneID == "" {
					continue
				}
				obs := fleetCensusPaneObs{PaneID: pane.PaneID, WorkspaceID: pane.WorkspaceID, TabID: pane.TabID, Status: pane.Status}
				if agent, live := view.agents[pane.PaneID]; live {
					obs = agent
				}
				view.panes = append(view.panes, obs)
			}
		}
	}
	if !view.paneListOK {
		for _, agent := range view.agents {
			view.panes = append(view.panes, agent)
		}
	}
	if rawTabs, err := client.Call(ctx, "tab.list", map[string]any{}); err == nil {
		var tabList struct {
			Tabs []map[string]any `json:"tabs"`
		}
		if json.Unmarshal(rawTabs, &tabList) == nil {
			view.tabsOK = true
			view.tabs = make(map[string]*int, len(tabList.Tabs))
			for _, entry := range tabList.Tabs {
				id := aString(entry, "tab_id")
				if id == "" {
					continue
				}
				if _, duplicate := view.tabs[id]; duplicate {
					// The same tab listed twice is wrk's multi-row case: the
					// count cannot be trusted, so the verdict is unknown.
					view.tabs[id] = nil
					continue
				}
				// pane_count must be a plain integer; anything else (absent,
				// string, bool) is wrk's "?" and keeps the tab unknown.
				switch value := entry["pane_count"].(type) {
				case float64:
					count := int(value)
					if float64(count) == value {
						view.tabs[id] = &count
					} else {
						view.tabs[id] = nil
					}
				default:
					view.tabs[id] = nil
				}
			}
		}
	}
	sort.Slice(view.panes, func(i, j int) bool { return view.panes[i].PaneID < view.panes[j].PaneID })
	return view, nil
}

// fleetCensusJobScan is the per-job result of walking jobs/<id>/events in
// filename order — the same inputs wrk reap_candidates consumes.
type fleetCensusJobScan struct {
	jobID        string
	ownerLane    string
	role         string
	claimRole    string
	spawnPane    string
	spawnTab     string
	spawnAt      time.Time
	sawSpawn     bool
	reaped       bool
	revived      bool
	malformed    bool
	terminalKind string
	terminalAt   time.Time
	hasTerminal  bool
	firstAt      time.Time
	hasFirst     bool
	pane         string
	tab          string
}

// fleetCensusMoment mirrors wrk's moment_of: created_at (or payload.at) parsed
// as ISO-8601, falling back to the file mtime on blank or unparseable values.
func fleetCensusMoment(value string, fallback time.Time) time.Time {
	text := strings.TrimSpace(value)
	if text == "" {
		return fallback
	}
	// fromisoformat also accepts a space separator; normalize it so both
	// parsers see the same instants.
	if parsed, err := time.Parse(time.RFC3339Nano, strings.Replace(text, " ", "T", 1)); err == nil {
		return parsed
	}
	if parsed, err := time.ParseInLocation("2006-01-02T15:04:05", text, time.Local); err == nil {
		return parsed
	}
	if parsed, err := time.ParseInLocation("2006-01-02 15:04:05", text, time.Local); err == nil {
		return parsed
	}
	return fallback
}

// scanFleetCensusJobs replays wrk reap_candidates over the jobs root. Event
// order is the sorted filename order; payload defaults to the document itself
// for flat records; the newest job.spawned receipt supplies pane and tab as
// one unit; claim/reclaim ownership is latest-wins and builder|captain role
// accumulates; a revive-kind event after a terminal event clears the terminal
// timestamp (reclaimed-after-terminal). The bool reports whether the root
// could be listed at all — an unreadable inbox is an observation gap, not an
// empty one.
func scanFleetCensusJobs(root string) ([]fleetCensusJobScan, bool) {
	entries, err := os.ReadDir(root)
	if err != nil {
		return nil, false
	}
	names := make([]string, 0, len(entries))
	for _, entry := range entries {
		names = append(names, entry.Name())
	}
	sort.Strings(names)
	scans := make([]fleetCensusJobScan, 0, len(names))
	for _, job := range names {
		eventsDir := filepath.Join(root, job, "events")
		info, err := os.Stat(eventsDir)
		if err != nil || !info.IsDir() {
			continue
		}
		eventFiles, err := os.ReadDir(eventsDir)
		if err != nil {
			continue
		}
		eventNames := make([]string, 0, len(eventFiles))
		for _, eventFile := range eventFiles {
			if strings.HasSuffix(eventFile.Name(), ".json") {
				eventNames = append(eventNames, eventFile.Name())
			}
		}
		sort.Strings(eventNames)
		scan := fleetCensusJobScan{jobID: job}
		pane, tab := "", ""
		for _, name := range eventNames {
			path := filepath.Join(eventsDir, name)
			raw, err := os.ReadFile(path)
			if err != nil {
				continue
			}
			var document map[string]json.RawMessage
			if json.Unmarshal(raw, &document) != nil {
				continue
			}
			payload := document
			if nested, exists := document["payload"]; exists {
				var decoded map[string]json.RawMessage
				if json.Unmarshal(nested, &decoded) == nil && decoded != nil {
					payload = decoded
				} else {
					payload = map[string]json.RawMessage{}
				}
			}
			kind := fleetCensusString(document["kind"])
			if kind == "job.claim" || kind == "job.reclaim" {
				scan.ownerLane = fleetCensusString(payload["owner_lane"])
				scan.claimRole = fleetCensusString(payload["role"])
				if scan.claimRole == "builder" || scan.claimRole == "captain" {
					scan.role = "builder"
				}
			}
			for _, key := range []string{"pane_id", "tab_id"} {
				if value := fleetCensusString(payload[key]); value != "" {
					if key == "pane_id" {
						pane = value
					} else {
						tab = value
					}
				}
			}
			stat, statErr := os.Stat(path)
			fallback := time.Now()
			if statErr == nil {
				fallback = stat.ModTime()
			}
			moment := fleetCensusMoment(fleetCensusMomentField(document["created_at"], payload["at"]), fallback)
			if !scan.hasFirst || moment.Before(scan.firstAt) {
				scan.firstAt, scan.hasFirst = moment, true
			}
			if kind == "job.spawned" {
				scan.sawSpawn = true
				scan.spawnPane = fleetCensusString(payload["pane_id"])
				scan.spawnTab = fleetCensusString(payload["tab_id"])
				scan.spawnAt = moment
			}
			if kind == "job.reaped" {
				scan.reaped = true
			}
			if fleetCensusReviveKinds[kind] && scan.hasTerminal {
				scan.hasTerminal = false
				scan.revived = true
			}
			if fleetCensusTerminalKinds[kind] {
				scan.revived = false
				scan.hasTerminal = true
				scan.terminalAt = moment
				scan.terminalKind = kind
			}
		}
		if scan.sawSpawn {
			pane, tab = scan.spawnPane, scan.spawnTab
			scan.malformed = pane == ""
		}
		scan.pane, scan.tab = pane, tab
		scans = append(scans, scan)
	}
	return scans, true
}

func fleetCensusString(raw json.RawMessage) string {
	var value string
	if json.Unmarshal(raw, &value) != nil {
		return ""
	}
	return value
}

// fleetCensusMomentField mirrors wrk's `created_at or payload.at`: the first
// field wins when it is a non-empty string, and a truthy non-string value
// (a number, true, an object) also wins — wrk's moment_of then cannot parse
// it and lands on the mtime fallback. Only absent/null/empty fields defer to
// the next.
func fleetCensusMomentField(values ...json.RawMessage) string {
	for _, raw := range values {
		if len(raw) == 0 {
			continue
		}
		if text := fleetCensusString(raw); text != "" {
			return text
		}
		trimmed := strings.TrimSpace(string(raw))
		if trimmed == "" || trimmed == "null" || trimmed == "false" || trimmed == "[]" || trimmed == "{}" {
			continue
		}
		var number float64
		if json.Unmarshal(raw, &number) == nil && number == 0 {
			continue
		}
		return "\x00"
	}
	return ""
}

func fleetCensusHasSpace(value string) bool {
	return strings.IndexFunc(value, unicode.IsSpace) >= 0
}

// fleetCensusJobVerdict applies the wrk reap pipeline to one scanned job
// against one local observation. Verdict precedence matches reap_cmd's loop:
// record filters first (reaped, pane-less, builder role, terminal state,
// grace, malformed), then the herdr probes (pane resolution, status, tab
// resolution, tab match, single-pane tab).
func fleetCensusJobVerdict(scan fleetCensusJobScan, view fleetCensusLocalView, grace time.Duration, now time.Time) (string, string, int64) {
	if scan.reaped {
		return fleetCensusVerdictSilent, fleetCensusReasonReaped, 0
	}
	if scan.pane == "" && !scan.malformed {
		return fleetCensusVerdictSilent, fleetCensusReasonNoPane, 0
	}
	if scan.role == "builder" {
		return fleetCensusVerdictSilent, fleetCensusReasonBuilderRole, 0
	}
	if !scan.hasTerminal {
		if scan.revived && !fleetCensusHasSpace(scan.jobID) {
			return fleetCensusVerdictSkip, fleetCensusReasonReclaimed, 0
		}
		return fleetCensusVerdictSilent, fleetCensusReasonNoTerminal, 0
	}
	age := int64(now.Sub(scan.terminalAt).Seconds())
	if age < int64(grace.Seconds()) {
		return fleetCensusVerdictSilent, fleetCensusReasonWithinGrace, age
	}
	if scan.malformed || fleetCensusHasSpace(scan.jobID) || fleetCensusHasSpace(scan.ownerLane) || fleetCensusHasSpace(scan.pane) || fleetCensusHasSpace(scan.tab) {
		return fleetCensusVerdictSkip, fleetCensusReasonMalformed, age
	}
	agent, live := view.agents[scan.pane]
	if !live {
		return fleetCensusVerdictSkip, "pane-unresolved(err:agent_not_found)", age
	}
	if agent.Status == "" {
		return fleetCensusVerdictSkip, "pane-unresolved(err:parse)", age
	}
	if agent.Status != "idle" && agent.Status != "done" {
		return fleetCensusVerdictSkip, "status=" + agent.Status, age
	}
	tab := scan.tab
	if tab == "" {
		tab = agent.TabID
	}
	if tab == "" {
		return fleetCensusVerdictSkip, fleetCensusReasonTabResolved, age
	}
	if agent.TabID != "" && agent.TabID != tab {
		return fleetCensusVerdictSkip, "tab-mismatch(pane-in=" + agent.TabID + ")", age
	}
	if !view.tabsOK {
		return fleetCensusVerdictSkip, fleetCensusReasonTabUnknown, age
	}
	count, listed := view.tabs[tab]
	if !listed || count == nil {
		return fleetCensusVerdictSkip, fleetCensusReasonTabUnknown, age
	}
	// wrk's reap_tab_verdict: exactly one is single; more is shared:N; zero,
	// negative, and every non-integer shape are unknown — never shared.
	if *count > 1 {
		return fleetCensusVerdictSkip, fmt.Sprintf("tab-shared(panes=%d)", *count), age
	}
	if *count != 1 {
		return fleetCensusVerdictSkip, fleetCensusReasonTabUnknown, age
	}
	return fleetCensusVerdictWouldClose, "", age
}

type fleetCensusPaneRow struct {
	Machine            string   `json:"machine"`
	PaneID             string   `json:"pane_id"`
	WorkspaceID        string   `json:"workspace_id,omitempty"`
	TabID              string   `json:"tab_id,omitempty"`
	AgentName          string   `json:"agent_name,omitempty"`
	Harness            string   `json:"harness,omitempty"`
	Status             string   `json:"status"`
	HasAgent           bool     `json:"has_agent"`
	JobID              string   `json:"job_id,omitempty"`
	JobIDs             []string `json:"job_ids,omitempty"`
	OwnerLane          string   `json:"owner_lane,omitempty"`
	Role               string   `json:"role,omitempty"`
	TerminalKind       string   `json:"terminal_kind,omitempty"`
	TerminalAt         string   `json:"terminal_at,omitempty"`
	AgeSeconds         *int64   `json:"age_seconds,omitempty"`
	TerminalAgeSeconds *int64   `json:"terminal_age_seconds,omitempty"`
	Class              string   `json:"class"`
	Detail             string   `json:"detail,omitempty"`
	Lanes              []string `json:"lanes,omitempty"`
	Fresh              *bool    `json:"fresh,omitempty"`
	SnapshotState      string   `json:"snapshot_state,omitempty"`
}

type fleetCensusJobRow struct {
	JobID        string `json:"job_id"`
	OwnerLane    string `json:"owner_lane,omitempty"`
	Role         string `json:"role,omitempty"`
	PaneID       string `json:"pane_id,omitempty"`
	TabID        string `json:"tab_id,omitempty"`
	TerminalKind string `json:"terminal_kind,omitempty"`
	TerminalAt   string `json:"terminal_at,omitempty"`
	AgeSeconds   *int64 `json:"age_seconds,omitempty"`
	Verdict      string `json:"verdict"`
	Reason       string `json:"reason,omitempty"`
}

// fleetCensusNodeCoverage carries the sessions-find state vocabulary plus the
// observation source. The local machine is observed through herdr directly;
// hub_state records what the hub last reported when it also lists this node.
type fleetCensusNodeCoverage struct {
	Machine   string `json:"machine"`
	Source    string `json:"source"`
	Covered   bool   `json:"covered"`
	State     string `json:"state,omitempty"`
	Reason    string `json:"reason,omitempty"`
	Sessions  *int   `json:"sessions,omitempty"`
	LastSeen  string `json:"last_seen,omitempty"`
	HubState  string `json:"hub_state,omitempty"`
	HubSeenAt string `json:"hub_seen_at,omitempty"`
}

type fleetCensusCoverage struct {
	Scope       string                    `json:"scope"`
	Expected    int                       `json:"expected"`
	Observed    int                       `json:"observed"`
	Missing     int                       `json:"missing"`
	Stale       int                       `json:"stale"`
	Truncated   int                       `json:"truncated"`
	Invalid     int                       `json:"invalid"`
	Unavailable int                       `json:"unavailable"`
	Nodes       []fleetCensusNodeCoverage `json:"nodes"`
	Reasons     []string                  `json:"reasons,omitempty"`
}

// fleetCensusLaneIssue flags one (machine, pane) target claimed by more than
// one non-sink lane, or a lane whose pane the latest usable snapshot does not
// contain (the lanes-audit dead verdict).
type fleetCensusLaneIssue struct {
	Kind    string   `json:"kind"`
	Machine string   `json:"machine"`
	Pane    string   `json:"pane"`
	Lanes   []string `json:"lanes"`
}

type fleetCensusSummary struct {
	Panes           int `json:"panes"`
	Reapable        int `json:"reapable"`
	Builder         int `json:"builder"`
	NoTerminalEvent int `json:"no_terminal_event"`
	TerminalHeld    int `json:"terminal_held"`
	NoJobRecord     int `json:"no_job_record"`
	Unobservable    int `json:"unobservable"`
}

type fleetCensusResult struct {
	FetchedAt  string                 `json:"fetched_at"`
	Machine    string                 `json:"machine"`
	Outcome    string                 `json:"outcome"`
	Coverage   fleetCensusCoverage    `json:"coverage"`
	Panes      []fleetCensusPaneRow   `json:"panes"`
	Jobs       []fleetCensusJobRow    `json:"jobs"`
	Lanes      []lanesAuditLaneRow    `json:"lanes,omitempty"`
	LaneIssues []fleetCensusLaneIssue `json:"lane_issues,omitempty"`
	Summary    fleetCensusSummary     `json:"summary"`
}

type fleetCensusHubJobsWire struct {
	Jobs []hubConsoleJob `json:"jobs"`
}

// fleetCensusRemotePaneRow builds the row for one hub-reported session. A
// stale or otherwise untrusted snapshot keeps the pane visible but marks it
// unobservable — a node's panes are never silently counted as absent. On a
// fresh observation the hub job registry (active jobs only) can name the
// owner and role; a missing record cannot distinguish "no job" from
// "finished before the hub saw it", so it stays unobservable.
func fleetCensusRemotePaneRow(machine string, session HubSession, fresh bool, snapshotState string, hubJobsByPane map[string]hubConsoleJob, laneIndex map[string][]string, now time.Time) fleetCensusPaneRow {
	row := fleetCensusPaneRow{
		Machine: machine, PaneID: session.PaneID, WorkspaceID: session.WorkspaceID,
		AgentName: session.AgentName, Status: session.Status, HasAgent: true,
		Fresh: &fresh, SnapshotState: snapshotState,
	}
	if snapshotState == sessionsFindStateObservedEmpty || snapshotState == sessionsFindStateInvalidSnapshot {
		row.SnapshotState = ""
	}
	if !fresh {
		row.Class = fleetCensusClassUnobservable
		row.Detail = fleetCensusDetailStaleSnapshot
	} else if job, known := hubJobsByPane[machine+"\x00"+session.PaneID]; known {
		row.JobID = job.JobID
		row.JobIDs = []string{job.JobID}
		row.OwnerLane = job.OwnerLane
		row.Role = job.Role
		if started, err := time.Parse(time.RFC3339, job.StartedAt); err == nil {
			age := int64(now.Sub(started).Seconds())
			row.AgeSeconds = &age
		}
		if fleetCensusTerminalKinds[job.LastEventKind] {
			row.TerminalKind = job.LastEventKind
			row.TerminalAt = job.LastEventAt
			if at, err := time.Parse(time.RFC3339, job.LastEventAt); err == nil {
				age := int64(now.Sub(at).Seconds())
				row.TerminalAgeSeconds = &age
			}
		}
		if job.Role == "builder" || job.Role == "captain" {
			row.Class = fleetCensusClassBuilder
		} else {
			row.Class = fleetCensusClassNoTerminalEvent
		}
	} else {
		row.Class = fleetCensusClassUnobservable
		row.Detail = fleetCensusDetailRemoteJobUnknown
	}
	if fresh && snapshotState == sessionsFindStatePartial {
		row.Detail = fleetCensusDetailPartialSnapshot
	}
	row.Lanes = laneIndex[machine+"\x00"+session.PaneID]
	return row
}

// censusLaneTargets indexes non-sink lane routes by machine+pane for both the
// per-pane lanes list and the multi-lane issue report.
func fleetCensusLaneIndex(lanes []hubLaneProjection) map[string][]string {
	index := make(map[string][]string)
	for _, lane := range lanes {
		if lane.Sink || !validHubLaneProjection(lane) {
			continue
		}
		key := lane.Machine + "\x00" + lane.Pane
		index[key] = append(index[key], lane.Lane)
	}
	for key := range index {
		sort.Strings(index[key])
	}
	return index
}

// buildFleetCensusResult assembles the census from its three independent
// inputs: the local observation (nil when herdr is unreachable), the local
// jobs scan, and the optional hub view (nodes coverage, remote sessions, hub
// job registry, lane projections).
func buildFleetCensusResult(options fleetCensusOptions, machineID string, view fleetCensusLocalView, localOK bool, jobsOK bool, scans []fleetCensusJobScan, nodes []sessionsFindNodeWire, hubJobs []hubConsoleJob, lanes []hubLaneProjection, nodesRejected string, hubJobsRejected bool, now time.Time) fleetCensusResult {
	result := fleetCensusResult{
		FetchedAt: now.UTC().Format(time.RFC3339),
		Machine:   machineID,
		Panes:     []fleetCensusPaneRow{},
		Jobs:      []fleetCensusJobRow{},
	}
	result.Coverage.Nodes = []fleetCensusNodeCoverage{}
	laneIndex := fleetCensusLaneIndex(lanes)

	// Job verdicts (wrk reap equivalence) run on the local observation only:
	// wrk itself is a local-only tool, so "reapable" is only ever asserted for
	// this machine.
	jobVerdicts := make(map[string]string, len(scans))
	jobReasons := make(map[string]string, len(scans))
	paneJobs := make(map[string][]fleetCensusJobScan)
	for _, scan := range scans {
		verdict, reason, _ := fleetCensusJobVerdict(scan, view, options.grace, now)
		jobVerdicts[scan.jobID] = verdict
		jobReasons[scan.jobID] = reason
		row := fleetCensusJobRow{JobID: scan.jobID, OwnerLane: scan.ownerLane, Role: scan.claimRole, Verdict: verdict, Reason: reason}
		if row.Role == "" {
			row.Role = scan.role
		}
		if scan.pane != "" && !scan.malformed {
			row.PaneID = scan.pane
			row.TabID = scan.tab
		}
		if scan.terminalKind != "" {
			row.TerminalKind = scan.terminalKind
			if !scan.terminalAt.IsZero() {
				row.TerminalAt = scan.terminalAt.UTC().Format(time.RFC3339)
			}
		}
		if scan.hasFirst {
			jobAge := int64(now.Sub(scan.firstAt).Seconds())
			row.AgeSeconds = &jobAge
		}
		result.Jobs = append(result.Jobs, row)
		if scan.pane != "" && !scan.malformed {
			paneJobs[scan.pane] = append(paneJobs[scan.pane], scan)
		}
	}

	// Local pane rows from pane.list (agent-bearing and agent-less alike).
	if localOK {
		coveredCount := len(view.panes)
		localEntry := fleetCensusNodeCoverage{Machine: machineID, Source: "local", Covered: true, Sessions: &coveredCount}
		for _, pane := range view.panes {
			row := fleetCensusPaneRow{
				Machine: machineID, PaneID: pane.PaneID, WorkspaceID: pane.WorkspaceID,
				TabID: pane.TabID, Status: pane.Status, HasAgent: pane.HasAgent,
				AgentName: pane.AgentName, Harness: pane.Harness,
			}
			linked := paneJobs[pane.PaneID]
			for _, job := range linked {
				row.JobIDs = append(row.JobIDs, job.jobID)
			}
			sort.Strings(row.JobIDs)
			if len(linked) > 0 {
				// The primary job is the one with the newest spawn receipt —
				// the pane's current occupant — with job_id breaking ties.
				primary := linked[0]
				for _, job := range linked[1:] {
					if job.spawnAt.After(primary.spawnAt) || (job.spawnAt.Equal(primary.spawnAt) && job.jobID > primary.jobID) {
						primary = job
					}
				}
				row.JobID = primary.jobID
				row.OwnerLane = primary.ownerLane
				row.Role = primary.claimRole
				if row.Role == "" {
					row.Role = primary.role
				}
				if primary.terminalKind != "" {
					row.TerminalKind = primary.terminalKind
					row.TerminalAt = primary.terminalAt.UTC().Format(time.RFC3339)
					age := int64(now.Sub(primary.terminalAt).Seconds())
					row.TerminalAgeSeconds = &age
				}
				if primary.sawSpawn && !primary.spawnAt.IsZero() {
					age := int64(now.Sub(primary.spawnAt).Seconds())
					row.AgeSeconds = &age
				}
				verdict, reason := jobVerdicts[primary.jobID], jobReasons[primary.jobID]
				switch {
				case verdict == fleetCensusVerdictWouldClose:
					row.Class = fleetCensusClassReapable
				case reason == fleetCensusReasonBuilderRole:
					row.Class = fleetCensusClassBuilder
				case reason == fleetCensusReasonNoTerminal:
					row.Class = fleetCensusClassNoTerminalEvent
				case reason == fleetCensusReasonReclaimed:
					row.Class = fleetCensusClassNoTerminalEvent
					row.Detail = reason
				case reason == fleetCensusReasonReaped:
					row.Class = fleetCensusClassTerminalHeld
					row.Detail = reason
				case reason != "":
					row.Class = fleetCensusClassTerminalHeld
					row.Detail = reason
				default:
					row.Class = fleetCensusClassNoTerminalEvent
				}
			} else if !jobsOK {
				// The jobs inbox could not be listed, so "this pane has no
				// job" was never actually checked.
				row.Class = fleetCensusClassUnobservable
				row.Detail = "jobs-inbox-unavailable"
			} else {
				row.Class = fleetCensusClassNoJobRecord
				if !pane.HasAgent {
					row.Detail = fleetCensusDetailNoAgent
				}
			}
			row.Lanes = laneIndex[machineID+"\x00"+pane.PaneID]
			result.Panes = append(result.Panes, row)
		}
		result.Coverage.Nodes = append(result.Coverage.Nodes, localEntry)
	} else {
		result.Coverage.Nodes = append(result.Coverage.Nodes, fleetCensusNodeCoverage{
			Machine: machineID, Source: "local", Covered: false,
			State: sessionsFindStateCollectorUnavailable, Reason: "unavailable",
		})
	}

	// Hub jobs index by machine+pane for remote classification.
	hubJobsByPane := make(map[string]hubConsoleJob, len(hubJobs))
	for _, job := range hubJobs {
		if job.Machine == "" || job.Pane == "" {
			continue
		}
		key := job.Machine + "\x00" + job.Pane
		current, exists := hubJobsByPane[key]
		if !exists || job.StartedAt > current.StartedAt || (job.StartedAt == current.StartedAt && job.JobID > current.JobID) {
			hubJobsByPane[key] = job
		}
	}

	// The local machine may also appear in /v1/nodes. It is one machine with
	// (at most) two observation paths, so merge it into a single coverage
	// entry and a single row set instead of counting it twice: direct herdr
	// observation wins when available, the hub snapshot stands in when herdr
	// is unreachable.
	var localHubObservation *sessionsFindNodeObservation
	var localHubNode *sessionsFindNodeWire
	for index, node := range nodes {
		if node.MachineID == machineID {
			observation := classifySessionsFindNode(node, now)
			localHubObservation = &observation
			local := node
			localHubNode = &local
			nodes = append(nodes[:index], nodes[index+1:]...)
			break
		}
	}
	localEntry := result.Coverage.Nodes[len(result.Coverage.Nodes)-1]
	if localHubObservation != nil {
		localEntry.Source = "local+hub"
		localEntry.HubState = localHubObservation.state
		if localHubObservation.state == "" {
			localEntry.HubState = "covered"
		}
		if localHubObservation.hasSeen {
			localEntry.HubSeenAt = localHubObservation.lastSeen.UTC().Format(time.RFC3339)
		}
	}
	if !localOK && localHubObservation != nil && localHubObservation.decodable && localHubObservation.sessions != nil {
		// Local herdr is unreachable but the hub's snapshot of this node
		// decoded: its sessions are the only local observation available.
		localEntry.Covered = localHubObservation.covered
		localEntry.State = localHubObservation.state
		localEntry.Reason = localHubObservation.reason
		count := len(localHubObservation.sessions)
		localEntry.Sessions = &count
		localEntry.LastSeen = localEntry.HubSeenAt
		fresh := localHubObservation.covered || localHubObservation.state == sessionsFindStatePartial
		for _, session := range localHubObservation.sessions {
			result.Panes = append(result.Panes, fleetCensusRemotePaneRow(machineID, session, fresh, localHubObservation.state, hubJobsByPane, laneIndex, now))
		}
	}
	result.Coverage.Nodes[len(result.Coverage.Nodes)-1] = localEntry

	// Remote nodes: reuse the sessions-find classification so the coverage
	// vocabulary is identical. Panes are emitted only from snapshots that
	// decoded; stale nodes surface their rows flagged unfresh instead of
	// silently counting them as absent.
	for _, node := range nodes {
		observation := classifySessionsFindNode(node, now)
		entry := fleetCensusNodeCoverage{Machine: node.MachineID, Source: "hub", Covered: observation.covered, State: observation.state, Reason: observation.reason}
		if observation.decodable && observation.sessions != nil {
			count := len(observation.sessions)
			entry.Sessions = &count
		}
		if observation.hasSeen {
			entry.LastSeen = observation.lastSeen.UTC().Format(time.RFC3339)
		}
		result.Coverage.Nodes = append(result.Coverage.Nodes, entry)
		fresh := observation.covered || observation.state == sessionsFindStatePartial
		for _, session := range observation.sessions {
			result.Panes = append(result.Panes, fleetCensusRemotePaneRow(node.MachineID, session, fresh, observation.state, hubJobsByPane, laneIndex, now))
		}
	}

	if nodesRejected != "" {
		result.Coverage.Reasons = append(result.Coverage.Reasons, nodesRejected)
	}

	// Lane verdicts reuse the lanes-audit judgement whole: dead only on a
	// fresh, complete, decodable snapshot of the lane's machine. The local
	// node's hub snapshot stays in the audit input — it is the only
	// observation of this machine when herdr is down — but a successful
	// direct herdr observation overrides it for local lanes: pane.list is
	// complete and fresh by construction.
	if lanes != nil {
		var audit lanesAuditResult
		if nodesRejected != "" {
			audit = buildLanesAuditNodesRejected(lanes, nodesRejected, now)
		} else {
			auditNodes := make([]lanesAuditNodeWire, 0, len(nodes)+1)
			for _, node := range nodes {
				auditNodes = append(auditNodes, lanesAuditNodeWire{MachineID: node.MachineID, State: node.State, SessionSnapshot: node.SessionSnapshot})
			}
			if localHubNode != nil {
				auditNodes = append(auditNodes, lanesAuditNodeWire{MachineID: localHubNode.MachineID, State: localHubNode.State, SessionSnapshot: localHubNode.SessionSnapshot})
			}
			audit = buildLanesAuditResult(lanes, auditNodes, now)
		}
		if localOK {
			localPanes := make(map[string]bool, len(view.panes))
			for _, pane := range view.panes {
				localPanes[pane.PaneID] = true
			}
			lanesByName := make(map[string]hubLaneProjection, len(lanes))
			for _, lane := range lanes {
				lanesByName[lane.Lane] = lane
			}
			audit.Summary.Alive, audit.Summary.Dead, audit.Summary.Indeterminate = 0, 0, 0
			for index, row := range audit.Lanes {
				lane, listed := lanesByName[row.Lane]
				if listed && lane.Machine == machineID && validHubLaneProjection(lane) {
					row.Verdict = lanesAuditVerdictDead
					if localPanes[lane.Pane] {
						row.Verdict = lanesAuditVerdictAlive
					}
					row.Reason = ""
					row.NodeState = "local-herdr"
					row.LastSeen = now.UTC().Format(time.RFC3339)
					audit.Lanes[index] = row
				}
				switch audit.Lanes[index].Verdict {
				case lanesAuditVerdictAlive:
					audit.Summary.Alive++
				case lanesAuditVerdictDead:
					audit.Summary.Dead++
				default:
					audit.Summary.Indeterminate++
				}
			}
			switch {
			case audit.Summary.Indeterminate > 0:
				audit.Outcome = lanesAuditOutcomePartial
			case audit.Summary.Dead > 0:
				audit.Outcome = lanesAuditOutcomeDeadLanes
			default:
				audit.Outcome = lanesAuditOutcomeOK
			}
		}
		result.Lanes = audit.Lanes
		shared := make(map[string][]string)
		for _, lane := range lanes {
			if lane.Sink || !validHubLaneProjection(lane) {
				continue
			}
			shared[lane.Machine+"\x00"+lane.Pane] = append(shared[lane.Machine+"\x00"+lane.Pane], lane.Lane)
		}
		for key, members := range shared {
			if len(members) < 2 {
				continue
			}
			machine, pane, _ := strings.Cut(key, "\x00")
			sort.Strings(members)
			result.LaneIssues = append(result.LaneIssues, fleetCensusLaneIssue{Kind: "multi-lane-pane", Machine: machine, Pane: pane, Lanes: members})
		}
		for _, row := range audit.Lanes {
			if row.Verdict == lanesAuditVerdictDead {
				result.LaneIssues = append(result.LaneIssues, fleetCensusLaneIssue{Kind: "lane-pane-missing", Machine: row.Machine, Pane: row.Pane, Lanes: []string{row.Lane}})
			}
		}
		sort.Slice(result.LaneIssues, func(i, j int) bool {
			if result.LaneIssues[i].Kind != result.LaneIssues[j].Kind {
				return result.LaneIssues[i].Kind < result.LaneIssues[j].Kind
			}
			if result.LaneIssues[i].Machine != result.LaneIssues[j].Machine {
				return result.LaneIssues[i].Machine < result.LaneIssues[j].Machine
			}
			return result.LaneIssues[i].Pane < result.LaneIssues[j].Pane
		})
	}

	sort.Slice(result.Panes, func(i, j int) bool {
		if result.Panes[i].Machine != result.Panes[j].Machine {
			return result.Panes[i].Machine < result.Panes[j].Machine
		}
		return result.Panes[i].PaneID < result.Panes[j].PaneID
	})
	sort.Slice(result.Coverage.Nodes, func(i, j int) bool { return result.Coverage.Nodes[i].Machine < result.Coverage.Nodes[j].Machine })
	for _, node := range result.Coverage.Nodes {
		result.Coverage.Expected++
		switch node.State {
		case sessionsFindStateUnobserved:
			result.Coverage.Missing++
		case sessionsFindStateCollectorUnavailable:
			result.Coverage.Unavailable++
		case sessionsFindStateInvalidSnapshot:
			result.Coverage.Invalid++
		case sessionsFindStateStale:
			result.Coverage.Stale++
		case sessionsFindStatePartial:
			result.Coverage.Truncated++
		}
		if node.Covered {
			result.Coverage.Observed++
		}
	}
	if options.hubURL == "" {
		result.Coverage.Scope = "local_only"
	} else {
		result.Coverage.Scope = "local_plus_hub_returned_nodes"
	}
	for _, reason := range []struct {
		name  string
		count int
	}{
		{"missing", result.Coverage.Missing},
		{"unavailable", result.Coverage.Unavailable},
		{"stale", result.Coverage.Stale},
		{"truncated", result.Coverage.Truncated},
		{"invalid", result.Coverage.Invalid},
	} {
		if reason.count > 0 {
			result.Coverage.Reasons = append(result.Coverage.Reasons, reason.name)
		}
	}
	for _, pane := range result.Panes {
		result.Summary.Panes++
		switch pane.Class {
		case fleetCensusClassReapable:
			result.Summary.Reapable++
		case fleetCensusClassBuilder:
			result.Summary.Builder++
		case fleetCensusClassNoTerminalEvent:
			result.Summary.NoTerminalEvent++
		case fleetCensusClassTerminalHeld:
			result.Summary.TerminalHeld++
		case fleetCensusClassNoJobRecord:
			result.Summary.NoJobRecord++
		default:
			result.Summary.Unobservable++
		}
	}
	if !jobsOK {
		result.Coverage.Reasons = append(result.Coverage.Reasons, "jobs-inbox-unavailable")
	}
	if localOK && !view.paneListOK {
		// Without pane.list the row set is agents only: agent-less panes are
		// invisible, which is a degraded observation, not a clean one.
		result.Coverage.Reasons = append(result.Coverage.Reasons, "pane-list-unavailable")
	}
	if localOK && !view.tabsOK {
		result.Coverage.Reasons = append(result.Coverage.Reasons, "tab-list-unavailable")
	}
	if hubJobsRejected {
		result.Coverage.Reasons = append(result.Coverage.Reasons, "hub-jobs-unavailable")
	}
	// A rejected or unlisted input is an uncounted gap on top of the per-node
	// tally — the census can only claim "ok" when every observation channel
	// answered.
	if result.Coverage.Observed == result.Coverage.Expected && nodesRejected == "" && !hubJobsRejected && jobsOK &&
		(!localOK || (view.paneListOK && view.tabsOK)) {
		result.Outcome = fleetCensusOutcomeOK
	} else {
		result.Outcome = fleetCensusOutcomePartial
	}
	return result
}

func renderFleetCensusResult(writer io.Writer, result fleetCensusResult, jsonOut bool) {
	if jsonOut {
		encoded, _ := json.Marshal(result)
		fmt.Fprintln(writer, string(encoded))
		return
	}
	fmt.Fprintf(writer, "fetched_at\t%s\n", result.FetchedAt)
	fmt.Fprintf(writer, "machine\t%s\n", result.Machine)
	fmt.Fprintf(writer, "outcome\t%s\n", result.Outcome)
	coverage := result.Coverage
	fmt.Fprintf(writer, "coverage\tscope=%s\texpected=%d\tobserved=%d\tmissing=%d\tstale=%d\ttruncated=%d\tinvalid=%d\tunavailable=%d\n",
		coverage.Scope, coverage.Expected, coverage.Observed, coverage.Missing, coverage.Stale, coverage.Truncated, coverage.Invalid, coverage.Unavailable)
	for _, node := range coverage.Nodes {
		state := node.State
		if state == "" {
			state = "-"
		}
		reason := node.Reason
		if reason == "" {
			reason = "-"
		}
		sessions := "-"
		if node.Sessions != nil {
			sessions = fmt.Sprint(*node.Sessions)
		}
		lastSeen := node.LastSeen
		if lastSeen == "" {
			lastSeen = "-"
		}
		fmt.Fprintf(writer, "node\t%s\tsource=%s\tcovered=%t\tstate=%s\treason=%s\tsessions=%s\tlast_seen=%s\n",
			node.Machine, node.Source, node.Covered, state, reason, sessions, lastSeen)
	}
	fmt.Fprintf(writer, "panes\t%d\n", len(result.Panes))
	for _, pane := range result.Panes {
		field := func(value string) string {
			if value == "" {
				return "-"
			}
			return value
		}
		age := "-"
		if pane.AgeSeconds != nil {
			age = fmt.Sprintf("%ds", *pane.AgeSeconds)
		}
		terminal := "-"
		if pane.TerminalKind != "" {
			terminal = pane.TerminalKind + "@" + pane.TerminalAt
		}
		detail := pane.Detail
		if detail == "" {
			detail = "-"
		}
		lanes := "-"
		if len(pane.Lanes) > 0 {
			lanes = strings.Join(pane.Lanes, ",")
		}
		fmt.Fprintf(writer, "pane\t%s\t%s\tagent=%s\tharness=%s\tstatus=%s\tjob=%s\tlane=%s\trole=%s\tterminal=%s\tage=%s\tclass=%s\tdetail=%s\tlanes=%s\n",
			pane.Machine, pane.PaneID, field(pane.AgentName), field(pane.Harness), field(pane.Status), field(pane.JobID), field(pane.OwnerLane), field(pane.Role), terminal, age, pane.Class, detail, lanes)
	}
	fmt.Fprintf(writer, "jobs\t%d\n", len(result.Jobs))
	for _, job := range result.Jobs {
		reason := job.Reason
		if reason == "" {
			reason = "-"
		}
		fmt.Fprintf(writer, "job\t%s\tpane=%s\tlane=%s\tverdict=%s\treason=%s\n",
			job.JobID, orDash(job.PaneID), orDash(job.OwnerLane), job.Verdict, reason)
	}
	for _, lane := range result.Lanes {
		reason := lane.Reason
		if reason == "" {
			reason = "-"
		}
		fmt.Fprintf(writer, "lane\t%s\t%s\t%s\tverdict=%s\treason=%s\n", lane.Lane, lane.Machine, lane.Pane, lane.Verdict, reason)
	}
	for _, issue := range result.LaneIssues {
		fmt.Fprintf(writer, "lane_issue\t%s\t%s\t%s\tlanes=%s\n", issue.Kind, issue.Machine, issue.Pane, strings.Join(issue.Lanes, ","))
	}
	summary := result.Summary
	fmt.Fprintf(writer, "summary\tpanes=%d\treapable=%d\tbuilder=%d\tno_terminal_event=%d\tterminal_held=%d\tno_job_record=%d\tunobservable=%d\n",
		summary.Panes, summary.Reapable, summary.Builder, summary.NoTerminalEvent, summary.TerminalHeld, summary.NoJobRecord, summary.Unobservable)
}

func fleetCensusDefaultJobsRoot() string {
	if root := os.Getenv("ARBITER_INBOX_ROOT"); root != "" {
		return root
	}
	home, _ := os.UserHomeDir()
	return filepath.Join(home, fleetCensusDefaultJobsRel)
}

// fleetCensusMachineID resolves the local machine label: --machine-id wins,
// then --node-env (or its fleet-conventional default path) HUB_MACHINE_ID,
// then the hostname. The id only labels rows and matches hub nodes — it is
// never sent anywhere.
func fleetCensusMachineID(options fleetCensusOptions) string {
	if options.machineID != "" {
		return options.machineID
	}
	nodeEnv := options.nodeEnv
	if nodeEnv == "" {
		if home, err := os.UserHomeDir(); err == nil {
			candidate := filepath.Join(home, fleetCensusDefaultNodeEnv)
			if info, statErr := os.Lstat(candidate); statErr == nil && info.Mode().IsRegular() {
				nodeEnv = candidate
			}
		}
	}
	if nodeEnv != "" {
		if values, err := loadMode0600Env(nodeEnv); err == nil {
			if id := values["HUB_MACHINE_ID"]; machineIDPattern.MatchString(id) {
				return id
			}
		}
	}
	if hostname, err := os.Hostname(); err == nil && hostname != "" {
		return hostname
	}
	return "local"
}

// runFleetCensusCLI is read-only by construction: it calls exactly three herdr
// read methods (agent.list, pane.list, tab.list), issues only GETs to the hub,
// and walks the jobs inbox without opening a single file for writing.
func runFleetCensusCLI(args []string, stdout, stderr io.Writer, deps hubCLIDeps) int {
	options, err := parseFleetCensusArgs(args)
	if err != nil {
		return ExitUsage
	}
	now := time.Now
	if deps.Now != nil {
		now = deps.Now
	}
	machineID := fleetCensusMachineID(options)
	jobsRoot := options.jobsRoot
	if jobsRoot == "" {
		jobsRoot = fleetCensusDefaultJobsRoot()
	}
	result := fleetCensusResult{FetchedAt: now().UTC().Format(time.RFC3339), Machine: machineID, Panes: []fleetCensusPaneRow{}, Jobs: []fleetCensusJobRow{}}
	fail := func(code int, reason string) int {
		result.Outcome = fleetCensusOutcomeError
		result.Coverage.Reasons = []string{reason}
		renderFleetCensusResult(stdout, result, options.jsonOut)
		fmt.Fprintln(stderr, "fleet-census failed:", reason)
		return code
	}

	scans, jobsOK := scanFleetCensusJobs(jobsRoot)

	socket := options.herdrSocket
	if socket == "" {
		home, _ := os.UserHomeDir()
		socket = filepath.Join(home, fleetCensusDefaultHerdrSock)
	}
	var view fleetCensusLocalView
	localOK := false
	if client, err := NewHerdrClient(socket); err == nil {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		localView, err := readFleetCensusLocal(ctx, client)
		cancel()
		_ = client.Close()
		if err == nil {
			view = localView
			localOK = true
		}
	}

	var nodes []sessionsFindNodeWire
	var hubJobs []hubConsoleJob
	var lanes []hubLaneProjection
	nodesRejected := ""
	hubJobsRejected := false
	if options.hubURL != "" {
		env, err := loadHubTokenEnv(options.hubTokenEnv)
		if err != nil || env.MachineID != hubOperatorMachineID {
			return fail(ExitConditionInvalid, "invalid_operator_token_env")
		}
		var cfAccess hubCFAccessEnv
		if options.hubCFEnv != "" {
			cfAccess, err = loadHubCFAccessEnv(options.hubCFEnv)
			if err != nil {
				return fail(ExitConditionInvalid, "invalid_cf_env")
			}
		}
		client, err := newHubOperatorClient(options.hubURL, env.Token, cfAccess, deps, lanesCLIRequestTimeout)
		if err != nil {
			return fail(ExitConditionInvalid, "invalid_hub_url")
		}
		ctx, cancel := context.WithTimeout(context.Background(), lanesCLIRequestTimeout)
		defer cancel()
		lanesResponse, err := client.do(ctx, http.MethodGet, "/v1/lanes", nil, nil)
		if err != nil {
			return fail(ExitInternal, "hub_unreachable")
		}
		lanesRaw, lanesErr := readLanesAuditResponse(lanesResponse.Body)
		lanesResponse.Body.Close()
		if lanesErr == nil {
			var lanesBody lanesAuditLanesWire
			if json.Unmarshal(lanesRaw, &lanesBody) == nil {
				lanes = lanesBody.Lanes
			} else {
				lanesErr = errors.New("lanes_decode_failed")
			}
		}
		if lanesErr != nil {
			return fail(ExitInternal, "lanes_body_rejected")
		}
		jobsResponse, err := client.do(ctx, http.MethodGet, "/v1/jobs", nil, nil)
		if err != nil {
			return fail(ExitInternal, "hub_unreachable")
		}
		jobsRaw, jobsErr := readLanesAuditResponse(jobsResponse.Body)
		jobsResponse.Body.Close()
		if jobsErr == nil {
			var jobsBody fleetCensusHubJobsWire
			if json.Unmarshal(jobsRaw, &jobsBody) == nil {
				hubJobs = jobsBody.Jobs
			} else {
				hubJobsRejected = true
			}
		} else {
			hubJobsRejected = true
		}
		nodesResponse, err := client.do(ctx, http.MethodGet, "/v1/nodes", nil, nil)
		if err != nil {
			return fail(ExitInternal, "hub_unreachable")
		}
		nodesRaw, nodesErr := readLanesAuditResponse(nodesResponse.Body)
		nodesResponse.Body.Close()
		var nodesBody struct {
			Nodes []sessionsFindNodeWire `json:"nodes"`
		}
		if nodesErr == nil && json.Unmarshal(nodesRaw, &nodesBody) != nil {
			nodesErr = errors.New("nodes body rejected")
		}
		if nodesErr != nil {
			nodesRejected = lanesAuditReasonNodesResponseBad
			if errors.Is(nodesErr, errLanesAuditResponseOversize) {
				nodesRejected = lanesAuditReasonNodesResponseBig
			}
		} else {
			nodes = nodesBody.Nodes
		}
		if hubJobsRejected {
			// The hub job registry was unreadable, so remote panes cannot be
			// matched to jobs at all — they stay unobservable rather than
			// collapsing into no-job-record.
			hubJobs = nil
		}
	}

	result = buildFleetCensusResult(options, machineID, view, localOK, jobsOK, scans, nodes, hubJobs, lanes, nodesRejected, hubJobsRejected, now())
	renderFleetCensusResult(stdout, result, options.jsonOut)
	if result.Outcome == fleetCensusOutcomePartial {
		return ExitPartial
	}
	return ExitOK
}
