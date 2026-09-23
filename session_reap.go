package panewire

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"
)

// session-reap (#603) stage 1 is report-only. A node judges which of its
// local agent panes a stage-2 closer could one day close, and reports that
// judgment to the hub, where the console shows it as cleanup candidates. No
// function in this file or in hub_session_reap.go closes, kills, or writes to
// a pane or tab: the only herdr calls are the agent.list, pane.list, and
// tab.list reads in readFleetCensusLocal.
//
// Judgment rules (hk:doc task/2026-09-23/session-reap-by-creator):
//
//   - A live agent pane that no job's newest spawn receipt names is a human
//     session. It is never a row, only a count.
//   - The job <-> pane key is an exact match on both pane_id AND label: the
//     spawn receipt's pane_id must equal the agent.list pane_id, and its label
//     must equal the agent.list name byte for byte. The claim's agent_label
//     must agree with the receipt. Any unknown or different value holds the
//     pane (#595 R3: a name-only match closed a working session whose name
//     was empty).
//   - A protected job (`wrk spawn --keep`, sticky on any claim or receipt)
//     and a claim role outside wrk's worker|builder|captain vocabulary are
//     held.
//   - A worker (testers are workers) is a candidate only when the wrk reap
//     pipeline would close it: fleetCensusJobVerdict, reused unchanged.
//   - A builder is never a candidate here. When its pane passes the same pane
//     gates it becomes builder-task-gate, and only the console, which owns
//     task state, may promote it once the linked task is merged or dropped.
//
// Every uncertainty resolves to "not a candidate".
const (
	sessionReapClassCandidate       = "candidate"
	sessionReapClassBuilderTaskGate = "builder-task-gate"
	sessionReapClassHeld            = "held"
)

const (
	sessionReapReasonLabelUnknown  = "label-unknown"
	sessionReapReasonLabelMismatch = "label-mismatch"
	sessionReapReasonLabelConflict = "label-conflict"
	sessionReapReasonLabelReused   = "label-reused"
	sessionReapReasonPaneAmbiguous = "pane-ambiguous"
	sessionReapReasonProtectedRole = "protected-role"
	// An event file of the job could not be read, so a revive may be hidden.
	sessionReapReasonRecordUnreadable = "record-unreadable"
	// A held reason that the wire grammar cannot carry (a wrk reason quoting
	// an unusual tab id) is replaced, never dropped.
	sessionReapReasonUnrepresentable = "reason-unrepresentable"
)

const (
	sessionReapSchema = 1
	// sessionReapMaxRows bounds both the node payload and the hub copy. The
	// byte budget in marshalSessionReapReport is still authoritative.
	sessionReapMaxRows = 64
	// sessionReapMinInterval keeps a misconfigured interval from turning the
	// report into a hot loop over the whole jobs inbox.
	sessionReapMinInterval = time.Minute
	sessionReapReadTimeout = 10 * time.Second

	sessionReapIntervalEnv = "PANEWIRE_SESSION_REAP_REPORT_INTERVAL"
	sessionReapGraceEnv    = "PANEWIRE_SESSION_REAP_GRACE"
	sessionReapEventKind   = "session.reap.report"
)

// SessionReapRow is one job-linked live agent pane. It is metadata only: no
// brief, transcript, cwd, or terminal text.
type SessionReapRow struct {
	PaneID             string `json:"pane_id"`
	TabID              string `json:"tab_id,omitempty"`
	WorkspaceID        string `json:"workspace_id,omitempty"`
	AgentName          string `json:"agent_name,omitempty"`
	Status             string `json:"status"`
	JobID              string `json:"job_id"`
	OwnerLane          string `json:"owner_lane,omitempty"`
	Role               string `json:"role,omitempty"`
	Class              string `json:"class"`
	Reason             string `json:"reason,omitempty"`
	TerminalKind       string `json:"terminal_kind,omitempty"`
	TerminalAt         string `json:"terminal_at,omitempty"`
	TerminalAgeSeconds *int64 `json:"terminal_age_seconds,omitempty"`
}

type SessionReapSummary struct {
	Panes           int `json:"panes"`
	NoJob           int `json:"no_job"`
	Candidate       int `json:"candidate"`
	BuilderTaskGate int `json:"builder_task_gate"`
	Held            int `json:"held"`
}

// SessionReapReport is the node -> hub payload of the session.reap.report
// event. Observed=false or JobsReadable=false always comes with zero rows.
type SessionReapReport struct {
	Schema       int                `json:"schema"`
	GeneratedAt  string             `json:"generated_at"`
	GraceSeconds int64              `json:"grace_seconds"`
	Observed     bool               `json:"observed"`
	JobsReadable bool               `json:"jobs_readable"`
	Truncated    bool               `json:"truncated"`
	Rows         []SessionReapRow   `json:"rows"`
	Summary      SessionReapSummary `json:"summary"`
}

// judgeSessionReap applies the #603 rules to one consistent local
// observation. It is pure: the caller supplies the jobs scan, the herdr view,
// the grace, and the clock.
func judgeSessionReap(scans []fleetCensusJobScan, view fleetCensusLocalView, grace time.Duration, now time.Time) ([]SessionReapRow, SessionReapSummary) {
	var summary SessionReapSummary
	rows := []SessionReapRow{}
	panes := make([]string, 0, len(view.agents))
	for pane := range view.agents {
		panes = append(panes, pane)
	}
	sort.Strings(panes)
	for _, pane := range panes {
		agent := view.agents[pane]
		if !validIdleWakePane(pane) {
			// Not representable on the wire, and a pane herdr names this way
			// is not one a closer should trust either.
			continue
		}
		summary.Panes++
		// Only a spawn receipt links a job to a pane. A pane_id carried by a
		// completion or legacy event cannot be paired with a label, so it
		// does not link the pane either.
		var linked []fleetCensusJobScan
		for _, scan := range scans {
			if scan.sawSpawn && !scan.malformed && scan.spawnPane == pane && hubJobIDPattern.MatchString(scan.jobID) {
				linked = append(linked, scan)
			}
		}
		if len(linked) == 0 {
			summary.NoJob++
			continue
		}
		row := sessionReapJudgePane(agent, linked, scans, view, grace, now)
		switch row.Class {
		case sessionReapClassCandidate:
			summary.Candidate++
		case sessionReapClassBuilderTaskGate:
			summary.BuilderTaskGate++
		default:
			summary.Held++
		}
		rows = append(rows, row)
	}
	sort.SliceStable(rows, func(i, j int) bool {
		left, right := sessionReapClassRank(rows[i].Class), sessionReapClassRank(rows[j].Class)
		if left != right {
			return left < right
		}
		return rows[i].PaneID < rows[j].PaneID
	})
	return rows, summary
}

func sessionReapClassRank(class string) int {
	switch class {
	case sessionReapClassCandidate:
		return 0
	case sessionReapClassBuilderTaskGate:
		return 1
	default:
		return 2
	}
}

// sessionReapJudgePane decides one pane that at least one spawn receipt
// names. The gate order is the contract: identity (label, ambiguity,
// conflict, reuse) before protection before role before the reused wrk
// pipeline, so no later gate can see a pane whose identity is unproven.
func sessionReapJudgePane(agent fleetCensusPaneObs, linked, scans []fleetCensusJobScan, view fleetCensusLocalView, grace time.Duration, now time.Time) SessionReapRow {
	held := func(scan fleetCensusJobScan, reason string) SessionReapRow {
		row := sessionReapBaseRow(agent, scan, now)
		row.Class, row.Reason = sessionReapClassHeld, reason
		if !sessionReapReasonPattern.MatchString(reason) {
			row.Reason = sessionReapReasonUnrepresentable
		}
		return row
	}
	newest := linked[0]
	for _, scan := range linked[1:] {
		if scan.spawnAt.After(newest.spawnAt) {
			newest = scan
		}
	}
	name := agent.AgentName
	if name == "" || !validIdleWakeMetadata(name) {
		return held(newest, sessionReapReasonLabelUnknown)
	}
	var matched []fleetCensusJobScan
	for _, scan := range linked {
		if scan.spawnLabel == name {
			matched = append(matched, scan)
		}
	}
	if len(matched) == 0 {
		return held(newest, sessionReapReasonLabelMismatch)
	}
	if len(matched) > 1 {
		return held(newest, sessionReapReasonPaneAmbiguous)
	}
	job := matched[0]
	// Another job's receipt for this pane that is as new or newer means the
	// pane was handed to someone else after this job; the name alone cannot
	// say which one is on it now.
	for _, other := range linked {
		if other.jobID != job.jobID && !other.spawnAt.Before(job.spawnAt) {
			return held(job, sessionReapReasonPaneAmbiguous)
		}
	}
	if job.claimLabel != job.spawnLabel {
		return held(job, sessionReapReasonLabelConflict)
	}
	if job.unreadable {
		return held(job, sessionReapReasonRecordUnreadable)
	}
	// A later claim under the same label that never wrote a spawn receipt may
	// be the session that now occupies this pane (herdr reuses pane ids across
	// restarts). Its receipt is missing, so identity is not provable.
	for _, other := range scans {
		if other.jobID == job.jobID || other.sawSpawn || other.hasTerminal || other.reaped {
			continue
		}
		if other.claimLabel == name && other.hasFirst && other.firstAt.After(job.spawnAt) {
			return held(job, sessionReapReasonLabelReused)
		}
	}
	if job.keep {
		return held(job, fleetCensusReasonProtected)
	}
	// The latest claim role must be in wrk's vocabulary before the sticky
	// builder marking is consulted: a builder later reclaimed as a resident
	// (or with no role) is held, never routed to the builder task gate.
	switch job.claimRole {
	case "worker", "builder", "captain":
	default:
		return held(job, sessionReapReasonProtectedRole)
	}
	switch {
	case job.role == "builder":
		if job.reaped {
			return held(job, fleetCensusReasonReaped)
		}
		verdict, reason := fleetCensusPaneGates(job, view)
		if verdict != fleetCensusVerdictWouldClose {
			return held(job, reason)
		}
		row := sessionReapBaseRow(agent, job, now)
		row.Class = sessionReapClassBuilderTaskGate
		return row
	default:
		verdict, reason, _ := fleetCensusJobVerdict(job, view, grace, now)
		if verdict != fleetCensusVerdictWouldClose {
			return held(job, reason)
		}
		row := sessionReapBaseRow(agent, job, now)
		row.Class = sessionReapClassCandidate
		return row
	}
}

func sessionReapBaseRow(agent fleetCensusPaneObs, scan fleetCensusJobScan, now time.Time) SessionReapRow {
	// Every field is normalized to what the hub decoder accepts: one
	// unrepresentable value must not cost the whole report.
	row := SessionReapRow{
		PaneID: agent.PaneID, TabID: normalizedIdleWakeMetadata(agent.TabID), WorkspaceID: normalizedIdleWakeMetadata(agent.WorkspaceID),
		AgentName: normalizedIdleWakeMetadata(agent.AgentName), Status: strings.TrimSpace(agent.Status),
		JobID: scan.jobID, OwnerLane: scan.ownerLane, Role: scan.claimRole,
	}
	if !validObservedAgentStatus(row.Status) {
		row.Status = "unknown"
	}
	if !hubAgentLabelPattern.MatchString(row.OwnerLane) {
		row.OwnerLane = ""
	}
	if scan.role == "builder" {
		// Builder role is sticky across claims, exactly as wrk reap reads it.
		row.Role = "builder"
	}
	if !hubLastEventKindPattern.MatchString(row.Role) {
		row.Role = ""
	}
	if scan.hasTerminal {
		row.TerminalKind = scan.terminalKind
		row.TerminalAt = scan.terminalAt.UTC().Format(time.RFC3339)
		age := int64(now.Sub(scan.terminalAt).Seconds())
		row.TerminalAgeSeconds = &age
	}
	return row
}

// collectSessionReapReport performs one local observation and judgment. A
// herdr or inbox failure yields an honest empty report, never a partial one.
func collectSessionReapReport(ctx context.Context, socket, jobsRoot string, grace time.Duration, now time.Time) SessionReapReport {
	report := SessionReapReport{Schema: sessionReapSchema, GeneratedAt: now.UTC().Format(time.RFC3339), GraceSeconds: int64(grace / time.Second), Rows: []SessionReapRow{}}
	scans, jobsOK := scanFleetCensusJobs(jobsRoot)
	report.JobsReadable = jobsOK
	var view fleetCensusLocalView
	if client, err := NewHerdrClient(socket); err == nil {
		lookup, cancel := context.WithTimeout(ctx, sessionReapReadTimeout)
		localView, readErr := readFleetCensusLocal(lookup, client)
		cancel()
		_ = client.Close()
		if readErr == nil {
			view = localView
			report.Observed = true
		}
	}
	if !report.Observed || !report.JobsReadable {
		return report
	}
	report.Rows, report.Summary = judgeSessionReap(scans, view, grace, now)
	return report
}

// marshalSessionReapReport bounds the report to the row cap and to the hub's
// message budget, marking the report truncated when rows had to go. Rows are
// ordered candidates first, so truncation drops held rows before candidates.
func marshalSessionReapReport(report SessionReapReport) (json.RawMessage, bool) {
	if len(report.Rows) > sessionReapMaxRows {
		report.Rows = report.Rows[:sessionReapMaxRows]
		report.Truncated = true
	}
	for {
		payload, err := json.Marshal(report)
		if err != nil {
			return nil, false
		}
		envelope, err := json.Marshal(hubClientWireEvent(hubClientEvent{Kind: sessionReapEventKind, Payload: payload}))
		if err == nil && len(envelope) < hubMaxMessageBytes {
			return payload, true
		}
		if len(report.Rows) == 0 {
			return nil, false
		}
		report.Rows = report.Rows[:len(report.Rows)-1]
		report.Truncated = true
	}
}

// EnqueueSessionReapReport hands one report to the connection writer. It is
// optional traffic: a full queue drops it rather than blocking the caller.
func (client *HubClient) EnqueueSessionReapReport(report SessionReapReport) bool {
	payload, ok := marshalSessionReapReport(report)
	if !ok {
		return false
	}
	select {
	case client.events <- hubClientEvent{Kind: sessionReapEventKind, Payload: payload}:
		return true
	default:
		client.warn("hub event queue full; dropping session reap report")
		return false
	}
}

// sessionReapReportInterval reads the opt-in schedule. Unset, empty, "off",
// unparseable, and non-positive values all mean off: the periodic report
// never runs unless an operator sets a valid duration. Positive values below
// sessionReapMinInterval are raised to it.
func sessionReapReportInterval(value string) (time.Duration, bool) {
	value = strings.TrimSpace(value)
	if value == "" || value == "off" {
		return 0, false
	}
	interval, err := time.ParseDuration(value)
	if err != nil || interval <= 0 {
		return 0, false
	}
	if interval < sessionReapMinInterval {
		interval = sessionReapMinInterval
	}
	return interval, true
}

// sessionReapMaxGrace is the largest grace the hub decoder accepts
// (grace_seconds <= 30 days). A node never sends a report the hub would drop.
const sessionReapMaxGrace = 30 * 24 * time.Hour

// sessionReapGrace reads the grace in wrk's duration grammar (600, 600s, 10m,
// 2h, 1d) and falls back to wrk reap's 10m default for anything unparseable,
// negative, or beyond sessionReapMaxGrace.
func sessionReapGrace(value string) time.Duration {
	if strings.TrimSpace(value) == "" {
		return fleetCensusDefaultGrace
	}
	grace, err := parseFleetCensusGrace(value)
	if err != nil || grace < 0 || grace > sessionReapMaxGrace {
		return fleetCensusDefaultGrace
	}
	return grace
}

// startSessionReapReporter launches the periodic report only when the
// interval env var enables it. It returns whether a reporter was started.
func startSessionReapReporter(ctx context.Context, client *HubClient, socket, inboxRoot string, logger *slog.Logger) bool {
	interval, enabled := sessionReapReportInterval(os.Getenv(sessionReapIntervalEnv))
	if !enabled || client == nil || socket == "" || inboxRoot == "" {
		return false
	}
	grace := sessionReapGrace(os.Getenv(sessionReapGraceEnv))
	jobsRoot := filepath.Join(inboxRoot, "jobs")
	if logger != nil {
		logger.Info("session reap report enabled (report only)", "interval", interval.String(), "grace", grace.String())
	}
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				client.EnqueueSessionReapReport(collectSessionReapReport(ctx, socket, jobsRoot, grace, time.Now()))
			}
		}
	}()
	return true
}

type sessionReapCLIOptions struct {
	jsonOut     bool
	grace       time.Duration
	jobsRoot    string
	herdrSocket string
}

func parseSessionReapCLIArgs(args []string) (sessionReapCLIOptions, error) {
	options := sessionReapCLIOptions{grace: fleetCensusDefaultGrace}
	seen := make(map[string]bool)
	for index := 0; index < len(args); index++ {
		name, value, hasValue := strings.Cut(args[index], "=")
		if seen[name] {
			return options, errors.New("duplicate session-reap flag")
		}
		seen[name] = true
		switch name {
		case "--json":
			options.jsonOut = true
			if hasValue {
				parsed, err := strconv.ParseBool(value)
				if err != nil {
					return options, errors.New("invalid session-reap flag value")
				}
				options.jsonOut = parsed
			}
		case "--grace", "--jobs-root", "--herdr-socket":
			if !hasValue {
				if index+1 >= len(args) {
					return options, errors.New("session-reap flag value is required")
				}
				index++
				value = args[index]
			}
			switch name {
			case "--grace":
				grace, err := parseFleetCensusGrace(value)
				if err != nil {
					return options, errors.New("invalid --grace value")
				}
				options.grace = grace
			case "--jobs-root":
				options.jobsRoot = value
			case "--herdr-socket":
				options.herdrSocket = value
			}
		default:
			return options, errors.New("unknown session-reap flag")
		}
	}
	return options, nil
}

// runSessionReapCLI prints the local judgment once. It is read-only and sends
// nothing to the hub: the only schedule is the opt-in node reporter.
func runSessionReapCLI(args []string, stdout, stderr io.Writer, deps hubCLIDeps) int {
	options, err := parseSessionReapCLIArgs(args)
	if err != nil {
		fmt.Fprintln(stderr, "session-reap:", err)
		return ExitUsage
	}
	now := time.Now
	if deps.Now != nil {
		now = deps.Now
	}
	jobsRoot := options.jobsRoot
	if jobsRoot == "" {
		jobsRoot = fleetCensusDefaultJobsRoot()
	}
	socket := options.herdrSocket
	if socket == "" {
		home, _ := os.UserHomeDir()
		socket = filepath.Join(home, fleetCensusDefaultHerdrSock)
	}
	report := collectSessionReapReport(context.Background(), socket, jobsRoot, options.grace, now())
	if options.jsonOut {
		encoder := json.NewEncoder(stdout)
		encoder.SetIndent("", "  ")
		_ = encoder.Encode(report)
	} else {
		fmt.Fprintf(stdout, "session-reap (report only) observed=%t jobs_readable=%t grace=%ds panes=%d no_job=%d candidate=%d builder_task_gate=%d held=%d\n",
			report.Observed, report.JobsReadable, report.GraceSeconds, report.Summary.Panes, report.Summary.NoJob, report.Summary.Candidate, report.Summary.BuilderTaskGate, report.Summary.Held)
		for _, row := range report.Rows {
			reason := row.Reason
			if reason == "" {
				reason = "-"
			}
			fmt.Fprintf(stdout, "%s pane=%s name=%s job=%s role=%s status=%s reason=%s\n", row.Class, row.PaneID, row.AgentName, row.JobID, row.Role, row.Status, reason)
		}
	}
	if !report.Observed || !report.JobsReadable {
		return ExitConditionInvalid
	}
	return 0
}
