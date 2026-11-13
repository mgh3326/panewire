package panewire

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"
)

const defaultHubJobActiveMaxAge = 72 * time.Hour

// hubJobActiveMaxAge bounds local inbox replay. PANEWIRE_JOB_ACTIVE_MAX_AGE
// accepts a Go duration and defaults to 72h when absent or invalid.
func hubJobActiveMaxAge() time.Duration {
	if value, err := time.ParseDuration(os.Getenv("PANEWIRE_JOB_ACTIVE_MAX_AGE")); err == nil && value > 0 {
		return value
	}
	return defaultHubJobActiveMaxAge
}

const defaultHubJobStaleAge = 24 * time.Hour

// hubJobStaleAge is the "silent and pane-absent" threshold for stale jobs
// (#770): a job record that carries no terminal event only counts as active
// while something still vouches for it — a live pane at the node, or a fresh
// LastSeen at the hub. PANEWIRE_JOB_STALE_AGE accepts a Go duration and
// defaults to 24h; it is deliberately shorter than hubJobActiveMaxAge, which
// bounds replay of records that still have a live pane behind them.
func hubJobStaleAge() time.Duration {
	if value, err := time.ParseDuration(os.Getenv("PANEWIRE_JOB_STALE_AGE")); err == nil && value > 0 {
		return value
	}
	return defaultHubJobStaleAge
}

type hubScannedJob struct {
	job       HubActiveJob
	lastEvent time.Time
	// paneID remains the local liveness cross-check. It is also sent as the
	// bounded HubActiveJob pane so the operator console can jump to a runtime
	// pane; briefs and transcripts are still never transported.
	paneID string
}

// panesAliveFunc reports the panes that currently host a live agent. A nil
// hook, or one that fails, leaves the active set untouched.
type panesAliveFunc func(context.Context) (map[string]bool, error)

// hubPanesAliveTimeout bounds the once-per-scan liveness lookup. The scan runs
// inline in the heartbeat, so a stalled herdr must not hold it open.
const hubPanesAliveTimeout = 2 * time.Second

// hubPanesAlive performs the single liveness lookup for one scan. The second
// result is false whenever the answer is unknown, which keeps every job active.
func hubPanesAlive(ctx context.Context, panesAlive panesAliveFunc) (map[string]bool, bool) {
	if panesAlive == nil {
		return nil, false
	}
	if ctx == nil {
		ctx = context.Background()
	}
	lookup, cancel := context.WithTimeout(ctx, hubPanesAliveTimeout)
	defer cancel()
	alive, err := panesAlive(lookup)
	if err != nil {
		return nil, false
	}
	return alive, true
}

// filterActiveJobsByPane drops jobs whose spawn pane no longer hosts an agent.
// Every uncertainty fails open toward "active": an unknown pane, or an
// unavailable liveness source (aliveKnown false), keeps the job. Counting a
// dead job as active only costs this node a placement refusal, while dropping
// a live one invites overplacement onto an already-loaded machine.
func filterActiveJobsByPane(jobs []hubScannedJob, alive map[string]bool, aliveKnown bool) []hubScannedJob {
	if !aliveKnown {
		return jobs
	}
	kept := make([]hubScannedJob, 0, len(jobs))
	for _, scanned := range jobs {
		if scanned.paneID != "" && !alive[scanned.paneID] {
			continue
		}
		kept = append(kept, scanned)
	}
	return kept
}

// scanHubActiveJobs reads only structured local event metadata. It is capped
// before it reaches a wire payload; brief text is never copied.
func scanHubActiveJobs(inboxRoot string) []HubActiveJob {
	return scanHubActiveJobsWithPanes(context.Background(), inboxRoot, nil)
}

// scanHubActiveJobsWithPanes cross-checks the scanned set against the panes
// that still host an agent. A job whose pane is gone left no terminal event
// behind — a killed pane writes nothing — so pane liveness is the only
// remaining truth about it. panesAlive is called at most once per scan.
func scanHubActiveJobsWithPanes(ctx context.Context, inboxRoot string, panesAlive panesAliveFunc) []HubActiveJob {
	return scanHubActiveJobsForLimit(ctx, inboxRoot, panesAlive, 32)
}

func scanHubActiveJobsForLimit(ctx context.Context, inboxRoot string, panesAlive panesAliveFunc, limit int) []HubActiveJob {
	if inboxRoot == "" {
		return nil
	}
	now := time.Now()
	activeCutoff, staleCutoff := now.Add(-hubJobActiveMaxAge()), now.Add(-hubJobStaleAge())
	jobs := make([]hubScannedJob, 0, 32)
	err := hubIndexFor(inboxRoot).visitJobs(nil, false, func(job *hubIndexedJob) bool {
		if scanned, ok := job.reduction.scanned(activeCutoff, staleCutoff); ok {
			jobs = append(jobs, scanned)
		}
		return true
	})
	if err != nil {
		return nil
	}
	// Filtering precedes the cap: stale entries must not consume the 32 slots
	// that the still-live jobs need.
	alive, aliveKnown := hubPanesAlive(ctx, panesAlive)
	jobs = filterActiveJobsByPane(jobs, alive, aliveKnown)
	// Do not let old lexically early inbox directories hide recent work.
	sort.Slice(jobs, func(i, j int) bool { return jobs[i].lastEvent.After(jobs[j].lastEvent) })
	if limit > 0 && len(jobs) > limit {
		jobs = jobs[:limit]
	}
	active := make([]HubActiveJob, 0, len(jobs))
	for _, scanned := range jobs {
		active = append(active, scanned.job)
	}
	sort.Slice(active, func(i, j int) bool { return active[i].JobID < active[j].JobID })
	return active
}

// scanHubCompletedJobs is intentionally separate from the active-set scan:
// only an explicit local completion event produces a hub completion message.
// A revocation is terminal locally but is never a completion acknowledgement.
func scanHubCompletedJobs(inboxRoot string) []HubActiveJob {
	if inboxRoot == "" {
		return nil
	}
	entries, err := os.ReadDir(filepath.Join(inboxRoot, "jobs"))
	if err != nil {
		return nil
	}
	completed := make([]HubActiveJob, 0)
	for _, entry := range entries {
		if !entry.IsDir() || !hubJobIDPattern.MatchString(entry.Name()) {
			continue
		}
		if job, ok := scanHubJobCompletion(filepath.Join(inboxRoot, "jobs", entry.Name(), "events"), entry.Name()); ok {
			completed = append(completed, job)
		}
	}
	sort.Slice(completed, func(i, j int) bool { return completed[i].JobID < completed[j].JobID })
	return completed
}

// lifecycleEnvelopeSettle is how long a lifecycle envelope with no flat record
// waits before it is treated as the file-only fallback. lifecycle.go writes the
// envelope and then runs emit, so a scan in between must not deliver it: the
// flat record would follow as a second note in the same pane.
const lifecycleEnvelopeSettle = time.Minute

type hubScannedRelayEvent struct {
	Late          bool
	DeliveryClass string
	NotifyType    string
	Cause         string
	Kind          string
	HubActiveJob
	Reason    string
	Question  string
	PR        string
	Head      string
	PaneID    string
	EventID   string
	EventTime time.Time
	Text      string
	Truncated bool
}

// scanHubRelayEvents retains the completion scan and also forwards the two
// compact escalation records emitted by workers and captains. No event body is
// executed; this only copies bounded metadata into the hub event envelope.
func scanHubRelayEvents(inboxRoot string) []hubScannedRelayEvent {
	return scanHubRelayEventsWithin(inboxRoot, 0)
}

// scanHubRelayEventsWithin additionally bounds the scan by event-file mtime.
// The node outbox passes relayOutboxMaxAge so a restart cannot resurrect days
// of retained files; a zero maxAge keeps the historical unbounded behavior.
// It reads through the inbox index (docs/job-event-index.md): only job
// directories that changed since the last pass are read from disk.
func scanHubRelayEventsWithin(inboxRoot string, maxAge time.Duration) []hubScannedRelayEvent {
	if inboxRoot == "" {
		return nil
	}
	now := time.Now()
	var mtimeCutoff time.Time
	if maxAge > 0 {
		mtimeCutoff = now.Add(-maxAge)
	}
	activeCutoff := now.Add(-hubJobActiveMaxAge())
	ix := hubIndexFor(inboxRoot)
	var events []hubScannedRelayEvent
	err := ix.visitJobs(nil, false, func(job *hubIndexedJob) bool {
		events = appendHubJobRelayEvents(events, ix.jobsDir, job, now, mtimeCutoff, activeCutoff)
		return true
	})
	if err != nil {
		return scanHubLaneEventsWithin(inboxRoot, maxAge)
	}
	sort.Slice(events, func(i, j int) bool {
		if events[i].JobID == events[j].JobID {
			return events[i].Kind < events[j].Kind
		}
		return events[i].JobID < events[j].JobID
	})
	return append(events, scanHubLaneEventsWithin(inboxRoot, maxAge)...)
}

// appendHubJobRelayEvents appends the relay events one job's records yield.
func appendHubJobRelayEvents(events []hubScannedRelayEvent, jobsDir string, job *hubIndexedJob, now, mtimeCutoff, activeCutoff time.Time) []hubScannedRelayEvent {
	// The claim record is the node-local source of a job's agent label. It
	// is carried onto the terminal record so the hub can late-register a job
	// that finished before any heartbeat advertised it (R19e). Files are in
	// sequence order, so the claim is seen before its own completion.
	var claimLabel string
	for i := range job.events {
		indexed := &job.events[i]
		if !mtimeCutoff.IsZero() && (!indexed.infoOK || indexed.modTime.Before(mtimeCutoff)) {
			continue
		}
		event := &indexed.event
		eventTime := indexed.eventTime
		if eventTime.Before(activeCutoff) {
			continue
		}
		kind := indexed.kind
		if kind == "job.claimed" || kind == "job.claim" {
			if label := event.agentLabel(); hubAgentLabelPattern.MatchString(label) {
				claimLabel = label
			}
			continue
		}
		if kind == "job.completion" {
			kind = "job.completed"
		}
		// The relay kind set is emitRelayKinds — one closed set read by the
		// producer and the scanner alike. lane.event lives in the
		// events-lane namespace and is scanned by its own reader below.
		if !emitRelayKinds[kind] || kind == "lane.event" {
			continue
		}
		agentLabel := event.agentLabel()
		if !hubAgentLabelPattern.MatchString(agentLabel) {
			agentLabel = claimLabel
		}
		epoch := event.Epoch
		if epoch == 0 {
			epoch = 1
		}
		// Every relayed job.* kind except a completion states why. This is
		// also the echo guard: the hub's own revocation marker carries no
		// reason, so a hub→node job.revoked is never relayed back.
		if kind != "job.completed" && event.reason() == "" {
			continue
		}
		// A terminal signal without a routable owner lane can never be
		// delivered — emitting it would only stamp an outbox row the hub's
		// relayJobEventFrom drops unanswered.
		if relayTerminalSignalKinds[kind] && !validLaneReference(event.ownerLane()) && !(kind == "job.lost" && event.reason() == "quota_stop" && event.Payload.Pool != "") {
			continue
		}
		reportPath := relaySignalReportPath(kind, event.reportPath())
		ownerLane := event.ownerLane()
		if kind == "job.completed" && event.isLifecycleEnvelope() {
			// hub.go's R18 contract: "R14 fencing answers \"may this job be
			// redispatched\"; R18 relay answers \"must this report reach its
			// lane\"." The envelope's owner_lane is the job's own lane, which
			// has no route once the job ends, so sending it answers neither
			// and stays unacknowledged until relayOutboxMaxAge drops the file.
			// The flat record is the delivery; the envelope only stands in
			// for it once emit has had time to write one and has not. The
			// claim label above still rides on it, per the R19e comment:
			// "carried onto the terminal record so the hub can late-register
			// a job that finished before any heartbeat advertised it".
			if hubJobHasFlatCompletion(job.events, epoch) || now.Sub(eventTime) < lifecycleEnvelopeSettle {
				continue
			}
			ownerLane, reportPath = event.lifecycleRouteLane(job.id), event.Payload.Report
		}
		if relayEventPathFallbackKinds[kind] && reportPath == "" {
			// The event is the durable full-question record when no separate
			// report exists. The hub payload must point operators back to it,
			// and `panewire emit` substitutes the very same path.
			reportPath = filepath.Join(jobsDir, job.id, "events", indexed.name)
		}
		reason := event.reason()
		if kind == "job.lost" && reason == "quota_stop" && event.Payload.Pool != "" {
			reset := event.Payload.ResetAt
			if reset == "" {
				reset = eventTime.Add(5 * time.Minute).UTC().Format(time.RFC3339Nano)
			}
			reason += " pool=" + event.Payload.Pool + " reset_at=" + reset
		}
		// The event file name is the job event's durable identity: it is
		// unique per emission, stable across node restarts, and shared by
		// `panewire emit`, which derives the same value for its file.
		events = append(events, hubScannedRelayEvent{DeliveryClass: event.DeliveryClass, Late: event.Late || event.Payload.Late, NotifyType: event.notifyType(), Cause: event.Cause, Kind: kind, HubActiveJob: HubActiveJob{JobID: job.id, Epoch: epoch, AgentLabel: agentLabel, OwnerLane: ownerLane, Label: event.label(), Host: event.host(), ReportPath: reportPath, ReportLastLine: event.reportLastLine()}, Reason: reason, Question: event.question(), PR: event.pr(), Head: event.head(), PaneID: event.paneID(), EventID: indexed.name, EventTime: eventTime})
	}
	return events
}

// scanHubLaneEventsWithin reads the isolated lane-event namespace. It never
// traverses jobs/, which keeps direct lane notifications out of active job
// registration, late registration, and orphan sweeping.
func scanHubLaneEventsWithin(inboxRoot string, maxAge time.Duration) []hubScannedRelayEvent {
	return hubIndexFor(inboxRoot).laneEvents(maxAge)
}

// hubJobHasFlatCompletion reports whether a job's event files hold a flat
// job.completed record for this epoch. It reads every event rather than only
// the ones the scan kept: an age cut on the record must not revive its envelope.
func hubJobHasFlatCompletion(events []hubIndexedEvent, epoch uint64) bool {
	for i := range events {
		event := &events[i].event
		if event.OwnerLane == "" {
			continue
		}
		kind, recordEpoch := events[i].kind, event.Epoch
		if recordEpoch == 0 {
			recordEpoch = 1
		}
		if (kind == "job.completed" || kind == "job.completion") && recordEpoch == epoch {
			return true
		}
	}
	return false
}

// laneEventTransportID is protocol plumbing only: lane events never enter the
// hub job map. The relay acknowledgement envelope already carries job_id, so
// this deterministic valid identifier lets its existing delivery cursor work
// without making producer event IDs look like job IDs.
func laneEventTransportID(lane, eventID string) string {
	sum := sha256.Sum256([]byte(lane + "\x00" + eventID))
	return fmt.Sprintf("lane-event-%x", sum[:16])
}

func scanHubJobCompletion(eventsDir, jobID string) (HubActiveJob, bool) {
	entries, err := os.ReadDir(eventsDir)
	if err != nil {
		return HubActiveJob{}, false
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].Name() < entries[j].Name() })
	var completed HubActiveJob
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".json") {
			continue
		}
		contents, err := os.ReadFile(filepath.Join(eventsDir, entry.Name()))
		if err != nil || len(contents) > 16<<10 {
			continue
		}
		var event hubInboxEvent
		if json.Unmarshal(contents, &event) != nil {
			continue
		}
		kind := event.eventKind()
		if (kind == "job.completed" || kind == "job.completion") && !event.eventTime(entry, eventsDir).Before(time.Now().Add(-hubJobActiveMaxAge())) {
			epoch := event.Epoch
			if epoch == 0 {
				epoch = 1
			}
			completed = HubActiveJob{JobID: jobID, Epoch: epoch, OwnerLane: event.ownerLane(), Label: event.label(), Host: event.host(), ReportPath: event.reportPath(), ReportLastLine: event.reportLastLine()}
		}
	}
	return completed, completed.JobID != ""
}

// hubInboxEvent accepts the arbiter envelope as the canonical form while
// retaining the older flat form emitted by existing completion writers.
type hubInboxEvent struct {
	Late           bool   `json:"late,omitempty"`
	DeliveryClass  string `json:"delivery_class,omitempty"`
	NotifyType     string `json:"notify_type,omitempty"`
	Cause          string `json:"cause,omitempty"`
	Type           string `json:"type"`
	Kind           string `json:"kind"`
	Event          string `json:"event"`
	CreatedAt      string `json:"created_at"`
	AgentLabel     string `json:"agent_label"`
	Epoch          uint64 `json:"epoch"`
	OwnerLane      string `json:"owner_lane"`
	Label          string `json:"label"`
	Host           string `json:"host"`
	ReportPath     string `json:"report_path"`
	ReportLastLine string `json:"report_last_line"`
	Reason         string `json:"reason"`
	Question       string `json:"question"`
	PR             string `json:"pr"`
	Head           string `json:"head"`
	PaneID         string `json:"pane_id"`
	Role           string `json:"role"`
	TLevel         string `json:"t_level"`
	EventID        string `json:"event_id"`
	Text           string `json:"text"`
	Truncated      bool   `json:"truncated"`
	Payload        struct {
		Late           bool   `json:"late,omitempty"`
		AgentLabel     string `json:"agent_label"`
		OwnerLane      string `json:"owner_lane"`
		ParentLane     string `json:"parent_lane"`
		Label          string `json:"label"`
		Host           string `json:"host"`
		Report         string `json:"report"`
		ReportPath     string `json:"report_path"`
		ReportLastLine string `json:"report_last_line"`
		Reason         string `json:"reason"`
		Pool           string `json:"pool"`
		ResetAt        string `json:"reset_at"`
		Question       string `json:"question"`
		PR             string `json:"pr"`
		Head           string `json:"head"`
		PaneID         string `json:"pane_id"`
		Role           string `json:"role"`
		TLevel         string `json:"t_level"`
	} `json:"payload"`
}

// isLifecycleEnvelope recognizes the record internal/fleet/lifecycle.go writes
// before it runs `panewire emit`: its lanes sit under payload and its report is
// payload.report. Other envelopes spell it report_path and have no emit twin.
func (e hubInboxEvent) isLifecycleEnvelope() bool {
	return e.OwnerLane == "" && e.Payload.Report != ""
}

// lifecycleRouteLane is the lane lifecycle.go hands emit: the parent for a
// builder or captain, otherwise the owner. Envelopes do not record a role today,
// so without one an owner lane naming the job itself marks the job's own lane.
func (e hubInboxEvent) lifecycleRouteLane(jobID string) string {
	owner, parent := e.ownerLane(), e.Payload.ParentLane
	if parent == "" {
		return owner
	}
	switch e.role() {
	case "builder", "captain":
		return parent
	case "":
		if owner == jobID {
			return parent
		}
	}
	return owner
}

func (e hubInboxEvent) eventKind() string {
	if e.Type != "" {
		return e.Type
	}
	if e.Kind != "" {
		return e.Kind
	}
	return e.Event
}
func firstHubValue(top, nested string) string {
	if top != "" {
		return top
	}
	return nested
}
func (e hubInboxEvent) agentLabel() string { return firstHubValue(e.AgentLabel, e.Payload.AgentLabel) }
func (e hubInboxEvent) ownerLane() string  { return firstHubValue(e.OwnerLane, e.Payload.OwnerLane) }
func (e hubInboxEvent) label() string      { return firstHubValue(e.Label, e.Payload.Label) }
func (e hubInboxEvent) host() string       { return firstHubValue(e.Host, e.Payload.Host) }
func (e hubInboxEvent) reportPath() string { return firstHubValue(e.ReportPath, e.Payload.ReportPath) }
func (e hubInboxEvent) reportLastLine() string {
	return firstHubValue(e.ReportLastLine, e.Payload.ReportLastLine)
}
func (e hubInboxEvent) reason() string   { return firstHubValue(e.Reason, e.Payload.Reason) }
func (e hubInboxEvent) question() string { return firstHubValue(e.Question, e.Payload.Question) }
func (e hubInboxEvent) pr() string       { return firstHubValue(e.PR, e.Payload.PR) }
func (e hubInboxEvent) head() string     { return firstHubValue(e.Head, e.Payload.Head) }
func (e hubInboxEvent) paneID() string   { return firstHubValue(e.PaneID, e.Payload.PaneID) }
func (e hubInboxEvent) role() string     { return firstHubValue(e.Role, e.Payload.Role) }
func (e hubInboxEvent) tLevel() string   { return firstHubValue(e.TLevel, e.Payload.TLevel) }
func (e hubInboxEvent) notifyType() string {
	if !validNotifyType(e.NotifyType) {
		return ""
	}
	return e.NotifyType
}
func (e hubInboxEvent) eventID() string { return e.EventID }
func (e hubInboxEvent) text() string    { return e.Text }
func (e hubInboxEvent) truncated() bool { return e.Truncated }
func (e hubInboxEvent) eventTime(entry os.DirEntry, eventsDir string) time.Time {
	info, err := entry.Info()
	if err != nil {
		return e.eventTimeAt(time.Time{}, false)
	}
	return e.eventTimeAt(info.ModTime(), true)
}

// eventTimeAt is the event's time: its created_at when that parses, else the
// file's mtime when it is known.
func (e hubInboxEvent) eventTimeAt(modTime time.Time, modTimeOK bool) time.Time {
	if parsed, err := time.Parse(time.RFC3339, e.CreatedAt); err == nil {
		return parsed
	}
	if modTimeOK {
		return modTime
	}
	return time.Time{}
}

// hubJobEndKind reports whether a job event kind ends the job's run. Besides
// the completion and the revocation, panewire writes these when a job ends
// without a completion: the quota release closes every run (done, failed
// spawn, reap, lost pane), and a lost or reaped job has no pane left.
func hubJobEndKind(kind string) bool {
	switch kind {
	case "job.completed", "job.completion", "job.revoked", "quota_pool.release", "quota_pool.release.force", "job.lost", "job.reaped":
		return true
	}
	return false
}

// scanHubJobEventDetails reads one job's events directory afresh, without the
// index. The daemon's scans go through hubInboxIndex; this stays the
// single-directory reader.
func scanHubJobEventDetails(eventsDir, jobID string) (hubScannedJob, bool) {
	load, ok := loadHubJobEvents(eventsDir)
	if !ok {
		return hubScannedJob{}, false
	}
	now := time.Now()
	return reduceHubJobEvents(jobID, load.events).scanned(now.Add(-hubJobActiveMaxAge()), now.Add(-hubJobStaleAge()))
}

func scanHubJobEvents(eventsDir, jobID string) (HubActiveJob, bool) {
	scanned, ok := scanHubJobEventDetails(eventsDir, jobID)
	return scanned.job, ok
}

func hubEventSequence(name string) (uint64, bool) {
	prefix, _, found := strings.Cut(name, "-")
	if !found || len(prefix) == 0 || len(prefix) > 20 {
		return 0, false
	}
	value, err := strconv.ParseUint(prefix, 10, 64)
	return value, err == nil
}

func writeHubRevocation(inboxRoot string, revoked hubJobRevokedEvent) error {
	if inboxRoot == "" || !hubJobIDPattern.MatchString(revoked.JobID) || revoked.Epoch == 0 {
		return fmt.Errorf("invalid job revocation")
	}
	eventsDir := filepath.Join(inboxRoot, "jobs", revoked.JobID, "events")
	if err := os.MkdirAll(eventsDir, 0700); err != nil {
		return err
	}
	entries, err := os.ReadDir(eventsDir)
	if err != nil {
		return err
	}
	var highest uint64
	for _, entry := range entries {
		if seq, ok := hubEventSequence(entry.Name()); ok && seq > highest {
			highest = seq
		}
	}
	contents, err := json.Marshal(struct {
		Type  string `json:"type"`
		JobID string `json:"job_id"`
		Epoch uint64 `json:"epoch"`
	}{Type: "job.revoked", JobID: revoked.JobID, Epoch: revoked.Epoch})
	if err != nil {
		return err
	}
	path := filepath.Join(eventsDir, fmt.Sprintf("%05d-job.revoked.json", highest+1))
	return replaceFileAtomically(path, ".revocation-*", contents, 0600)
}
