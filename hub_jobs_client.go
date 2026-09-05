package panewire

import (
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

type hubScannedJob struct {
	job       HubActiveJob
	lastEvent time.Time
}

// scanHubActiveJobs reads only structured local event metadata. It is capped
// before it reaches a wire payload; brief text is never copied.
func scanHubActiveJobs(inboxRoot string) []HubActiveJob {
	if inboxRoot == "" {
		return nil
	}
	entries, err := os.ReadDir(filepath.Join(inboxRoot, "jobs"))
	if err != nil {
		return nil
	}
	jobs := make([]hubScannedJob, 0, len(entries))
	for _, entry := range entries {
		if !entry.IsDir() || !hubJobIDPattern.MatchString(entry.Name()) {
			continue
		}
		if scanned, ok := scanHubJobEventDetails(filepath.Join(inboxRoot, "jobs", entry.Name(), "events"), entry.Name()); ok {
			jobs = append(jobs, scanned)
		}
	}
	// Do not let old lexically early inbox directories hide recent work.
	sort.Slice(jobs, func(i, j int) bool { return jobs[i].lastEvent.After(jobs[j].lastEvent) })
	if len(jobs) > 32 {
		jobs = jobs[:32]
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

type hubScannedRelayEvent struct {
	Kind string
	HubActiveJob
	Reason   string
	Question string
	PR       string
	Head     string
	PaneID   string
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
func scanHubRelayEventsWithin(inboxRoot string, maxAge time.Duration) []hubScannedRelayEvent {
	if inboxRoot == "" {
		return nil
	}
	var mtimeCutoff time.Time
	if maxAge > 0 {
		mtimeCutoff = time.Now().Add(-maxAge)
	}
	entries, err := os.ReadDir(filepath.Join(inboxRoot, "jobs"))
	if err != nil {
		return nil
	}
	var events []hubScannedRelayEvent
	for _, entry := range entries {
		if !entry.IsDir() || !hubJobIDPattern.MatchString(entry.Name()) {
			continue
		}
		dir := filepath.Join(inboxRoot, "jobs", entry.Name(), "events")
		files, err := os.ReadDir(dir)
		if err != nil {
			continue
		}
		sort.Slice(files, func(i, j int) bool { return files[i].Name() < files[j].Name() })
		// The claim record is the node-local source of a job's agent label. It
		// is carried onto the terminal record so the hub can late-register a job
		// that finished before any heartbeat advertised it (R19e). Files are in
		// sequence order, so the claim is seen before its own completion.
		var claimLabel string
		for _, file := range files {
			if file.IsDir() || !strings.HasSuffix(file.Name(), ".json") {
				continue
			}
			if !mtimeCutoff.IsZero() {
				info, statErr := file.Info()
				if statErr != nil || info.ModTime().Before(mtimeCutoff) {
					continue
				}
			}
			contents, err := os.ReadFile(filepath.Join(dir, file.Name()))
			if err != nil || len(contents) > 16<<10 {
				continue
			}
			var event hubInboxEvent
			if json.Unmarshal(contents, &event) != nil || event.eventTime(file, dir).Before(time.Now().Add(-hubJobActiveMaxAge())) {
				continue
			}
			kind := event.eventKind()
			if kind == "job.claimed" || kind == "job.claim" {
				if label := event.agentLabel(); hubAgentLabelPattern.MatchString(label) {
					claimLabel = label
				}
				continue
			}
			if kind != "job.completed" && kind != "job.completion" && kind != "job.escalate" && kind != "job.joined" {
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
			if (kind == "job.escalate" || kind == "job.joined") && event.reason() == "" {
				continue
			}
			if kind == "job.completion" {
				kind = "job.completed"
			}
			reportPath := event.reportPath()
			if kind == "job.escalate" && reportPath == "" {
				// The event is the durable full-question record when no separate
				// report exists. The hub payload must point operators back to it.
				reportPath = filepath.Join(dir, file.Name())
			}
			events = append(events, hubScannedRelayEvent{Kind: kind, HubActiveJob: HubActiveJob{JobID: entry.Name(), Epoch: epoch, AgentLabel: agentLabel, OwnerLane: event.ownerLane(), Label: event.label(), Host: event.host(), ReportPath: reportPath, ReportLastLine: event.reportLastLine()}, Reason: event.reason(), Question: event.question(), PR: event.pr(), Head: event.head(), PaneID: event.paneID()})
		}
	}
	sort.Slice(events, func(i, j int) bool {
		if events[i].JobID == events[j].JobID {
			return events[i].Kind < events[j].Kind
		}
		return events[i].JobID < events[j].JobID
	})
	return events
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
// retaining the older flat form emitted by existing wrk done installations.
type hubInboxEvent struct {
	Type           string `json:"type"`
	Kind           string `json:"kind"`
	Event          string `json:"event"`
	CreatedAt      string `json:"created_at"`
	AgentLabel     string `json:"agent_label"`
	PushSHA        string `json:"push_sha"`
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
	Payload        struct {
		AgentLabel     string `json:"agent_label"`
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
	} `json:"payload"`
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
func (e hubInboxEvent) eventTime(entry os.DirEntry, eventsDir string) time.Time {
	if parsed, err := time.Parse(time.RFC3339, e.CreatedAt); err == nil {
		return parsed
	}
	if info, err := entry.Info(); err == nil {
		return info.ModTime()
	}
	return time.Time{}
}

func scanHubJobEventDetails(eventsDir, jobID string) (hubScannedJob, bool) {
	entries, err := os.ReadDir(eventsDir)
	if err != nil {
		return hubScannedJob{}, false
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].Name() < entries[j].Name() })
	var active HubActiveJob
	var claimTime, lastEvent time.Time
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
		eventTime := event.eventTime(entry, eventsDir)
		if eventTime.After(lastEvent) {
			lastEvent = eventTime
		}
		seq, seqOK := hubEventSequence(entry.Name())
		switch event.eventKind() {
		case "job.claimed", "job.claim":
			epoch := event.Epoch
			if epoch == 0 {
				epoch = 1
			}
			candidate := HubActiveJob{JobID: jobID, AgentLabel: event.agentLabel(), LastEventSeq: seq, PushSHA: event.PushSHA, Epoch: epoch}
			if seqOK && validHubActiveJob(candidate) {
				active, claimTime = candidate, eventTime
			}
		case "job.completed", "job.completion", "job.revoked":
			active, claimTime = HubActiveJob{}, time.Time{}
		}
	}
	if !validHubActiveJob(active) || claimTime.Before(time.Now().Add(-hubJobActiveMaxAge())) {
		return hubScannedJob{}, false
	}
	return hubScannedJob{job: active, lastEvent: lastEvent}, true
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
	temporary, err := os.CreateTemp(eventsDir, ".revocation-*")
	if err != nil {
		return err
	}
	name := temporary.Name()
	defer os.Remove(name)
	if err := temporary.Chmod(0600); err != nil {
		_ = temporary.Close()
		return err
	}
	if _, err := temporary.Write(contents); err != nil {
		_ = temporary.Close()
		return err
	}
	if err := temporary.Close(); err != nil {
		return err
	}
	return os.Rename(name, path)
}
