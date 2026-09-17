package panewire

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"
)

// Stall-detect v1 observes worker panes and records what it sees. It never
// judges a worker stopped, never writes to a pane, and never touches batch or
// quota state. Every human-facing record is a bounded observation with the
// evidence that produced it.

const (
	defaultStallPollInterval    = 60 * time.Second
	defaultStallStartupGrace    = 90 * time.Second
	defaultStallNotifyGrace     = 15 * time.Minute
	defaultStallNotifyRetry     = 5 * time.Minute
	defaultStallReadTimeout     = 3 * time.Second
	defaultStallMaxReads        = 8
	defaultStallSuspendFailures = 5
	stallResubscribeMinInterval = 30 * time.Second
	stallMinScanInterval        = 5 * time.Second
	stallScreenExcerptRunes     = 600
	stallReportMaxBytes         = 1 << 20
	stallMaxProcLookups         = 8
	stallRepsMinSamples         = 3
	stallRepsFloor              = 30 * time.Minute
	stallRepsCap                = 8 * time.Hour
	stallTailAnchorLines        = 3
	stallReportMaxAge           = 72 * time.Hour
	// stallUnreadableAfterReads is the consecutive read-failure streak that
	// turns a pane's unobservability into a recorded gap and a degraded beat.
	stallUnreadableAfterReads = 2
)

// StallDetectConfig gates the whole feature. Notify and ReportUpload stay
// false through the shadow rollout step: shadow means zero outward side
// effects — incidents and their would-be notifications are recorded locally,
// but nothing is emitted to a lane and no report body leaves the node.
type StallDetectConfig struct {
	Enabled              bool
	Notify               bool
	ReportUpload         bool
	Harnesses            []string
	PollInterval         time.Duration
	StartupGrace         time.Duration
	NotifyGrace          time.Duration
	NotifyRetry          time.Duration
	ReadTimeout          time.Duration
	MaxReadsPerCycle     int
	SuspendAfterFailures int
}

func (cfg StallDetectConfig) withDefaults() StallDetectConfig {
	if cfg.PollInterval <= 0 {
		cfg.PollInterval = defaultStallPollInterval
	}
	if cfg.StartupGrace <= 0 {
		cfg.StartupGrace = defaultStallStartupGrace
	}
	if cfg.NotifyGrace <= 0 {
		cfg.NotifyGrace = defaultStallNotifyGrace
	}
	if cfg.NotifyRetry <= 0 {
		cfg.NotifyRetry = defaultStallNotifyRetry
	}
	if cfg.ReadTimeout <= 0 {
		cfg.ReadTimeout = defaultStallReadTimeout
	}
	if cfg.MaxReadsPerCycle <= 0 {
		cfg.MaxReadsPerCycle = defaultStallMaxReads
	}
	if cfg.SuspendAfterFailures <= 0 {
		cfg.SuspendAfterFailures = defaultStallSuspendFailures
	}
	return cfg
}

func (cfg StallDetectConfig) harnessAllowed(family string) bool {
	if len(cfg.Harnesses) == 0 {
		return true
	}
	for _, allowed := range cfg.Harnesses {
		if strings.EqualFold(allowed, family) {
			return true
		}
	}
	return false
}

// stallReader is the only herdr handle the detector may hold. The interface
// cannot express input — prompt submission, key injection, and the generic
// request method are simply not members — so no wiring can hand them in and
// no seam can smuggle them through.
type stallReader interface {
	AgentDetails(context.Context) ([]paneIdentity, error)
	ReadPane(context.Context, string) (readEvidence, error)
	Close() error
}

// stallDeps are the side-effecting seams. Production wiring dials herdr per
// call the way the heartbeat hooks do; fixtures replace every one of them.
type stallDeps struct {
	listAgents    func(context.Context) ([]paneIdentity, error)
	readPane      func(context.Context, string) (readEvidence, error)
	subscribed    func() (map[string]bool, time.Time)
	resubscribe   func()
	worktree      func(context.Context, string) string
	procs         func(context.Context) ([]stallProc, error)
	procCWD       func(context.Context, int64) (string, error)
	writeRecord   func(string, emitRecord) (string, error)
	enqueue       func(hubScannedRelayEvent) bool
	upload        func(context.Context, string, []byte) error
	lanePersisted func(context.Context, string, string) (bool, error)
	readDisk      func(string) ([]byte, error)
	now           func() time.Time
}

type stallDetectManager struct {
	cfg       StallDetectConfig
	store     *Store
	inboxRoot string
	reportDir string
	deps      stallDeps
	logger    *slog.Logger

	wake          chan string
	wakePending   map[string]struct{}
	readFailures  int
	suspendedTill time.Time
	lastResub     time.Time
	lastBeat      time.Time
	lastScanAt    time.Time
	trackedPanes  int
	procCWDCache  map[int64]string
	// coverageUnknownSince marks when the proven-subscription set first went
	// missing while tracked panes were live. A gap has to persist before it
	// becomes [unobserved] — a subscribe in flight is not a coverage proof
	// either way.
	coverageUnknownSince time.Time
	// unsubStreak and cleanReads are per-pane volatile counters for the
	// two-read confirmations (queued-input banner stable, clean-read
	// recovery). They reset on daemon restart, which only delays the
	// observation by one poll.
	unsubStreak map[string]int
	cleanReads  map[string]int
	// paneReadStreak counts consecutive read failures per pane. A listing
	// that succeeds while one pane can never be read is partial blindness —
	// the same class as a failed agent.list — so the streak surfaces as an
	// incident and a degraded beat instead of passing for healthy coverage.
	paneReadStreak map[string]int
	// scanCache skips re-parsing an unchanged job event journal. Directory
	// mtime is the change signal — events are append-only files.
	scanCache map[string]stallJobScanCache
}

type stallJobScanCache struct {
	modTime time.Time
	scan    stallJobScan
}

func newStallDetectManager(store *Store, inboxRoot string, cfg StallDetectConfig, deps stallDeps, logger *slog.Logger) (*stallDetectManager, error) {
	if store == nil || inboxRoot == "" {
		return nil, errors.New("stall-detect configuration is incomplete")
	}
	if logger == nil {
		logger = slog.Default()
	}
	cfg = cfg.withDefaults()
	if deps.now == nil {
		deps.now = time.Now
	}
	if deps.writeRecord == nil {
		deps.writeRecord = ensureLaneEmitRecord
	}
	if deps.worktree == nil {
		deps.worktree = stallGitToplevel
	}
	if deps.procs == nil {
		deps.procs = stallHarnessProcs
	}
	if deps.procCWD == nil {
		deps.procCWD = stallProcCWD
	}
	if deps.upload == nil {
		deps.upload = stallHandoffkeepUpload
	}
	if deps.lanePersisted == nil {
		deps.lanePersisted = store.relayLanePersisted
	}
	if deps.readDisk == nil {
		deps.readDisk = os.ReadFile
	}
	return &stallDetectManager{
		cfg: cfg, store: store, inboxRoot: inboxRoot,
		reportDir: filepath.Join(inboxRoot, "jobs"),
		deps:      deps, logger: logger,
		wake: make(chan string, 64), wakePending: make(map[string]struct{}),
		procCWDCache: make(map[int64]string),
		unsubStreak:  make(map[string]int), cleanReads: make(map[string]int),
		paneReadStreak: make(map[string]int),
		scanCache:      make(map[string]stallJobScanCache),
	}, nil
}

// Wake is the output_matched half of the hybrid trigger. The event carries no
// text, so it only marks the pane due for a bounded read.
func (m *stallDetectManager) Wake(paneID string) {
	if paneID == "" {
		return
	}
	select {
	case m.wake <- paneID:
	default:
	}
}

// StallBeat is the node half of the hub no-data contract. The pane count is
// nullable on purpose: nil means the scan could not observe (agent.list
// failed or has not run yet), which must surface as degraded on the hub — a
// placeholder zero would disguise blindness as healthy emptiness.
func (m *stallDetectManager) StallBeat() *hubStallBeatPayload {
	beat, interval, panes, degraded, err := m.store.stallBeat(context.Background())
	if err != nil || beat.IsZero() {
		if !m.lastBeat.IsZero() {
			return m.beatPayload(m.lastBeat, m.cfg.PollInterval, m.trackedPanes, true)
		}
		return m.beatPayload(time.Time{}, m.cfg.PollInterval, 0, true)
	}
	return m.beatPayload(beat, interval, panes, degraded)
}

func (m *stallDetectManager) beatPayload(beat time.Time, interval time.Duration, panes int, degraded bool) *hubStallBeatPayload {
	payload := &hubStallBeatPayload{BeatMS: beat.UnixMilli(), IntervalMS: interval.Milliseconds(), Degraded: degraded}
	if !degraded {
		payload.Panes = &panes
	}
	return payload
}

func (m *stallDetectManager) Run(ctx context.Context) {
	// One scan at start so a restarted daemon re-baselines quickly; later
	// scans run on the measured poll cadence or when output_matched wakes us.
	m.scan(ctx)
	ticker := time.NewTicker(m.cfg.PollInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case pane := <-m.wake:
			m.wakePending[pane] = struct{}{}
			// Coalesce a burst of wake events into a single bounded scan,
			// but never more often than the floor: output_matched is a wake
			// hint, not a scan schedule. The pane stays marked due and the
			// next tick takes it.
			drain := time.NewTimer(200 * time.Millisecond)
			select {
			case <-ctx.Done():
				drain.Stop()
				return
			case <-drain.C:
			}
			if now := m.deps.now().UTC(); !m.lastScanAt.IsZero() && now.Sub(m.lastScanAt) < stallMinScanInterval {
				continue
			}
			m.scan(ctx)
		case <-ticker.C:
			m.scan(ctx)
		}
	}
}

// stallJobScan is the per-scan view of one job's local event journal.
type stallJobScan struct {
	JobID          string
	OwnerLane      string
	ParentLane     string
	AgentLabel     string
	Role           string
	Tier           string
	Family         string
	ClaimedAt      time.Time
	IssuerDeadline time.Time
	Spawns         []stallSpawnScan
	Extensions     []stallDeadlineExt
	Terminal       bool
	TerminalKind   string
	// TerminalSeq is the journal sequence of the terminal record. A spawn
	// after it is a fresh attempt, not a continuation of the ended one.
	TerminalSeq    int64
	LastEventAt    time.Time
	ReportPath     string
	// ReportKind names what ReportPath holds: "path" for a local file,
	// "doc_key" for a handoffkeep document key, "" when nothing resolves.
	ReportKind  string
}

type stallSpawnScan struct {
	PaneID      string
	WorkspaceID string
	TabID       string
	Label       string
	Profile     string
	Round       int64
	Seq         int64
	At          time.Time
}

type stallDeadlineExt struct {
	Seq           int64
	IssuerLane    string
	Reason        string
	NewDeadlineAt time.Time
	Applied       bool
}

// stallInboxEvent decodes the fields stall-detect reads that hubInboxEvent
// does not carry. The generic payload map keeps this scanner forward-tolerant:
// unknown payload keys are ignored, never executed.
type stallInboxEvent struct {
	Type       string                     `json:"type"`
	Kind       string                     `json:"kind"`
	Event      string                     `json:"event"`
	CreatedAt  string                     `json:"created_at"`
	Epoch      uint64                     `json:"epoch"`
	ReportPath string                     `json:"report_path"`
	// Report is the wrk-era completion record's report field (worker.complete
	// carries it at the top level, relative to the job directory).
	Report  string                     `json:"report"`
	Payload map[string]json.RawMessage `json:"payload"`
}

func (e stallInboxEvent) eventKind() string {
	if e.Type != "" {
		return e.Type
	}
	if e.Kind != "" {
		return e.Kind
	}
	return e.Event
}

func (e stallInboxEvent) eventTime(name, dir string) time.Time {
	if parsed, err := time.Parse(time.RFC3339, e.CreatedAt); err == nil {
		return parsed.UTC()
	}
	if info, err := os.Stat(filepath.Join(dir, name)); err == nil {
		return info.ModTime().UTC()
	}
	return time.Time{}
}

func stallPayloadString(payload map[string]json.RawMessage, keys ...string) string {
	for _, key := range keys {
		raw, ok := payload[key]
		if !ok {
			continue
		}
		var value string
		if json.Unmarshal(raw, &value) == nil {
			return value
		}
	}
	return ""
}

func stallPayloadInt(payload map[string]json.RawMessage, keys ...string) int64 {
	for _, key := range keys {
		raw, ok := payload[key]
		if !ok {
			continue
		}
		var value int64
		if json.Unmarshal(raw, &value) == nil && value > 0 {
			return value
		}
		var asFloat float64
		if json.Unmarshal(raw, &asFloat) == nil && asFloat > 0 {
			return int64(asFloat)
		}
	}
	return 0
}

func stallPayloadTime(payload map[string]json.RawMessage, keys ...string) time.Time {
	for _, key := range keys {
		raw, ok := payload[key]
		if !ok {
			continue
		}
		var value string
		if json.Unmarshal(raw, &value) != nil {
			continue
		}
		if parsed, err := time.Parse(time.RFC3339, value); err == nil {
			return parsed.UTC()
		}
	}
	return time.Time{}
}

// stallFamily names the harness family used by the shadow filter and by reps
// deadline suggestion. The quota pool name wins because it is the stable
// family (devin for devin-swe2); profile's first dash segment is the fallback.
func stallFamily(pool, profile, harness string) string {
	for _, candidate := range []string{pool, profile, harness} {
		if candidate == "" {
			continue
		}
		family := candidate
		if index := strings.IndexByte(candidate, '-'); index > 0 {
			family = candidate[:index]
		}
		return strings.ToLower(family)
	}
	return ""
}

// stallTerminalKinds is the closed set of journal records that end a job.
// Membership is earned from the real journal, not assumed from the name:
// every kind here records the job's end state, and what little follows it in
// the journal is bookkeeping (reaper marks, quota release, a late duplicate
// completion). The counts from the real journal:
//
//	job.completed  543 events, followers are lost/reaped/dup-completed bookkeeping
//	job.lost       502 events, 52 followed — late completed/reaped records
//	job.reaped      87 events, 51 followed — paired job.lost bookkeeping
//	job.revoked      3 events, followers are quota release only
//	job.aborted      1 event,  followed by quota_pool.release.force only
//	worker.complete  1 event,  nothing follows (wrk-era end record)
//
// Explicitly NOT terminal — the journal proves work continues after them:
//
//	job.escalate   274 events, 242 followed (escalate→joined/completed work)
//	job.joined      88 events,  65 followed (joined→completed/lost/escalate)
//
// An escalation is a question to an upper lane and a join is one outcome
// record inside a live job; either one marked terminal would silence the
// detector on exactly the jobs most likely to stall.
var stallTerminalKinds = map[string]bool{
	"job.completed": true, "job.completion": true, "job.revoked": true,
	"job.lost": true, "job.reaped": true, "job.aborted": true,
	"worker.complete": true,
}

// scanStallJobs reads each job's event journal into the detector's view.
// File contents are metadata only; no event body is ever executed.
func scanStallJobs(inboxRoot string) []stallJobScan {
	entries, err := os.ReadDir(filepath.Join(inboxRoot, "jobs"))
	if err != nil {
		return nil
	}
	var jobs []stallJobScan
	for _, entry := range entries {
		if !entry.IsDir() || !hubJobIDPattern.MatchString(entry.Name()) {
			continue
		}
		if job, ok := scanStallJobEvents(filepath.Join(inboxRoot, "jobs", entry.Name(), "events"), entry.Name()); ok {
			jobs = append(jobs, job)
		}
	}
	return jobs
}

func scanStallJobEvents(eventsDir, jobID string) (stallJobScan, bool) {
	entries, err := os.ReadDir(eventsDir)
	if err != nil {
		return stallJobScan{}, false
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].Name() < entries[j].Name() })
	job := stallJobScan{JobID: jobID}
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".json") {
			continue
		}
		contents, err := os.ReadFile(filepath.Join(eventsDir, entry.Name()))
		if err != nil || len(contents) > 16<<10 {
			continue
		}
		var event stallInboxEvent
		if json.Unmarshal(contents, &event) != nil {
			continue
		}
		at := event.eventTime(entry.Name(), eventsDir)
		if at.After(job.LastEventAt) {
			job.LastEventAt = at
		}
		seq, _ := hubEventSequence(entry.Name())
		switch event.eventKind() {
		case "job.claimed", "job.claim":
			job.OwnerLane = stallPayloadString(event.Payload, "owner_lane")
			job.ParentLane = stallPayloadString(event.Payload, "parent_lane")
			job.AgentLabel = stallPayloadString(event.Payload, "agent_label")
			job.Role = stallPayloadString(event.Payload, "role")
			job.Tier = stallPayloadString(event.Payload, "t_level", "tier")
			job.ClaimedAt = at
			if deadline := stallPayloadTime(event.Payload, "deadline_at", "check_at", "first_check_at"); !deadline.IsZero() {
				job.IssuerDeadline = deadline
			}
		case "quota_pool.record":
			job.Family = stallFamily(stallPayloadString(event.Payload, "pool"), stallPayloadString(event.Payload, "profile", "launch_profile"), "")
		case "job.spawned":
			spawn := stallSpawnScan{
				PaneID:      stallPayloadString(event.Payload, "pane_id", "pane"),
				WorkspaceID: stallPayloadString(event.Payload, "workspace", "workspace_id"),
				TabID:       stallPayloadString(event.Payload, "tab_id"),
				Label:       stallPayloadString(event.Payload, "label"),
				Profile:     stallPayloadString(event.Payload, "profile"),
				Round:       stallPayloadInt(event.Payload, "round"),
				Seq:         int64(seq),
				At:          at,
			}
			if job.Family == "" {
				job.Family = stallFamily("", spawn.Profile, "")
			}
			job.Spawns = append(job.Spawns, spawn)
		case "job.deadline":
			// Issuer-only extension record. The issuer check happens at apply
			// time against the claim's owner_lane; anything else is inert.
			ext := stallDeadlineExt{Seq: int64(seq), IssuerLane: stallPayloadString(event.Payload, "issuer_lane", "owner_lane"), Reason: stallPayloadString(event.Payload, "reason"), NewDeadlineAt: stallPayloadTime(event.Payload, "deadline_at", "new_deadline_at")}
			job.Extensions = append(job.Extensions, ext)
		default:
			if stallTerminalKinds[event.eventKind()] {
				job.Terminal = true
				job.TerminalKind = event.eventKind()
				job.TerminalSeq = int64(seq)
				if kind, ref := stallReportRefOf(event, filepath.Dir(eventsDir)); kind != "" {
					job.ReportKind = kind
					job.ReportPath = ref
				}
			}
		}
	}
	if job.ClaimedAt.IsZero() && len(job.Spawns) == 0 {
		return stallJobScan{}, false
	}
	return job, true
}

// Report reference kinds. A "path" is a local file the finalize path can
// read; a "doc_key" is a handoffkeep document key — a different identifier
// family that must never be opened as a file.
const (
	stallReportRefPath   = "path"
	stallReportRefDocKey = "doc_key"
)

// stallNoteReportPattern finds `report=<token>` inside a free-text note. The
// wrk-era journal records that carry no report_path field keep the reference
// in payload.note in exactly this shape — and the token is not always a
// path: the same note grammar also carries handoffkeep keys (`marker=` and
// `hk:doc` companions show the family), so the token's shape decides.
var stallNoteReportPattern = regexp.MustCompile(`(?:^|\s)report=(\S+)`)

// stallReportRefOf resolves the report reference a terminal event declares,
// by shape. Sources in precedence order: the top-level report_path field
// (the current panewire emit shape, 535 of 543 real records), the wrk-era
// top-level report field (worker.complete), payload.report_path, then the
// `report=` token inside payload.note — free text and therefore the weakest
// signal. Declared fields are paths by contract; a note token is classified
// by stallResolveReportToken. Nothing is guessed here: jobs/<job>/report.md
// convention fallback happens per-scan in refreshJob so a report written
// after the terminal record is still found.
func stallReportRefOf(event stallInboxEvent, jobDir string) (kind, ref string) {
	for _, candidate := range []string{event.ReportPath, event.Report, stallPayloadString(event.Payload, "report_path")} {
		if candidate != "" {
			return stallReportRefPath, candidate
		}
	}
	if note := stallPayloadString(event.Payload, "note"); note != "" {
		if match := stallNoteReportPattern.FindStringSubmatch(note); match != nil {
			if token := strings.TrimRight(match[1], ";,"); token != "" {
				return stallResolveReportToken(token, jobDir)
			}
		}
	}
	return "", ""
}

// stallResolveReportToken classifies a free-text `report=` token. Absolute
// and home-relative forms are paths. A relative token that lands on a real
// file under the job directory is a path (the wrk-era `events/x.md` shape).
// A bare namespaced token with no file on disk and no extension is a
// handoffkeep document key (report/<ns>/<name>) — recorded as a remote
// reference, never read from the filesystem.
func stallResolveReportToken(token, jobDir string) (kind, ref string) {
	switch {
	case filepath.IsAbs(token):
		return stallReportRefPath, token
	case strings.HasPrefix(token, "~/"):
		if home, err := os.UserHomeDir(); err == nil {
			return stallReportRefPath, filepath.Join(home, token[2:])
		}
		return stallReportRefPath, token
	}
	joined := filepath.Join(jobDir, filepath.FromSlash(token))
	if info, err := os.Stat(joined); err == nil && !info.IsDir() {
		return stallReportRefPath, joined
	}
	if filepath.Ext(token) != "" {
		// The extension marks path intent even when the file is gone; the
		// read fails and the row records missing_local — an honest answer.
		return stallReportRefPath, joined
	}
	return stallReportRefDocKey, token
}

// repsDeadlineSuggestion computes a default deadline proposal from every
// same-family job in the journal, completed or not. A job that was cut short
// still measures how long work was allowed to run before someone intervened;
// restricting the sample to completions would under-report exactly the stalls
// this detector exists to see. Too few samples is reported as unmeasured
// rather than replaced by a hand-picked constant.
func repsDeadlineSuggestion(jobs []stallJobScan, family string) (time.Duration, int) {
	var durations []time.Duration
	for _, job := range jobs {
		if job.Family != family || job.ClaimedAt.IsZero() || job.LastEventAt.Before(job.ClaimedAt) || job.LastEventAt.Equal(job.ClaimedAt) {
			continue
		}
		durations = append(durations, job.LastEventAt.Sub(job.ClaimedAt))
	}
	if len(durations) < stallRepsMinSamples {
		return 0, len(durations)
	}
	sort.Slice(durations, func(i, j int) bool { return durations[i] < durations[j] })
	p90 := durations[(len(durations)*9-1)/10]
	if p90 < stallRepsFloor {
		p90 = stallRepsFloor
	}
	if p90 > stallRepsCap {
		p90 = stallRepsCap
	}
	return p90, len(durations)
}

// scanJobs is the cached journal read: a job directory whose events dir has
// not changed since the last scan returns its memoized view. Events are
// append-only seq-named files, so directory mtime is a sound change signal.
func (m *stallDetectManager) scanJobs() []stallJobScan {
	jobsDir := filepath.Join(m.inboxRoot, "jobs")
	entries, err := os.ReadDir(jobsDir)
	if err != nil {
		return nil
	}
	var jobs []stallJobScan
	for _, entry := range entries {
		if !entry.IsDir() || !hubJobIDPattern.MatchString(entry.Name()) {
			continue
		}
		eventsDir := filepath.Join(jobsDir, entry.Name(), "events")
		info, err := os.Stat(eventsDir)
		if err == nil {
			if cached, ok := m.scanCache[entry.Name()]; ok && cached.modTime.Equal(info.ModTime()) {
				jobs = append(jobs, cached.scan)
				continue
			}
		}
		if job, ok := scanStallJobEvents(eventsDir, entry.Name()); ok {
			if err == nil {
				m.scanCache[entry.Name()] = stallJobScanCache{modTime: info.ModTime(), scan: job}
			}
			jobs = append(jobs, job)
		}
	}
	return jobs
}

// refreshJobs folds the current journal scan into the durable job table and
// resolves each attempt's deadline: issuer value first, then a reps proposal,
// then an explicit unmeasured mark. Issuer extensions are applied in sequence
// order and never erase already-recorded overdue incidents.
func (m *stallDetectManager) refreshJobs(ctx context.Context, at time.Time) ([]stallJobRow, error) {
	scans := m.scanJobs()
	for _, scan := range scans {
		if !m.cfg.harnessAllowed(scan.Family) {
			continue
		}
		if err := m.refreshJob(ctx, scan, scans); err != nil {
			return nil, err
		}
	}
	return m.store.stallJobs(ctx, false)
}

func (m *stallDetectManager) refreshJob(ctx context.Context, scan stallJobScan, all []stallJobScan) error {
	if scan.Terminal && scan.ReportKind == "" {
		// jobs/<job>/report.md is the fleet's fixed reporting convention: a
		// terminal record that names nothing still yields the report when
		// the file exists. Resolved here, outside the cached journal scan,
		// so a report written after the terminal record is still picked up.
		conventional := filepath.Join(m.inboxRoot, "jobs", scan.JobID, "report.md")
		if info, err := os.Stat(conventional); err == nil && !info.IsDir() {
			scan.ReportKind = stallReportRefPath
			scan.ReportPath = conventional
		}
	}
	base := stallJobRow{
		JobID: scan.JobID, OwnerLane: scan.OwnerLane, ParentLane: scan.ParentLane,
		AgentLabel: scan.AgentLabel, Harness: scan.Family, ClaimedAt: scan.ClaimedAt,
		Terminal: scan.Terminal, TerminalKind: scan.TerminalKind, LastEventAt: scan.LastEventAt,
		ReportPath: scan.ReportPath, ReportKind: scan.ReportKind,
	}
	if len(scan.Spawns) == 0 {
		row := base
		row.Attempt, row.Round = 0, 0
		m.resolveDeadline(&row, scan, all)
		return m.store.upsertStallJob(ctx, row)
	}
	for index, spawn := range scan.Spawns {
		row := base
		row.Attempt = int64(index + 1)
		row.Round = spawn.Round
		if row.Round == 0 {
			row.Round = row.Attempt
		}
		row.PaneID = spawn.PaneID
		row.WorkspaceID = spawn.WorkspaceID
		row.Profile = spawn.Profile
		row.SpawnedAt = spawn.At
		if scan.Terminal && spawn.Seq > 0 && scan.TerminalSeq > 0 && spawn.Seq > scan.TerminalSeq {
			// A spawn recorded after the terminal record is a fresh attempt
			// of a recycled job id — it did not inherit the earlier ending.
			row.Terminal = false
			row.TerminalKind = ""
		}
		if index < len(scan.Spawns)-1 {
			// A later spawn supersedes this attempt: only the newest attempt
			// of a job may still be live, so the older row closes instead of
			// racing the new attempt's reads and deadline.
			row.Terminal = true
			row.TerminalKind = "superseded"
		}
		m.resolveDeadline(&row, scan, all)
		if err := m.store.upsertStallJob(ctx, row); err != nil {
			return err
		}
	}
	return m.applyDeadlineExtensions(ctx, scan)
}

func (m *stallDetectManager) resolveDeadline(row *stallJobRow, scan stallJobScan, all []stallJobScan) {
	anchor := row.SpawnedAt
	if anchor.IsZero() {
		anchor = row.ClaimedAt
	}
	if !scan.IssuerDeadline.IsZero() {
		row.DeadlineAt = scan.IssuerDeadline
		row.DeadlineSource = stallDeadlineIssuer
		return
	}
	if suggestion, samples := repsDeadlineSuggestion(all, scan.Family); samples >= stallRepsMinSamples && !anchor.IsZero() {
		row.DeadlineAt = anchor.Add(suggestion)
		row.DeadlineSource = stallDeadlineReps
		return
	}
	row.DeadlineAt = time.Time{}
	row.DeadlineSource = stallDeadlineUnmeasured
}

// applyDeadlineExtensions honors job.deadline events only when the recorded
// issuer lane is the job's ordering lane. An extension moves the current
// deadline; it cannot retract a past overrun because those incidents are rows.
func (m *stallDetectManager) applyDeadlineExtensions(ctx context.Context, scan stallJobScan) error {
	for _, ext := range scan.Extensions {
		inserted, err := m.store.insertStallDeadlineExt(ctx, scan.JobID, ext.Seq, ext.IssuerLane, ext.Reason, ext.NewDeadlineAt, m.deps.now().UTC())
		if err != nil {
			return err
		}
		if !inserted {
			continue
		}
		if ext.IssuerLane != scan.OwnerLane || ext.NewDeadlineAt.IsZero() {
			continue
		}
		jobs, err := m.store.stallJobs(ctx, true)
		if err != nil {
			return err
		}
		var latest int64 = -1
		for _, job := range jobs {
			if job.JobID == scan.JobID && job.Attempt > latest {
				latest = job.Attempt
			}
		}
		if latest < 0 {
			continue
		}
		if err := m.store.applyStallDeadline(ctx, scan.JobID, latest, ext.NewDeadlineAt, stallDeadlineIssuer); err != nil {
			return err
		}
		// The extension ends the lapse it forgives. The overdue rows stay as
		// the preserved history; only their open marker moves.
		rows, err := m.store.stallJobs(ctx, true)
		if err != nil {
			return err
		}
		for _, row := range rows {
			if row.JobID == scan.JobID && row.Attempt == latest {
				_ = m.store.recoverStallIncidents(ctx, row.JobID, row.Attempt, row.Round, []string{stallCauseOverdue}, m.deps.now().UTC())
			}
		}
		if err := m.store.markStallDeadlineExtApplied(ctx, scan.JobID, ext.Seq); err != nil {
			return err
		}
	}
	return nil
}

// stallPattern is a confirmed-error rule built exclusively from the real
// on-screen strings. A match names only the observed text; it never asserts
// what the worker is or is not doing.
var stallPatterns = []struct {
	cause string
	all   []string
}{
	{stallCauseLimitRefused, []string{"provider.auth_error", "usage limit"}},
	{stallCauseAuthRefused, []string{"access token could not be refreshed", "sign in again"}},
	{stallCauseInputUnsubmitted, []string{"Press Enter to send queued messages now"}},
}

type stallMatch struct {
	cause       string
	fingerprint string
	count       int64
	line        string
	// tail is true when a matching line sits inside the last
	// stallTailAnchorLines non-empty lines — the region where current
	// output lives. A baseline match anchored there is suspect even when
	// the job is outside its startup grace.
	tail bool
}

// classifyScreen returns the patterns visible in this read. It deliberately
// takes no agent status: the contract distrusts status strings, so the
// unsubmitted/submitted-input distinction is made from read persistence,
// not from what the pane claims to be doing.
func classifyScreen(text string) []stallMatch {
	if text == "" {
		return nil
	}
	lines := strings.Split(text, "\n")
	// Index of the first line inside the tail anchor region: the last
	// stallTailAnchorLines non-empty lines of the buffer.
	tailStart := len(lines)
	for i, nonEmpty := len(lines)-1, 0; i >= 0; i-- {
		if strings.TrimSpace(lines[i]) == "" {
			continue
		}
		nonEmpty++
		tailStart = i
		if nonEmpty >= stallTailAnchorLines {
			break
		}
	}
	var matches []stallMatch
	for _, pattern := range stallPatterns {
		var count int64
		var first string
		var inTail bool
		for index, line := range lines {
			matched := true
			for _, required := range pattern.all {
				if !strings.Contains(line, required) {
					matched = false
					break
				}
			}
			if !matched {
				continue
			}
			count++
			if first == "" {
				first = strings.TrimSpace(line)
			}
			if index >= tailStart {
				inTail = true
			}
		}
		if count == 0 {
			continue
		}
		sum := sha256.Sum256([]byte(pattern.cause + "\x00" + first))
		matches = append(matches, stallMatch{cause: pattern.cause, fingerprint: hex.EncodeToString(sum[:8]), count: count, line: first, tail: inTail})
	}
	return matches
}

// scan is one detector cycle: journal refresh → superseded → coverage →
// conflicts → pane reads → overdue → unowned → reports → notifications →
// beat. When the live-agent listing cannot be obtained the cycle is degraded:
// no observation is made on a pane set the detector never saw, and the beat
// records observation=nil so the hub surfaces it. A tool failure is not
// evidence about a pane — a failed agent.list must not read as "no panes".
func (m *stallDetectManager) scan(ctx context.Context) {
	at := m.deps.now().UTC()
	m.lastScanAt = at
	if at.Before(m.suspendedTill) {
		return
	}
	jobs, err := m.refreshJobs(ctx, at)
	if err != nil {
		m.logger.Warn("stall-detect job refresh unavailable")
		m.finishScan(ctx, at, -1, true)
		return
	}
	if m.deps.listAgents == nil {
		m.finishScan(ctx, at, -1, true)
		return
	}
	agents, listErr := m.deps.listAgents(ctx)
	if listErr != nil {
		m.logger.Warn("stall-detect agent list failed; judgement withheld this cycle")
		m.noteReadFailure()
		m.coverageUnknownSince = time.Time{}
		m.finishScan(ctx, at, -1, true)
		if m.readFailures >= m.cfg.SuspendAfterFailures {
			// A listing that keeps failing is the same class of problem as a
			// read loop that keeps failing: suspend and let the frozen beat
			// raise the hub no-data alert.
			m.suspendedTill = at.Add(m.cfg.PollInterval * 10)
			m.logger.Warn("stall-detect suspended after repeated observation failures")
		}
		return
	}
	m.observeSuperseded(ctx, jobs, at)
	m.observeCoverage(ctx, jobs, agents, at)
	m.observeConflicts(ctx, jobs, agents, at)
	m.readPanes(ctx, jobs, agents, at)
	m.observeOverdue(ctx, jobs, agents, at)
	m.observeUnowned(ctx, agents, at)
	m.persistReports(ctx, at)
	m.notifyTick(ctx, at)
	m.readFailures = 0
	m.finishScan(ctx, at, len(agents), m.anyUnreadable(jobs, agents))
}

// anyUnreadable reports whether a tracked, live pane has failed its reads
// past the surfacing streak — partial blindness that must mark the beat
// degraded rather than pass for healthy coverage.
func (m *stallDetectManager) anyUnreadable(jobs []stallJobRow, agents []paneIdentity) bool {
	live := make(map[string]bool, len(agents))
	for _, agent := range agents {
		live[agent.PaneID] = true
	}
	for _, job := range jobs {
		if job.PaneID != "" && live[job.PaneID] && m.paneReadStreak[job.PaneID] >= stallUnreadableAfterReads {
			return true
		}
	}
	return false
}

// finishScan records the detector beat. A degraded cycle keeps the beat
// moving but marks the observation missing; the hub is required to tell that
// apart from a healthy beat that observed zero panes.
func (m *stallDetectManager) finishScan(ctx context.Context, at time.Time, panes int, degraded bool) {
	m.lastBeat = at
	if !degraded {
		m.trackedPanes = panes
	}
	if err := m.store.recordStallBeat(ctx, at, m.cfg.PollInterval, panes, degraded); err != nil {
		m.logger.Warn("stall-detect beat was not recorded")
	}
	for key := range m.wakePending {
		delete(m.wakePending, key)
	}
}

// observeSuperseded records [superseded] when one pane carries two open job
// rows: the newer spawn proves the worker moved on while the older job never
// produced a terminal event. Observation only — the old row is marked so the
// detector stops reading it; nothing is sent to the pane and no job state is
// otherwise mutated.
func (m *stallDetectManager) observeSuperseded(ctx context.Context, jobs []stallJobRow, at time.Time) {
	byPane := make(map[string][]stallJobRow)
	for _, job := range jobs {
		if job.PaneID == "" || job.Terminal {
			continue
		}
		byPane[job.PaneID] = append(byPane[job.PaneID], job)
	}
	for pane, rows := range byPane {
		if len(rows) < 2 {
			continue
		}
		sort.Slice(rows, func(i, j int) bool {
			if !rows[i].SpawnedAt.Equal(rows[j].SpawnedAt) {
				return rows[i].SpawnedAt.Before(rows[j].SpawnedAt)
			}
			return rows[i].JobID < rows[j].JobID
		})
		newest := rows[len(rows)-1]
		for _, row := range rows[:len(rows)-1] {
			if open, err := m.openCauseExists(ctx, row, stallCauseSuperseded); err != nil || open {
				continue
			}
			m.openIncident(ctx, row, stallCauseSuperseded, stallMatch{line: "worker took a new job before this job reached a terminal event"}, at, true, map[string]any{
				"pane": pane, "superseded_by": newest.JobID, "superseded_by_attempt": newest.Attempt,
			})
			_ = m.store.markStallJobTerminal(ctx, row.JobID, row.Attempt, "superseded")
		}
	}
}

// observeCoverage verifies each tracked pane is inside the daemon's proven
// subscription set. A pane that is not covered is recorded as [unobserved]
// and a resubscribe is requested; coverage coming back marks it recovered.
// Unmonitored panes are left alone — observation must not pretend coverage.
func (m *stallDetectManager) observeCoverage(ctx context.Context, jobs []stallJobRow, agents []paneIdentity, at time.Time) {
	if m.deps.subscribed == nil {
		return
	}
	covered, _ := m.deps.subscribed()
	live := make(map[string]bool, len(agents))
	for _, agent := range agents {
		live[agent.PaneID] = true
	}
	tracked := 0
	for _, job := range jobs {
		if job.PaneID != "" && live[job.PaneID] {
			tracked++
		}
	}
	if covered == nil {
		// The subscription set itself is unavailable — that is not a proven
		// empty set. Give the resubscribe path a grace window before
		// recording [unobserved] so a restart or reconnect does not open
		// rows on a coverage gap nobody could have verified.
		if tracked == 0 {
			m.coverageUnknownSince = time.Time{}
			return
		}
		if m.coverageUnknownSince.IsZero() {
			m.coverageUnknownSince = at
		}
		if m.deps.resubscribe != nil && at.Sub(m.lastResub) >= stallResubscribeMinInterval {
			m.lastResub = at
			m.deps.resubscribe()
		}
		if at.Sub(m.coverageUnknownSince) < 2*m.cfg.PollInterval {
			return
		}
		for _, job := range jobs {
			if job.PaneID == "" || !live[job.PaneID] {
				continue
			}
			if open, err := m.openCauseExists(ctx, job, stallCauseUnobserved); err == nil && open {
				continue
			}
			m.openIncident(ctx, job, stallCauseUnobserved, stallMatch{line: "subscription coverage unproven"}, at, true, map[string]any{
				"pane": job.PaneID, "reason": "subscription set unavailable past grace",
			})
		}
		return
	}
	m.coverageUnknownSince = time.Time{}
	resubscribeNeeded := false
	for _, job := range jobs {
		if job.PaneID == "" || !live[job.PaneID] {
			continue
		}
		if covered[job.PaneID] {
			continue
		}
		resubscribeNeeded = true
		// One open row covers the whole gap; a still-uncovered pane on the
		// next scan is the same observation, not a new one.
		if open, err := m.openCauseExists(ctx, job, stallCauseUnobserved); err == nil && open {
			continue
		}
		m.openIncident(ctx, job, stallCauseUnobserved, stallMatch{line: "pane not in proven subscription set"}, at, true, map[string]any{
			"pane": job.PaneID, "reason": "subscription attach unverified",
		})
	}
	if resubscribeNeeded && m.deps.resubscribe != nil && at.Sub(m.lastResub) >= stallResubscribeMinInterval {
		m.lastResub = at
		m.deps.resubscribe()
	}
	// Coverage that returns recovers the observation; the row stays as the
	// record of the gap.
	for _, job := range jobs {
		if job.PaneID == "" || covered[job.PaneID] {
			if err := m.store.recoverStallIncidents(ctx, job.JobID, job.Attempt, job.Round, []string{stallCauseUnobserved}, at); err != nil {
				m.logger.Warn("stall-detect unobserved recovery was not recorded")
			}
		}
	}
}

// observeConflicts records [conflict] when two or more live agent sessions
// share one worktree. Observation only: no cleanup, no input, no termination.
func (m *stallDetectManager) observeConflicts(ctx context.Context, jobs []stallJobRow, agents []paneIdentity, at time.Time) {
	if m.deps.worktree == nil {
		return
	}
	byTree := make(map[string][]paneIdentity)
	for _, agent := range agents {
		root := agent.CWD
		if root == "" {
			continue
		}
		if resolved := m.deps.worktree(ctx, root); resolved != "" {
			root = resolved
		}
		byTree[root] = append(byTree[root], agent)
	}
	paneJob := make(map[string]stallJobRow, len(jobs))
	for _, job := range jobs {
		if job.PaneID != "" {
			paneJob[job.PaneID] = job
		}
	}
	for root, members := range byTree {
		if len(members) < 2 {
			continue
		}
		panes := make([]string, 0, len(members))
		for _, member := range members {
			panes = append(panes, member.PaneID)
		}
		sort.Strings(panes)
		set := strings.Join(panes, ",")
		recorded := make(map[string]bool)
		for _, member := range members {
			job, ok := paneJob[member.PaneID]
			if !ok {
				job = stallJobRow{JobID: "", Attempt: 0, Round: 0, PaneID: member.PaneID}
			}
			if recorded[job.JobID+":"+strconv.FormatInt(job.Attempt, 10)] {
				continue
			}
			recorded[job.JobID+":"+strconv.FormatInt(job.Attempt, 10)] = true
			if m.conflictOpen(ctx, job, set) {
				continue
			}
			m.openIncident(ctx, job, stallCauseConflict, stallMatch{line: "multiple live sessions share one worktree"}, at, true, map[string]any{
				"worktree": root, "panes": panes, "set": set,
			})
		}
	}
}

// openCauseExists reports whether the (job, attempt, round) scope already has
// an unrecovered incident of this cause.
func (m *stallDetectManager) openCauseExists(ctx context.Context, job stallJobRow, cause string) (bool, error) {
	incidents, err := m.store.stallIncidents(ctx, job.JobID, job.Attempt, job.Round, true)
	if err != nil {
		return false, err
	}
	for _, incident := range incidents {
		if incident.Cause == cause {
			return true, nil
		}
	}
	return false, nil
}

func (m *stallDetectManager) conflictOpen(ctx context.Context, job stallJobRow, set string) bool {
	incidents, err := m.store.stallIncidents(ctx, job.JobID, job.Attempt, job.Round, true)
	if err != nil {
		return false
	}
	for _, incident := range incidents {
		if incident.Cause != stallCauseConflict {
			continue
		}
		var evidence struct {
			Set string `json:"set"`
		}
		if json.Unmarshal(incident.Evidence, &evidence) == nil && evidence.Set == set {
			return true
		}
	}
	return false
}

// readPanes performs the bounded periodic read half of the hybrid trigger.
// Woken panes go first; every tracked pane is due once per poll interval.
func (m *stallDetectManager) readPanes(ctx context.Context, jobs []stallJobRow, agents []paneIdentity, at time.Time) {
	if m.deps.readPane == nil {
		return
	}
	live := make(map[string]paneIdentity, len(agents))
	for _, agent := range agents {
		live[agent.PaneID] = agent
	}
	// Streaks for panes that vanished from the listing are stale — a dead
	// pane's old failures must not keep the beat degraded.
	for pane := range m.paneReadStreak {
		if _, ok := live[pane]; !ok {
			delete(m.paneReadStreak, pane)
		}
	}
	reads := 0
	for _, job := range jobs {
		if job.PaneID == "" {
			continue
		}
		agent, alive := live[job.PaneID]
		if !alive {
			continue
		}
		pane, found, err := m.store.stallPane(ctx, job.PaneID)
		if err != nil {
			continue
		}
		_, woken := m.wakePending[job.PaneID]
		due := woken || pane.LastReadAt.IsZero() || !at.Before(pane.LastReadAt.Add(m.cfg.PollInterval))
		if !due || reads >= m.cfg.MaxReadsPerCycle {
			continue
		}
		reads++
		m.readOnePane(ctx, job, agent, pane, found, at)
		if m.readFailures >= m.cfg.SuspendAfterFailures {
			// herdr reads failing in a run is exactly the detector-induced load
			// signal F8 describes; suspend the read path, keep recording.
			m.suspendedTill = at.Add(m.cfg.PollInterval * 10)
			m.logger.Warn("stall-detect reads suspended after repeated failures")
			return
		}
	}
}

func (m *stallDetectManager) readOnePane(ctx context.Context, job stallJobRow, agent paneIdentity, pane stallPaneRow, found bool, at time.Time) {
	readCtx, cancel := context.WithTimeout(ctx, m.cfg.ReadTimeout)
	evidence, err := m.deps.readPane(readCtx, job.PaneID)
	cancel()
	if err != nil {
		m.readFailures++
		m.paneReadStreak[job.PaneID]++
		if found {
			pane.LastReadAt, pane.LastReadOK = at, false
			m.savePane(ctx, pane)
		}
		if m.paneReadStreak[job.PaneID] >= stallUnreadableAfterReads {
			// One open row covers the whole gap; the beat marks degraded in
			// finishScan so the hub sees the partial blindness too.
			if open, oerr := m.openCauseExists(ctx, job, stallCauseUnreadable); oerr == nil && !open {
				m.openIncident(ctx, job, stallCauseUnreadable, stallMatch{line: "pane read keeps failing"}, at, true, map[string]any{
					"pane": job.PaneID, "read_failures": m.paneReadStreak[job.PaneID],
					"last_error": redactSecrets(err.Error()),
				})
			}
		}
		return
	}
	m.readFailures = 0
	delete(m.paneReadStreak, job.PaneID)
	// A successful read ends the gap even after a restart wiped the streak.
	if err := m.store.recoverStallIncidents(ctx, job.JobID, job.Attempt, job.Round, []string{stallCauseUnreadable}, at); err != nil {
		m.logger.Warn("stall-detect unreadable recovery was not recorded")
	}
	if !found {
		pane = stallPaneRow{PaneID: job.PaneID, Fingerprints: map[string]int64{}, FirstSeenAt: at}
	} else if pane.JobID != job.JobID || pane.Attempt != job.Attempt {
		// A reused pane's fingerprints belong to the previous attempt. A new
		// attempt is a new attach: the first read baselines again so a banner
		// left over in scrollback is not a new sighting of the old one.
		pane.BaselineDone = false
		pane.Fingerprints = map[string]int64{}
		delete(m.unsubStreak, job.PaneID)
		delete(m.cleanReads, job.PaneID)
	}
	pane.JobID, pane.Attempt = job.JobID, job.Attempt
	pane.WorkspaceID = agent.WorkspaceID
	pane.CWD = agent.CWD
	pane.Harness = agent.Harness
	pane.LastReadAt, pane.LastReadOK = at, true
	pane.SpawnedRecent = !job.SpawnedAt.IsZero() && at.Sub(job.SpawnedAt) <= m.cfg.StartupGrace

	newOutput := evidence.Revision != 0 && evidence.Revision != pane.LastRevision
	if evidence.Revision != 0 {
		pane.LastRevision = evidence.Revision
	}
	if pane.Fingerprints == nil {
		pane.Fingerprints = map[string]int64{}
	}
	matches := classifyScreen(evidence.Text)
	if !pane.BaselineDone {
		// The first read after attach is baseline: matching lines are old
		// scrollback until proven new. Two exceptions keep a real current
		// failure from being silently swallowed: a job inside its startup
		// grace, and a match anchored at the buffer tail where live output
		// sits. Both record startup_block_suspect — a named suspicion, never
		// a stall verdict.
		for _, match := range matches {
			pane.Fingerprints[match.fingerprint] = match.count
		}
		pane.BaselineDone = true
		seen := map[string]bool{}
		for _, match := range matches {
			if seen[match.cause] || (!pane.SpawnedRecent && !match.tail) {
				continue
			}
			seen[match.cause] = true
			m.openIncident(ctx, job, stallCauseStartupSuspect, match, at, true, map[string]any{
				"pane": job.PaneID, "matched_cause": match.cause, "matched_line": redactSecrets(match.line),
				"spawned_recent": pane.SpawnedRecent, "tail_anchored": match.tail,
			})
		}
		m.savePane(ctx, pane)
		return
	}
	if len(matches) == 0 && newOutput {
		// Clean reads on an advancing pane are the subsequent-success half of
		// the recovery rule — decided by revision movement, not by a status
		// string. Two in a row, so a banner momentarily hidden by a redraw
		// does not retract a real observation.
		m.cleanReads[job.PaneID]++
		if m.cleanReads[job.PaneID] >= 2 {
			if err := m.store.recoverStallIncidents(ctx, job.JobID, job.Attempt, job.Round, []string{stallCauseLimitRefused, stallCauseAuthRefused, stallCauseInputUnsubmitted}, at); err != nil {
				m.logger.Warn("stall-detect recovery was not recorded")
			}
		}
	} else {
		m.cleanReads[job.PaneID] = 0
	}
	bannerSeen := false
	present := make(map[string]bool, len(matches))
	for _, match := range matches {
		present[match.fingerprint] = true
		if match.cause == stallCauseInputUnsubmitted {
			bannerSeen = true
		}
		seen := pane.Fingerprints[match.fingerprint]
		if match.count < seen {
			// The buffer dropped matching lines (scrollback eviction or a
			// redraw). Lower the watermark to what is actually on screen so a
			// stale high count cannot suppress a genuinely new sighting.
			pane.Fingerprints[match.fingerprint] = match.count
			continue
		}
		if match.count <= seen {
			continue
		}
		if match.cause == stallCauseInputUnsubmitted {
			// A submitted prompt leaves the same banner queued behind it, so
			// one sighting cannot tell unsubmitted input from queued input
			// already sent. The banner must hold across a second read.
			m.unsubStreak[job.PaneID]++
			if m.unsubStreak[job.PaneID] < 2 {
				continue
			}
		}
		pane.Fingerprints[match.fingerprint] = match.count
		for i := seen + 1; i <= match.count; i++ {
			m.openIncident(ctx, job, match.cause, match, at, true, map[string]any{
				"pane": job.PaneID, "matched_line": redactSecrets(match.line), "revision": evidence.Revision,
			})
		}
	}
	// A fingerprint absent from this read scrolled out of the buffer
	// entirely. Its old watermark must not suppress a genuinely new
	// identical line — a transient redraw that re-shows the same banner
	// fires once more, a visible duplicate rather than a silent miss.
	for fingerprint := range pane.Fingerprints {
		if !present[fingerprint] {
			delete(pane.Fingerprints, fingerprint)
		}
	}
	if !bannerSeen {
		m.unsubStreak[job.PaneID] = 0
	}
	m.savePane(ctx, pane)
}

func (m *stallDetectManager) savePane(ctx context.Context, pane stallPaneRow) {
	if err := m.store.upsertStallPane(ctx, pane); err != nil {
		m.logger.Warn("stall-detect pane state was not recorded")
	}
}

func (m *stallDetectManager) noteReadFailure() {
	m.readFailures++
}

// openIncident is the single insert path. Occurrence is the next free slot
// for (job, attempt, round, cause), which is what keeps a job's later rounds
// and repeated sightings distinct — the job-lifetime dedupe key is forbidden.
func (m *stallDetectManager) openIncident(ctx context.Context, job stallJobRow, cause string, match stallMatch, at time.Time, observable bool, evidence map[string]any) {
	occurrence, err := m.store.maxStallOccurrence(ctx, job.JobID, job.Attempt, job.Round, cause)
	if err != nil {
		m.logger.Warn("stall-detect occurrence lookup unavailable")
		return
	}
	occurrence++
	if evidence == nil {
		evidence = map[string]any{}
	}
	evidence["observed_at"] = at.Format(time.RFC3339)
	encoded, err := json.Marshal(evidence)
	if err != nil {
		return
	}
	row := stallIncidentRow{
		JobID: job.JobID, Attempt: job.Attempt, Round: job.Round,
		Cause: cause, Occurrence: occurrence, PaneID: job.PaneID,
		Observable: observable, FirstSeenAt: at, LastSeenAt: at,
		Evidence: encoded,
	}
	row.NotifyState = stallNotifyPending
	inserted, err := m.store.recordStallIncident(ctx, row)
	if err != nil || !inserted {
		return
	}
	m.armNotification(ctx, row, job, at)
}

// armNotification creates the level-0 ladder row for a new incident. Shadow
// mode records the would-be notification with suppressed_reason=shadow and
// emits nothing; the send path exists and is exercised only when Notify is on.
func (m *stallDetectManager) armNotification(ctx context.Context, incident stallIncidentRow, job stallJobRow, at time.Time) {
	lane := job.OwnerLane
	row := stallNotificationRow{
		IncidentKey: incident.incidentKey(), Lane: lane, Level: 0,
		GraceDeadlineAt: at.Add(m.cfg.NotifyGrace),
	}
	if lane == "" || !hubAgentLabelPattern.MatchString(lane) {
		row.SuppressedReason = stallNotifyNoLane
		row.Lane = ""
	} else if !m.cfg.Notify {
		row.SuppressedReason = stallNotifyShadow
	}
	if err := m.store.upsertStallNotification(ctx, row); err != nil {
		m.logger.Warn("stall-detect notification was not recorded")
	}
}

// notifyTick delivers due notifications when Notify is on, then advances the
// owner→parent ladder for anything still unacknowledged past its grace. emit
// acceptance is not receipt: acked only means the relay_sent persisted stamp.
func (m *stallDetectManager) notifyTick(ctx context.Context, at time.Time) {
	due, err := m.store.stallNotificationsDue(ctx, at)
	if err != nil {
		return
	}
	for _, row := range due {
		if row.SuppressedReason != "" {
			continue
		}
		acked := false
		if row.EventID != "" && m.deps.lanePersisted != nil {
			if ok, err := m.deps.lanePersisted(ctx, row.Lane, row.EventID); err == nil {
				acked = ok
			}
		}
		if acked {
			row.AckedAt = at
			_ = m.store.upsertStallNotification(ctx, row)
			m.markIncidentNotified(ctx, row.IncidentKey, stallNotifyAcked)
			continue
		}
		if !m.cfg.Notify {
			row.SuppressedReason = stallNotifyShadow
			_ = m.store.upsertStallNotification(ctx, row)
			continue
		}
		if m.deps.enqueue == nil {
			continue
		}
		if !row.NotifiedAt.IsZero() && !row.GraceDeadlineAt.IsZero() && at.After(row.GraceDeadlineAt) {
			m.escalateNotification(ctx, row, at)
			continue
		}
		m.sendNotification(ctx, &row, at)
	}
}

func (m *stallDetectManager) escalateNotification(ctx context.Context, row stallNotificationRow, at time.Time) {
	incident, ok := m.incidentForKey(ctx, row.IncidentKey)
	if !ok {
		return
	}
	jobs, err := m.store.stallJobs(ctx, true)
	if err != nil {
		return
	}
	var parent string
	for _, job := range jobs {
		if job.JobID == incident.JobID && job.Attempt == incident.Attempt {
			parent = job.ParentLane
		}
	}
	if parent == "" || !hubAgentLabelPattern.MatchString(parent) {
		row.SuppressedReason = stallNotifyNoLane
		_ = m.store.upsertStallNotification(ctx, row)
		return
	}
	next := stallNotificationRow{
		IncidentKey: row.IncidentKey, Lane: parent, Level: row.Level + 1,
		GraceDeadlineAt: at.Add(m.cfg.NotifyGrace),
	}
	if !m.cfg.Notify {
		next.SuppressedReason = stallNotifyShadow
	}
	_ = m.store.upsertStallNotification(ctx, next)
	row.SuppressedReason = stallNotifyExhausted
	_ = m.store.upsertStallNotification(ctx, row)
}

func (m *stallDetectManager) sendNotification(ctx context.Context, row *stallNotificationRow, at time.Time) {
	incident, ok := m.incidentForKey(ctx, row.IncidentKey)
	if !ok {
		return
	}
	text := stallNotifyText(incident)
	// Each send is its own durable lane.event: the events-lane namespace keys
	// records on (lane, event_id), so a redelivery binds the attempt number
	// into the id and reuses never overwrites a first writer.
	sum := sha256.Sum256([]byte(row.IncidentKey + "\x00" + strconv.FormatInt(row.Level, 10) + "\x00" + strconv.FormatInt(row.Attempts+1, 10)))
	row.EventID = "stall:" + hex.EncodeToString(sum[:16])
	record := emitRecord{
		Type: "lane.event", Epoch: 1, CreatedAt: at.Format(time.RFC3339),
		OwnerLane: row.Lane, EventID: row.EventID, Text: text, PaneID: incident.PaneID,
	}
	if _, err := m.deps.writeRecord(m.inboxRoot, record); err != nil {
		return
	}
	m.deps.enqueue(hubScannedRelayEvent{
		Kind:         "lane.event",
		HubActiveJob: HubActiveJob{JobID: laneEventTransportID(row.Lane, row.EventID), Epoch: 1, OwnerLane: row.Lane},
		PaneID:       incident.PaneID, EventID: row.EventID, Text: text,
	})
	row.Attempts++
	row.NotifiedAt = at
	row.NextRetryAt = at.Add(m.cfg.NotifyRetry)
	if row.GraceDeadlineAt.IsZero() {
		row.GraceDeadlineAt = at.Add(m.cfg.NotifyGrace)
	}
	_ = m.store.upsertStallNotification(ctx, *row)
	m.markIncidentNotified(ctx, row.IncidentKey, stallNotifySent)
}

// stallNotifyText is the one-line lane payload. The bracketed token names the
// observation class; nothing in this text may assert that a worker stopped.
func stallNotifyText(incident stallIncidentRow) string {
	summary := "[" + incident.Cause + "] 확인 필요"
	body := map[string]any{
		"observation": incident.Cause, "job": incident.JobID,
		"attempt": incident.Attempt, "round": incident.Round, "occurrence": incident.Occurrence,
		"pane": incident.PaneID, "summary": summary,
	}
	var evidence map[string]any
	if json.Unmarshal(incident.Evidence, &evidence) == nil {
		delete(evidence, "matched_line")
		body["evidence"] = evidence
	}
	encoded, _ := json.Marshal(body)
	if utf8.RuneCountInString(string(encoded)) > laneEventTextLimit {
		delete(body, "evidence")
		encoded, _ = json.Marshal(body)
	}
	return string(encoded)
}

func (m *stallDetectManager) incidentForKey(ctx context.Context, key string) (stallIncidentRow, bool) {
	parts := strings.Split(key, "\x00")
	if len(parts) != 5 {
		return stallIncidentRow{}, false
	}
	attempt, err1 := strconv.ParseInt(parts[1], 10, 64)
	round, err2 := strconv.ParseInt(parts[2], 10, 64)
	occurrence, err3 := strconv.ParseInt(parts[4], 10, 64)
	if err1 != nil || err2 != nil || err3 != nil {
		return stallIncidentRow{}, false
	}
	incidents, err := m.store.stallIncidents(ctx, parts[0], attempt, round, false)
	if err != nil {
		return stallIncidentRow{}, false
	}
	for _, incident := range incidents {
		if incident.Cause == parts[3] && incident.Occurrence == occurrence {
			return incident, true
		}
	}
	return stallIncidentRow{}, false
}

func (m *stallDetectManager) markIncidentNotified(ctx context.Context, key, state string) {
	incident, ok := m.incidentForKey(ctx, key)
	if !ok {
		return
	}
	s := m.store
	s.mu.Lock()
	defer s.mu.Unlock()
	_, _ = s.db.ExecContext(ctx, `UPDATE stall_incidents SET notify_state=? WHERE job_id=? AND attempt=? AND round=? AND cause=? AND occurrence=?`,
		state, incident.JobID, incident.Attempt, incident.Round, incident.Cause, incident.Occurrence)
}

// observeOverdue records [overdue] 확인 필요 once per (job, attempt, round)
// per lapse. The evidence bundle names the deadline, its source, the owner of
// the next action, and what was last seen — it never asserts a stop.
func (m *stallDetectManager) observeOverdue(ctx context.Context, jobs []stallJobRow, agents []paneIdentity, at time.Time) {
	for _, job := range jobs {
		if job.Terminal || job.DeadlineAt.IsZero() || !at.After(job.DeadlineAt) {
			continue
		}
		existing, err := m.store.stallIncidents(ctx, job.JobID, job.Attempt, job.Round, false)
		if err != nil {
			continue
		}
		// Each lapse is one occurrence. An open row already covers the current
		// lapse; a lapse ends when the issuer extends the deadline, which
		// recovers the open row and lets a later crossing count again.
		open := false
		for _, incident := range existing {
			if incident.Cause == stallCauseOverdue && incident.RecoveredAt.IsZero() {
				open = true
			}
		}
		if open {
			continue
		}
		m.openIncident(ctx, job, stallCauseOverdue, stallMatch{line: "deadline passed without completion"}, at, true,
			m.overdueEvidence(ctx, job, agents, at))
	}
}

// overdueEvidence assembles the bounded bundle. Each field is a measurement
// or an explicit "unmeasured" — never an inference about worker state.
func (m *stallDetectManager) overdueEvidence(ctx context.Context, job stallJobRow, agents []paneIdentity, at time.Time) map[string]any {
	evidence := map[string]any{
		"deadline_at":     job.DeadlineAt.Format(time.RFC3339),
		"deadline_source": job.DeadlineSource,
		"action_owner":    job.OwnerLane,
		"summary":         "[overdue] 확인 필요",
	}
	var candidates []string
	if incidents, err := m.store.stallIncidents(ctx, job.JobID, job.Attempt, job.Round, true); err == nil {
		for _, incident := range incidents {
			candidates = append(candidates, incident.Cause)
		}
	}
	evidence["cause_candidates"] = candidates
	if lastActivity := stallLastFileActivity(m.inboxRoot, job); !lastActivity.IsZero() {
		evidence["last_file_activity"] = lastActivity.Format(time.RFC3339)
	} else {
		evidence["last_file_activity"] = "unmeasured"
	}
	dir := job.CWD
	if pane, found, err := m.store.stallPane(ctx, job.PaneID); err == nil && found {
		if dir == "" {
			dir = pane.CWD
		}
		if !pane.LastReadAt.IsZero() {
			evidence["observed_freshness_ms"] = at.Sub(pane.LastReadAt).Milliseconds()
		} else {
			evidence["observed_freshness_ms"] = "unmeasured"
		}
	} else {
		evidence["observed_freshness_ms"] = "unmeasured"
	}
	if cpu := m.cpuForDir(ctx, dir); cpu != "" {
		evidence["process_cpu"] = cpu
	} else {
		evidence["process_cpu"] = "unmeasured"
	}
	if report, found, err := m.store.stallReport(ctx, job.JobID, job.Attempt); err == nil && found {
		evidence["report_local"] = report.LocalPath != ""
		evidence["report_remote"] = report.RemoteState
	} else {
		evidence["report_local"] = false
		evidence["report_remote"] = "none"
	}
	if m.deps.readPane != nil && job.PaneID != "" {
		readCtx, cancel := context.WithTimeout(ctx, m.cfg.ReadTimeout)
		ev, err := m.deps.readPane(readCtx, job.PaneID)
		cancel()
		if err == nil && ev.Text != "" {
			runes := []rune(ev.Text)
			if len(runes) > stallScreenExcerptRunes {
				runes = runes[len(runes)-stallScreenExcerptRunes:]
			}
			evidence["screen_excerpt"] = redactSecrets(string(runes))
		}
	}
	return evidence
}

func stallLastFileActivity(inboxRoot string, job stallJobRow) time.Time {
	var latest time.Time
	eventsDir := filepath.Join(inboxRoot, "jobs", job.JobID, "events")
	if entries, err := os.ReadDir(eventsDir); err == nil {
		for _, entry := range entries {
			if info, err := entry.Info(); err == nil && info.ModTime().After(latest) {
				latest = info.ModTime()
			}
		}
	}
	if job.ReportPath != "" {
		if info, err := os.Stat(job.ReportPath); err == nil && info.ModTime().After(latest) {
			latest = info.ModTime()
		}
	}
	return latest.UTC()
}

// stallProc is one harness-process observation for the [unowned] scan.
type stallProc struct {
	PID, PPID int64
	StartedAt time.Time
	Name, CWD string
}

var stallHarnessNames = map[string]bool{
	"devin": true, "claude": true, "codex": true, "kimi": true,
	"astra": true, "fable": true, "gemini": true, "opencode": true, "amp": true,
}

func stallHarnessProcs(ctx context.Context) ([]stallProc, error) {
	// LC_ALL=C pins the lstart format; parsing it in time.Local matches the
	// zone ps printed it in.
	cmd := exec.CommandContext(ctx, "ps", "-axo", "pid=,ppid=,lstart=,comm=")
	cmd.Env = append(os.Environ(), "LC_ALL=C")
	out, err := cmd.Output()
	if err != nil {
		return nil, err
	}
	var procs []stallProc
	for _, line := range strings.Split(string(out), "\n") {
		fields := strings.Fields(line)
		if len(fields) < 7 {
			continue
		}
		pid, err1 := strconv.ParseInt(fields[0], 10, 64)
		ppid, err2 := strconv.ParseInt(fields[1], 10, 64)
		started, err3 := time.ParseInLocation("Mon Jan 2 15:04:05 2006", strings.Join(fields[2:7], " "), time.Local)
		if err1 != nil || err2 != nil || err3 != nil {
			continue
		}
		name := filepath.Base(strings.Join(fields[7:], " "))
		if !stallHarnessNames[strings.ToLower(name)] {
			continue
		}
		procs = append(procs, stallProc{PID: pid, PPID: ppid, StartedAt: started, Name: name})
	}
	return procs, nil
}

func stallProcCWD(ctx context.Context, pid int64) (string, error) {
	out, err := exec.CommandContext(ctx, "lsof", "-a", "-p", strconv.FormatInt(pid, 10), "-d", "cwd", "-Fn").Output()
	if err != nil {
		return "", err
	}
	for _, line := range strings.Split(string(out), "\n") {
		if strings.HasPrefix(line, "n/") {
			return strings.TrimPrefix(line, "n"), nil
		}
	}
	return "", errors.New("cwd not found")
}

func stallGitToplevel(ctx context.Context, dir string) string {
	out, err := exec.CommandContext(ctx, "git", "-C", dir, "rev-parse", "--show-toplevel").Output()
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(out))
}

// observeUnowned records [unowned] for harness processes inside known
// worktrees that no live agent accounts for. cwd alone never proves a
// process unowned — it must be seen unclaimed on two consecutive scans, and
// the record is explicitly an investigation target, not a verdict.
func (m *stallDetectManager) observeUnowned(ctx context.Context, agents []paneIdentity, at time.Time) {
	if m.deps.procs == nil || m.deps.procCWD == nil || m.deps.worktree == nil {
		return
	}
	procs, err := m.deps.procs(ctx)
	if err != nil {
		return
	}
	knownTrees := map[string]bool{}
	liveTrees := map[string]int{}
	for _, agent := range agents {
		root := agent.CWD
		if root == "" {
			continue
		}
		if resolved := m.deps.worktree(ctx, root); resolved != "" {
			root = resolved
		}
		liveTrees[root]++
		knownTrees[root] = true
	}
	// Previously observed pane cwds widen "known" beyond what is live now;
	// a process in a never-seen directory is not this fleet's concern.
	if cwds, err := m.store.stallPaneCWDs(ctx); err == nil {
		for _, cwd := range cwds {
			root := cwd
			if resolved := m.deps.worktree(ctx, root); resolved != "" {
				root = resolved
			}
			knownTrees[root] = true
		}
	}
	lookups := 0
	byTree := map[string][]stallProc{}
	for _, proc := range procs {
		if lookups >= stallMaxProcLookups {
			break
		}
		cwd, ok := m.procCWDCache[proc.PID]
		if !ok {
			resolved, err := m.deps.procCWD(ctx, proc.PID)
			if err != nil || resolved == "" {
				continue
			}
			cwd = resolved
			m.procCWDCache[proc.PID] = cwd
			lookups++
		}
		proc.CWD = cwd
		root := cwd
		if resolved := m.deps.worktree(ctx, cwd); resolved != "" {
			root = resolved
		}
		if !knownTrees[root] {
			continue
		}
		byTree[root] = append(byTree[root], proc)
	}
	for root, members := range byTree {
		// One process tree is one worker: a launcher and its harness child
		// (the devin wrapper shape) both surface in ps, but the pane owns
		// the whole tree. Collapse parent/child chains and count only the
		// topmost member, or every wrapper would double-count as unowned.
		memberPIDs := make(map[int64]bool, len(members))
		for _, proc := range members {
			memberPIDs[proc.PID] = true
		}
		var roots []stallProc
		for _, proc := range members {
			if memberPIDs[proc.PPID] {
				continue
			}
			roots = append(roots, proc)
		}
		excess := len(roots) - liveTrees[root]
		if excess <= 0 {
			continue
		}
		sort.Slice(roots, func(i, j int) bool { return roots[i].PID < roots[j].PID })
		for _, proc := range roots[:excess] {
			key := fmt.Sprintf("%d:%d", proc.PID, proc.StartedAt.Unix())
			row := stallUnownedRow{ProcKey: key, PID: proc.PID, PPID: proc.PPID, CWD: proc.CWD, StartedAt: proc.StartedAt, FirstSeenAt: at, Confirmations: 1}
			if existing, err := m.store.stallUnownedCandidates(ctx); err == nil {
				for _, candidate := range existing {
					if candidate.ProcKey == key {
						row.FirstSeenAt = candidate.FirstSeenAt
						row.Confirmations = candidate.Confirmations + 1
						row.Recorded = candidate.Recorded
					}
				}
			}
			if row.Confirmations >= 2 && !row.Recorded {
				row.Recorded = true
				m.openIncident(ctx, stallJobRow{PaneID: "", OwnerLane: ""}, stallCauseUnowned,
					stallMatch{line: "harness process without an owning job"}, at, true, map[string]any{
						"pid": proc.PID, "ppid": proc.PPID, "cwd": proc.CWD, "worktree": root,
						"started_at": proc.StartedAt.Format(time.RFC3339), "owning_job": "unknown",
					})
			}
			_ = m.store.upsertStallUnowned(ctx, row)
		}
	}
}

// cpuForDir measures the harness processes inside a directory. Unknown cwd
// stays unmeasured; the measurement never widens into a verdict.
func (m *stallDetectManager) cpuForDir(ctx context.Context, dir string) string {
	if dir == "" || m.deps.procs == nil || m.deps.procCWD == nil {
		return ""
	}
	procs, err := m.deps.procs(ctx)
	if err != nil {
		return ""
	}
	var pids []string
	for _, proc := range procs {
		cwd, ok := m.procCWDCache[proc.PID]
		if !ok {
			resolved, err := m.deps.procCWD(ctx, proc.PID)
			if err != nil {
				continue
			}
			cwd = resolved
			m.procCWDCache[proc.PID] = cwd
		}
		if strings.HasPrefix(cwd, dir) && len(pids) < 4 {
			pids = append(pids, strconv.FormatInt(proc.PID, 10))
		}
	}
	if len(pids) == 0 {
		return ""
	}
	out, err := exec.CommandContext(ctx, "ps", "-o", "%cpu=", "-p", strings.Join(pids, ",")).Output()
	if err != nil {
		return ""
	}
	return strings.Join(strings.Fields(string(out)), ",")
}

// persistReports finalizes terminal-job reports locally; upload to
// handoffkeep happens only when ReportUpload is enabled — shadow means zero
// outward side effects, so the default path never writes to the shared
// remote. Local finalize is atomic; the remote receipt is a separate column
// so an upload failure can never masquerade as worker incompletion.
//
// Every terminal job gets a stall_reports row — a skipped report is a
// recorded decision, never a silent drop: "remote_only" when the event named
// a handoffkeep document key, "no_report" when nothing was declared and the
// conventional jobs/<job>/report.md is absent, "stale" when the job ended
// before the retention window (a late finalize would fabricate history).
// Only the resolved local path is ever opened.
func (m *stallDetectManager) persistReports(ctx context.Context, at time.Time) {
	jobs, err := m.store.stallJobs(ctx, true)
	if err != nil {
		return
	}
	for _, job := range jobs {
		if !job.Terminal {
			continue
		}
		report, found, err := m.store.stallReport(ctx, job.JobID, job.Attempt)
		if err != nil {
			continue
		}
		if !found {
			switch {
			case job.ReportKind == stallReportRefDocKey:
				// The report body already lives in handoffkeep under this
				// key — the event's `report=` token named a document, not a
				// file. Recording the remote reference is the handling: a
				// key is never opened as a local path, and shadow fetches
				// nothing.
				_ = m.store.upsertStallReport(ctx, stallReportRow{
					JobID: job.JobID, Attempt: job.Attempt, ReportPath: job.ReportPath,
					RemoteKey: job.ReportPath, RemoteState: "remote_only",
				})
				continue
			case job.ReportPath == "":
				// A journal-terminal job that named no report and has no
				// jobs/<job>/report.md either. Dropping it silently was the
				// B2 defect: the row records exactly why nothing finalized.
				if stallTerminalKinds[job.TerminalKind] {
					_ = m.store.upsertStallReport(ctx, stallReportRow{
						JobID: job.JobID, Attempt: job.Attempt, RemoteState: "no_report",
						RemoteError: "no report declared and jobs/<job>/report.md absent",
					})
				}
				continue
			default:
				report = stallReportRow{JobID: job.JobID, Attempt: job.Attempt, ReportPath: job.ReportPath, RemoteState: "pending"}
			}
		}
		if report.RemoteState == "no_report" && job.ReportPath != "" {
			// The convention file materialized after the bookkeeping row
			// was recorded — upgrade it into a real finalize candidate.
			report.ReportPath = job.ReportPath
			report.RemoteState = "pending"
			report.RemoteError = ""
		}
		if report.RemoteState == "pending" && !job.LastEventAt.IsZero() && at.Sub(job.LastEventAt) > stallReportMaxAge {
			// The job finished long before this detector saw it; a late
			// finalize would fabricate history. The row records the skip.
			report.RemoteState = "stale"
			report.RemoteError = "terminal record older than the report window"
			_ = m.store.upsertStallReport(ctx, report)
			continue
		}
		if report.LocalPath == "" {
			switch report.RemoteState {
			case "pending", "missing_local", "finalize_failed":
				m.finalizeReport(ctx, &report, job, at)
			default:
				continue
			}
		}
		if report.LocalPath == "" {
			continue
		}
		if !m.cfg.ReportUpload {
			if report.RemoteState == "" || report.RemoteState == "pending" {
				report.RemoteState = "disabled"
				report.RemoteError = "shadow: upload off"
				_ = m.store.upsertStallReport(ctx, report)
			}
			continue
		}
		if report.RemoteState == "uploaded" {
			continue
		}
		m.uploadReport(ctx, &report, at)
	}
}

func (m *stallDetectManager) finalizeReport(ctx context.Context, report *stallReportRow, job stallJobRow, at time.Time) {
	contents, err := os.ReadFile(report.ReportPath)
	if err != nil || len(contents) > stallReportMaxBytes {
		report.RemoteState = "missing_local"
		report.RemoteError = "unreadable"
		_ = m.store.upsertStallReport(ctx, *report)
		return
	}
	sum := sha256.Sum256(contents)
	report.SHA256 = hex.EncodeToString(sum[:])
	dir := filepath.Join(m.reportDir, job.JobID, "final")
	if err := os.MkdirAll(dir, 0700); err != nil {
		return
	}
	final := filepath.Join(dir, fmt.Sprintf("report-%d.md", job.Attempt))
	temporary, err := os.CreateTemp(dir, ".report-*")
	if err != nil {
		return
	}
	name := temporary.Name()
	defer os.Remove(name)
	writeErr := func() error {
		if err := temporary.Chmod(0600); err != nil {
			return err
		}
		if _, err := temporary.Write(contents); err != nil {
			return err
		}
		if err := temporary.Sync(); err != nil {
			return err
		}
		return temporary.Close()
	}()
	if writeErr != nil {
		_ = temporary.Close()
		return
	}
	// The recorded hash must describe the bytes that actually landed. A write
	// can report success and still leave different bytes on disk, so the file
	// is re-read and re-hashed before it earns the final name; a mismatch is
	// a finalize failure, never a renamed truncated copy.
	disk, err := m.deps.readDisk(name)
	if err != nil {
		return
	}
	if sha256.Sum256(disk) != sum {
		report.RemoteState = "finalize_failed"
		report.RemoteError = "disk_hash_mismatch"
		_ = m.store.upsertStallReport(ctx, *report)
		return
	}
	if err := os.Rename(name, final); err != nil {
		return
	}
	report.LocalPath = final
	report.FinalizedAt = at
	_ = m.store.upsertStallReport(ctx, *report)
}

func (m *stallDetectManager) uploadReport(ctx context.Context, report *stallReportRow, at time.Time) {
	if m.deps.upload == nil {
		return
	}
	contents, err := os.ReadFile(report.LocalPath)
	if err != nil {
		return
	}
	// The remote body is the redacted copy. The local finalized file keeps the
	// verbatim record; what leaves the node has no secrets in it.
	redacted := redactSecrets(string(contents))
	key := fmt.Sprintf("report/%s/%d", report.JobID, report.Attempt)
	if err := m.deps.upload(ctx, key, []byte(redacted)); err != nil {
		report.RemoteState = "failed"
		report.RemoteError = "upload_failed"
		_ = m.store.upsertStallReport(ctx, *report)
		return
	}
	report.RemoteState = "uploaded"
	report.RemoteKey = key
	report.RemoteAt = at
	report.RemoteError = ""
	_ = m.store.upsertStallReport(ctx, *report)
}

// stallHandoffkeepUpload is the production upload path: the redacted body is
// written to a private temp file and handed to `handoffkeep doc put`, which
// returns the server's stored row. Upload is bounded and never retried
// inline; the caller's remote_state column is the retry cursor.
func stallHandoffkeepUpload(ctx context.Context, key string, body []byte) error {
	if key == "" {
		return errors.New("empty handoffkeep key")
	}
	temporary, err := os.CreateTemp("", "panewire-report-*")
	if err != nil {
		return err
	}
	name := temporary.Name()
	defer os.Remove(name)
	if _, err := temporary.Write(body); err != nil {
		_ = temporary.Close()
		return err
	}
	if err := temporary.Close(); err != nil {
		return err
	}
	putCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	return exec.CommandContext(putCtx, "handoffkeep", "doc", "put", "--key", key, "--kind", "report", "--file", name).Run()
}

// stallSecretPatterns is the bounded redaction set applied to anything that
// leaves the node: evidence excerpts, notification text, report uploads.
var stallSecretPatterns = []*regexp.Regexp{
	regexp.MustCompile(`(?i)bearer\s+[A-Za-z0-9._~+/=-]{8,}`),
	regexp.MustCompile(`\bsk-[A-Za-z0-9_-]{8,}`),
	regexp.MustCompile(`\b(ghp|gho|ghu|ghs|ghr)_[A-Za-z0-9]{16,}`),
	regexp.MustCompile(`\bgithub_pat_[A-Za-z0-9_]{20,}`),
	regexp.MustCompile(`\bxox[baprs]-[A-Za-z0-9-]{8,}`),
	regexp.MustCompile(`\bAKIA[0-9A-Z]{16}\b`),
	regexp.MustCompile(`eyJ[A-Za-z0-9_-]{10,}\.[A-Za-z0-9_-]{10,}\.[A-Za-z0-9_-]{5,}`),
	regexp.MustCompile(`(?i)(api[_-]?key|token|secret|password|passwd)\s*[:=]\s*["']?[A-Za-z0-9._~+/=-]{8,}`),
	regexp.MustCompile(`-----BEGIN [A-Z ]*PRIVATE KEY-----`),
}

// redactSecrets strips credential-shaped substrings. It is deliberately
// conservative about replacing whole matches: the goal is that no secret
// value survives, not that surrounding context stays pretty.
func redactSecrets(text string) string {
	for _, pattern := range stallSecretPatterns {
		text = pattern.ReplaceAllString(text, "[redacted]")
	}
	return text
}
