package panewire

// The inbox event index. Every hot path of the node daemon (the heartbeat's
// active-job scan, the relay outbox scan, the quota stop's pane lookup, the
// pane-to-herdr-session lookup) used to walk every job directory under
// <inbox>/jobs and re-read and re-parse every event file in it, on every call.
// With thousands of retained job directories that was the daemon's CPU.
//
// The index keeps what those scans derive from a job's events directory and
// refreshes it by the directory's own stat: a node's event files are written
// once, by create or rename, so a job's events can change only when its
// events directory's mtime changes. A pass therefore costs one stat per job
// directory plus a re-read of only the directories that changed; a job whose
// newest event is older than the active window is "dormant" and contributes no
// event to any scan, so its parsed events are dropped and only its stat is kept.
// The contract, refresh rule and bounds are in docs/job-event-index.md.

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
)

const (
	// hubIndexRacyWindow is how close to the read an events directory's mtime
	// may be before the read is not trusted to be final. A write that lands in
	// the same timestamp tick as the read leaves the mtime unchanged, so a read
	// that close to the mtime is repeated until the mtime is older than this
	// (the same rule git applies to "racily clean" index entries). It also
	// covers filesystems with whole-second mtimes.
	hubIndexRacyWindow = 2 * time.Second
	// hubIndexMaxRoots bounds the number of inbox roots indexed at once. A
	// daemon has one; the rest is test fixtures.
	hubIndexMaxRoots = 8
	// hubIndexByteBudget bounds the parsed event data one root's index keeps.
	// Data beyond it is served for the pass that read it and read again on the
	// next one, which is the pre-index cost for that data and nothing worse.
	hubIndexByteBudget = 48 << 20
	// hubIndexMaxLaneFiles bounds the lane-event directory listing kept.
	hubIndexMaxLaneFiles = 200_000
	// hubIndexMaxEventBytes is the largest event file the scanners read.
	hubIndexMaxEventBytes = 16 << 10

	hubIndexJobOverhead   = 512
	hubIndexEventOverhead = 1024
	hubIndexLaneOverhead  = 192
)

// hubIndexedEvent is one parsed event file of a job: the record plus the two
// derived values the scanners key on.
type hubIndexedEvent struct {
	name      string
	modTime   time.Time
	infoOK    bool
	eventTime time.Time
	kind      string
	event     hubInboxEvent
}

// hubPaneBindingFact is a job event that binds a pane to a herdr session.
type hubPaneBindingFact struct {
	pane      string
	session   string
	createdAt string
	seq       int64
}

// hubJobReduction is the time-independent result of folding a job's events in
// file order: the claim, the spawned pane and the newest event. The age checks
// that depend on the clock are applied per pass by scanned.
type hubJobReduction struct {
	active        HubActiveJob
	claimTime     time.Time
	lastEvent     time.Time
	lastEventKind string
	paneID        string
}

// hubIndexedJob is the retained view of one job's events directory. Once
// stored it is immutable: a refresh or a compaction replaces it.
type hubIndexedJob struct {
	id      string
	dir     os.FileInfo
	settled bool
	gen     uint64
	// compact marks a dormant job whose parsed events were dropped.
	compact   bool
	events    []hubIndexedEvent
	newest    time.Time
	reduction hubJobReduction
	// ended and bindings serve the pane-to-session lookup, which has no age
	// window and so keeps them for dormant jobs too.
	ended    bool
	bindings []hubPaneBindingFact
	cost     int64
}

type hubLaneFile struct {
	name    string
	modTime time.Time
	size    int64
	infoOK  bool
	parsed  bool
	ok      bool
	event   hubScannedRelayEvent
	cost    int64
}

// hubInboxIndex indexes one inbox root. All state is guarded by mu; the scans
// run under it, which also makes concurrent callers share one refresh.
type hubInboxIndex struct {
	mu      sync.Mutex
	jobsDir string
	laneDir string
	lastUse uint64
	// budget bounds jobBytes plus laneBytes.
	budget int64

	jobsInfo    os.FileInfo
	jobsSettled bool
	names       []string
	sweep       bool
	jobs        map[string]*hubIndexedJob
	jobBytes    int64
	gen         uint64

	laneInfo    os.FileInfo
	laneSettled bool
	lane        []hubLaneFile
	laneBytes   int64
}

var hubIndexRegistry struct {
	mu     sync.Mutex
	byRoot map[string]*hubInboxIndex
	tick   uint64
}

// hubIndexFor returns the index of an inbox root, creating it on first use.
func hubIndexFor(inboxRoot string) *hubInboxIndex {
	key := filepath.Clean(inboxRoot)
	registry := &hubIndexRegistry
	registry.mu.Lock()
	defer registry.mu.Unlock()
	registry.tick++
	if ix, ok := registry.byRoot[key]; ok {
		ix.lastUse = registry.tick
		return ix
	}
	if registry.byRoot == nil {
		registry.byRoot = make(map[string]*hubInboxIndex, hubIndexMaxRoots)
	}
	if len(registry.byRoot) >= hubIndexMaxRoots {
		var oldest string
		var oldestUse uint64
		for root, ix := range registry.byRoot {
			if oldest == "" || ix.lastUse < oldestUse {
				oldest, oldestUse = root, ix.lastUse
			}
		}
		delete(registry.byRoot, oldest)
	}
	ix := &hubInboxIndex{jobsDir: filepath.Join(key, "jobs"), laneDir: filepath.Join(key, "events-lane"), lastUse: registry.tick, jobs: map[string]*hubIndexedJob{}, budget: hubIndexByteBudget}
	registry.byRoot[key] = ix
	return ix
}

// hubDirUnchanged reports whether a directory is the one a read described: the same
// directory with the same mtime, and that read was not racy.
func hubDirUnchanged(prev, now os.FileInfo, settled bool) bool {
	return prev != nil && settled && os.SameFile(prev, now) && prev.ModTime().Equal(now.ModTime())
}

func hubDirSettled(started time.Time, info os.FileInfo) bool {
	return started.Sub(info.ModTime()) > hubIndexRacyWindow
}

// hubJobEventsLoad is what one read of a job's events directory yields.
type hubJobEventsLoad struct {
	events   []hubIndexedEvent
	ended    bool
	bindings []hubPaneBindingFact
	bytes    int64
}

func hubBindingEventName(name string) bool {
	return strings.HasSuffix(name, "-job.spawned.json") ||
		strings.HasSuffix(name, "-job.claim.json") ||
		strings.HasSuffix(name, "-job.claimed.json") ||
		strings.HasSuffix(name, "-job.reclaim.json")
}

// loadHubJobEvents reads one events directory in file-name order. A file that
// cannot be read, is over the size cap, or is not JSON is skipped, exactly as
// the scans always skipped it.
func loadHubJobEvents(eventsDir string) (hubJobEventsLoad, bool) {
	entries, err := os.ReadDir(eventsDir)
	if err != nil {
		return hubJobEventsLoad{}, false
	}
	var load hubJobEventsLoad
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || !strings.HasSuffix(name, ".json") {
			continue
		}
		// The end/restart markers come from the file name alone, as the
		// pane-binding lookup always read them, before the file is opened.
		switch {
		case strings.HasSuffix(name, "-job.completed.json") ||
			strings.HasSuffix(name, "-job.lost.json") ||
			strings.HasSuffix(name, "-job.revoked.json") ||
			strings.HasSuffix(name, "-job.handed_off.json") ||
			strings.HasSuffix(name, "-job.joined.json"):
			load.ended = true
		case strings.HasSuffix(name, "-job.spawned.json") ||
			strings.HasSuffix(name, "-job.claim.json") ||
			strings.HasSuffix(name, "-job.claimed.json") ||
			strings.HasSuffix(name, "-job.reclaim.json") ||
			strings.HasSuffix(name, "-job.reprompted.json"):
			load.ended = false
		}
		contents, err := os.ReadFile(filepath.Join(eventsDir, name))
		if err != nil {
			continue
		}
		if hubBindingEventName(name) {
			// The binding lookup never had the event size cap.
			var binding struct {
				Seq       int64  `json:"seq"`
				CreatedAt string `json:"created_at"`
				Payload   struct {
					PaneID       string `json:"pane_id"`
					HerdrSession string `json:"herdr_session"`
				} `json:"payload"`
			}
			if json.Unmarshal(contents, &binding) == nil && binding.Payload.PaneID != "" && binding.Payload.HerdrSession != "" {
				load.bindings = append(load.bindings, hubPaneBindingFact{pane: binding.Payload.PaneID, session: binding.Payload.HerdrSession, createdAt: binding.CreatedAt, seq: binding.Seq})
			}
		}
		if len(contents) > hubIndexMaxEventBytes {
			continue
		}
		var event hubInboxEvent
		if json.Unmarshal(contents, &event) != nil {
			continue
		}
		var modTime time.Time
		info, infoErr := entry.Info()
		if infoErr == nil {
			modTime = info.ModTime()
		}
		load.events = append(load.events, hubIndexedEvent{name: name, modTime: modTime, infoOK: infoErr == nil, eventTime: event.eventTimeAt(modTime, infoErr == nil), kind: event.eventKind(), event: event})
		load.bytes += int64(len(contents)) + hubIndexEventOverhead
	}
	return load, true
}

// reduceHubJobEvents folds a job's events, in file order, into its claim,
// spawned pane and newest event.
func reduceHubJobEvents(jobID string, events []hubIndexedEvent) hubJobReduction {
	var r hubJobReduction
	for i := range events {
		ev := &events[i]
		if !ev.eventTime.Before(r.lastEvent) {
			r.lastEvent = ev.eventTime
			r.lastEventKind = ev.kind
		}
		seq, seqOK := hubEventSequence(ev.name)
		switch ev.kind {
		case "job.claimed", "job.claim":
			epoch := ev.event.Epoch
			if epoch == 0 {
				epoch = 1
			}
			candidate := HubActiveJob{JobID: jobID, AgentLabel: ev.event.agentLabel(), LastEventSeq: seq, Epoch: epoch, OwnerLane: ev.event.ownerLane(), Tier: ev.event.tLevel(), Role: ev.event.role()}
			if !ev.eventTime.IsZero() {
				candidate.StartedAt = ev.eventTime.UTC().Format(time.RFC3339)
			}
			normalizeHubActiveJobMetadata(&candidate)
			if seqOK && validHubActiveJob(candidate) {
				r.active, r.claimTime = candidate, ev.eventTime
			}
		case "job.spawned":
			// The spawn record is the only place the worker pane is named.
			r.paneID = ev.event.paneID()
		default:
			if hubJobEndKind(ev.kind) {
				r.active, r.claimTime, r.paneID = HubActiveJob{}, time.Time{}, ""
			}
		}
	}
	return r
}

// scanned applies the clock-dependent checks to a reduction: the claim must be
// inside the active window, and a job that never named a pane must have been
// heard from inside the stale window (#770).
func (r hubJobReduction) scanned(activeCutoff, staleCutoff time.Time) (hubScannedJob, bool) {
	if !validHubActiveJob(r.active) || r.claimTime.Before(activeCutoff) {
		return hubScannedJob{}, false
	}
	if r.paneID == "" && !r.lastEvent.IsZero() && r.lastEvent.Before(staleCutoff) {
		return hubScannedJob{}, false
	}
	active := r.active
	active.Pane = r.paneID
	active.LastEventKind = r.lastEventKind
	if !r.lastEvent.IsZero() {
		active.LastEventAt = r.lastEvent.UTC().Format(time.RFC3339)
	}
	normalizeHubActiveJobMetadata(&active)
	return hubScannedJob{job: active, lastEvent: r.lastEvent, paneID: r.paneID}, true
}

func newHubIndexedJob(id string, dir os.FileInfo, settled bool, load hubJobEventsLoad) *hubIndexedJob {
	job := &hubIndexedJob{id: id, dir: dir, settled: settled, events: load.events, ended: load.ended, bindings: load.bindings, reduction: reduceHubJobEvents(id, load.events)}
	for i := range load.events {
		if load.events[i].eventTime.After(job.newest) {
			job.newest = load.events[i].eventTime
		}
	}
	job.cost = hubIndexJobOverhead + load.bytes + int64(len(load.bindings))*128
	return job
}

// dormant reports whether no scan can take anything from the job: its newest
// event is older than the active window, so the claim is, and every event is.
func (job *hubIndexedJob) dormant(activeCutoff time.Time) bool {
	return job.newest.Before(activeCutoff)
}

// jobNamesLocked lists the job directories, in directory order, reusing the
// last listing while the jobs directory is unchanged.
func (ix *hubInboxIndex) jobNamesLocked() ([]string, error) {
	info, err := os.Stat(ix.jobsDir)
	if err != nil {
		ix.resetJobsLocked()
		return nil, err
	}
	if hubDirUnchanged(ix.jobsInfo, info, ix.jobsSettled) {
		return ix.names, nil
	}
	started := time.Now()
	entries, err := os.ReadDir(ix.jobsDir)
	if err != nil {
		ix.resetJobsLocked()
		return nil, err
	}
	names := make([]string, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() && hubJobIDPattern.MatchString(entry.Name()) {
			names = append(names, entry.Name())
		}
	}
	ix.jobsInfo, ix.names, ix.jobsSettled = info, names, hubDirSettled(started, info)
	// A job directory that went away changes the jobs directory, so a changed
	// listing is the one moment a retained record can have lost its job.
	ix.sweep = true
	return names, nil
}

func (ix *hubInboxIndex) resetJobsLocked() {
	ix.jobsInfo, ix.names, ix.jobsSettled = nil, nil, false
	clear(ix.jobs)
	ix.jobBytes = 0
}

func (ix *hubInboxIndex) dropJobLocked(id string) {
	if prev, ok := ix.jobs[id]; ok {
		ix.jobBytes -= prev.cost
		delete(ix.jobs, id)
	}
}

// storeJobLocked retains a job within the byte budget; over it the job is only
// served for this pass.
func (ix *hubInboxIndex) storeJobLocked(job *hubIndexedJob) {
	var prevCost int64
	if prev, ok := ix.jobs[job.id]; ok {
		prevCost = prev.cost
	}
	if ix.jobBytes+ix.laneBytes-prevCost+job.cost > ix.budget {
		ix.dropJobLocked(job.id)
		return
	}
	ix.jobBytes += job.cost - prevCost
	job.gen = ix.gen
	ix.jobs[job.id] = job
}

// refreshJobLocked returns the job's current record: the retained one when its
// events directory is unchanged, else a fresh read. needEvents forces a read
// of a compacted job. A job with no readable events directory has no record.
func (ix *hubInboxIndex) refreshJobLocked(id string, needEvents bool) *hubIndexedJob {
	eventsDir := filepath.Join(ix.jobsDir, id, "events")
	info, err := os.Stat(eventsDir)
	if err != nil {
		ix.dropJobLocked(id)
		return nil
	}
	if prev := ix.jobs[id]; prev != nil && (!prev.compact || !needEvents) && hubDirUnchanged(prev.dir, info, prev.settled) {
		prev.gen = ix.gen
		return prev
	}
	started := time.Now()
	load, ok := loadHubJobEvents(eventsDir)
	if !ok {
		ix.dropJobLocked(id)
		return nil
	}
	job := newHubIndexedJob(id, info, hubDirSettled(started, info), load)
	ix.storeJobLocked(job)
	return job
}

// compactJobLocked drops a dormant job's parsed events, keeping what the
// pane lookup needs and the stat that detects a change.
func (ix *hubInboxIndex) compactJobLocked(job *hubIndexedJob) *hubIndexedJob {
	if ix.jobs[job.id] != job {
		return job
	}
	compact := &hubIndexedJob{id: job.id, dir: job.dir, settled: job.settled, compact: true, newest: job.newest, ended: job.ended, bindings: job.bindings}
	compact.cost = hubIndexJobOverhead + int64(len(job.bindings))*128
	ix.jobBytes += compact.cost - job.cost
	compact.gen = job.gen
	ix.jobs[job.id] = compact
	return compact
}

// visitJobs calls fn for each job directory, in directory order, with its
// current record. Dormant jobs, which no scan takes anything from, are visited
// only when includeDormant is set, and then without their events. fn runs
// under the index lock and must stay in memory. A nil ctx never cancels. The
// error is the jobs directory's read error or the context's.
func (ix *hubInboxIndex) visitJobs(ctx context.Context, includeDormant bool, fn func(*hubIndexedJob) bool) error {
	ix.mu.Lock()
	defer ix.mu.Unlock()
	names, err := ix.jobNamesLocked()
	if err != nil {
		return err
	}
	activeCutoff := time.Now().Add(-hubJobActiveMaxAge())
	ix.gen++
	for _, id := range names {
		if ctx != nil {
			if err := ctx.Err(); err != nil {
				return err
			}
		}
		job := ix.refreshJobLocked(id, false)
		if job == nil {
			continue
		}
		if job.dormant(activeCutoff) {
			if !job.compact {
				job = ix.compactJobLocked(job)
			}
			if !includeDormant {
				continue
			}
		} else if job.compact {
			// The active window grew since the job was compacted.
			if job = ix.refreshJobLocked(id, true); job == nil {
				continue
			}
		}
		if !fn(job) {
			return nil
		}
	}
	if ix.sweep {
		for id, job := range ix.jobs {
			if job.gen != ix.gen {
				ix.jobBytes -= job.cost
				delete(ix.jobs, id)
			}
		}
		ix.sweep = false
	}
	return nil
}

// laneEvents is the lane-event scan over the index: the same files, filters and
// order as reading events-lane afresh, with files parsed once.
func (ix *hubInboxIndex) laneEvents(maxAge time.Duration) []hubScannedRelayEvent {
	ix.mu.Lock()
	defer ix.mu.Unlock()
	files, ok := ix.laneFilesLocked()
	if !ok {
		return nil
	}
	var cutoff time.Time
	if maxAge > 0 {
		cutoff = time.Now().Add(-maxAge)
	}
	out := make([]hubScannedRelayEvent, 0, 8)
	for i := range files {
		file := &files[i]
		if !cutoff.IsZero() && (!file.infoOK || file.modTime.Before(cutoff)) {
			continue
		}
		event, ok := file.event, file.ok
		if !file.parsed {
			var cost int64
			event, ok, cost = readLaneEventFile(filepath.Join(ix.laneDir, file.name))
			if ix.laneInfo != nil && ix.jobBytes+ix.laneBytes+cost <= ix.budget {
				file.parsed, file.ok, file.event, file.cost = true, ok, event, cost
				ix.laneBytes += cost
			}
		}
		if ok {
			out = append(out, event)
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].OwnerLane == out[j].OwnerLane {
			return out[i].EventID < out[j].EventID
		}
		return out[i].OwnerLane < out[j].OwnerLane
	})
	return out
}

// laneFilesLocked lists the lane-event files in name order. While the
// directory is unchanged the previous listing, with its parsed events, stands;
// after a change only new or modified files are parsed again.
func (ix *hubInboxIndex) laneFilesLocked() ([]hubLaneFile, bool) {
	info, err := os.Stat(ix.laneDir)
	if err != nil {
		ix.resetLaneLocked()
		return nil, false
	}
	if hubDirUnchanged(ix.laneInfo, info, ix.laneSettled) {
		return ix.lane, true
	}
	started := time.Now()
	entries, err := os.ReadDir(ix.laneDir)
	if err != nil {
		ix.resetLaneLocked()
		return nil, false
	}
	prev := ix.lane
	files := make([]hubLaneFile, 0, len(entries))
	var bytes int64
	p := 0
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || !strings.HasSuffix(name, ".json") {
			continue
		}
		file := hubLaneFile{name: name}
		if fi, err := entry.Info(); err == nil {
			file.modTime, file.size, file.infoOK = fi.ModTime(), fi.Size(), true
		}
		for p < len(prev) && prev[p].name < name {
			p++
		}
		if p < len(prev) && prev[p].name == name && prev[p].parsed && file.infoOK && prev[p].infoOK && prev[p].modTime.Equal(file.modTime) && prev[p].size == file.size {
			file = prev[p]
		}
		bytes += hubIndexLaneOverhead + file.cost
		files = append(files, file)
	}
	if len(files) > hubIndexMaxLaneFiles {
		// Too many to keep: serve this pass from the listing, keep nothing.
		ix.resetLaneLocked()
		return files, true
	}
	if ix.jobBytes+bytes > ix.budget {
		// Over budget: drop the parsed events, which are read again on demand.
		for i := range files {
			bytes -= files[i].cost
			files[i] = hubLaneFile{name: files[i].name, modTime: files[i].modTime, size: files[i].size, infoOK: files[i].infoOK}
		}
		if ix.jobBytes+bytes > ix.budget {
			ix.resetLaneLocked()
			return files, true
		}
	}
	ix.laneInfo, ix.laneSettled, ix.lane, ix.laneBytes = info, hubDirSettled(started, info), files, bytes
	return files, true
}

func (ix *hubInboxIndex) resetLaneLocked() {
	ix.laneInfo, ix.laneSettled, ix.lane, ix.laneBytes = nil, false, nil, 0
}

// readLaneEventFile parses one lane-event file; the bool is false for a file
// the scan skips. The cost is what keeping the result retains.
func readLaneEventFile(path string) (hubScannedRelayEvent, bool, int64) {
	contents, err := os.ReadFile(path)
	if err != nil || len(contents) > hubIndexMaxEventBytes {
		return hubScannedRelayEvent{}, false, hubIndexLaneOverhead
	}
	var event hubInboxEvent
	if json.Unmarshal(contents, &event) != nil || event.eventKind() != "lane.event" {
		return hubScannedRelayEvent{}, false, hubIndexLaneOverhead
	}
	lane, eventID, text := event.ownerLane(), event.eventID(), event.text()
	if !validInboxCause(event.Cause) || !hubAgentLabelPattern.MatchString(lane) || !validLaneEventID(eventID) || !validLaneEventText(text) || len(text) > laneEventTextLimitSink {
		return hubScannedRelayEvent{}, false, hubIndexLaneOverhead
	}
	epoch := event.Epoch
	if epoch == 0 {
		epoch = 1
	}
	return hubScannedRelayEvent{DeliveryClass: event.DeliveryClass, NotifyType: event.notifyType(), Cause: event.Cause, Head: event.head(), PR: event.pr(), Kind: "lane.event", HubActiveJob: HubActiveJob{JobID: laneEventTransportID(lane, eventID), Epoch: epoch, OwnerLane: lane, Label: event.label(), Host: event.host()}, PaneID: event.paneID(), EventID: eventID, Text: text, Truncated: event.truncated()}, true, hubIndexLaneOverhead + int64(len(contents)) + hubIndexEventOverhead
}
