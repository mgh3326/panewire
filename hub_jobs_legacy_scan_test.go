package panewire

// The functions below are the pre-index implementations of the job and lane
// event scans, kept verbatim (renamed) as the reference the index is proven
// against: every pass walked every job directory and re-read every event file.
// TestHubInboxIndexMatchesFullScan compares the index to them byte for byte and
// the benchmarks time both.

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

func legacyScanHubActiveJobsForLimit(ctx context.Context, inboxRoot string, panesAlive panesAliveFunc, limit int) []HubActiveJob {
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
		if scanned, ok := legacyScanHubJobEventDetails(filepath.Join(inboxRoot, "jobs", entry.Name(), "events"), entry.Name()); ok {
			jobs = append(jobs, scanned)
		}
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

func legacyScanHubRelayEventsWithin(inboxRoot string, maxAge time.Duration) []hubScannedRelayEvent {
	if inboxRoot == "" {
		return nil
	}
	var mtimeCutoff time.Time
	if maxAge > 0 {
		mtimeCutoff = time.Now().Add(-maxAge)
	}
	entries, err := os.ReadDir(filepath.Join(inboxRoot, "jobs"))
	if err != nil {
		return legacyScanHubLaneEventsWithin(inboxRoot, maxAge)
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
			if json.Unmarshal(contents, &event) != nil {
				continue
			}
			eventTime := event.eventTime(file, dir)
			if eventTime.Before(time.Now().Add(-hubJobActiveMaxAge())) {
				continue
			}
			kind := event.eventKind()
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
			if relayTerminalSignalKinds[kind] && !validReportRelayLaneName(event.ownerLane()) && !(kind == "job.lost" && event.reason() == "quota_stop" && event.Payload.Pool != "") {
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
				if legacyJobHasFlatCompletion(dir, files, epoch) || time.Since(eventTime) < lifecycleEnvelopeSettle {
					continue
				}
				ownerLane, reportPath = event.lifecycleRouteLane(entry.Name()), event.Payload.Report
			}
			if relayEventPathFallbackKinds[kind] && reportPath == "" {
				// The event is the durable full-question record when no separate
				// report exists. The hub payload must point operators back to it,
				// and `panewire emit` substitutes the very same path.
				reportPath = filepath.Join(dir, file.Name())
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
			events = append(events, hubScannedRelayEvent{DeliveryClass: event.DeliveryClass, Late: event.Late || event.Payload.Late, NotifyType: event.notifyType(), Cause: event.Cause, Kind: kind, HubActiveJob: HubActiveJob{JobID: entry.Name(), Epoch: epoch, AgentLabel: agentLabel, OwnerLane: ownerLane, Label: event.label(), Host: event.host(), ReportPath: reportPath, ReportLastLine: event.reportLastLine()}, Reason: reason, Question: event.question(), PR: event.pr(), Head: event.head(), PaneID: event.paneID(), EventID: file.Name(), EventTime: eventTime})
		}
	}
	sort.Slice(events, func(i, j int) bool {
		if events[i].JobID == events[j].JobID {
			return events[i].Kind < events[j].Kind
		}
		return events[i].JobID < events[j].JobID
	})
	return append(events, legacyScanHubLaneEventsWithin(inboxRoot, maxAge)...)
}

func legacyScanHubLaneEventsWithin(inboxRoot string, maxAge time.Duration) []hubScannedRelayEvent {
	dir := filepath.Join(inboxRoot, "events-lane")
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil
	}
	var cutoff time.Time
	if maxAge > 0 {
		cutoff = time.Now().Add(-maxAge)
	}
	out := make([]hubScannedRelayEvent, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".json") {
			continue
		}
		if !cutoff.IsZero() {
			info, statErr := entry.Info()
			if statErr != nil || info.ModTime().Before(cutoff) {
				continue
			}
		}
		contents, err := os.ReadFile(filepath.Join(dir, entry.Name()))
		if err != nil || len(contents) > 16<<10 {
			continue
		}
		var event hubInboxEvent
		if json.Unmarshal(contents, &event) != nil || event.eventKind() != "lane.event" {
			continue
		}
		lane, eventID, text := event.ownerLane(), event.eventID(), event.text()
		if !validInboxCause(event.Cause) || !hubAgentLabelPattern.MatchString(lane) || !validLaneEventID(eventID) || !validLaneEventText(text) || len(text) > laneEventTextLimitSink {
			continue
		}
		epoch := event.Epoch
		if epoch == 0 {
			epoch = 1
		}
		out = append(out, hubScannedRelayEvent{DeliveryClass: event.DeliveryClass, NotifyType: event.notifyType(), Cause: event.Cause, Head: event.head(), PR: event.pr(), Kind: "lane.event", HubActiveJob: HubActiveJob{JobID: laneEventTransportID(lane, eventID), Epoch: epoch, OwnerLane: lane, Label: event.label(), Host: event.host()}, PaneID: event.paneID(), EventID: eventID, Text: text, Truncated: event.truncated()})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].OwnerLane == out[j].OwnerLane {
			return out[i].EventID < out[j].EventID
		}
		return out[i].OwnerLane < out[j].OwnerLane
	})
	return out
}

func legacyJobHasFlatCompletion(dir string, files []os.DirEntry, epoch uint64) bool {
	for _, file := range files {
		if file.IsDir() || !strings.HasSuffix(file.Name(), ".json") {
			continue
		}
		contents, err := os.ReadFile(filepath.Join(dir, file.Name()))
		if err != nil || len(contents) > 16<<10 {
			continue
		}
		var event hubInboxEvent
		if json.Unmarshal(contents, &event) != nil || event.OwnerLane == "" {
			continue
		}
		kind, recordEpoch := event.eventKind(), event.Epoch
		if recordEpoch == 0 {
			recordEpoch = 1
		}
		if (kind == "job.completed" || kind == "job.completion") && recordEpoch == epoch {
			return true
		}
	}
	return false
}

func legacyScanHubJobEventDetails(eventsDir, jobID string) (hubScannedJob, bool) {
	entries, err := os.ReadDir(eventsDir)
	if err != nil {
		return hubScannedJob{}, false
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].Name() < entries[j].Name() })
	var active HubActiveJob
	var claimTime, lastEvent time.Time
	var lastEventKind string
	var paneID string
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
		if !eventTime.Before(lastEvent) {
			lastEvent = eventTime
			lastEventKind = event.eventKind()
		}
		seq, seqOK := hubEventSequence(entry.Name())
		switch event.eventKind() {
		case "job.claimed", "job.claim":
			epoch := event.Epoch
			if epoch == 0 {
				epoch = 1
			}
			candidate := HubActiveJob{JobID: jobID, AgentLabel: event.agentLabel(), LastEventSeq: seq, Epoch: epoch, OwnerLane: event.ownerLane(), Tier: event.tLevel(), Role: event.role()}
			if !eventTime.IsZero() {
				candidate.StartedAt = eventTime.UTC().Format(time.RFC3339)
			}
			normalizeHubActiveJobMetadata(&candidate)
			if seqOK && validHubActiveJob(candidate) {
				active, claimTime = candidate, eventTime
			}
		case "job.spawned":
			// The spawn record is the only place the worker pane is named.
			paneID = event.paneID()
		default:
			if hubJobEndKind(event.eventKind()) {
				active, claimTime, paneID = HubActiveJob{}, time.Time{}, ""
			}
		}
	}
	if !validHubActiveJob(active) || claimTime.Before(time.Now().Add(-hubJobActiveMaxAge())) {
		return hubScannedJob{}, false
	}
	// #770: a job that never recorded a pane has no liveness cross-check at
	// all — a pane-named job is dropped by filterActiveJobsByPane the moment
	// its pane dies, but a paneless claim only has event freshness. Past the
	// stale age it leaves the active set; the local records are untouched.
	if paneID == "" && !lastEvent.IsZero() && lastEvent.Before(time.Now().Add(-hubJobStaleAge())) {
		return hubScannedJob{}, false
	}
	active.Pane = paneID
	active.LastEventKind = lastEventKind
	if !lastEvent.IsZero() {
		active.LastEventAt = lastEvent.UTC().Format(time.RFC3339)
	}
	normalizeHubActiveJobMetadata(&active)
	return hubScannedJob{job: active, lastEvent: lastEvent, paneID: paneID}, true
}

func legacyNodeJobHerdrBindingByPaneFromRoot(inboxRoot, pane string) (nodePaneBinding, bool) {
	if pane == "" || inboxRoot == "" {
		return nodePaneBinding{}, false
	}
	jobsRoot := filepath.Join(inboxRoot, "jobs")
	entries, err := os.ReadDir(jobsRoot)
	if err != nil {
		return nodePaneBinding{}, false
	}
	type paneCandidate struct {
		jobID       string
		session     string
		eventsMtime time.Time
		live        bool
		createdAt   string
		seq         int64
	}
	var best paneCandidate
	found := false

	for _, entry := range entries {
		if !entry.IsDir() || !hubJobIDPattern.MatchString(entry.Name()) {
			continue
		}
		eventsDir := filepath.Join(jobsRoot, entry.Name(), "events")
		eventEntries, err := os.ReadDir(eventsDir)
		if err != nil {
			continue
		}
		var eventsMtime time.Time
		if fi, err := os.Stat(eventsDir); err == nil {
			eventsMtime = fi.ModTime()
		}
		sort.Slice(eventEntries, func(i, j int) bool {
			return eventEntries[i].Name() < eventEntries[j].Name()
		})

		jobEnded := false
		var jobSession string
		var jobCreatedAt string
		var jobSeq int64
		jobMatched := false

		for _, ee := range eventEntries {
			name := ee.Name()
			if ee.IsDir() || !strings.HasSuffix(name, ".json") {
				continue
			}
			if strings.HasSuffix(name, "-job.completed.json") ||
				strings.HasSuffix(name, "-job.lost.json") ||
				strings.HasSuffix(name, "-job.revoked.json") ||
				strings.HasSuffix(name, "-job.handed_off.json") ||
				strings.HasSuffix(name, "-job.joined.json") {
				jobEnded = true
			}
			if strings.HasSuffix(name, "-job.spawned.json") ||
				strings.HasSuffix(name, "-job.claim.json") ||
				strings.HasSuffix(name, "-job.claimed.json") ||
				strings.HasSuffix(name, "-job.reclaim.json") ||
				strings.HasSuffix(name, "-job.reprompted.json") {
				jobEnded = false
			}
			if strings.HasSuffix(name, "-job.spawned.json") ||
				strings.HasSuffix(name, "-job.claim.json") ||
				strings.HasSuffix(name, "-job.claimed.json") ||
				strings.HasSuffix(name, "-job.reclaim.json") {
				data, err := os.ReadFile(filepath.Join(eventsDir, name))
				if err != nil {
					continue
				}
				var ev struct {
					Seq       int64  `json:"seq"`
					CreatedAt string `json:"created_at"`
					Payload   struct {
						PaneID       string `json:"pane_id"`
						HerdrSession string `json:"herdr_session"`
					} `json:"payload"`
				}
				if json.Unmarshal(data, &ev) == nil && ev.Payload.PaneID == pane && ev.Payload.HerdrSession != "" {
					jobMatched = true
					jobSession = ev.Payload.HerdrSession
					jobCreatedAt = ev.CreatedAt
					jobSeq = ev.Seq
				}
			}
		}

		if jobMatched && jobSession != "" {
			cand := paneCandidate{
				jobID:       entry.Name(),
				session:     jobSession,
				eventsMtime: eventsMtime,
				live:        !jobEnded,
				createdAt:   jobCreatedAt,
				seq:         jobSeq,
			}
			if !found {
				best = cand
				found = true
			} else {
				if (!best.live && cand.live) ||
					(best.live == cand.live && (cand.createdAt > best.createdAt || (cand.createdAt == best.createdAt && cand.seq > best.seq))) {
					best = cand
				}
			}
		}
	}
	if found {
		return nodePaneBinding{
			jobID:   best.jobID,
			session: best.session,
		}, true
	}
	return nodePaneBinding{}, false
}

func legacyJobForPane(ctx context.Context, root string, pane string) (HubActiveJob, bool) {
	var found HubActiveJob
	count := 0
	entries, err := os.ReadDir(filepath.Join(root, "jobs"))
	if err != nil {
		return found, false
	}
	for _, entry := range entries {
		if ctx.Err() != nil {
			return HubActiveJob{}, false
		}
		if !entry.IsDir() || !hubJobIDPattern.MatchString(entry.Name()) {
			continue
		}
		scanned, ok := legacyScanHubJobEventDetails(filepath.Join(root, "jobs", entry.Name(), "events"), entry.Name())
		if ok && scanned.job.Pane == pane {
			found = scanned.job
			count++
		}
	}
	return found, count == 1 && found.JobID != "" && !quotaExcluded(found.AgentLabel)
}
