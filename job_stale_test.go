package panewire

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// #770: a never-ended job record stops counting as active once nothing fresh
// vouches for it, but the record itself is never deleted — local event files
// stay on disk and the hub keeps its in-memory copy for review.

func TestJobStaleLocalScanDropsPanelessSilentJob(t *testing.T) {
	// Stale age is deliberately tiny so the fixture controls the verdict with
	// fixed synthetic timestamps rather than wall-clock sleeps.
	t.Setenv("PANEWIRE_JOB_STALE_AGE", "1h")
	root := t.TempDir()
	write := func(job, name, body string) {
		events := filepath.Join(root, "jobs", job, "events")
		if err := os.MkdirAll(events, 0700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(events, name), []byte(body), 0600); err != nil {
			t.Fatal(err)
		}
	}
	// Paneless and silent past the stale age: no terminal, no pane to
	// cross-check, no fresh event. This is the 104-task-less-active-jobs
	// shape. Timestamps sit between hubJobStaleAge and hubJobActiveMaxAge —
	// older than 72h would exit at the claim-age gate and never reach the
	// paneless stale filter, making this assertion vacuous (CodeRabbit).
	now := time.Now().UTC()
	staleClaim := now.Add(-2 * time.Hour).Format(time.RFC3339)
	staleProgress := now.Add(-90 * time.Minute).Format(time.RFC3339)
	write("job-stale-paneless", "00001-job.claimed.json",
		`{"type":"job.claimed","created_at":"`+staleClaim+`","agent_label":"wrk-a","owner_lane":"lane-a","epoch":1}`)
	write("job-stale-paneless", "00002-job.progress.json",
		`{"type":"job.progress","created_at":"`+staleProgress+`"}`)
	// Paneless but still emitting: fresh events keep it active.
	write("job-fresh-paneless", "00001-job.claimed.json",
		`{"type":"job.claimed","created_at":"`+time.Now().Add(-time.Minute).UTC().Format(time.RFC3339)+`","agent_label":"wrk-b","owner_lane":"lane-a","epoch":1}`)

	jobs := scanHubActiveJobs(root)
	ids := make(map[string]bool, len(jobs))
	for _, job := range jobs {
		ids[job.JobID] = true
	}
	if ids["job-stale-paneless"] {
		t.Fatalf("a paneless job silent past the stale age must leave the active set: %+v", jobs)
	}
	if !ids["job-fresh-paneless"] {
		t.Fatalf("a paneless job with fresh events must stay active: %+v", jobs)
	}
	// The local record itself is retained — exclusion is a view, not a delete.
	if _, err := os.Stat(filepath.Join(root, "jobs", "job-stale-paneless", "events", "00001-job.claimed.json")); err != nil {
		t.Fatalf("stale job records must be retained: %v", err)
	}
}

func TestJobStaleHubRegistryDropsSilentRecord(t *testing.T) {
	t.Setenv("PANEWIRE_JOB_STALE_AGE", "30s")
	clock := time.Date(2026, 9, 27, 6, 0, 0, 0, time.UTC)
	hub, err := NewHubServer(HubServerConfig{
		Tokens: map[string]string{"operator": r6OperatorToken, "node-a": r6NodeAToken},
		Now:    func() time.Time { return clock }, GracePeriod: time.Hour, OrphanGrace: time.Hour,
		KeepaliveInterval: time.Hour,
	})
	if err != nil {
		t.Fatal(err)
	}
	agent := &hubAgent{revocations: make(chan hubJobRevokedEvent, 1)}
	hub.connect("node-a", "t770", "fixture", agent, false)
	stale := HubActiveJob{JobID: "job-stale", AgentLabel: "wrk-a", LastEventSeq: 1, Epoch: 1}
	live := HubActiveJob{JobID: "job-live", AgentLabel: "wrk-b", LastEventSeq: 1, Epoch: 1}
	hub.observeActiveJobs("node-a", []HubActiveJob{stale, live}, clock)

	clock = clock.Add(31 * time.Second)
	// A heartbeat still refreshes the live job's LastSeen — only the silent
	// one ages out.
	hub.observeActiveJobs("node-a", []HubActiveJob{live}, clock)

	hub.mu.Lock()
	active := hub.activeJobRecordsLocked()
	hub.mu.Unlock()
	for _, job := range active {
		if job.JobID == "job-stale" {
			t.Fatalf("silent past the stale age must leave the active set: %+v", active)
		}
	}
	foundLive := false
	for _, job := range active {
		foundLive = foundLive || job.JobID == "job-live"
	}
	if !foundLive {
		t.Fatalf("a heartbeated job must stay active: %+v", active)
	}
	// Retained, not deleted: the hub still holds the record for review.
	hub.mu.Lock()
	_, kept := hub.jobs["job-stale"]
	hub.mu.Unlock()
	if !kept {
		t.Fatal("a stale job record must be retained in the hub registry")
	}
	// And it is not visible to the console's active list either.
	for _, job := range hub.activeConsoleJobs("node-a") {
		if job.JobID == "job-stale" {
			t.Fatalf("console active list must omit stale jobs: %+v", job)
		}
	}
}

func TestJobStaleHubRegistryNeverDropsOrphan(t *testing.T) {
	t.Setenv("PANEWIRE_JOB_STALE_AGE", "30s")
	clock := time.Date(2026, 9, 27, 6, 0, 0, 0, time.UTC)
	hub, err := NewHubServer(HubServerConfig{
		Tokens: map[string]string{"operator": r6OperatorToken, "node-a": r6NodeAToken},
		Now:    func() time.Time { return clock }, GracePeriod: time.Second, OrphanGrace: time.Second,
		KeepaliveInterval: time.Hour,
	})
	if err != nil {
		t.Fatal(err)
	}
	agent := &hubAgent{revocations: make(chan hubJobRevokedEvent, 1)}
	hub.connect("node-a", "t770", "fixture", agent, false)
	job := HubActiveJob{JobID: "job-orphan", AgentLabel: "wrk-a", LastEventSeq: 1, Epoch: 1}
	hub.observeActiveJobs("node-a", []HubActiveJob{job}, clock)
	hub.disconnect("node-a", agent)
	clock = clock.Add(2 * time.Second)
	hub.sweepOrphanedJobs(clock)
	if got := hub.orphanedJobs(); len(got) != 1 {
		t.Fatalf("expected one orphan, got %+v", got)
	}
	// Far past both the orphan grace and the stale age: orphaned is a distinct
	// operator-visible state, never silently dropped from the active set.
	clock = clock.Add(time.Hour)
	hub.sweepOrphanedJobs(clock)
	if got := hub.orphanedJobs(); len(got) != 1 || got[0].JobID != "job-orphan" {
		t.Fatalf("an orphaned job must stay listed regardless of age: %+v", got)
	}
}

func TestJobStaleAgeEnvOverride(t *testing.T) {
	t.Setenv("PANEWIRE_JOB_STALE_AGE", "")
	if hubJobStaleAge() != defaultHubJobStaleAge {
		t.Fatalf("unset env must use the default: %s", hubJobStaleAge())
	}
	t.Setenv("PANEWIRE_JOB_STALE_AGE", "not-a-duration")
	if hubJobStaleAge() != defaultHubJobStaleAge {
		t.Fatalf("an unparsable env must fall back, never disable the check: %s", hubJobStaleAge())
	}
	t.Setenv("PANEWIRE_JOB_STALE_AGE", "6h")
	if hubJobStaleAge() != 6*time.Hour {
		t.Fatalf("env override ignored: %s", hubJobStaleAge())
	}
}

// The wrk-side half of AC5 lives in bin/wrk's prune dry-run; here we only pin
// that the marker wrk prune writes is one the local replay already treats as
// an end — a job.revoked record removes the job from the active scan.
func TestJobRevokedMarkerLeavesActiveScan(t *testing.T) {
	root := t.TempDir()
	events := filepath.Join(root, "jobs", "job-marked", "events")
	if err := os.MkdirAll(events, 0700); err != nil {
		t.Fatal(err)
	}
	write := func(name, body string) {
		if err := os.WriteFile(filepath.Join(events, name), []byte(body), 0600); err != nil {
			t.Fatal(err)
		}
	}
	write("00001-job.claimed.json",
		`{"type":"job.claimed","created_at":"`+time.Now().UTC().Format(time.RFC3339)+`","agent_label":"wrk-a","owner_lane":"lane-a","epoch":1}`)
	if jobs := scanHubActiveJobs(root); len(jobs) != 1 {
		t.Fatalf("claimed job must scan active before the marker: %+v", jobs)
	}
	write("00002-job.revoked.json",
		`{"type":"job.revoked","created_at":"`+time.Now().UTC().Format(time.RFC3339)+`","job_id":"job-marked","outcome":"abandoned","reason":"stale: silent, pane absent","source":"wrk prune"}`)
	if jobs := scanHubActiveJobs(root); len(jobs) != 0 {
		t.Fatalf("the prune end declaration must remove the job from active replay: %+v", jobs)
	}
	if !strings.Contains(filepath.Join(events), "job-marked") {
		t.Fatal("unreachable")
	}
}
