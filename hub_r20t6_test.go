package panewire

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// writeR20T6Job lays down the arbiter-shaped records a claimed job leaves in
// the inbox. An empty pane omits the spawn record entirely, which is the
// "pane unknown" case.
func writeR20T6Job(t *testing.T, root, jobID, pane string, at time.Time) {
	t.Helper()
	dir := filepath.Join(root, "jobs", jobID, "events")
	if err := os.MkdirAll(dir, 0700); err != nil {
		t.Fatal(err)
	}
	stamp := at.UTC().Format(time.RFC3339)
	claim := fmt.Sprintf(`{"created_at":%q,"job_id":%q,"kind":"job.claim","payload":{"agent_label":"wrk-a","owner_lane":"lane-a","t_level":"T1"},"seq":1}`, stamp, jobID)
	if err := os.WriteFile(filepath.Join(dir, "00001-job.claim.json"), []byte(claim), 0600); err != nil {
		t.Fatal(err)
	}
	if pane == "" {
		return
	}
	spawned := fmt.Sprintf(`{"created_at":%q,"job_id":%q,"kind":"job.spawned","payload":{"label":"wrk-a","pane_id":%q,"profile":"profile-a","workspace":"w1"},"seq":2}`, stamp, jobID, pane)
	if err := os.WriteFile(filepath.Join(dir, "00002-job.spawned.json"), []byte(spawned), 0600); err != nil {
		t.Fatal(err)
	}
}

func aliveR20T6Hook(panes ...string) (panesAliveFunc, *int) {
	calls := 0
	alive := make(map[string]bool, len(panes))
	for _, pane := range panes {
		alive[pane] = true
	}
	return func(context.Context) (map[string]bool, error) {
		calls++
		return alive, nil
	}, &calls
}

func jobIDsR20T6(jobs []HubActiveJob) []string {
	ids := make([]string, 0, len(jobs))
	for _, job := range jobs {
		ids = append(ids, job.JobID)
	}
	return ids
}

// TL1/TL2/TL3: a pane herdr no longer lists is stale; a listed pane and an
// unnamed pane both stay active.
func TestR20T6ScanDropsDeadPanesAndKeepsUnknownOnes(t *testing.T) {
	root := t.TempDir()
	now := time.Now()
	writeR20T6Job(t, root, "job-dead", "w1:p1A", now)
	writeR20T6Job(t, root, "job-live", "w1:p1B", now)
	writeR20T6Job(t, root, "job-nopane", "", now)
	hook, calls := aliveR20T6Hook("w1:p1B")
	jobs := scanHubActiveJobsWithPanes(context.Background(), root, hook)
	got := strings.Join(jobIDsR20T6(jobs), ",")
	if got != "job-live,job-nopane" {
		t.Fatalf("TL1/TL2/TL3: pane cross-check kept the wrong set: %v", got)
	}
	if *calls != 1 {
		t.Fatalf("TL7: agent.list was called %d times, want 1", *calls)
	}
}

// TL4: an unavailable liveness source must not shrink the active set. Failing
// the other way would let placement overload this machine.
func TestR20T6ScanKeepsEverythingWhenLookupFails(t *testing.T) {
	root := t.TempDir()
	now := time.Now()
	writeR20T6Job(t, root, "job-dead", "w1:p1A", now)
	writeR20T6Job(t, root, "job-live", "w1:p1B", now)
	calls := 0
	failing := func(context.Context) (map[string]bool, error) {
		calls++
		return nil, errors.New("herdr unavailable")
	}
	jobs := scanHubActiveJobsWithPanes(context.Background(), root, failing)
	if got := strings.Join(jobIDsR20T6(jobs), ","); got != "job-dead,job-live" {
		t.Fatalf("TL4: a failed lookup dropped jobs: %v", got)
	}
	if calls != 1 {
		t.Fatalf("TL4: lookup called %d times, want 1", calls)
	}
	slow := func(ctx context.Context) (map[string]bool, error) {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	deadline, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if got := strings.Join(jobIDsR20T6(scanHubActiveJobsWithPanes(deadline, root, slow)), ","); got != "job-dead,job-live" {
		t.Fatalf("TL4: a timed-out lookup dropped jobs: %v", got)
	}
}

// TL5: a nil hook is byte-for-byte the pre-existing scan.
func TestR20T6NilHookMatchesLegacyScan(t *testing.T) {
	root := t.TempDir()
	now := time.Now()
	writeR20T6Job(t, root, "job-dead", "w1:p1A", now)
	writeR20T6Job(t, root, "job-live", "w1:p1B", now)
	writeR20T6Job(t, root, "job-nopane", "", now)
	legacy, err := json.Marshal(scanHubActiveJobs(root))
	if err != nil {
		t.Fatal(err)
	}
	hooked, err := json.Marshal(scanHubActiveJobsWithPanes(context.Background(), root, nil))
	if err != nil {
		t.Fatal(err)
	}
	if string(legacy) != string(hooked) {
		t.Fatalf("TL5: nil hook changed the scan: %s vs %s", legacy, hooked)
	}
	if !strings.Contains(string(legacy), "job-dead") {
		t.Fatalf("TL5: legacy scan should still report the dead job: %s", legacy)
	}
}

// TL6: the 32-entry cap is applied after the pane cross-check. With the order
// reversed the stale majority consumes every slot and the live jobs vanish.
func TestR20T6FilterRunsBeforeTheThirtyTwoCap(t *testing.T) {
	root := t.TempDir()
	now := time.Now()
	for i := 0; i < 40; i++ {
		// Stale jobs are the most recent, so an unfiltered cap keeps only them.
		writeR20T6Job(t, root, fmt.Sprintf("job-stale-%04d", i), fmt.Sprintf("w1:pS%04d", i), now)
	}
	live := []string{"w1:pL1", "w1:pL2", "w1:pL3"}
	for i, pane := range live {
		writeR20T6Job(t, root, fmt.Sprintf("job-live-%04d", i), pane, now.Add(-time.Hour))
	}
	if capped := scanHubActiveJobs(root); len(capped) != 32 {
		t.Fatalf("TL6: precondition, unfiltered scan should cap at 32: %d", len(capped))
	}
	hook, calls := aliveR20T6Hook(live...)
	jobs := scanHubActiveJobsWithPanes(context.Background(), root, hook)
	if got := strings.Join(jobIDsR20T6(jobs), ","); got != "job-live-0000,job-live-0001,job-live-0002" {
		t.Fatalf("TL6: live jobs were displaced by stale ones: %v", got)
	}
	if *calls != 1 {
		t.Fatalf("TL7: agent.list was called %d times, want 1", *calls)
	}
}

// TL7: one lookup per scan, not one per job.
func TestR20T6LookupIsOncePerScan(t *testing.T) {
	root := t.TempDir()
	now := time.Now()
	panes := make([]string, 0, 50)
	for i := 0; i < 50; i++ {
		pane := fmt.Sprintf("w1:p%04d", i)
		panes = append(panes, pane)
		writeR20T6Job(t, root, fmt.Sprintf("job-%04d", i), pane, now)
	}
	hook, calls := aliveR20T6Hook(panes...)
	if jobs := scanHubActiveJobsWithPanes(context.Background(), root, hook); len(jobs) != 32 {
		t.Fatalf("TL7: cap changed: %d", len(jobs))
	}
	if *calls != 1 {
		t.Fatalf("TL7: agent.list was called %d times for 50 jobs, want 1", *calls)
	}
}

// TL8: pane liveness is node-local. The wire payload must not grow a field.
func TestR20T6WireContractHasNoPaneID(t *testing.T) {
	root := t.TempDir()
	writeR20T6Job(t, root, "job-live", "w1:p1B", time.Now())
	hook, _ := aliveR20T6Hook("w1:p1B")
	encoded, err := json.Marshal(scanHubActiveJobsWithPanes(context.Background(), root, hook))
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(encoded), "pane_id") || strings.Contains(string(encoded), "w1:p1B") {
		t.Fatalf("TL8: pane leaked onto the wire payload: %s", encoded)
	}
	single, err := json.Marshal(HubActiveJob{JobID: "job-live", AgentLabel: "wrk-a", Epoch: 1})
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(single), "pane_id") {
		t.Fatalf("TL8: HubActiveJob grew a pane field: %s", single)
	}
}

// TL10: pane extraction reads the shape wrk actually writes, nested payload
// and the older flat form alike.
func TestR20T6PaneExtractionReadsNestedAndFlatRecords(t *testing.T) {
	root := t.TempDir()
	stamp := time.Now().UTC().Format(time.RFC3339)
	for _, shape := range []struct{ jobID, spawned string }{
		{"job-nested", `{"created_at":"` + stamp + `","job_id":"job-nested","kind":"job.spawned","payload":{"label":"wrk-a","pane_id":"w1:pN","profile":"profile-a","workspace":"w1"},"seq":2}`},
		{"job-flat", `{"created_at":"` + stamp + `","job_id":"job-flat","type":"job.spawned","pane_id":"w1:pF","label":"wrk-a"}`},
	} {
		dir := filepath.Join(root, "jobs", shape.jobID, "events")
		if err := os.MkdirAll(dir, 0700); err != nil {
			t.Fatal(err)
		}
		claim := `{"created_at":"` + stamp + `","job_id":"` + shape.jobID + `","kind":"job.claim","payload":{"agent_label":"wrk-a","owner_lane":"lane-a","t_level":"T1"},"seq":1}`
		if err := os.WriteFile(filepath.Join(dir, "00001-job.claim.json"), []byte(claim), 0600); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dir, "00002-job.spawned.json"), []byte(shape.spawned), 0600); err != nil {
			t.Fatal(err)
		}
		scanned, ok := scanHubJobEventDetails(dir, shape.jobID)
		if !ok {
			t.Fatalf("TL10: %s was not active", shape.jobID)
		}
		if scanned.paneID == "" {
			t.Fatalf("TL10: %s spawn record yielded no pane", shape.jobID)
		}
	}
	hook, _ := aliveR20T6Hook("w1:pN")
	if got := strings.Join(jobIDsR20T6(scanHubActiveJobsWithPanes(context.Background(), root, hook)), ","); got != "job-nested" {
		t.Fatalf("TL10: pane extraction did not drive the filter: %v", got)
	}
}

// A terminal record ends the claim episode, so the pane it named must not
// outlive it and disqualify a later re-claim.
func TestR20T6TerminalRecordClearsThePane(t *testing.T) {
	root := t.TempDir()
	stamp := time.Now().UTC().Format(time.RFC3339)
	dir := filepath.Join(root, "jobs", "job-reclaimed", "events")
	if err := os.MkdirAll(dir, 0700); err != nil {
		t.Fatal(err)
	}
	records := []struct{ name, body string }{
		{"00001-job.claim.json", `{"created_at":"` + stamp + `","kind":"job.claim","payload":{"agent_label":"wrk-a"},"seq":1}`},
		{"00002-job.spawned.json", `{"created_at":"` + stamp + `","kind":"job.spawned","payload":{"pane_id":"w1:pOld"},"seq":2}`},
		{"00003-job.completed.json", `{"created_at":"` + stamp + `","kind":"job.completed","payload":{},"seq":3}`},
		{"00004-job.claim.json", `{"created_at":"` + stamp + `","kind":"job.claim","payload":{"agent_label":"wrk-a"},"seq":4}`},
	}
	for _, record := range records {
		if err := os.WriteFile(filepath.Join(dir, record.name), []byte(record.body), 0600); err != nil {
			t.Fatal(err)
		}
	}
	scanned, ok := scanHubJobEventDetails(dir, "job-reclaimed")
	if !ok || scanned.paneID != "" {
		t.Fatalf("a completed episode's pane survived the re-claim: ok=%v pane=%q", ok, scanned.paneID)
	}
	hook, _ := aliveR20T6Hook("w1:pOther")
	if got := len(scanHubActiveJobsWithPanes(context.Background(), "", hook)); got != 0 {
		t.Fatalf("empty inbox root should scan to nothing: %d", got)
	}
	if got := strings.Join(jobIDsR20T6(scanHubActiveJobsWithPanes(context.Background(), root, hook)), ","); got != "job-reclaimed" {
		t.Fatalf("the re-claimed job was dropped on a stale pane: %v", got)
	}
}

// The filter decision table, exercised without herdr.
func TestR20T6FilterActiveJobsByPaneTable(t *testing.T) {
	job := func(id, pane string) hubScannedJob {
		return hubScannedJob{job: HubActiveJob{JobID: id, AgentLabel: "wrk-a", Epoch: 1}, paneID: pane}
	}
	input := []hubScannedJob{job("job-dead", "w1:p1A"), job("job-live", "w1:p1B"), job("job-nopane", "")}
	for _, testCase := range []struct {
		name       string
		alive      map[string]bool
		aliveKnown bool
		want       string
	}{
		{"lookup unavailable keeps everything", nil, false, "job-dead,job-live,job-nopane"},
		{"dead pane drops, live and unknown stay", map[string]bool{"w1:p1B": true}, true, "job-live,job-nopane"},
		{"empty live set still keeps unknown panes", map[string]bool{}, true, "job-nopane"},
		{"all panes live keeps everything", map[string]bool{"w1:p1A": true, "w1:p1B": true}, true, "job-dead,job-live,job-nopane"},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			kept := filterActiveJobsByPane(input, testCase.alive, testCase.aliveKnown)
			ids := make([]string, 0, len(kept))
			for _, scanned := range kept {
				ids = append(ids, scanned.job.JobID)
			}
			if got := strings.Join(ids, ","); got != testCase.want {
				t.Fatalf("got %q want %q", got, testCase.want)
			}
			if len(input) != 3 {
				t.Fatalf("filter mutated its input: %+v", input)
			}
		})
	}
}

// AC16: the shape this machine's inbox is actually in — a large stale majority
// with a handful of live jobs. Before the cross-check the node advertises the
// 32-entry cap and placement refuses it; after, it advertises only real work.
func TestR20T6StaleInboxNoLongerAdvertisesAFullMachine(t *testing.T) {
	root := t.TempDir()
	now := time.Now()
	for i := 0; i < 42; i++ {
		writeR20T6Job(t, root, fmt.Sprintf("job-stale-%04d", i), fmt.Sprintf("w1:pS%04d", i), now.Add(-24*time.Hour))
	}
	live := []string{"w1:pL1", "w1:pL2", "w1:pL3"}
	for i, pane := range live {
		writeR20T6Job(t, root, fmt.Sprintf("job-live-%04d", i), pane, now)
	}
	before := scanHubActiveJobs(root)
	hook, calls := aliveR20T6Hook(live...)
	after := scanHubActiveJobsWithPanes(context.Background(), root, hook)
	if len(before) != 32 || len(after) != 3 {
		t.Fatalf("AC16: active before=%d (want 32) after=%d (want 3)", len(before), len(after))
	}
	if *calls != 1 {
		t.Fatalf("AC16: agent.list was called %d times, want 1", *calls)
	}
	policy := DefaultPlacementPolicy()
	if !(len(before) >= policy.MaxActiveJobs) || len(after) >= policy.MaxActiveJobs {
		t.Fatalf("AC16: placement still refuses the node: before=%d after=%d max=%d", len(before), len(after), policy.MaxActiveJobs)
	}
	t.Logf("AC16: active jobs advertised before=%d after=%d", len(before), len(after))
}
