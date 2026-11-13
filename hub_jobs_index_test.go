package panewire

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"
)

// assertIndexEquivalent holds the index's answers to the full scan's, byte for
// byte, on every path the daemon reads through the index. A quick check (full
// false) compares fewer parameter combinations; each legacy scan reads every
// event file, which is the cost the index removes.
func (f *indexFixture) assertIndexEquivalent(t *testing.T, label string, full bool) {
	t.Helper()
	same := func(what string, want, got any) {
		t.Helper()
		wantJSON, err := json.Marshal(want)
		if err != nil {
			t.Fatal(err)
		}
		gotJSON, err := json.Marshal(got)
		if err != nil {
			t.Fatal(err)
		}
		if string(wantJSON) != string(gotJSON) || !reflect.DeepEqual(want, got) {
			t.Fatalf("%s: %s differs from the full scan\nfull:  %.600s\nindex: %.600s", label, what, wantJSON, gotJSON)
		}
	}
	ctx := context.Background()
	alive := map[string]bool{}
	for i, pane := range f.panes {
		if i%2 == 0 {
			alive[pane] = true
		}
	}
	hooks := map[string]panesAliveFunc{
		"half-alive": func(context.Context) (map[string]bool, error) {
			return alive, nil
		},
	}
	limits := []int{32}
	maxAges := []time.Duration{90 * time.Minute}
	panes := []string{"w1:pshared"}
	if full {
		hooks["no-hook"] = nil
		limits = []int{32, 0}
		maxAges = []time.Duration{0, 24 * time.Hour, 90 * time.Minute}
		panes = append(panes, "w1:pnone")
		for i := 0; i < len(f.panes); i += max(1, len(f.panes)/5) {
			panes = append(panes, f.panes[i])
		}
		for _, maxAge := range []time.Duration{0, 6 * time.Hour} {
			same(fmt.Sprintf("lane events (maxAge %s)", maxAge),
				legacyScanHubLaneEventsWithin(f.root, maxAge),
				scanHubLaneEventsWithin(f.root, maxAge))
		}
	} else {
		panes = append(panes, f.panes[len(f.panes)-1])
	}
	for name, hook := range hooks {
		for _, limit := range limits {
			same(fmt.Sprintf("active jobs (%s, limit %d)", name, limit),
				legacyScanHubActiveJobsForLimit(ctx, f.root, hook, limit),
				scanHubActiveJobsForLimit(ctx, f.root, hook, limit))
		}
	}
	for _, maxAge := range maxAges {
		same(fmt.Sprintf("relay events (maxAge %s)", maxAge),
			legacyScanHubRelayEventsWithin(f.root, maxAge),
			scanHubRelayEventsWithin(f.root, maxAge))
	}
	q := &quotaStopManager{root: f.root}
	for _, pane := range panes {
		wantJob, wantOK := legacyJobForPane(ctx, f.root, pane)
		gotJob, gotOK := q.jobForPane(ctx, pane)
		same("job for pane "+pane, []any{wantJob, wantOK}, []any{gotJob, gotOK})
		wantBinding, wantFound := legacyNodeJobHerdrBindingByPaneFromRoot(f.root, pane)
		gotBinding, gotFound := nodeJobHerdrBindingByPaneFromRoot(f.root, pane)
		if wantBinding != gotBinding || wantFound != gotFound {
			t.Fatalf("%s: binding of %s: full=%+v/%v index=%+v/%v", label, pane, wantBinding, wantFound, gotBinding, gotFound)
		}
	}
}

// TestHubInboxIndexMatchesFullScan runs the index against the pre-index scans
// on thousands of job directories, cold, warm, and after every kind of change
// to the inbox: a live job ending, a long-dormant job claimed again, an event
// or a whole job directory removed, a new job, lane events added and removed,
// and a longer active window reviving dormant jobs.
func TestHubInboxIndexMatchesFullScan(t *testing.T) {
	f := newIndexFixture(t, 2000)
	f.assertIndexEquivalent(t, "cold", true)
	f.assertIndexEquivalent(t, "warm", true)

	jobs := filepath.Join(f.root, "jobs")
	live, dormant := f.jobs[2], f.jobs[0]
	f.now = time.Now()
	f.put(live, 4, "job.completed", f.completed(2, 0), 0)
	f.put(dormant, 5, "job.claimed", f.claim(0, 2, 0, "w1:preborn"), 0)
	f.put(dormant, 6, "job.spawned", f.spawned(0, "w1:preborn", 0), 0)
	f.panes = append(f.panes, "w1:preborn")
	f.assertIndexEquivalent(t, "live job ended, dormant job re-claimed", false)

	f.settle()
	f.assertIndexEquivalent(t, "changes settled", false)

	if err := os.Remove(filepath.Join(jobs, f.jobs[1], "events", "00002-job.spawned.json")); err != nil {
		t.Fatal(err)
	}
	if err := os.RemoveAll(filepath.Join(jobs, f.jobs[22])); err != nil {
		t.Fatal(err)
	}
	f.writeJob(len(f.jobs))
	f.writeLaneEvent(9000, true)
	f.assertIndexEquivalent(t, "event removed, job removed, job added, lane event added", false)

	f.settle()
	if err := os.Remove(filepath.Join(f.root, "events-lane", "00003-lane.event.json")); err != nil {
		t.Fatal(err)
	}
	f.assertIndexEquivalent(t, "lane event removed", false)

	t.Setenv("PANEWIRE_JOB_ACTIVE_MAX_AGE", fmt.Sprintf("%dh", 24*30))
	f.assertIndexEquivalent(t, "active window grown past the old jobs", false)
	t.Setenv("PANEWIRE_JOB_ACTIVE_MAX_AGE", "1h")
	f.assertIndexEquivalent(t, "active window shrunk", false)
	t.Setenv("PANEWIRE_JOB_ACTIVE_MAX_AGE", "")
	t.Setenv("PANEWIRE_JOB_STALE_AGE", "10m")
	f.assertIndexEquivalent(t, "stale age shortened", false)
}

// TestHubInboxIndexRefreshRule pins the rule docs/job-event-index.md states: a
// job's events are read again exactly when its events directory changed or its
// last read was too close to the directory's mtime to be final.
func TestHubInboxIndexRefreshRule(t *testing.T) {
	f := newIndexFixture(t, 0)
	id := "job-refresh"
	f.put(id, 1, "job.claimed", f.claim(1, 1, time.Hour, ""), time.Hour)
	ix := hubIndexFor(f.root)
	scan := func() []HubActiveJob { return scanHubActiveJobsForLimit(context.Background(), f.root, nil, 0) }
	recorded := func() *hubIndexedJob {
		ix.mu.Lock()
		defer ix.mu.Unlock()
		return ix.jobs[id]
	}

	// A directory written just now is not trusted: the next pass reads it again.
	scan()
	racy := recorded()
	if scan(); recorded() == racy {
		t.Fatal("a read within the racy window of the directory's mtime was reused")
	}

	// Once the directory is older than the window, one read is final.
	f.settle()
	scan()
	settled := recorded()
	if scan(); recorded() != settled {
		t.Fatal("an unchanged, settled directory was read again")
	}
	if got := scan(); len(got) != 1 || got[0].LastEventKind != "job.claimed" {
		t.Fatalf("active=%+v", got)
	}

	// A new event changes the directory; the next pass sees it.
	f.now = time.Now()
	f.put(id, 2, "job.completed", f.completed(1, 0), 0)
	if got := scan(); len(got) != 0 {
		t.Fatalf("a completion written after a settled read was not seen: %+v", got)
	}
	if recorded() == settled {
		t.Fatal("a changed directory kept its old record")
	}
}

// TestHubInboxIndexDropsDormantJobs: a job whose newest event is older than
// the active window keeps no parsed events, yet still answers the pane lookup
// and comes back, with its events, when the window grows over it.
func TestHubInboxIndexDropsDormantJobs(t *testing.T) {
	f := newIndexFixture(t, 40)
	ix := hubIndexFor(f.root)
	dormantID := f.jobs[0]
	wantSession := fmt.Sprintf("sess-%d", 0)
	scanHubRelayEventsWithin(f.root, 0)
	ix.mu.Lock()
	dormant := ix.jobs[dormantID]
	ix.mu.Unlock()
	if dormant == nil || !dormant.compact || dormant.events != nil {
		t.Fatalf("a job past the active window kept its events: %+v", dormant)
	}
	binding, ok := nodeJobHerdrBindingByPaneFromRoot(f.root, "w1:p0")
	if !ok || binding.jobID != dormantID || binding.session != wantSession {
		t.Fatalf("pane lookup lost the dormant job: %+v %v", binding, ok)
	}
	ix.mu.Lock()
	if ix.jobs[dormantID] != dormant {
		ix.mu.Unlock()
		t.Fatal("an unchanged dormant job was read again")
	}
	ix.mu.Unlock()
	t.Setenv("PANEWIRE_JOB_ACTIVE_MAX_AGE", fmt.Sprintf("%dh", 24*30))
	if events := scanHubRelayEventsWithin(f.root, 0); len(events) == 0 || events[0].JobID != dormantID {
		t.Fatalf("a window grown over a dormant job did not bring its events back: %d events", len(events))
	}
}

// TestHubInboxIndexByteBudget: past its budget the index keeps no more, and
// every answer is still the full scan's.
func TestHubInboxIndexByteBudget(t *testing.T) {
	f := newIndexFixture(t, 400)
	ix := hubIndexFor(f.root)
	ix.mu.Lock()
	ix.budget = 64 << 10
	ix.mu.Unlock()
	f.assertIndexEquivalent(t, "over budget, first pass", true)
	f.assertIndexEquivalent(t, "over budget, second pass", true)
	ix.mu.Lock()
	defer ix.mu.Unlock()
	if used := ix.jobBytes + ix.laneBytes; used > ix.budget {
		t.Fatalf("index holds %d bytes over a %d byte budget", used, ix.budget)
	}
}

func TestHubIndexRegistryIsBounded(t *testing.T) {
	for range 3 * hubIndexMaxRoots {
		hubIndexFor(filepath.Join(t.TempDir(), "inbox"))
	}
	hubIndexRegistry.mu.Lock()
	defer hubIndexRegistry.mu.Unlock()
	if len(hubIndexRegistry.byRoot) > hubIndexMaxRoots {
		t.Fatalf("%d indexes for %d roots allowed", len(hubIndexRegistry.byRoot), hubIndexMaxRoots)
	}
}

// TestHubInboxIndexConcurrentUse is for -race: scans on every path run while
// the inbox changes underneath them, and the index still ends equivalent.
func TestHubInboxIndexConcurrentUse(t *testing.T) {
	f := newIndexFixture(t, 300)
	var wg sync.WaitGroup
	q := &quotaStopManager{root: f.root}
	for range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 40 {
				scanHubActiveJobsForLimit(context.Background(), f.root, nil, 0)
				scanHubRelayEventsWithin(f.root, 24*time.Hour)
				q.jobForPane(context.Background(), "w1:p2")
				nodeJobHerdrBindingByPaneFromRoot(f.root, "w1:p22")
			}
		}()
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := range 40 {
			id := fmt.Sprintf("job-race-%03d", i)
			dir := filepath.Join(f.root, "jobs", id, "events")
			if err := os.MkdirAll(dir, 0o700); err != nil {
				t.Error(err)
				return
			}
			claim := f.claim(i, 1, time.Minute, fmt.Sprintf("w1:prace%d", i))
			if err := os.WriteFile(filepath.Join(dir, "00001-job.claimed.json"), []byte(claim), 0o600); err != nil {
				t.Error(err)
				return
			}
			f.writeLaneEvent(10000+i, true)
		}
	}()
	wg.Wait()
	f.assertIndexEquivalent(t, "after concurrent use", true)
}

// TestHubInboxIndexRetriesFailedReads: a read that fails says nothing about the
// file, so it must not be kept under an unchanged directory mtime. A dangling
// link stands in for a transient open failure (permission checks do not bind
// root); making its target appear changes no directory mtime.
func TestHubInboxIndexRetriesFailedReads(t *testing.T) {
	f := newIndexFixture(t, 0)
	target := filepath.Join(f.root, "target.json")

	id := "job-retry"
	f.put(id, 1, "job.claimed", f.claim(1, 1, time.Hour, ""), time.Hour)
	link := filepath.Join(f.root, "jobs", id, "events", "00002-job.completed.json")
	if err := os.Symlink(target, link); err != nil {
		t.Fatal(err)
	}
	laneLink := filepath.Join(f.root, "events-lane", "00001-lane.event.json")
	if err := os.Symlink(target, laneLink); err != nil {
		t.Fatal(err)
	}
	f.settle()
	scanJobs := func() []HubActiveJob { return scanHubActiveJobsForLimit(context.Background(), f.root, nil, 0) }
	if got := scanJobs(); len(got) != 1 {
		t.Fatalf("before the completion is readable, active=%+v", got)
	}
	if got := scanHubLaneEventsWithin(f.root, 0); len(got) != 0 {
		t.Fatalf("lane events=%+v", got)
	}

	// Both files become readable; neither directory's mtime moves.
	f.now = time.Now()
	completion := f.completed(1, 0)
	if err := os.WriteFile(target, []byte(completion), 0o600); err != nil {
		t.Fatal(err)
	}
	if got := scanJobs(); len(got) != 0 {
		t.Fatalf("a completion that became readable under a settled directory was not seen: %+v", got)
	}
	lane := `{"type":"lane.event","owner_lane":"lane-a","event_id":"ev-1","text":"hello","epoch":1}`
	if err := os.WriteFile(target, []byte(lane), 0o600); err != nil {
		t.Fatal(err)
	}
	if got := scanHubLaneEventsWithin(f.root, 0); len(got) != 1 || got[0].EventID != "ev-1" {
		t.Fatalf("a lane event that became readable under a settled directory was not seen: %+v", got)
	}
}

// TestHubInboxIndexDoesNotReuseAReplacedLaneFile: a restored or replaced
// events-lane can hold other content under the same name, mtime and size; a
// parsed event is reused only for the same file in the same directory.
func TestHubInboxIndexDoesNotReuseAReplacedLaneFile(t *testing.T) {
	f := newIndexFixture(t, 0)
	laneDir := filepath.Join(f.root, "events-lane")
	name := "00001-lane.event.json"
	at := f.now.Add(-3 * time.Hour)
	write := func(dir, id string) {
		t.Helper()
		body := `{"type":"lane.event","owner_lane":"lane-a","event_id":"` + id + `","text":"hello","epoch":1}`
		path := filepath.Join(dir, name)
		if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
			t.Fatal(err)
		}
		if err := os.Chtimes(path, at, at); err != nil {
			t.Fatal(err)
		}
	}
	eventID := func() string {
		t.Helper()
		got := scanHubLaneEventsWithin(f.root, 0)
		if len(got) != 1 {
			t.Fatalf("lane events=%+v", got)
		}
		return got[0].EventID
	}
	write(laneDir, "ev-1")
	f.settle()
	if got := eventID(); got != "ev-1" {
		t.Fatalf("first read=%s", got)
	}

	// The same name replaced inside the directory by rename: same mtime, same length.
	replacement := t.TempDir()
	write(replacement, "ev-2")
	if err := os.Rename(filepath.Join(replacement, name), filepath.Join(laneDir, name)); err != nil {
		t.Fatal(err)
	}
	if got := eventID(); got != "ev-2" {
		t.Fatalf("a replaced file was reported as the old event: %s", got)
	}

	// The whole directory replaced, as a restore does.
	f.settle()
	if got := eventID(); got != "ev-2" {
		t.Fatalf("settled read=%s", got)
	}
	restored := filepath.Join(f.root, "events-lane.restored")
	if err := os.Mkdir(restored, 0o700); err != nil {
		t.Fatal(err)
	}
	write(restored, "ev-3")
	if err := os.Chtimes(restored, f.now.Add(-indexFixtureSettle), f.now.Add(-indexFixtureSettle)); err != nil {
		t.Fatal(err)
	}
	if err := os.RemoveAll(laneDir); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(restored, laneDir); err != nil {
		t.Fatal(err)
	}
	if got := eventID(); got != "ev-3" {
		t.Fatalf("a restored directory was reported as the old events: %s", got)
	}
}

// TestHubInboxIndexChargesBindingStrings: the budget counts what a retained
// pane binding holds, not a fixed size, so a large binding cannot slip past it.
func TestHubInboxIndexChargesBindingStrings(t *testing.T) {
	f := newIndexFixture(t, 0)
	id := "job-bigbinding"
	big := strings.Repeat("s", 200<<10)
	f.put(id, 1, "job.claimed", fmt.Sprintf(`{"type":"job.claimed","created_at":%q,"agent_label":"agent-1","epoch":1,"payload":{"pane_id":"w1:p1","herdr_session":%q}}`, f.stamp(time.Hour), big), time.Hour)
	f.settle()
	ix := hubIndexFor(f.root)
	scanHubActiveJobsForLimit(context.Background(), f.root, nil, 0)
	ix.mu.Lock()
	job := ix.jobs[id]
	ix.mu.Unlock()
	if job == nil || job.cost < int64(len(big)) {
		t.Fatalf("a %d byte binding string was charged %v", len(big), job)
	}
	ix.mu.Lock()
	ix.budget = int64(len(big)) / 2
	ix.mu.Unlock()
	f.put(id, 2, "job.progress", `{"type":"job.progress"}`, time.Hour)
	scanHubActiveJobsForLimit(context.Background(), f.root, nil, 0)
	ix.mu.Lock()
	defer ix.mu.Unlock()
	if ix.jobs[id] != nil || ix.jobBytes != 0 {
		t.Fatalf("a record over the budget was retained: %+v bytes=%d", ix.jobs[id], ix.jobBytes)
	}
}
