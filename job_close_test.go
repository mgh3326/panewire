package panewire

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"
)

// jobCloseFixture is an isolated jobs root: ARBITER_INBOX_ROOT, HOSTNAME,
// the panewire socket and herdr all point into a temp dir, so a close under
// test can reach neither the real inbox nor a real daemon or pane. Any herdr
// invocation and any socket request is recorded.
type jobCloseFixture struct {
	t        *testing.T
	root     string
	jobs     string
	herdrLog string
	socket   *jobFakeSocket
	now      time.Time
	stdout   bytes.Buffer
	stderr   bytes.Buffer
}

func newJobCloseFixture(t *testing.T) *jobCloseFixture {
	t.Helper()
	// Unix socket paths are length-limited; t.TempDir() can exceed it.
	root, err := os.MkdirTemp("/tmp", "pwclose")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(root) })
	f := &jobCloseFixture{t: t, root: root, jobs: filepath.Join(root, "jobs"), herdrLog: filepath.Join(root, "herdr.log"), now: time.Date(2026, 9, 22, 1, 2, 3, 0, time.UTC)}
	if err := os.MkdirAll(f.jobs, 0o755); err != nil {
		t.Fatal(err)
	}
	bin := filepath.Join(root, "bin")
	if err := os.MkdirAll(bin, 0o755); err != nil {
		t.Fatal(err)
	}
	stub := "#!/bin/sh\necho \"$@\" >> " + f.herdrLog + "\nexit 0\n"
	if err := os.WriteFile(filepath.Join(bin, "herdr"), []byte(stub), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", bin+string(os.PathListSeparator)+os.Getenv("PATH"))
	t.Setenv("HERDR_BIN", filepath.Join(bin, "herdr"))
	t.Setenv("ARBITER_INBOX_ROOT", f.jobs)
	t.Setenv("HOSTNAME", "fixture-host")
	t.Setenv("HANDOFFKEEP_BIN", filepath.Join(root, "absent"))
	socket := filepath.Join(root, "s")
	t.Setenv("PANEWIRE_SOCKET", socket)
	f.socket = startJobFakeSocket(t, socket, "ok")
	return f
}

// event writes one events/<name> file for job.
func (f *jobCloseFixture) event(job, name, body string) {
	f.t.Helper()
	dir := filepath.Join(f.jobs, job, "events")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		f.t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, name), []byte(body), 0o644); err != nil {
		f.t.Fatal(err)
	}
}

// claim writes an arbiter-envelope claim (or reclaim) naming owner.
func (f *jobCloseFixture) claim(job, name, kind, owner string) {
	f.t.Helper()
	payload, _ := json.Marshal(map[string]any{"agent_label": job, "owner_lane": owner, "role": "worker"})
	f.event(job, name, fmt.Sprintf(`{"job_id":%q,"seq":1,"kind":%q,"payload":%s,"created_at":"2026-09-21T00:00:00Z"}`, job, kind, payload))
}

func (f *jobCloseFixture) spawned(job, name, pane, tab string) {
	f.event(job, name, fmt.Sprintf(`{"job_id":%q,"kind":"job.spawned","payload":{"pane_id":%q,"tab_id":%q}}`, job, pane, tab))
}

func (f *jobCloseFixture) run(args ...string) int {
	f.stdout.Reset()
	f.stderr.Reset()
	c := &jobCLI{stdout: &f.stdout, stderr: &f.stderr, socket: os.Getenv("PANEWIRE_SOCKET"), now: func() time.Time { return f.now }}
	return c.close(args)
}

func (f *jobCloseFixture) names(job string) []string {
	f.t.Helper()
	entries, err := os.ReadDir(filepath.Join(f.jobs, job, "events"))
	if err != nil {
		f.t.Fatal(err)
	}
	var names []string
	for _, entry := range entries {
		names = append(names, entry.Name())
	}
	return names
}

func (f *jobCloseFixture) revokedRecords(job string) []map[string]any {
	f.t.Helper()
	var records []map[string]any
	for _, name := range f.names(job) {
		if !strings.HasSuffix(name, "-job.revoked.json") {
			continue
		}
		contents, err := os.ReadFile(filepath.Join(f.jobs, job, "events", name))
		if err != nil {
			f.t.Fatal(err)
		}
		var record map[string]any
		if err := json.Unmarshal(contents, &record); err != nil {
			f.t.Fatalf("%s: %v", name, err)
		}
		records = append(records, record)
	}
	return records
}

// assertNoSideEffects: the declaration never reaches herdr or the daemon,
// and leaves nothing in the job directories besides events/ and the events
// lock (an attempted emit would leave emit-failures.log).
func (f *jobCloseFixture) assertNoSideEffects() {
	f.t.Helper()
	jobs, err := os.ReadDir(f.jobs)
	if err != nil {
		f.t.Fatal(err)
	}
	for _, job := range jobs {
		entries, err := os.ReadDir(filepath.Join(f.jobs, job.Name()))
		if err != nil {
			f.t.Fatal(err)
		}
		for _, entry := range entries {
			if entry.Name() != "events" && entry.Name() != ".wrk-events.lock" {
				f.t.Fatalf("close left %s/%s behind", job.Name(), entry.Name())
			}
		}
	}
	if contents, err := os.ReadFile(f.herdrLog); err == nil {
		f.t.Fatalf("close invoked herdr: %q", contents)
	}
	if lines := f.socket.stop(); len(lines) != 0 {
		f.t.Fatalf("close sent %d socket requests: %q", len(lines), lines)
	}
}

func TestJobCloseOwnerWritesOneRevokedRecord(t *testing.T) {
	f := newJobCloseFixture(t)
	f.claim("j1", "00001-job.claim.json", "job.claim", "director-1")
	f.spawned("j1", "00002-job.spawned.json", "w16:p28P", "w16:t9")
	if rc := f.run("j1", "--lane", "director-1", "--outcome", "completed", "--reason", "merged as #69; worker never ran wrk done"); rc != ExitOK {
		t.Fatalf("rc=%d stderr=%q", rc, f.stderr.String())
	}
	want := "OK job=j1 kind=job.revoked outcome=completed closed_by=director-1 owner_lane=director-1 record=00003-job.revoked.json\n"
	if f.stdout.String() != want {
		t.Fatalf("stdout = %q, want %q", f.stdout.String(), want)
	}
	path := filepath.Join(f.jobs, "j1", "events", "00003-job.revoked.json")
	contents, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	wantBody := `{"kind":"job.revoked","job_id":"j1","owner_lane":"director-1","closed_by":"director-1","outcome":"completed","reason":"merged as #69; worker never ran wrk done","source":"panewire job close","host":"fixture-host","created_at":"2026-09-22T01:02:03Z","epoch":1}` + "\n"
	if string(contents) != wantBody {
		t.Fatalf("record =\n%s\nwant\n%s", contents, wantBody)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0o600 {
		t.Fatalf("record mode = %v, want 0600", info.Mode().Perm())
	}
	// No leftover temp file, nothing but the one record added.
	if got := f.names("j1"); len(got) != 3 {
		t.Fatalf("events after close = %q", got)
	}
	f.assertNoSideEffects()
}

// AC2: the caller's lane must be byte-equal to the latest claim's owner_lane.
// Each row is a lane that a normalizing, substring, prefix or case-folding
// comparison would accept.
func TestJobCloseOwnershipIsExactMatch(t *testing.T) {
	cases := []struct{ name, owner, caller string }{
		{"upper case caller", "director-1", "DIRECTOR-1"},
		{"title case caller", "director-1", "Director-1"},
		{"case folded owner", "Director-1", "director-1"},
		{"caller is prefix of owner", "director-1", "director"},
		{"caller is substring of owner", "director-1", "rector-1"},
		{"owner is prefix of caller", "director-1", "director-10"},
		{"caller trailing space", "director-1", "director-1 "},
		{"caller leading space", "director-1", " director-1"},
		{"caller trailing newline", "director-1", "director-1\n"},
		{"owner trailing space", "director-1 ", "director-1"},
		{"owner leading tab", "\tdirector-1", "director-1"},
		{"another lane", "director-1", "b502-owner-close"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := newJobCloseFixture(t)
			f.claim("j1", "00001-job.claim.json", "job.claim", tc.owner)
			if rc := f.run("j1", "--lane", tc.caller, "--outcome", "abandoned", "--reason", "r"); rc != jobCloseRefused {
				t.Fatalf("owner %q caller %q: rc=%d, want refusal %d (stdout=%q)", tc.owner, tc.caller, rc, jobCloseRefused, f.stdout.String())
			}
			if !strings.Contains(f.stderr.String(), "close refused") {
				t.Fatalf("stderr = %q", f.stderr.String())
			}
			if got := f.revokedRecords("j1"); len(got) != 0 {
				t.Fatalf("refused close wrote %d records", len(got))
			}
			f.assertNoSideEffects()
		})
	}
}

// AC2: ownership is the newest claim/reclaim in event order. The lane that
// held the job before a reclaim is no longer its owner.
func TestJobCloseUsesLatestClaim(t *testing.T) {
	f := newJobCloseFixture(t)
	f.claim("j1", "00001-job.claim.json", "job.claim", "old-lane")
	f.claim("j1", "00002-job.reclaim.json", "job.reclaim", "new-lane")
	if rc := f.run("j1", "--lane", "old-lane", "--outcome", "abandoned", "--reason", "r"); rc != jobCloseRefused {
		t.Fatalf("previous owner closed a reclaimed job: rc=%d", rc)
	}
	if rc := f.run("j1", "--lane", "new-lane", "--outcome", "abandoned", "--reason", "r"); rc != ExitOK {
		t.Fatalf("latest owner refused: rc=%d stderr=%q", rc, f.stderr.String())
	}
	records := f.revokedRecords("j1")
	if len(records) != 1 || records[0]["owner_lane"] != "new-lane" {
		t.Fatalf("records = %v", records)
	}
}

// A claim record that cannot be parsed must not let an older claim decide.
func TestJobCloseUnreadableLatestClaimRefuses(t *testing.T) {
	f := newJobCloseFixture(t)
	f.claim("j1", "00001-job.claim.json", "job.claim", "old-lane")
	f.event("j1", "00002-job.reclaim.json", `{"kind":"job.reclaim","payload":{"owner_lane":"new-lane"`)
	if rc := f.run("j1", "--lane", "old-lane", "--outcome", "abandoned", "--reason", "r"); rc != jobCloseRefused {
		t.Fatalf("rc=%d, want refusal", rc)
	}
	if !strings.Contains(f.stderr.String(), "00002-job.reclaim.json") {
		t.Fatalf("stderr = %q", f.stderr.String())
	}
	if len(f.revokedRecords("j1")) != 0 {
		t.Fatal("record written")
	}
}

// AC2: another lane closes only with --operator-override, and the record
// keeps both the real owner and the caller plus the override marker.
func TestJobCloseOperatorOverrideIsRecorded(t *testing.T) {
	f := newJobCloseFixture(t)
	f.claim("j1", "00001-job.claim.json", "job.claim", "b434-openai-gate")
	if rc := f.run("j1", "--lane", "operator", "--outcome", "abandoned", "--reason", "owner lane pane is gone"); rc != jobCloseRefused {
		t.Fatalf("no override: rc=%d", rc)
	}
	if rc := f.run("j1", "--lane", "operator", "--outcome", "abandoned", "--reason", "owner lane pane is gone", "--operator-override"); rc != ExitOK {
		t.Fatalf("override: rc=%d stderr=%q", rc, f.stderr.String())
	}
	if !strings.Contains(f.stdout.String(), " owner_lane=b434-openai-gate override=operator ") {
		t.Fatalf("stdout = %q", f.stdout.String())
	}
	records := f.revokedRecords("j1")
	if len(records) != 1 {
		t.Fatalf("records = %v", records)
	}
	record := records[0]
	if record["override"] != "operator" || record["owner_lane"] != "b434-openai-gate" || record["closed_by"] != "operator" {
		t.Fatalf("override record = %v", record)
	}
	f.assertNoSideEffects()
}

// The owner's own close carries no override field at all.
func TestJobCloseOwnerRecordHasNoOverride(t *testing.T) {
	f := newJobCloseFixture(t)
	f.claim("j1", "00001-job.claim.json", "job.claim", "lane-a")
	if rc := f.run("j1", "--lane", "lane-a", "--outcome", "superseded", "--reason", "replaced by j2"); rc != ExitOK {
		t.Fatalf("rc=%d", rc)
	}
	if _, present := f.revokedRecords("j1")[0]["override"]; present {
		t.Fatal("owner close recorded an override")
	}
}

// A job with no claim, or a claim without an owner, has no owner to match:
// an empty --lane is rejected and an empty owner never matches.
func TestJobCloseWithoutOwnerNeedsOverride(t *testing.T) {
	f := newJobCloseFixture(t)
	f.spawned("j1", "00001-job.spawned.json", "w1:p1", "w1:t1")
	if rc := f.run("j1", "--lane", "lane-a", "--outcome", "abandoned", "--reason", "r"); rc != jobCloseRefused {
		t.Fatalf("no claim: rc=%d", rc)
	}
	f.event("j2", "00001-job.claim.json", `{"kind":"job.claim","payload":{"agent_label":"j2"}}`)
	if rc := f.run("j2", "--lane", "", "--outcome", "abandoned", "--reason", "r"); rc != ExitUsage {
		t.Fatalf("empty lane: rc=%d", rc)
	}
	if rc := f.run("j2", "--lane", "lane-a", "--outcome", "abandoned", "--reason", "r"); rc != jobCloseRefused {
		t.Fatalf("ownerless claim: rc=%d", rc)
	}
	if rc := f.run("j1", "--lane", "operator", "--outcome", "abandoned", "--reason", "r", "--operator-override"); rc != ExitOK {
		t.Fatalf("override on unclaimed job: rc=%d stderr=%q", rc, f.stderr.String())
	}
	if len(f.revokedRecords("j2")) != 0 {
		t.Fatal("ownerless claim was closed")
	}
}

// AC4: reason and outcome are required; outcome is a closed vocabulary.
func TestJobCloseRequiresReasonAndOutcome(t *testing.T) {
	cases := []struct {
		name string
		args []string
	}{
		{"no reason", []string{"j1", "--lane", "lane-a", "--outcome", "completed"}},
		{"empty reason", []string{"j1", "--lane", "lane-a", "--outcome", "completed", "--reason", ""}},
		{"blank reason", []string{"j1", "--lane", "lane-a", "--outcome", "completed", "--reason", " \t\n"}},
		{"reason without value", []string{"j1", "--lane", "lane-a", "--outcome", "completed", "--reason"}},
		{"no outcome", []string{"j1", "--lane", "lane-a", "--reason", "r"}},
		{"outcome outside vocabulary", []string{"j1", "--lane", "lane-a", "--outcome", "done", "--reason", "r"}},
		{"outcome case variant", []string{"j1", "--lane", "lane-a", "--outcome", "Completed", "--reason", "r"}},
		{"no lane", []string{"j1", "--outcome", "completed", "--reason", "r"}},
		{"duplicate lane", []string{"j1", "--lane", "x", "--lane", "lane-a", "--outcome", "completed", "--reason", "r"}},
		{"unknown option", []string{"j1", "--lane", "lane-a", "--outcome", "completed", "--reason", "r", "--force"}},
		{"oversize reason", []string{"j1", "--lane", "lane-a", "--outcome", "completed", "--reason", strings.Repeat("x", jobCloseReasonMax+1)}},
		{"invalid utf8 reason", []string{"j1", "--lane", "lane-a", "--outcome", "completed", "--reason", "bad\xff"}},
		{"no job", []string{}},
		{"path job", []string{"../j1", "--lane", "lane-a", "--outcome", "completed", "--reason", "r"}},
		{"nested job", []string{"j1/events", "--lane", "lane-a", "--outcome", "completed", "--reason", "r"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := newJobCloseFixture(t)
			f.claim("j1", "00001-job.claim.json", "job.claim", "lane-a")
			if rc := f.run(tc.args...); rc != ExitUsage {
				t.Fatalf("rc=%d, want %d (stderr=%q)", rc, ExitUsage, f.stderr.String())
			}
			if len(f.revokedRecords("j1")) != 0 {
				t.Fatal("record written")
			}
		})
	}
}

// AC4: the three outcomes stay distinct in the record, so a count of
// completed closes never includes an abandoned or superseded one.
func TestJobCloseOutcomesStayDistinct(t *testing.T) {
	f := newJobCloseFixture(t)
	for _, outcome := range []string{"completed", "abandoned", "superseded"} {
		job := "j-" + outcome
		f.claim(job, "00001-job.claim.json", "job.claim", "lane-a")
		if rc := f.run(job, "--lane", "lane-a", "--outcome", outcome, "--reason", "why "+outcome); rc != ExitOK {
			t.Fatalf("%s: rc=%d", outcome, rc)
		}
		records := f.revokedRecords(job)
		if len(records) != 1 || records[0]["outcome"] != outcome || records[0]["reason"] != "why "+outcome {
			t.Fatalf("%s: records = %v", outcome, records)
		}
	}
}

// An unknown job is refused without creating its directory.
func TestJobCloseUnknownJobCreatesNothing(t *testing.T) {
	f := newJobCloseFixture(t)
	if rc := f.run("nope", "--lane", "lane-a", "--outcome", "completed", "--reason", "r"); rc != ExitUsage {
		t.Fatalf("rc=%d", rc)
	}
	if _, err := os.Stat(filepath.Join(f.jobs, "nope")); !os.IsNotExist(err) {
		t.Fatalf("job directory created: %v", err)
	}
}

// A job already ended after its latest claim is not closed again; a job
// revived after its terminal event (reclaim/spawn) can be.
func TestJobCloseTerminalState(t *testing.T) {
	f := newJobCloseFixture(t)
	f.claim("j1", "00001-job.claim.json", "job.claim", "lane-a")
	f.event("j1", "00002-job.completed.json", `{"kind":"job.completed","job_id":"j1","owner_lane":"lane-a"}`)
	if rc := f.run("j1", "--lane", "lane-a", "--outcome", "completed", "--reason", "r"); rc != jobCloseRefused {
		t.Fatalf("already terminal: rc=%d", rc)
	}
	if !strings.Contains(f.stderr.String(), "00002-job.completed.json") {
		t.Fatalf("stderr = %q", f.stderr.String())
	}
	f.claim("j1", "00003-job.reclaim.json", "job.reclaim", "lane-a")
	if rc := f.run("j1", "--lane", "lane-a", "--outcome", "completed", "--reason", "r"); rc != ExitOK {
		t.Fatalf("revived job: rc=%d stderr=%q", rc, f.stderr.String())
	}
	if rc := f.run("j1", "--lane", "lane-a", "--outcome", "completed", "--reason", "again"); rc != jobCloseRefused {
		t.Fatalf("second close: rc=%d", rc)
	}
	if got := len(f.revokedRecords("j1")); got != 1 {
		t.Fatalf("revoked records = %d, want 1", got)
	}
}

// AC3: the declaration closes nothing. The pane stays for wrk reap, which
// (vendored reference, wrk ae1f544) sees the record as terminal and still
// honours its grace from the record's created_at.
func TestJobCloseLeavesThePaneToReapAfterGrace(t *testing.T) {
	f := newJobCloseFixture(t)
	f.now = time.Now()
	f.claim("j1", "00001-job.claim.json", "job.claim", "lane-a")
	f.spawned("j1", "00002-job.spawned.json", "w1:p1", "w1:t1")
	agents := map[string]map[string]string{"w1:p1": {"status": "idle", "tab_id": "w1:t1"}}
	tabs := []any{map[string]any{"tab_id": "w1:t1", "pane_count": 1}}
	if verdict, listed := t501RunReference(t, f.jobs, agents, tabs, false, "0")["j1"]; listed {
		t.Fatalf("reap listed an unterminated job: %v", verdict)
	}
	if rc := f.run("j1", "--lane", "lane-a", "--outcome", "completed", "--reason", "r"); rc != ExitOK {
		t.Fatalf("rc=%d stderr=%q", rc, f.stderr.String())
	}
	f.assertNoSideEffects()
	if verdict, listed := t501RunReference(t, f.jobs, agents, tabs, false, "600")["j1"]; listed {
		t.Fatalf("reap ignored the grace right after the declaration: %v", verdict)
	}
	if verdict := t501RunReference(t, f.jobs, agents, tabs, false, "0")["j1"]; verdict[0] != "would-close" {
		t.Fatalf("reap does not see the declaration as terminal: %v", verdict)
	}
}

// AC6: concurrent declarations serialize on the job's .wrk-events.lock —
// exactly one lands, the rest see it and refuse, and no seq is reused.
func TestJobCloseConcurrentDeclarationsLandOnce(t *testing.T) {
	f := newJobCloseFixture(t)
	f.claim("j1", "00001-job.claim.json", "job.claim", "lane-a")
	const writers = 8
	codes := make([]int, writers)
	var wg sync.WaitGroup
	for i := 0; i < writers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			var stdout, stderr bytes.Buffer
			c := &jobCLI{stdout: &stdout, stderr: &stderr, now: time.Now}
			codes[i] = c.close([]string{"j1", "--lane", "lane-a", "--outcome", "completed", "--reason", fmt.Sprint("writer ", i)})
		}(i)
	}
	wg.Wait()
	ok := 0
	for _, code := range codes {
		switch code {
		case ExitOK:
			ok++
		case jobCloseRefused:
		default:
			t.Fatalf("unexpected rc %d in %v", code, codes)
		}
	}
	if ok != 1 {
		t.Fatalf("successful closes = %d, want 1 (%v)", ok, codes)
	}
	if got := f.names("j1"); len(got) != 2 || got[1] != "00002-job.revoked.json" {
		t.Fatalf("events = %q", got)
	}
}

// AC6: the lock is the one job.completed writers take, so a close waits for
// a holder of .wrk-events.lock instead of scanning past it.
func TestJobCloseWaitsForTheEventsLock(t *testing.T) {
	f := newJobCloseFixture(t)
	f.claim("j1", "00001-job.claim.json", "job.claim", "lane-a")
	lock, err := os.OpenFile(filepath.Join(f.jobs, "j1", ".wrk-events.lock"), os.O_WRONLY|os.O_CREATE, 0o666)
	if err != nil {
		t.Fatal(err)
	}
	defer lock.Close()
	if err := syscall.Flock(int(lock.Fd()), syscall.LOCK_EX); err != nil {
		t.Fatal(err)
	}
	done := make(chan int, 1)
	go func() {
		var stdout, stderr bytes.Buffer
		c := &jobCLI{stdout: &stdout, stderr: &stderr, now: time.Now}
		done <- c.close([]string{"j1", "--lane", "lane-a", "--outcome", "completed", "--reason", "r"})
	}()
	select {
	case rc := <-done:
		t.Fatalf("close ran while the events lock was held: rc=%d", rc)
	case <-time.After(300 * time.Millisecond):
	}
	// A locked writer lands its record meanwhile; the close must see it.
	f.event("j1", "00002-job.completed.json", `{"kind":"job.completed","job_id":"j1"}`)
	if err := syscall.Flock(int(lock.Fd()), syscall.LOCK_UN); err != nil {
		t.Fatal(err)
	}
	if rc := <-done; rc != jobCloseRefused {
		t.Fatalf("close after the locked completion: rc=%d, want refusal", rc)
	}
}

// AC6: publish never replaces an existing name.
func TestJobClosePublishDoesNotReplace(t *testing.T) {
	dir := t.TempDir()
	existing := filepath.Join(dir, "00002-job.revoked.json")
	if err := os.WriteFile(existing, []byte("theirs\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	published, err := publishJobCloseRecord(dir, "00002-job.revoked.json", jobCloseRecord{Kind: jobCloseKind})
	if err != nil || published {
		t.Fatalf("published=%v err=%v", published, err)
	}
	if contents, _ := os.ReadFile(existing); string(contents) != "theirs\n" {
		t.Fatalf("existing record replaced: %q", contents)
	}
	if entries, _ := os.ReadDir(dir); len(entries) != 1 {
		t.Fatalf("temp file left behind: %d entries", len(entries))
	}
}

func TestJobCloseHelpAndDispatch(t *testing.T) {
	var stdout, stderr bytes.Buffer
	if rc := runJobCLI([]string{"close", "--help"}, &stdout, &stderr, CLIConfig{}); rc != ExitOK || !strings.HasPrefix(stdout.String(), "Usage: panewire job close JOB") {
		t.Fatalf("help rc=%d stdout=%q", rc, stdout.String())
	}
}
