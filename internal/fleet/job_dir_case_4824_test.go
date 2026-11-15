package fleet

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
)

// hk 4824: two job ids that differ only in letter case would share one
// jobs-inbox directory on a case-insensitive volume (macOS APFS), where one
// job's brief-delivered.md and report overwrite the other's. The launcher
// refuses the second id before it creates or reuses the directory, and does so
// on every volume type.

// jobDirLaunchCalls keeps the host commands a spawn runs once it commits to
// starting something. The refusal path's own deferred fleet event may read the
// repo with git; that is not a launch.
func jobDirLaunchCalls(calls []Command) []Command {
	var launch []Command
	for _, c := range calls {
		if c.Name == "herdr" || c.Name == "panewire" {
			launch = append(launch, c)
		}
	}
	return launch
}

func jobDirTestNames(entries []os.DirEntry) []string {
	names := make([]string, 0, len(entries))
	for _, entry := range entries {
		names = append(names, entry.Name())
	}
	return names
}

func TestSpawnRefusesCaseOnlyJobDirCollision(t *testing.T) {
	a, s, runner, _ := cmdFixture(t)
	existing := filepath.Join(a.Jobs, "Review-A")
	if e := os.MkdirAll(existing, 0700); e != nil {
		t.Fatal(e)
	}
	s.Job = "review-a"
	err := a.Spawn(context.Background(), s)
	if code(err) != exitUsage {
		t.Fatalf("spawn err=%v, want exit %d", err, exitUsage)
	}
	for _, want := range []string{"review-a", "Review-A"} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("refusal %q does not name %q", err, want)
		}
	}
	// The refusal is before the claim that creates jobs/<id>/events and before
	// any pane is started: nothing was written and no host command ran.
	if launch := jobDirLaunchCalls(runner.Calls); len(launch) != 0 {
		t.Fatalf("a refused spawn started host work: %+v", launch)
	}
	if hkClaimed(t) {
		t.Fatal("a refused spawn claimed the task")
	}
	entries, e := os.ReadDir(a.Jobs)
	if e != nil {
		t.Fatal(e)
	}
	if names := jobDirTestNames(entries); len(names) != 1 || names[0] != "Review-A" {
		t.Fatalf("jobs root entries=%v, want only Review-A", names)
	}
	if kept, e := os.ReadDir(existing); e != nil || len(kept) != 0 {
		t.Fatalf("the existing job dir changed: %v err=%v", jobDirTestNames(kept), e)
	}
}

// A non-directory entry owns the name too. On a case-insensitive volume a
// create-first check would answer with the filesystem's "not a directory"
// instead of this refusal, so the entry also proves the check runs before the
// create.
func TestSpawnRefusesCaseOnlyCollisionWithFileEntry(t *testing.T) {
	a, s, runner, _ := cmdFixture(t)
	if e := os.MkdirAll(a.Jobs, 0700); e != nil {
		t.Fatal(e)
	}
	existing := filepath.Join(a.Jobs, "Review-A")
	if e := os.WriteFile(existing, []byte("stale entry\n"), 0600); e != nil {
		t.Fatal(e)
	}
	s.Job = "review-a"
	err := a.Spawn(context.Background(), s)
	if code(err) != exitUsage {
		t.Fatalf("spawn err=%v, want exit %d", err, exitUsage)
	}
	for _, want := range []string{"review-a", "Review-A"} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("refusal %q does not name %q", err, want)
		}
	}
	if launch := jobDirLaunchCalls(runner.Calls); len(launch) != 0 {
		t.Fatalf("a refused spawn started host work: %+v", launch)
	}
	entries, e := os.ReadDir(a.Jobs)
	if e != nil {
		t.Fatal(e)
	}
	if names := jobDirTestNames(entries); len(names) != 1 || names[0] != "Review-A" {
		t.Fatalf("jobs root entries=%v, want only Review-A", names)
	}
	if b, e := os.ReadFile(existing); e != nil || string(b) != "stale entry\n" {
		t.Fatalf("the existing entry changed: %q err=%v", b, e)
	}
}

// An entry whose name is exactly the job id stays today's behaviour: the
// launcher reuses that directory and the brief lands in it.
func TestSpawnReusesExactlyNamedJobDir(t *testing.T) {
	a, s, _, _ := cmdFixture(t)
	if e := os.MkdirAll(filepath.Join(a.Jobs, s.Job), 0700); e != nil {
		t.Fatal(e)
	}
	if e := a.Spawn(context.Background(), s); e != nil {
		t.Fatalf("exact-name reuse refused: %v", e)
	}
	if _, e := os.Stat(filepath.Join(a.Jobs, s.Job, "brief-delivered.md")); e != nil {
		t.Fatalf("brief absent from the reused job dir: %v", e)
	}
}

// A --parent-job takeover is the same namespace: the predecessor's directory
// is the entry a case-only successor id collides with (the reported bug's
// shape: Review-A still running, review-a taking over).
func TestSpawnRefusesCaseOnlyCollisionOnTakeover(t *testing.T) {
	a, s, runner, _ := cmdFixture(t)
	parent := "Review-A"
	putEvent(t, a, parent, "job.claim", map[string]any{"owner_lane": "parent-lane", "task_id": "42", "t_level": "T2", "role": "implementer"})
	// The parent resolves, so a case-only id is the only refusal below.
	if recorded, e := a.resolveParent(context.Background(), parent); e != nil || recorded.Task != "42" {
		t.Fatalf("parent fixture unresolved: %+v %v", recorded, e)
	}
	s.Job = "review-a"
	s.Task = ""
	s.ParentJob = parent
	s.Report = filepath.Join(a.Jobs, parent, "report.md")
	err := a.Spawn(context.Background(), s)
	if code(err) != exitUsage {
		t.Fatalf("takeover err=%v, want exit %d", err, exitUsage)
	}
	for _, want := range []string{"review-a", "Review-A"} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("refusal %q does not name %q", err, want)
		}
	}
	if launch := jobDirLaunchCalls(runner.Calls); len(launch) != 0 {
		t.Fatalf("a refused takeover started host work: %+v", launch)
	}
	entries, e := os.ReadDir(a.Jobs)
	if e != nil {
		t.Fatal(e)
	}
	if names := jobDirTestNames(entries); len(names) != 1 || names[0] != parent {
		t.Fatalf("jobs root entries=%v, want only %s", names, parent)
	}
	// The predecessor's dir holds only its claim event: no delivered brief and
	// no report landed under the successor id.
	kept, e := os.ReadDir(filepath.Join(a.Jobs, parent))
	if e != nil || strings.Join(jobDirTestNames(kept), ",") != "events" {
		t.Fatalf("the predecessor dir changed: %v err=%v", jobDirTestNames(kept), e)
	}
}

// Two launchers that start at once both find no job dir, so the identity
// record's link is the only decision between them: exactly one spelling wins
// and every other spelling is refused naming the winner.
func TestJobDirIdentityConcurrentClaimsAdmitExactlyOne(t *testing.T) {
	root := t.TempDir()
	spellings := []string{"Review-A", "review-a", "REVIEW-A"}
	results := make([]error, len(spellings))
	start := make(chan struct{})
	var wg sync.WaitGroup
	for i, id := range spellings {
		wg.Add(1)
		go func(i int, id string) {
			defer wg.Done()
			<-start
			results[i] = claimJobDirIdentity(root, id)
		}(i, id)
	}
	close(start)
	wg.Wait()
	winner := -1
	for i, err := range results {
		if err == nil {
			if winner != -1 {
				t.Fatalf("%s and %s both claimed one identity", spellings[winner], spellings[i])
			}
			winner = i
		}
	}
	if winner == -1 {
		t.Fatalf("no spelling claimed the identity; errors=%v", results)
	}
	for i, err := range results {
		if i == winner {
			continue
		}
		if code(err) != exitUsage {
			t.Fatalf("%s err=%v, want exit %d", spellings[i], err, exitUsage)
		}
		for _, want := range []string{spellings[i], spellings[winner]} {
			if !strings.Contains(err.Error(), want) {
				t.Fatalf("%s refusal %q does not name %q", spellings[i], err, want)
			}
		}
	}
	// The winner's exact spelling owns the record and can retry; another
	// spelling still refuses.
	if e := claimJobDirIdentity(root, spellings[winner]); e != nil {
		t.Fatalf("the winning spelling was refused its own record: %v", e)
	}
	for i, id := range spellings {
		if i == winner {
			continue
		}
		if e := claimJobDirIdentity(root, id); code(e) != exitUsage {
			t.Fatalf("%s retry err=%v, want exit %d", id, e, exitUsage)
		}
	}
}

// The record is written by a real spawn, so a later spawn under another case
// is refused even when no directory of that case ever existed.
func TestSpawnRefusesOtherCaseAfterIdentityClaimed(t *testing.T) {
	a, s, _, _ := cmdFixture(t)
	if e := a.Spawn(context.Background(), s); e != nil {
		t.Fatal(e)
	}
	record, e := os.ReadFile(filepath.Join(a.Jobs, ".job-ids", strings.ToLower(s.Job)))
	if e != nil || string(record) != s.Job+"\n" {
		t.Fatalf("identity record=%q err=%v, want %q", record, e, s.Job+"\n")
	}
	s.Job = strings.ToUpper(s.Job)
	err := a.Spawn(context.Background(), s)
	if code(err) != exitUsage {
		t.Fatalf("second spawn err=%v, want exit %d", err, exitUsage)
	}
	if !strings.Contains(err.Error(), "job-a") || !strings.Contains(err.Error(), "JOB-A") {
		t.Fatalf("refusal %q does not name both ids", err)
	}
}
