package panewire

import (
	"os"
	"path/filepath"
	"sync"
	"syscall"
	"testing"
	"time"
)

// holdJobEventsLock takes a job's .wrk-events.lock on its own descriptor, the
// way a concurrent completion writer does, and returns an idempotent release.
func holdJobEventsLock(t *testing.T, jobDir string) func() {
	t.Helper()
	lock, err := os.OpenFile(filepath.Join(jobDir, ".wrk-events.lock"), os.O_WRONLY|os.O_CREATE, 0o666)
	if err != nil {
		t.Fatal(err)
	}
	if err := syscall.Flock(int(lock.Fd()), syscall.LOCK_EX); err != nil {
		lock.Close()
		t.Fatal(err)
	}
	var once sync.Once
	release := func() {
		once.Do(func() {
			syscall.Flock(int(lock.Fd()), syscall.LOCK_UN)
			lock.Close()
		})
	}
	t.Cleanup(release)
	return release
}

// T5692: writeEmitRecord allocates its event seq under the job's
// .wrk-events.lock, the lock completionEvent holds across its scan and write.
// While a completion holds it, a concurrent emit must wait. Otherwise both pick
// 00001-job.completed.json, the later rename replaces the earlier record, and
// the completion is lost.
func TestEmitAllocatesSeqUnderJobEventsLock_t5692(t *testing.T) {
	inbox := t.TempDir()
	jobDir := filepath.Join(inbox, "jobs", "job-e")
	eventsDir := filepath.Join(jobDir, "events")
	if err := os.MkdirAll(eventsDir, 0o700); err != nil {
		t.Fatal(err)
	}
	release := holdJobEventsLock(t, jobDir)

	type emitResult struct {
		path string
		err  error
	}
	done := make(chan emitResult, 1)
	go func() {
		path, err := writeEmitRecord(inbox, emitRecord{
			Type: "job.completed", JobID: "job-e", Epoch: 1, OwnerLane: "lane-e",
			ReportPath: "/reports/emit.md", CreatedAt: "2026-11-20T00:00:00Z",
		})
		done <- emitResult{path, err}
	}()

	// Fixture pacing: a window in which a lock-respecting emit must still be
	// blocked. It proves emit does not finish while the lock is held; the
	// window itself has nothing else to wait for.
	time.Sleep(200 * time.Millisecond)
	select {
	case r := <-done:
		t.Fatalf("emit finished while the job events lock was held (path=%s err=%v)", r.path, r.err)
	default:
	}

	// The simulated completion writes seq 1 under the lock, as completionEvent
	// does, then releases it.
	const completionBody = `{"kind":"job.completed","report_path":"/reports/completion.md"}`
	completion := filepath.Join(eventsDir, "00001-job.completed.json")
	if err := os.WriteFile(completion, []byte(completionBody), 0o600); err != nil {
		t.Fatal(err)
	}
	release()

	var got emitResult
	if !waitUntil(5*time.Second, func() bool {
		select {
		case got = <-done:
			return true
		default:
			return false
		}
	}) {
		t.Fatal("emit did not finish after the job events lock was released")
	}
	if got.err != nil {
		t.Fatalf("emit failed: %v", got.err)
	}
	if want := filepath.Join(eventsDir, "00002-job.completed.json"); got.path != want {
		t.Fatalf("emit wrote %s, want %s (the completion's seq must not be reused)", got.path, want)
	}
	contents, err := os.ReadFile(completion)
	if err != nil || string(contents) != completionBody {
		t.Fatalf("the completion record was overwritten: %q err=%v", contents, err)
	}
}
