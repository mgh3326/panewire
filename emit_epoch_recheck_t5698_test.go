package panewire

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/mgh3326/panewire/internal/jobowner"
)

// T5698: writeEmitRecord revalidates the producer epoch it captured after it
// takes the job's events lock. A revocation or regrant that lands while emit
// waits for that lock must refuse the record, with no event file written under
// the old epoch (docs/producer-attempt-grant.md).
func TestEmitRevalidatesEpochUnderEventsLock_t5698(t *testing.T) {
	cases := []struct {
		name string
		// land writes the grant change the way the hub writer does, while the
		// test holds the job's events lock.
		land func(t *testing.T, jobDir string)
		want error
	}{
		{
			name: "revocation",
			land: func(t *testing.T, jobDir string) {
				writeTestFile(t, filepath.Join(jobDir, "events", "00001-job.revoked.json"),
					`{"type":"job.revoked","job_id":"job-r","epoch":2}`)
			},
			want: jobowner.ErrRevoked,
		},
		{
			name: "regrant",
			land: func(t *testing.T, jobDir string) {
				writeTestFile(t, filepath.Join(jobDir, ".panewire-owner-epoch.json"), `{"epoch":2}`)
			},
			want: jobowner.ErrStaleAttempt,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			inbox := t.TempDir()
			jobDir := filepath.Join(inbox, "jobs", "job-r")
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
					Type: "job.completed", JobID: "job-r", Epoch: 1, OwnerLane: "lane-r",
					ReportPath: "/reports/r.md", CreatedAt: "2026-11-20T00:00:00Z",
				})
				done <- emitResult{path, err}
			}()

			// Fixture pacing: a window in which a lock-respecting emit must still
			// be blocked. It proves emit does not finish while the lock is held;
			// the window itself has nothing else to wait for.
			time.Sleep(200 * time.Millisecond)
			select {
			case r := <-done:
				t.Fatalf("emit finished while the job events lock was held (path=%s err=%v)", r.path, r.err)
			default:
			}

			tc.land(t, jobDir)
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
			if !errors.Is(got.err, tc.want) {
				t.Fatalf("emit err = %v, want %v (the captured epoch 1 is no longer the grant)", got.err, tc.want)
			}
			entries, err := os.ReadDir(eventsDir)
			if err != nil {
				t.Fatal(err)
			}
			for _, entry := range entries {
				if strings.HasSuffix(entry.Name(), "job.completed.json") {
					t.Fatalf("emit published %s under a revoked or superseded epoch", entry.Name())
				}
			}
		})
	}
}
