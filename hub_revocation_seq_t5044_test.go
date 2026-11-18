package panewire

import (
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"sync"
	"syscall"
	"testing"
	"time"
)

func TestWriteHubRevocationWaitsForEventsLock_t5044(t *testing.T) {
	inbox := t.TempDir()
	jobDir := filepath.Join(inbox, "jobs", "job-a")
	eventsDir := filepath.Join(jobDir, "events")
	if err := os.MkdirAll(eventsDir, 0700); err != nil {
		t.Fatal(err)
	}
	lock, err := os.OpenFile(filepath.Join(jobDir, ".wrk-events.lock"), os.O_WRONLY|os.O_CREATE, 0o666)
	if err != nil {
		t.Fatal(err)
	}
	defer lock.Close()
	if err := syscall.Flock(int(lock.Fd()), syscall.LOCK_EX); err != nil {
		t.Fatal(err)
	}

	done := make(chan error, 1)
	go func() {
		done <- writeHubRevocation(inbox, hubJobRevokedEvent{Type: "job.revoked", JobID: "job-a", Epoch: 2})
	}()

	select {
	case err := <-done:
		t.Fatalf("writeHubRevocation wrote while .wrk-events.lock was held: %v", err)
	case <-time.After(200 * time.Millisecond):
	}

	spawned := filepath.Join(eventsDir, "00001-job.spawned.json")
	if err := os.WriteFile(spawned, []byte(`{"type":"job.spawned"}`), 0600); err != nil {
		t.Fatal(err)
	}

	if err := syscall.Flock(int(lock.Fd()), syscall.LOCK_UN); err != nil {
		t.Fatal(err)
	}

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("writeHubRevocation failed: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for writeHubRevocation to complete")
	}

	entries, err := os.ReadDir(eventsDir)
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	for _, e := range entries {
		names = append(names, e.Name())
	}
	sort.Strings(names)
	want := []string{"00001-job.spawned.json", "00002-job.revoked.json"}
	if !slices.Equal(names, want) {
		t.Fatalf("events = %v, want %v", names, want)
	}
}

func TestWriteHubRevocationConcurrentWritersNeverShareSeq_t5044(t *testing.T) {
	inbox := t.TempDir()
	const writers = 8
	var start sync.WaitGroup
	start.Add(1)
	var done sync.WaitGroup
	errors := make([]error, writers)

	for i := 0; i < writers; i++ {
		done.Add(1)
		go func(i int) {
			defer done.Done()
			start.Wait()
			errors[i] = writeHubRevocation(inbox, hubJobRevokedEvent{
				Type:  "job.revoked",
				JobID: "job-concurrent",
				Epoch: uint64(i + 2),
			})
		}(i)
	}

	start.Done()
	done.Wait()

	for i, err := range errors {
		if err != nil {
			t.Fatalf("writer %d failed: %v", i, err)
		}
	}

	eventsDir := filepath.Join(inbox, "jobs", "job-concurrent", "events")
	entries, err := os.ReadDir(eventsDir)
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	for _, e := range entries {
		names = append(names, e.Name())
	}
	sort.Strings(names)

	if len(names) != writers {
		t.Fatalf("got %d event files (%v), want %d distinct files (sequences were shared/overwritten)", len(names), names, writers)
	}

	var want []string
	for i := 1; i <= writers; i++ {
		want = append(want, fmt.Sprintf("%05d-job.revoked.json", i))
	}
	if !slices.Equal(names, want) {
		t.Fatalf("events = %v, want %v", names, want)
	}
}
