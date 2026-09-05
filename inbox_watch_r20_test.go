package panewire

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// TE8 pins the decision table. macOS must default to poll: a recursive
// fsnotify watch there opens one descriptor per job directory.
func TestR20InboxWatchModeTable(t *testing.T) {
	for _, testCase := range []struct {
		goos, env, want string
	}{
		{"darwin", "", inboxWatchPoll},
		{"linux", "", inboxWatchFsnotify},
		{"darwin", "fsnotify", inboxWatchFsnotify},
		{"linux", "poll", inboxWatchPoll},
		{"darwin", "kqueue", inboxWatchPoll},
		{"linux", "nonsense", inboxWatchFsnotify},
		{"windows", "", inboxWatchFsnotify},
	} {
		if got := inboxWatchMode(testCase.goos, testCase.env); got != testCase.want {
			t.Fatalf("inboxWatchMode(%q,%q)=%q, want %q", testCase.goos, testCase.env, got, testCase.want)
		}
	}
}

// AC15: poll mode is indistinguishable downstream - the same two event kinds.
func TestR20PollWatcherRecordsTheSameEventKinds(t *testing.T) {
	t.Setenv("PANEWIRE_INBOX_WATCH", inboxWatchPoll)
	t.Setenv("PANEWIRE_INBOX_POLL", "20ms")
	root := t.TempDir()
	store := NewMemoryStore(t)
	defer store.Close()
	watcher, err := NewInboxWatcher(root, store)
	if err != nil {
		t.Fatal(err)
	}
	if watcher.mode != inboxWatchPoll || watcher.watcher != nil {
		t.Fatalf("mode=%q fsnotify=%v; poll mode must open no descriptors", watcher.mode, watcher.watcher != nil)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = watcher.Run(ctx) }()

	// The constructor already took its baseline, so anything below is new.
	nested := filepath.Join(root, "jobs", "r20-poll", "events")
	if err := os.MkdirAll(nested, 0700); err != nil {
		t.Fatal(err)
	}
	target := filepath.Join(nested, "00001-job.completed.json")
	if err := os.WriteFile(target, []byte(`{"type":"job.completed"}`), 0600); err != nil {
		t.Fatal(err)
	}
	waitForEventKind(t, store, "inbox.file_created", 1)

	// A later write to the same path is a change, not another creation.
	later := time.Now().Add(time.Second)
	if err := os.WriteFile(target, []byte(`{"type":"job.completed","epoch":1}`), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Chtimes(target, later, later); err != nil {
		t.Fatal(err)
	}
	waitForEventKind(t, store, "inbox.file_changed", 1)
	if got := store.CountEventKind("inbox.file_created"); got != 1 {
		t.Fatalf("inbox.file_created=%d, want 1", got)
	}
}

func waitForEventKind(t *testing.T, store *Store, kind string, want int) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if store.CountEventKind(kind) >= want {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("%s never reached %d (have %d)", kind, want, store.CountEventKind(kind))
}

// A pre-existing tree is the baseline, not a burst of creations.
func TestR20PollWatcherDoesNotReplayExistingFiles(t *testing.T) {
	t.Setenv("PANEWIRE_INBOX_WATCH", inboxWatchPoll)
	t.Setenv("PANEWIRE_INBOX_POLL", "20ms")
	root := t.TempDir()
	existing := filepath.Join(root, "jobs", "r20-existing", "events")
	if err := os.MkdirAll(existing, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(existing, "00001-job.completed.json"), []byte(`{}`), 0600); err != nil {
		t.Fatal(err)
	}
	store := NewMemoryStore(t)
	defer store.Close()
	watcher, err := NewInboxWatcher(root, store)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = watcher.Run(ctx) }()
	time.Sleep(150 * time.Millisecond)
	if got := store.CountEventKind("inbox.file_created"); got != 0 {
		t.Fatalf("startup replayed %d existing files as new", got)
	}
}
