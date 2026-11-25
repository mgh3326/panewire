package arbiter

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestForbidHomeForTestsPanicsOnStoreUnderHome proves the test-only guard
// fires on both the database and the events inbox under a forbidden home
// (hk 4685); the fleet and root TestMains register the launch home.
func TestForbidHomeForTestsPanicsOnStoreUnderHome(t *testing.T) {
	home := t.TempDir()
	t.Chdir(home)
	ForbidHomeForTests(home)
	for name, s := range map[string]*Store{
		"db under home":     {DB: StatePath(filepath.Join(home, "data")), Inbox: filepath.Join(t.TempDir(), "jobs")},
		"inbox under home":  {DB: StatePath(filepath.Join(t.TempDir(), "data")), Inbox: filepath.Join(home, "jobs")},
		"both under home":   {DB: StatePath(filepath.Join(home, "data")), Inbox: filepath.Join(home, "jobs")},
		"empty db":          {DB: "", Inbox: filepath.Join(t.TempDir(), "jobs")},
		"empty inbox":       {DB: StatePath(filepath.Join(t.TempDir(), "data")), Inbox: ""},
		"outside forbidden": {DB: StatePath(filepath.Join(t.TempDir(), "data")), Inbox: filepath.Join(t.TempDir(), "jobs")},
	} {
		func() {
			defer func() {
				r := recover()
				if name == "outside forbidden" || name == "empty db" || name == "empty inbox" {
					if r != nil {
						t.Fatalf("%s: unexpected panic %v", name, r)
					}
					return
				}
				msg, ok := r.(string)
				if !ok || !strings.Contains(msg, "real home") {
					t.Fatalf("%s: want guard panic, got %v", name, r)
				}
			}()
			_, _ = s.ro(ctx)
		}()
	}
	if _, err := os.Stat(home); err != nil {
		t.Fatal(err)
	}
}

func TestEmptyInboxRefusesEventWriteUnderForbiddenHome(t *testing.T) {
	home := t.TempDir()
	t.Chdir(home)
	ForbidHomeForTests(home)
	s := &Store{DB: StatePath(filepath.Join(t.TempDir(), "data")), Inbox: ""}

	if _, err := s.AppendEvent(t.Context(), "job-1", "job.note", nil, nil); err == nil {
		t.Fatal("AppendEvent with empty inbox unexpectedly succeeded")
	}
	entries, err := os.ReadDir(home)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Fatalf("empty inbox created files under forbidden home: %v", entries)
	}
}
