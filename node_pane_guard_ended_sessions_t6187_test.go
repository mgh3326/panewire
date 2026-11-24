package panewire

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"
)

type fakeHerdrDir struct {
	dir string
}

func newFakeHerdr(t *testing.T) *fakeHerdrDir {
	t.Helper()
	if runtime.GOOS == "windows" {
		t.Skip("shell fake needs a POSIX shell")
	}
	dir := t.TempDir()
	script := fmt.Sprintf(`#!/bin/sh
out="%s/$HERDR_SESSION.out"
err="%s/$HERDR_SESSION.err"
rc="%s/$HERDR_SESSION.rc"

if [ -f "$err" ]; then
    cat "$err" >&2
fi
if [ -f "$out" ]; then
    cat "$out"
fi
if [ -f "$rc" ]; then
    exit $(cat "$rc")
fi
exit 1
`, dir, dir, dir)
	fake := filepath.Join(t.TempDir(), "herdr")
	if err := os.WriteFile(fake, []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	prev := nodeHerdrExecutable
	nodeHerdrExecutable = fake
	t.Cleanup(func() { nodeHerdrExecutable = prev })
	return &fakeHerdrDir{dir: dir}
}

func (f *fakeHerdrDir) setPanes(session, jsonPanes string) {
	_ = os.WriteFile(filepath.Join(f.dir, session+".out"), []byte(jsonPanes), 0o600)
	_ = os.WriteFile(filepath.Join(f.dir, session+".rc"), []byte("0"), 0o600)
}

func (f *fakeHerdrDir) setServerNotRunning(session string) {
	errJSON := fmt.Sprintf(`{"id":"cli:pane:list","error":{"code":"server_not_running","message":"no herdr server is running at /sessions/%s/herdr.sock; run \x60herdr session attach %s\x60 to start or attach it"}}`, session, session)
	_ = os.WriteFile(filepath.Join(f.dir, session+".err"), []byte(errJSON), 0o600)
	_ = os.WriteFile(filepath.Join(f.dir, session+".rc"), []byte("1"), 0o600)
}

func (f *fakeHerdrDir) setReadError(session string) {
	errJSON := `{"id":"cli:pane:list","error":{"code":"internal_error","message":"failed to read panes"}}`
	_ = os.WriteFile(filepath.Join(f.dir, session+".err"), []byte(errJSON), 0o600)
	_ = os.WriteFile(filepath.Join(f.dir, session+".rc"), []byte("1"), 0o600)
}

func setupEndedJobSession(t *testing.T, inbox, jobID, paneID, session, createdAt string) {
	t.Helper()
	eventsDir := filepath.Join(inbox, "jobs", jobID, "events")
	if err := os.MkdirAll(eventsDir, 0o700); err != nil {
		t.Fatal(err)
	}
	payload := map[string]any{
		"pane_id":       paneID,
		"herdr_session": session,
		"label":         jobID,
		"role":          "worker",
	}
	spawnEv := map[string]any{
		"kind":       "job.spawned",
		"payload":    payload,
		"created_at": createdAt,
	}
	b, err := json.Marshal(spawnEv)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(eventsDir, "0001-job.spawned.json"), b, 0o600); err != nil {
		t.Fatal(err)
	}
	completeEv := map[string]any{
		"kind":       "job.completed",
		"created_at": createdAt,
	}
	b, err = json.Marshal(completeEv)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(eventsDir, "0002-job.completed.json"), b, 0o600); err != nil {
		t.Fatal(err)
	}
}

// TestNodePaneSessionsFilteringEndedJobs6187 proves that nodePaneSessions skips
// bindings of ended jobs outside the active window, while keeping live jobs,
// recently ended jobs within the active window, and the spawn session.
func TestNodePaneSessionsFilteringEndedJobs6187(t *testing.T) {
	inbox := t.TempDir()
	now := time.Now().UTC()

	// 1. Old ended job (older than active window 72h)
	setupEndedJobSession(t, inbox, "job-old-ended", "w1:p1", "sess-old-ended", now.Add(-100*time.Hour).Format(time.RFC3339))

	// 2. Recent ended job (inside active window 72h)
	setupEndedJobSession(t, inbox, "job-recent-ended", "w1:p2", "sess-recent-ended", now.Add(-1*time.Hour).Format(time.RFC3339))

	// 3. Live un-ended job
	setupSpawnedJobSession(t, inbox, "job-live", "w1:p3", "sess-live")

	fixture := newNodeWorktreeFixture(t)
	contents, err := json.Marshal(map[string]any{"work_root": fixture.workRoot, "workspace": "w", "enabled": true, "herdr_session": "spawn-sess"})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(fixture.config, contents, 0o600); err != nil {
		t.Fatal(err)
	}
	client := &HubClient{
		spawnConfigPath: fixture.config,
		jobsInboxRoot:   inbox,
		now:             func() time.Time { return now },
	}

	sessions, err := client.nodePaneSessions()
	if err != nil {
		t.Fatalf("nodePaneSessions error: %v", err)
	}

	seen := make(map[string]bool)
	for _, s := range sessions {
		seen[s] = true
	}

	if !seen["spawn-sess"] {
		t.Errorf("spawn session not in sessions: %v", sessions)
	}
	if !seen["sess-live"] {
		t.Errorf("live job session not in sessions: %v", sessions)
	}
	if !seen["sess-recent-ended"] {
		t.Errorf("recent ended job session not in sessions: %v", sessions)
	}
	if seen["sess-old-ended"] {
		t.Errorf("old ended job session unexpectedly in sessions: %v", sessions)
	}
}

// TestNodePaneGuardEndedSessions6187 verifies the 3 acceptance cases for both
// held branch release and worktree.remove:
// (1) an ended job's session that no longer exists: release proceeds and worktree.remove succeeds;
// (2) a live job's session that exists but errors on read: both fail closed;
// (3) a live pane in a live job's non-spawn session still counts as in use.
func TestNodePaneGuardEndedSessions6187(t *testing.T) {
	now := time.Now().UTC()

	t.Run("held branch release", func(t *testing.T) {
		cases := []struct {
			name      string
			setupJob  func(t *testing.T, inbox string) string
			setupFake func(fake *fakeHerdrDir, session, held string)
			wantKept  bool
		}{
			{
				name: "case 1a: old ended job whose session no longer exists (absent/unreadable)",
				setupJob: func(t *testing.T, inbox string) string {
					sess := "sess-old-ended"
					setupEndedJobSession(t, inbox, "job-ended", "w9:p1", sess, now.Add(-100*time.Hour).Format(time.RFC3339))
					return sess
				},
				setupFake: func(fake *fakeHerdrDir, session, held string) {
					// session is absent / unreadable from fake herdr. Because the job is an old
					// ended job, nodePaneSessions skips it and release proceeds.
				},
				wantKept: false,
			},
			{
				name: "case 1b: ended job whose session no longer exists (server_not_running)",
				setupJob: func(t *testing.T, inbox string) string {
					sess := "sess-recent-ended"
					setupEndedJobSession(t, inbox, "job-ended", "w9:p1", sess, now.Add(-1*time.Hour).Format(time.RFC3339))
					return sess
				},
				setupFake: func(fake *fakeHerdrDir, session, held string) {
					fake.setServerNotRunning(session)
				},
				wantKept: false,
			},
			{
				name: "case 2: live job whose session exists but errors on read",
				setupJob: func(t *testing.T, inbox string) string {
					sess := "sess-live-err"
					setupSpawnedJobSession(t, inbox, "job-live", "w9:p1", sess)
					return sess
				},
				setupFake: func(fake *fakeHerdrDir, session, held string) {
					fake.setReadError(session)
				},
				wantKept: true,
			},
			{
				name: "case 3: live pane in live job session",
				setupJob: func(t *testing.T, inbox string) string {
					sess := "sess-live-pane"
					setupSpawnedJobSession(t, inbox, "job-live", "w9:p1", sess)
					return sess
				},
				setupFake: func(fake *fakeHerdrDir, session, held string) {
					fake.setPanes(session, herdrPanesAt(t, filepath.Join(held, "sub")))
				},
				wantKept: true,
			},
		}

		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				fake := newFakeHerdr(t)
				fake.setPanes("spawn-a", herdrNoPanes)

				fixture := newNodeWorktreeFixture(t)
				contents, err := json.Marshal(map[string]any{"work_root": fixture.workRoot, "workspace": "w", "enabled": true, "herdr_session": "spawn-a"})
				if err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(fixture.config, contents, 0o600); err != nil {
					t.Fatal(err)
				}
				inbox := t.TempDir()
				session := tc.setupJob(t, inbox)
				held := fixture.holdBranch(t, "repo.parent", "feat")
				tc.setupFake(fake, session, held)

				client := &HubClient{
					spawnConfigPath: fixture.config,
					jobsInboxRoot:   inbox,
					now:             func() time.Time { return now },
				}
				ctx := boundedTestContext(t)

				if tc.wantKept && tc.name == "case 2: live job whose session exists but errors on read" {
					inUse, err := client.nodePaneInUse(ctx, held)
					if !inUse || err == nil || !strings.Contains(err.Error(), session) {
						t.Fatalf("inUse=%v err=%v, want in use with session named", inUse, err)
					}
				}

				released := client.nodeReleaseHeldBranch(ctx, client.nodeGitRunner(), fixture.clone, nodeWorktreeEntry{Path: held, Branch: "feat"}, fixture.feat)
				if released == tc.wantKept {
					t.Fatalf("release=%v with wantKept=%v", released, tc.wantKept)
				}
				if _, err := os.Stat(held); (err == nil) != tc.wantKept {
					t.Fatalf("holder %s present=%v, want kept=%v", held, err == nil, tc.wantKept)
				}
			})
		}
	})

	t.Run("worktree.remove", func(t *testing.T) {
		cases := []struct {
			name      string
			setupJob  func(t *testing.T, inbox string) string
			setupFake func(fake *fakeHerdrDir, session, dir string)
			wantOut   string
		}{
			{
				name: "case 1a: old ended job whose session no longer exists (absent/unreadable)",
				setupJob: func(t *testing.T, inbox string) string {
					sess := "sess-old-ended"
					setupEndedJobSession(t, inbox, "job-ended", "w9:p1", sess, now.Add(-100*time.Hour).Format(time.RFC3339))
					return sess
				},
				setupFake: func(fake *fakeHerdrDir, session, dir string) {
					// session is absent / unreadable from fake herdr
				},
				wantOut: nodeRemoveRemoved,
			},
			{
				name: "case 1b: ended job whose session no longer exists (server_not_running)",
				setupJob: func(t *testing.T, inbox string) string {
					sess := "sess-recent-ended"
					setupEndedJobSession(t, inbox, "job-ended", "w9:p1", sess, now.Add(-1*time.Hour).Format(time.RFC3339))
					return sess
				},
				setupFake: func(fake *fakeHerdrDir, session, dir string) {
					fake.setServerNotRunning(session)
				},
				wantOut: nodeRemoveRemoved,
			},
			{
				name: "case 2: live job whose session exists but errors on read",
				setupJob: func(t *testing.T, inbox string) string {
					sess := "sess-live-err"
					setupSpawnedJobSession(t, inbox, "job-live", "w9:p1", sess)
					return sess
				},
				setupFake: func(fake *fakeHerdrDir, session, dir string) {
					fake.setReadError(session)
				},
				wantOut: nodeRemovePaneScanFailed,
			},
			{
				name: "case 3: live pane in live job session",
				setupJob: func(t *testing.T, inbox string) string {
					sess := "sess-live-pane"
					setupSpawnedJobSession(t, inbox, "job-live", "w9:p1", sess)
					return sess
				},
				setupFake: func(fake *fakeHerdrDir, session, dir string) {
					fake.setPanes(session, herdrPanesAt(t, dir))
				},
				wantOut: nodeRemovePaneInUse,
			},
		}

		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				fake := newFakeHerdr(t)
				fake.setPanes("spawn-a", herdrNoPanes)

				fixture := newNodeWorktreeFixture(t)
				contents, err := json.Marshal(map[string]any{"work_root": fixture.workRoot, "workspace": "w", "enabled": true, "herdr_session": "spawn-a"})
				if err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(fixture.config, contents, 0o600); err != nil {
					t.Fatal(err)
				}
				inbox := t.TempDir()
				session := tc.setupJob(t, inbox)

				fixture.cloneRepo(t)
				dir, head := fixture.addWorktree(t, "guard")
				tc.setupFake(fake, session, dir)

				client := &HubClient{
					spawnConfigPath: fixture.config,
					jobsInboxRoot:   inbox,
					now:             func() time.Time { return now },
				}

				result, _ := nodeRun(t, client, nodeCommandWorktreeRemove, fixture.removeArgs(dir, head, nil))
				if result["outcome"] != tc.wantOut {
					t.Fatalf("outcome=%v, want %q", result["outcome"], tc.wantOut)
				}
				_, err = os.Stat(dir)
				if removed := err != nil; removed != (tc.wantOut == nodeRemoveRemoved) {
					t.Fatalf("worktree %s removed=%v for outcome %v", dir, removed, result["outcome"])
				}
			})
		}
	})
}
