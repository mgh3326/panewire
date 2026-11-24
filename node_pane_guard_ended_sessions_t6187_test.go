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

// setServerNotRunning answers like herdr for a stopped session: the session exists
// and `herdr session attach` restores it, so it is unreadable, not absent (hk 6214).
func (f *fakeHerdrDir) setServerNotRunning(session string) {
	_ = os.Remove(filepath.Join(f.dir, session+".out"))
	// Same text herdr 0.9.5 prints for a socket with no server behind it.
	errJSON := fmt.Sprintf("{\"id\":\"cli:pane:list\",\"error\":{\"code\":\"server_not_running\",\"message\":\"no herdr server is running at /sessions/%s/herdr.sock; run `herdr session attach %s` to start or attach it\"}}", session, session)
	_ = os.WriteFile(filepath.Join(f.dir, session+".err"), []byte(errJSON), 0o600)
	_ = os.WriteFile(filepath.Join(f.dir, session+".rc"), []byte("1"), 0o600)
}

// setSessionNotFound answers with the one code that proves the session is absent.
func (f *fakeHerdrDir) setSessionNotFound(session string) {
	_ = os.Remove(filepath.Join(f.dir, session+".out"))
	errJSON := fmt.Sprintf(`{"id":"cli:pane:list","error":{"code":"session_not_found","message":"no such session: %s"}}`, session)
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

// TestNodePaneSessionsKeepEndedJobs6187 proves that nodePaneSessions keeps the
// binding of every job, ended or live, whatever its age: a job ending does not
// close its pane, so an old ended job's session is still scanned (hk 6214 removed
// the age skip that hk 6210 added). The spawn session is listed first.
func TestNodePaneSessionsKeepEndedJobs6187(t *testing.T) {
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
	if !seen["sess-old-ended"] {
		t.Errorf("old ended job session not in sessions (age must not skip it): %v", sessions)
	}
}

// TestNodePaneGuardEndedSessions6187 verifies the acceptance cases for both
// held branch release and worktree.remove (hk 6214):
// (a) a job session answering server_not_running is unreadable: fail closed;
// (b) the spawn session answering server_not_running is unreadable: fail closed;
// (c) a session answering session_not_found is absent: release and removal proceed;
// (d) an ended job older than the active window whose session has a pane in the
// worktree still counts as in use. Also: a live job whose session errors on read
// (fail closed) and a live pane in a live job's session (in use).
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
				name: "case c: old ended job whose session is absent (session_not_found)",
				setupJob: func(t *testing.T, inbox string) string {
					sess := "sess-old-ended"
					setupEndedJobSession(t, inbox, "job-ended", "w9:p1", sess, now.Add(-100*time.Hour).Format(time.RFC3339))
					return sess
				},
				setupFake: func(fake *fakeHerdrDir, session, held string) {
					fake.setSessionNotFound(session)
				},
				wantKept: false,
			},
			{
				name: "case a: recent ended job whose session is stopped (server_not_running)",
				setupJob: func(t *testing.T, inbox string) string {
					sess := "sess-recent-ended"
					setupEndedJobSession(t, inbox, "job-ended", "w9:p1", sess, now.Add(-1*time.Hour).Format(time.RFC3339))
					return sess
				},
				setupFake: func(fake *fakeHerdrDir, session, held string) {
					fake.setServerNotRunning(session)
				},
				wantKept: true,
			},
			{
				name: "case b: spawn session stopped (server_not_running)",
				setupJob: func(t *testing.T, inbox string) string {
					return "spawn-a"
				},
				setupFake: func(fake *fakeHerdrDir, session, held string) {
					fake.setServerNotRunning("spawn-a")
				},
				wantKept: true,
			},
			{
				name: "case d: old ended job whose session still has a pane in the worktree",
				setupJob: func(t *testing.T, inbox string) string {
					sess := "sess-old-pane"
					setupEndedJobSession(t, inbox, "job-ended", "w9:p1", sess, now.Add(-100*time.Hour).Format(time.RFC3339))
					return sess
				},
				setupFake: func(fake *fakeHerdrDir, session, held string) {
					fake.setPanes(session, herdrPanesAt(t, filepath.Join(held, "sub")))
				},
				wantKept: true,
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
				name: "case c: old ended job whose session is absent (session_not_found)",
				setupJob: func(t *testing.T, inbox string) string {
					sess := "sess-old-ended"
					setupEndedJobSession(t, inbox, "job-ended", "w9:p1", sess, now.Add(-100*time.Hour).Format(time.RFC3339))
					return sess
				},
				setupFake: func(fake *fakeHerdrDir, session, dir string) {
					fake.setSessionNotFound(session)
				},
				wantOut: nodeRemoveRemoved,
			},
			{
				name: "case a: recent ended job whose session is stopped (server_not_running)",
				setupJob: func(t *testing.T, inbox string) string {
					sess := "sess-recent-ended"
					setupEndedJobSession(t, inbox, "job-ended", "w9:p1", sess, now.Add(-1*time.Hour).Format(time.RFC3339))
					return sess
				},
				setupFake: func(fake *fakeHerdrDir, session, dir string) {
					fake.setServerNotRunning(session)
				},
				wantOut: nodeRemovePaneScanFailed,
			},
			{
				name: "case b: spawn session stopped (server_not_running)",
				setupJob: func(t *testing.T, inbox string) string {
					return "spawn-a"
				},
				setupFake: func(fake *fakeHerdrDir, session, dir string) {
					fake.setServerNotRunning("spawn-a")
				},
				wantOut: nodeRemovePaneScanFailed,
			},
			{
				name: "case d: old ended job whose session still has a pane in the worktree",
				setupJob: func(t *testing.T, inbox string) string {
					sess := "sess-old-pane"
					setupEndedJobSession(t, inbox, "job-ended", "w9:p1", sess, now.Add(-100*time.Hour).Format(time.RFC3339))
					return sess
				},
				setupFake: func(fake *fakeHerdrDir, session, dir string) {
					fake.setPanes(session, herdrPanesAt(t, dir))
				},
				wantOut: nodeRemovePaneInUse,
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
