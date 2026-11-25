package panewire

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// Orphaned checkouts (hk 6291, docs/pane-sweep.md §Checkout sweep): the job
// dir TTL deletes an ended job's directory while its <job>-checkout still
// lies on the host. These cases run the daily revisit over real git, the real
// spawn journal and the real fleet pass, with hk and herdr faked.

// orphanHost is one node: a clone under its work root, a jobs root holding no
// record of the orphans, a spawn journal, hk records and a herdr whose two
// lists the test sets.
type orphanHost struct {
	t      *testing.T
	root   string
	herdr  string
	client *HubClient
	hk     *roleJobs
}

func newOrphanHost(t *testing.T) *orphanHost {
	t.Helper()
	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	nodeLoopTestGit(t, root, "init", "-b", "main", "clone")
	h := &orphanHost{t: t, root: root, herdr: t.TempDir(), hk: &roleJobs{records: map[string]map[string]any{}}}
	h.write(filepath.Join(root, "clone", "notes.txt"), "seed\n", 0o644)
	nodeLoopTestGit(t, filepath.Join(root, "clone"), "add", "notes.txt")
	nodeLoopTestGit(t, filepath.Join(root, "clone"), "commit", "-m", "seed")
	if err := os.Mkdir(filepath.Join(root, "jobs"), 0o755); err != nil {
		t.Fatal(err)
	}
	spawnCfg := filepath.Join(root, "spawn.json")
	h.write(spawnCfg, `{"work_root": "`+root+`"}`, 0o600)
	script := filepath.Join(h.herdr, "herdr")
	h.write(script, "#!/bin/sh\ncase \"$1 $2\" in\n\"pane list\") cat '"+h.herdr+"/panes.json' ;;\n\"agent list\") cat '"+h.herdr+"/agents.json' ;;\n*) exit 1 ;;\nesac\n", 0o700)
	t.Setenv("HERDR_BIN", script)
	h.setPanes()
	h.setAgents()
	h.client = &HubClient{jobsInboxRoot: root, spawnConfigPath: spawnCfg, spawnJournalDir: t.TempDir(), nodeJobs: h.hk}
	// Held for the whole test, as a running node holds it.
	if _, err := h.client.acquireSpawnJournal(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(h.client.releaseSpawnJournal)
	return h
}

func (h *orphanHost) write(path, body string, mode os.FileMode) {
	h.t.Helper()
	if err := os.WriteFile(path, []byte(body), mode); err != nil {
		h.t.Fatal(err)
	}
}

func (h *orphanHost) herdrList(key string, rows []map[string]string) {
	h.t.Helper()
	if rows == nil {
		rows = []map[string]string{}
	}
	body, err := json.Marshal(map[string]any{"result": map[string]any{key: rows}})
	if err != nil {
		h.t.Fatal(err)
	}
	h.write(filepath.Join(h.herdr, key+".json"), string(body), 0o600)
}

func (h *orphanHost) setPanes(rows ...map[string]string)  { h.herdrList("panes", rows) }
func (h *orphanHost) setAgents(rows ...map[string]string) { h.herdrList("agents", rows) }

// worktree adds the detached worktree <work root>/name holding unsaved work.
func (h *orphanHost) worktree(name string) string {
	h.t.Helper()
	dir := filepath.Join(h.root, name)
	nodeLoopTestGit(h.t, filepath.Join(h.root, "clone"), "worktree", "add", "--detach", dir)
	h.write(filepath.Join(dir, "unsaved.txt"), "unsaved work of "+name+"\n", 0o600)
	return dir
}

// spawned journals a finished spawn of job into dir: rc 0 with its pane, or a
// failure before any pane when pane is empty.
func (h *orphanHost) spawned(request, job, dir, pane string) {
	h.t.Helper()
	h.spawnedAs(request, job, dir, pane, "label-"+job)
}

func (h *orphanHost) spawnedAs(request, job, dir, pane, label string) {
	h.t.Helper()
	h.claimedAs(request, job, label)
	result := hubSpawnResult{RequestID: request, JobID: job, Pane: pane}
	if pane == "" {
		result.RC, result.Error = 70, "spawn_failed"
	}
	receipt := `{"receipt":{"job":"` + job + `","cwd":"` + dir + `","pane":"` + pane + `","label":"` + label + `"}}`
	if err := json.Unmarshal([]byte(receipt), &result); err != nil {
		h.t.Fatal(err)
	}
	if err := h.client.spawnJournal.complete(context.Background(), result); err != nil {
		h.t.Fatal(err)
	}
}

// claimed journals a spawn of job the node has not finished.
func (h *orphanHost) claimed(request, job string) {
	h.t.Helper()
	h.claimedAs(request, job, "label-"+job)
}

func (h *orphanHost) claimedAs(request, job, label string) {
	h.t.Helper()
	claimed, _, err := h.client.spawnJournal.claim(context.Background(), hubOutboundMessage{RequestID: request, Args: []string{"--job", job, "-l", label}}, spawnJournalHold{})
	if err != nil || !claimed {
		h.t.Fatalf("claim %s: claimed=%v err=%v", request, claimed, err)
	}
}

func (h *orphanHost) rows() map[string]string {
	h.t.Helper()
	answer, code := h.client.nodeCheckoutSweep(context.Background(), nodeCheckoutSweepArgs{})
	if code != "" {
		h.t.Fatalf("sweep: %s", code)
	}
	rows := map[string]string{}
	for _, row := range answer.(nodeCheckoutSweepResult).Checkouts {
		rows[filepath.Base(row.Path)] = row.Outcome + " " + row.Reason
	}
	return rows
}

// Acceptance (a) and (b): the next revisit removes an ended reviewer's and an
// ended consult's checkout although the job record is deleted, saves each
// patch outside the gone job directory, and does not make that directory
// again. A spawn that failed before its pane, which hk never held, goes too.
func TestCheckoutOrphanRevisitRemovesEndedJobs_t6291(t *testing.T) {
	h := newOrphanHost(t)
	jobs := map[string]string{
		"mwr-433-7fa212f-p2-1794948989": "completed", // reviewer
		"consult-5076-706d34d8":         "lost",      // consult
		"mwr-662-8e11df7-p1-1794898931": "",          // failed spawn, no hk record
	}
	dirs := map[string]string{}
	for job, state := range jobs {
		dirs[job] = h.worktree(job + "-checkout")
		pane := "w5:p-" + job
		if state == "" {
			pane = ""
		} else {
			h.hk.records[job] = map[string]any{"state": state}
		}
		h.spawned("request-"+job, job, dirs[job], pane)
	}
	// Another job's pane, and a shell in the clone itself, hold nothing here.
	h.setPanes(map[string]string{"pane_id": "w5:other", "label": "other", "cwd": filepath.Join(h.root, "clone")})

	h.client.revisitKeptWorktrees(context.Background())

	for job, dir := range dirs {
		if _, err := os.Lstat(dir); !os.IsNotExist(err) {
			t.Errorf("%s: orphaned checkout still there: %v", job, err)
			continue
		}
		patch := filepath.Join(h.root, "orphan-checkouts", job, "abandoned-worktree.patch")
		body, err := os.ReadFile(patch)
		if err != nil || !strings.Contains(string(body), "unsaved work of "+job+"-checkout") {
			t.Errorf("%s: patch does not hold the work: %v\n%s", job, err, body)
		}
		if info, err := os.Stat(patch); err == nil && info.Mode().Perm() != 0o600 {
			t.Errorf("%s: patch mode %v", job, info.Mode().Perm())
		}
		if _, err := os.Lstat(filepath.Join(h.root, "jobs", job)); !os.IsNotExist(err) {
			t.Errorf("%s: the expired job directory was made again: %v", job, err)
		}
	}
	if snap := h.client.nodeLoopSnapshot()[nodeWorktreeFlow]; snap.Made != 3 || snap.Done != 3 {
		t.Errorf("revisit made=%d done=%d, want 3 and 3", snap.Made, snap.Done)
	}
}

// Acceptance (c) and (d): each checkout that a live job, an open pane or a
// pane inside still uses stays, and so does one whose name no ended job owns.
func TestCheckoutOrphanSweepKeeps_t6291(t *testing.T) {
	h := newOrphanHost(t)
	want := map[string]string{}
	var panes, agents []map[string]string
	// orphan is an ended job whose record is gone; only what a case adds after
	// it may keep the checkout.
	orphan := func(job, verdict string) string {
		dir := h.worktree(job + "-checkout")
		h.spawned("request-"+job, job, dir, "w5:p-"+job)
		h.hk.records[job] = map[string]any{"state": "completed"}
		want[job+"-checkout"] = verdict
		return dir
	}

	// A live job with its local record.
	live := h.worktree("job-live-checkout")
	nodeLoopTestJobEvent(t, filepath.Join(h.root, "jobs"), "job-live", "01.json", "job.spawned", "2026-11-18T10:00:00Z", map[string]any{"cwd": live, "role": "consult"})
	want["job-live-checkout"] = "kept job_live"

	// A live job whose local record is gone: hk still holds it running.
	orphan("job-running", "kept job_live")
	h.hk.records["job-running"]["state"] = "spawned"
	orphan("job-claimed", "kept job_live")
	h.hk.records["job-claimed"]["state"] = "claimed"
	orphan("job-escalated", "kept job_live")
	h.hk.records["job-escalated"]["state"] = "escalated"
	// A spawn of an ended job is running again.
	orphan("job-respawn", "kept job_live")
	h.claimed("request-again", "job-respawn")
	// A launched job hk holds nothing about, and one hk cannot be read for.
	orphan("job-unknown", "kept job_unconfirmed")
	delete(h.hk.records, "job-unknown")
	orphan("job-hkdown", "kept job_unconfirmed")
	h.hk.getErr = map[string]error{"job-hkdown": context.DeadlineExceeded}

	// The job's own pane is open: by pane id, by label, by agent name.
	orphan("job-paneid", "kept pane_open")
	panes = append(panes, map[string]string{"pane_id": "w5:p-job-paneid"})
	orphan("job-label", "kept pane_open")
	panes = append(panes, map[string]string{"pane_id": "w5:p-moved", "label": "label-job-label"})
	orphan("job-agent", "kept pane_open")
	agents = append(agents, map[string]string{"pane_id": "w5:p-agent", "name": "label-job-agent"})
	// Another pane works inside: a plain shell, a foreground process, an agent.
	dir := orphan("job-shell", "kept pane_open")
	if err := os.Mkdir(filepath.Join(dir, "sub"), 0o700); err != nil {
		t.Fatal(err)
	}
	panes = append(panes, map[string]string{"pane_id": "w5:p-shell", "cwd": filepath.Join(dir, "sub")})
	dir = orphan("job-fg", "kept pane_open")
	panes = append(panes, map[string]string{"pane_id": "w5:p-fg", "cwd": h.root, "foreground_cwd": dir})
	dir = orphan("job-inside", "kept pane_open")
	agents = append(agents, map[string]string{"pane_id": "w5:p-inside", "cwd": dir})

	// An ended job spawned into a directory that is not its own <job>-checkout.
	named := h.worktree("session-checkout")
	h.spawned("request-named", "job-named", named, "w5:p-job-named")
	h.hk.records["job-named"] = map[string]any{"state": "completed"}
	want["session-checkout"] = "kept task_open_kept"
	// A name that matches an ended job, which no spawn here put there.
	h.worktree("job-foreign-checkout")
	h.hk.records["job-foreign"] = map[string]any{"state": "completed"}
	want["job-foreign-checkout"] = "kept no_job"

	h.setPanes(panes...)
	h.setAgents(agents...)
	got := h.rows()
	for name, verdict := range want {
		if got[name] != verdict {
			t.Errorf("%s: got %q, want %q", name, got[name], verdict)
		}
		if _, err := os.Stat(filepath.Join(h.root, name, "unsaved.txt")); err != nil {
			t.Errorf("%s: kept checkout was touched: %v", name, err)
		}
	}
	if len(got) != len(want) {
		t.Errorf("rows = %v", got)
	}
}

// A job spawned more than once keeps its checkout while any of those spawns'
// panes is open, not only the newest one's: by pane id or by label, whether the
// later spawn landed elsewhere or failed before a pane.
func TestCheckoutOrphanKeepsEverySpawnsPane_t6291(t *testing.T) {
	h := newOrphanHost(t)
	twice := func(job, secondPane string) {
		dir := h.worktree(job + "-checkout")
		h.spawnedAs("first-"+job, job, dir, "w5:old-"+job, "old-"+job)
		h.spawnedAs("second-"+job, job, dir, secondPane, "new-"+job)
		h.hk.records[job] = map[string]any{"state": "completed"}
	}
	twice("job-oldpane", "w5:new-job-oldpane")
	twice("job-oldlabel", "w5:new-job-oldlabel")
	twice("job-failedretry", "")
	twice("job-allgone", "w5:new-job-allgone")
	// None of these panes works inside a checkout.
	h.setPanes(
		map[string]string{"pane_id": "w5:old-job-oldpane", "cwd": h.root},
		map[string]string{"pane_id": "w5:moved", "label": "old-job-oldlabel", "cwd": h.root},
		map[string]string{"pane_id": "w5:old-job-failedretry", "cwd": h.root},
	)
	got := h.rows()
	for name, verdict := range map[string]string{
		"job-oldpane-checkout":     "kept pane_open",
		"job-oldlabel-checkout":    "kept pane_open",
		"job-failedretry-checkout": "kept pane_open",
		"job-allgone-checkout":     "removed ",
	} {
		if got[name] != verdict {
			t.Errorf("%s: got %q, want %q", name, got[name], verdict)
		}
	}
}

// A herdr list that cannot be read keeps an orphan: no pane is proven absent.
func TestCheckoutOrphanKeptWhenPaneListUnreadable_t6291(t *testing.T) {
	h := newOrphanHost(t)
	dir := h.worktree("job-a-checkout")
	h.spawned("request-a", "job-a", dir, "w5:p1")
	h.hk.records["job-a"] = map[string]any{"state": "completed"}
	if err := os.Remove(filepath.Join(h.herdr, "panes.json")); err != nil {
		t.Fatal(err)
	}
	if got := h.rows()["job-a-checkout"]; got != "kept agents_unreadable" {
		t.Fatalf("got %q", got)
	}
}

// An orphan is removed only under the clone's git operation lock, the one a
// spawn's worktree preparation takes.
func TestCheckoutOrphanWaitsForCloneLock_t6291(t *testing.T) {
	h := newOrphanHost(t)
	dir := h.worktree("job-a-checkout")
	h.spawned("request-a", "job-a", dir, "w5:p1")
	h.hk.records["job-a"] = map[string]any{"state": "completed"}
	path, err := nodeGitOpLockPath(filepath.Join(h.root, "clone"))
	if err != nil {
		t.Fatal(err)
	}
	release, err := tryNodeGitOpLock(path)
	if err != nil {
		t.Fatal(err)
	}
	h.client.nodeGitLockWait = 1 // do not wait out the production budget
	if got := h.rows()["job-a-checkout"]; got != "kept clone_busy" {
		release()
		t.Fatalf("got %q", got)
	}
	release()
	if got := h.rows()["job-a-checkout"]; got != "removed " {
		t.Fatalf("after release: got %q", got)
	}
}

// Acceptance (e): a bad invocation names what is wrong and prints the usage; a
// good one prints the JSON answer.
func TestNodeCheckoutSweepCLIUsage_t6291(t *testing.T) {
	complete := []string{"sweep-checkouts", "--hub-url", "https://hub.example.test", "--hub-token-env", "/nonexistent", "--host", "node-a"}
	for _, test := range []struct {
		name, reason string
		args         []string
	}{
		{"unknown flag", "-dry-runn", append(append([]string{}, complete...), "--dry-runn")},
		{"extra argument", `unexpected argument "now"`, append(append([]string{}, complete...), "now")},
		{"no hub url", "--hub-url is required", []string{"sweep-checkouts", "--host", "node-a", "--dry-run"}},
		{"no token env", "--hub-token-env is required", []string{"sweep-checkouts", "--hub-url", "https://hub.example.test", "--host", "node-a"}},
		{"no host", "--host (or --machine)", complete[:5]},
		{"operator as host", "--host (or --machine)", append(append([]string{}, complete[:5]...), "--host", hubOperatorMachineID)},
	} {
		var out, errout bytes.Buffer
		if code := runNodeCLI(test.args, &out, &errout, hubCLIDeps{}); code != ExitUsage {
			t.Errorf("%s: code=%d", test.name, code)
		}
		if got := errout.String(); !strings.Contains(got, test.reason) || !strings.Contains(got, "Usage: panewire node sweep-checkouts --host MACHINE") {
			t.Errorf("%s: stderr=%q", test.name, got)
		}
		if out.Len() != 0 {
			t.Errorf("%s: stdout=%q", test.name, out.String())
		}
	}
	var out, errout bytes.Buffer
	if code := runNodeCLI([]string{"sweep-checkouts", "--help"}, &out, &errout, hubCLIDeps{}); code != ExitOK || !strings.Contains(out.String(), "Usage: panewire node sweep-checkouts") {
		t.Errorf("--help: code=%d stdout=%q stderr=%q", code, out.String(), errout.String())
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, map[string]any{"result": nodeCheckoutSweepResult{DryRun: true}})
	}))
	defer server.Close()
	credential := filepath.Join(t.TempDir(), "service.env")
	if err := os.WriteFile(credential, []byte("HUB_MACHINE_ID=svc-inspect\nHUB_TOKEN="+pw2ServiceToken("svc-inspect")+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	out.Reset()
	errout.Reset()
	code := runNodeCLI([]string{"sweep-checkouts", "--hub-url", server.URL, "--hub-token-env", credential, "--host", "node-a", "--dry-run"}, &out, &errout, hubCLIDeps{AllowInsecureForTests: true})
	var answer nodeCheckoutSweepResult
	if err := json.Unmarshal(out.Bytes(), &answer); code != ExitOK || err != nil || !answer.DryRun {
		t.Fatalf("success: code=%d err=%v stdout=%q stderr=%q", code, err, out.String(), errout.String())
	}
}
