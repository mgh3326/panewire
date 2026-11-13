package fleet

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/mgh3326/panewire/internal/fleetevent"
)

// hk 3624: the job dir TTL deletes a local job directory only after hk
// confirms the job ended and holds everything the directory recorded, the
// pane is gone and the directory is at least the TTL old. It is a dry run
// unless `reap --apply --expire-job-dirs` (or the node's [reap] job_dir_ttl)
// asks for deletion. Every job directory these tests build lives under
// t.TempDir(); none touches the real ~/work/herdr-inbox.

// ttlRig is one test's jobs root, hk fake, job store, herdr answer and clock.
type ttlRig struct {
	t      *testing.T
	a      *App
	hk     *spawnHK
	store  *fakeJobStore
	agents string
	herdr  int // exit status of `herdr agent list`
	panes  string
	paneRC int // exit status of `herdr pane list`
	base   time.Time
	now    time.Time
	events []fleetevent.Event
}

func newTTLRig(t *testing.T) *ttlRig {
	t.Helper()
	a := testApp(t)
	tmp, err := filepath.EvalSymlinks(os.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if !within(tmp, a.Jobs) {
		t.Fatalf("jobs root %s is not under the temp dir %s", a.Jobs, tmp)
	}
	if home, err := os.UserHomeDir(); err == nil && within(filepath.Join(home, "work", "herdr-inbox"), a.Jobs) {
		t.Fatalf("jobs root %s is under the real herdr-inbox", a.Jobs)
	}
	r := &ttlRig{t: t, a: a, store: a.JobStore.(*fakeJobStore), agents: `{"result":{"agents":[]}}`, panes: `{"result":{"panes":[]}}`, base: a.Now()}
	r.now = r.base
	a.Now = func() time.Time { return r.now }
	r.hk = useFakeHK(t)
	a.Runner = &fakeRunner{Do: func(c Command) Result {
		if c.Name == "herdr" && strings.Join(c.Args, " ") == "agent list" {
			return Result{Out: r.agents, Code: r.herdr}
		}
		if c.Name == "herdr" && strings.Join(c.Args, " ") == "pane list" {
			return Result{Out: r.panes, Code: r.paneRC}
		}
		return Result{}
	}}
	a.EventSink = func(e fleetevent.Event) { r.events = append(r.events, e) }
	if err := os.MkdirAll(a.Jobs, 0700); err != nil {
		t.Fatal(err)
	}
	return r
}

func (r *ttlRig) dir(id string) string { return filepath.Join(r.a.Jobs, id) }

func (r *ttlRig) write(id, name string, body []byte) {
	r.t.Helper()
	path := filepath.Join(r.dir(id), name)
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		r.t.Fatal(err)
	}
	if err := os.WriteFile(path, body, 0600); err != nil {
		r.t.Fatal(err)
	}
}

// age sets every mtime under the job dir to the rig's base time, so the
// directory is as old as the clock says.
func (r *ttlRig) age(id string) {
	r.t.Helper()
	err := filepath.WalkDir(r.dir(id), func(path string, _ fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		return os.Chtimes(path, r.base, r.base)
	})
	if err != nil {
		r.t.Fatal(err)
	}
}

func sha(b []byte) string {
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

// doc stores an hk document whose sha256 is that of body unless want names
// another.
func (r *ttlRig) doc(key string, body []byte, want string) {
	if want == "" {
		want = sha(body)
	}
	b, _ := json.Marshal(map[string]any{"key": key, "kind": "report", "body": string(body), "sha256": want})
	r.hk.mu.Lock()
	defer r.hk.mu.Unlock()
	r.hk.docs[key] = string(b)
}

// undoc removes a document from the fake hk.
func (r *ttlRig) undoc(key string) {
	r.hk.mu.Lock()
	defer r.hk.mu.Unlock()
	delete(r.hk.docs, key)
}

// seedJob builds a job dir every gate condition passes for: claimed,
// spawned, completed and reaped, its report published to hk in a completed hk record,
// its pane gone and every file at the base time.
func (r *ttlRig) seedJob(id string) {
	r.t.Helper()
	putEvent(r.t, r.a, id, "job.claim", map[string]any{"owner_lane": "owner", "parent_lane": "parent", "task_id": "42", "t_level": "T1", "role": "worker"})
	putEvent(r.t, r.a, id, "job.spawned", map[string]any{"pane_id": "w:" + id, "tab_id": "w:t1", "herdr_session": "default", "label": id, "profile": "cmd-dsflash41", "family": "deepseek", "keep": false})
	putEvent(r.t, r.a, id, "job.completed", map[string]any{"report": filepath.Join(r.dir(id), "report.md")})
	putEvent(r.t, r.a, id, "job.reaped", map[string]any{"pane_id": "w:" + id})
	report := []byte("work done\nREADY: done\n")
	r.write(id, "report.md", report)
	r.write(id, "brief.md", []byte("the brief\n"))
	r.store.records[id] = map[string]any{"job_id": id, "task_id": "42", "state": "completed"}
	r.pin(id, "report.md", report)
	r.age(id)
}

// seedSubmission builds a submitter-side dir: brief.md, spawn.out and
// submission.json, no events.
func (r *ttlRig) seedSubmission(id string, extra ...string) {
	r.t.Helper()
	args := append([]string{"-c", "/work/tree", "-m", "cmd-dsflash41", "-p", "brief.md", "-l", id, "--job", id, "--task", "42", "--owner", "owner"}, extra...)
	raw, _ := json.Marshal(Submission{RequestID: "0f8fad5b-d9cb-469f-a165-70867728950e", Args: args, Brief: "the brief", OriginMachine: "test-host", DeadlineSeconds: 60})
	r.write(id, "submission.json", raw)
	r.write(id, "brief.md", []byte("the brief\n"))
	r.write(id, "spawn.out", []byte("ACCEPTED\n"))
	r.age(id)
}

// pass runs one TTL pass with the default TTL at base+73h and returns its
// output.
func (r *ttlRig) pass(apply bool) string {
	r.t.Helper()
	out, err := r.passWith(&JobDirTTL{TTL: DefaultJobDirTTL, Apply: apply})
	if err != nil {
		r.t.Fatal(err)
	}
	return out
}

func (r *ttlRig) passWith(p *JobDirTTL) (string, error) {
	r.t.Helper()
	if r.now.Equal(r.base) {
		r.now = r.base.Add(73 * time.Hour)
	}
	var out bytes.Buffer
	p.Out = &out
	_, err := p.Pass(context.Background(), r.a)
	return out.String(), err
}

func (r *ttlRig) exists(id string) bool {
	_, err := os.Lstat(r.dir(id))
	return err == nil
}

// line is the pass's line for job id.
func line(out, id string) string {
	for _, l := range strings.Split(out, "\n") {
		if strings.HasPrefix(l, "job="+id+" ") {
			return l
		}
	}
	return ""
}

func wantKeep(t *testing.T, r *ttlRig, out, id, reason string) {
	t.Helper()
	if got := line(out, id); !strings.HasPrefix(got, "job="+id+" action=keep reason="+reason+" ") {
		t.Fatalf("line for %s = %q, want action=keep reason=%s\n%s", id, got, reason, out)
	}
	if !r.exists(id) {
		t.Fatalf("%s was deleted, want it kept for %s", id, reason)
	}
}

func wantDelete(t *testing.T, r *ttlRig, out, id, reason string) {
	t.Helper()
	if got := line(out, id); !strings.HasPrefix(got, "job="+id+" action=delete reason="+reason+" ") {
		t.Fatalf("line for %s = %q, want action=delete reason=%s\n%s", id, got, reason, out)
	}
	if r.exists(id) {
		t.Fatalf("%s still exists after an apply pass", id)
	}
}

func TestJobDirTTLDryRunListsAndApplyDeletes(t *testing.T) {
	r := newTTLRig(t)
	r.seedJob("j1")
	r.write("scratch", "notes.md", []byte("lead scratch\n"))
	r.age("scratch")
	out := r.pass(false)
	if got := line(out, "j1"); !strings.HasPrefix(got, "job=j1 action=delete reason=confirmed bytes=") {
		t.Fatalf("dry-run line = %q\n%s", got, out)
	}
	wantKeep(t, r, out, "scratch", "not-a-job")
	if !r.exists("j1") {
		t.Fatal("a dry run deleted j1")
	}
	if !strings.Contains(out, "job-dir-ttl apply=false ttl=72h0m0s delete=1 keep=1 bytes=") || !strings.Contains(out, "reasons=confirmed:1,not-a-job:1") {
		t.Fatalf("dry-run summary missing:\n%s", out)
	}
	if len(r.events) != 0 {
		t.Fatalf("a dry run recorded fleet events %+v", r.events)
	}
	t.Logf("dry-run sample:\n%s", out)

	out = r.pass(true)
	wantDelete(t, r, out, "j1", "confirmed")
	if !r.exists("scratch") {
		t.Fatal("the lead scratch dir was deleted")
	}
	if len(r.events) != 1 || r.events[0].Kind != "job_dir_expired" || r.events[0].Job != "j1" || r.events[0].Outcome != "deleted" || r.events[0].Reason != "confirmed" || !strings.HasPrefix(r.events[0].Detail, "bytes=") || r.events[0].Detail == "bytes=0" {
		t.Fatalf("fleet events = %+v, want one job_dir_expired for j1 with its bytes", r.events)
	}
}

// Each gate condition alone keeps a dir every other condition would let go.
func TestJobDirTTLEachGateConditionAloneKeeps(t *testing.T) {
	cases := []struct {
		name, reason string
		break_       func(r *ttlRig)
	}{
		{"keep flag", "protected", func(r *ttlRig) {
			putEvent(r.t, r.a, "j1", "job.note", map[string]any{"keep": true})
			r.age("j1")
		}},
		{"watcher active", "watcher-active", func(r *ttlRig) {
			r.write("j1", "completion-sentinel.pid", []byte("123\n"))
			r.age("j1")
		}},
		{"hk claimed", "hk-not-terminal", func(r *ttlRig) { r.store.records["j1"]["state"] = "claimed" }},
		{"hk escalated", "hk-not-terminal", func(r *ttlRig) { r.store.records["j1"]["state"] = "escalated" }},
		{"hk record missing", "hk-missing", func(r *ttlRig) { delete(r.store.records, "j1") }},
		{"live pane", "pane-live", func(r *ttlRig) {
			r.agents = `{"result":{"agents":[{"pane_id":"w:j1","tab_id":"w:t1","name":"j1","agent_status":"idle"}]}}`
		}},
		{"live pane under the label", "pane-live", func(r *ttlRig) {
			r.agents = `{"result":{"agents":[{"pane_id":"w:other","tab_id":"w:t9","name":"j1","agent_status":"idle"}]}}`
		}},
		{"too young", "too-young", func(r *ttlRig) { r.now = r.base.Add(71 * time.Hour) }},
		{"newest file too young", "too-young", func(r *ttlRig) {
			r.write("j1", "late.txt", []byte("x"))
			_ = os.Chtimes(filepath.Join(r.dir("j1"), "late.txt"), r.base.Add(2*time.Hour), r.base.Add(2*time.Hour))
		}},
		{"report edited after done", "report-unconfirmed", func(r *ttlRig) {
			r.write("j1", "report.md", []byte("edited after done\n"))
			r.age("j1")
		}},
		{"report not in hk", "report-unconfirmed", func(r *ttlRig) { r.undoc(reportDocumentKey("j1", sha([]byte("work done\nREADY: done\n")))) }},
		{"rotated report without a doc", "report-unconfirmed", func(r *ttlRig) {
			r.write("j1", "report-r2.md", []byte("second round\n"))
			r.age("j1")
		}},
		{"legacy report doc sha differs", "report-sha-mismatch", func(r *ttlRig) {
			r.undoc(reportDocumentKey("j1", sha([]byte("work done\nREADY: done\n"))))
			r.doc("reports/j1/report.md", []byte("work done\nREADY: done\n"), sha([]byte("other bytes")))
		}},
		{"findings without a result", "result-unconfirmed", func(r *ttlRig) {
			r.write("j1", "findings.json", []byte(`{"findings":[]}`))
			r.age("j1")
		}},
		{"result.json without a result", "result-unconfirmed", func(r *ttlRig) {
			r.write("j1", "result.json", []byte(`{"ok":true}`))
			r.store.records["j1"]["result"] = map[string]any{}
			r.age("j1")
		}},
		{"unconfirmed evidence", "evidence-unconfirmed", func(r *ttlRig) {
			r.hk.SetTask("42", hkTaskJSON(42, "merged", "t", "", "proj", "{}"))
			r.hk.Handle(func(req hkRequest) (int, string, bool) {
				return http.StatusOK, `{"documents":[]}`, req.Method == http.MethodGet && req.Path == "/v1/documents"
			})
			r.write("j1", "evidence/run.log", []byte("log\n"))
			r.age("j1")
		}},
		{"unconfirmed final build", "evidence-unconfirmed", func(r *ttlRig) {
			r.write("j1", "final/app.bin", []byte("bin\n"))
			r.age("j1")
		}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			r := newTTLRig(t)
			r.seedJob("j1")
			c.break_(r)
			out := r.pass(true)
			wantKeep(t, r, out, "j1", c.reason)
			if len(r.events) != 0 {
				t.Fatalf("a kept dir recorded fleet events %+v", r.events)
			}
		})
	}
}

// The artifact checks pass when hk holds what the dir holds.
func TestJobDirTTLConfirmedArtifactsDelete(t *testing.T) {
	cases := []struct {
		name, note string
		add        func(r *ttlRig)
	}{
		{"unpinned report matches its doc", "", func(r *ttlRig) {
			delete(r.store.records["j1"], "report_sha256")
			r.doc("reports/j1/report.md", []byte("work done\nREADY: done\n"), "")
		}},
		{"rotated report matches its doc", "", func(r *ttlRig) {
			r.write("j1", "report-r2.md", []byte("second round\n"))
			r.doc("reports/j1/report-r2.md", []byte("second round\n"), "")
			r.age("j1")
		}},
		{"findings with a result", "", func(r *ttlRig) {
			r.write("j1", "findings.json", []byte(`{"findings":[]}`))
			r.store.records["j1"]["result"] = map[string]any{"findings": []any{}}
			r.age("j1")
		}},
		{"screen matches its doc", "", func(r *ttlRig) {
			r.write("j1", PaneScreenFile, []byte("$ \n"))
			r.doc(PaneScreenDocKey("j1"), []byte("$ \n"), "")
			r.age("j1")
		}},
		// hk 3702: a leftover pane-screen.txt is an ordinary file of the dir;
		// hk's copy of the screen does not decide the gate.
		{"leftover screen without a doc", "", func(r *ttlRig) {
			r.write("j1", PaneScreenFile, []byte("$ \n"))
			r.age("j1")
		}},
		{"leftover screen with another sha", "", func(r *ttlRig) {
			r.write("j1", PaneScreenFile, []byte("$ \n"))
			r.doc(PaneScreenDocKey("j1"), []byte("$ other\n"), "")
			r.age("j1")
		}},
		{"leftover screen over the cap", "", func(r *ttlRig) {
			big := bytes.Repeat([]byte("x"), PaneScreenDocMaxBytes+10)
			r.write("j1", PaneScreenFile, big)
			r.age("j1")
		}},
		{"drive-confirmed evidence", "", func(r *ttlRig) {
			log := []byte("log\n")
			r.write("j1", "evidence/run.log", log)
			r.age("j1")
			src := filepath.Join(r.dir("j1"), "evidence")
			rec := ArtifactRecord{Schema: "fleet.artifact.v2", Name: "run", Host: r.a.machine(), Job: "j1", Task: "42", Archive: Archive{Source: src, State: "drive_confirmed", Contents: []Content{{Path: "run.log", Bytes: int64(len(log)), SHA: sha(log)}}}}
			body, _ := json.Marshal(rec)
			key := "artifacts/proj/42/j1/run"
			r.hk.SetTask("42", hkTaskJSON(42, "merged", "t", "", "proj", "{}"))
			r.hk.SetDoc(key, string(body))
			r.hk.Handle(func(req hkRequest) (int, string, bool) {
				return http.StatusOK, `{"documents":[{"key":"` + key + `"}]}`, req.Method == http.MethodGet && req.Path == "/v1/documents"
			})
		}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			r := newTTLRig(t)
			r.seedJob("j1")
			c.add(r)
			out := r.pass(true)
			wantDelete(t, r, out, "j1", "confirmed")
			if c.note != "" && !strings.HasSuffix(line(out, "j1"), c.note) {
				t.Fatalf("line %q lacks%s", line(out, "j1"), c.note)
			}
		})
	}
}

// An hk outage or an answer panewire cannot read keeps the dir.
func TestJobDirTTLHKOutageOrUnreadableAnswerKeeps(t *testing.T) {
	cases := []struct {
		name   string
		break_ func(r *ttlRig)
	}{
		{"job record read fails", func(r *ttlRig) { r.store.getErr = errors.New("connection refused") }},
		{"job record answer unreadable", func(r *ttlRig) { r.store.getErr = errHKUnreadable }},
		{"record names another job", func(r *ttlRig) { r.store.records["j1"]["job_id"] = "j2" }},
		{"hk down for the report doc", func(r *ttlRig) {
			delete(r.store.records["j1"], "report_sha256")
			r.hk.Close()
		}},
		{"report doc answer unreadable", func(r *ttlRig) {
			delete(r.store.records["j1"], "report_sha256")
			r.hk.Handle(func(req hkRequest) (int, string, bool) {
				return http.StatusOK, `{"key":`, strings.HasPrefix(req.Path, "/v1/documents/")
			})
		}},
		{"hk unconfigured", func(r *ttlRig) {
			delete(r.store.records["j1"], "report_sha256")
			r.t.Setenv("HANDOFFKEEP_URL", "")
			r.t.Setenv("HANDOFFKEEP_TOKEN_SVC_PANEWIRE", "")
		}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			r := newTTLRig(t)
			r.seedJob("j1")
			c.break_(r)
			out := r.pass(true)
			wantKeep(t, r, out, "j1", "hk-unreadable")
		})
	}
}

func TestJobDirTTLUnreadablePaneStateKeeps(t *testing.T) {
	r := newTTLRig(t)
	r.seedJob("j1")
	r.herdr = 1
	out := r.pass(true)
	wantKeep(t, r, out, "j1", "pane-unreadable")
}

// A plain `reap --apply` closes panes as before and deletes no dir; only
// --expire-job-dirs adds the pass, and only --apply makes it delete.
func TestJobDirTTLReapNeedsExpireFlag(t *testing.T) {
	r := newTTLRig(t)
	r.seedJob("j1")
	r.now = r.base.Add(73 * time.Hour)
	run := func(args ...string) string {
		var out bytes.Buffer
		r.a.Out = &out
		if err := r.a.hygiene(context.Background(), "reap", args); err != nil {
			t.Fatalf("reap %v: %v", args, err)
		}
		return out.String()
	}
	if out := run("--apply"); !r.exists("j1") || strings.Contains(out, "action=delete") || strings.Contains(out, "job-dir-ttl") {
		t.Fatalf("plain reap --apply touched the job dir (exists=%t):\n%s", r.exists("j1"), out)
	}
	if out := run("--expire-job-dirs"); !r.exists("j1") || !strings.Contains(line(out, "j1"), "action=delete reason=confirmed") {
		t.Fatalf("reap --expire-job-dirs without --apply: exists=%t\n%s", r.exists("j1"), out)
	}
	out := run("--apply", "--expire-job-dirs")
	wantDelete(t, r, out, "j1", "confirmed")
	if err := r.a.hygiene(context.Background(), "prune", []string{"--expire-job-dirs"}); code(err) != exitUsage {
		t.Fatalf("prune --expire-job-dirs: %v, want a usage error", err)
	}
}

// ttlHub answers the hub spawn operation's status for a submitter-side dir.
type ttlHub struct {
	status string
	err    error
	asked  []string
}

func (h *ttlHub) Spawn(context.Context, HubSpawnRequest) (HubSpawnReply, error) {
	return HubSpawnReply{}, errors.New("unused")
}
func (h *ttlHub) Refresh(context.Context, string, string) error { return nil }
func (h *ttlHub) SpawnStatus(_ context.Context, id string) (string, error) {
	h.asked = append(h.asked, id)
	return h.status, h.err
}

func TestJobDirTTLSubmitterSideDirs(t *testing.T) {
	cases := []struct {
		name, action, reason string
		setup                func(r *ttlRig, hub *ttlHub)
	}{
		{"terminal record", "delete", "submission-terminal", func(r *ttlRig, _ *ttlHub) {
			r.store.records["s1"] = map[string]any{"job_id": "s1", "state": "completed"}
		}},
		{"lost record", "delete", "submission-terminal", func(r *ttlRig, _ *ttlHub) {
			r.store.records["s1"] = map[string]any{"job_id": "s1", "state": "lost"}
		}},
		{"live record", "keep", "hk-not-terminal", func(r *ttlRig, _ *ttlHub) {
			r.store.records["s1"] = map[string]any{"job_id": "s1", "state": "spawned"}
		}},
		{"escalated record", "keep", "hk-not-terminal", func(r *ttlRig, _ *ttlHub) {
			r.store.records["s1"] = map[string]any{"job_id": "s1", "state": "escalated"}
		}},
		{"no record, spawn failed", "delete", "submission-spawn-failed", func(r *ttlRig, hub *ttlHub) { hub.status = "failed" }},
		{"no record, spawn succeeded", "keep", "hk-missing", func(r *ttlRig, hub *ttlHub) { hub.status = "succeeded" }},
		{"no record, spawn still dispatching", "keep", "hk-missing", func(r *ttlRig, hub *ttlHub) { hub.status = "dispatching" }},
		{"no record, spawn status unreadable", "keep", "spawn-status-unreadable", func(r *ttlRig, hub *ttlHub) { hub.err = errors.New("hub down") }},
		{"hk outage", "keep", "hk-unreadable", func(r *ttlRig, _ *ttlHub) { r.store.getErr = errors.New("connection refused") }},
		{"too young", "keep", "too-young", func(r *ttlRig, _ *ttlHub) {
			r.store.records["s1"] = map[string]any{"job_id": "s1", "state": "completed"}
			r.now = r.base.Add(71 * time.Hour)
		}},
		{"report without a doc", "keep", "report-unconfirmed", func(r *ttlRig, _ *ttlHub) {
			r.store.records["s1"] = map[string]any{"job_id": "s1", "state": "completed"}
			r.write("s1", "report.md", []byte("x\n"))
			r.age("s1")
		}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			r := newTTLRig(t)
			hub := &ttlHub{}
			r.a.Hub = hub
			r.seedSubmission("s1")
			c.setup(r, hub)
			out := r.pass(true)
			if c.action == "delete" {
				wantDelete(t, r, out, "s1", c.reason)
			} else {
				wantKeep(t, r, out, "s1", c.reason)
			}
		})
	}
	t.Run("submission for another job", func(t *testing.T) {
		r := newTTLRig(t)
		r.seedSubmission("s1")
		raw, _ := os.ReadFile(filepath.Join(r.dir("s1"), "submission.json"))
		r.write("s2", "submission.json", raw)
		r.age("s2")
		r.store.records["s1"] = map[string]any{"job_id": "s1", "state": "completed"}
		r.store.records["s2"] = map[string]any{"job_id": "s2", "state": "completed"}
		wantKeep(t, r, r.pass(true), "s2", "submission-mismatch")
	})
	t.Run("keep submission", func(t *testing.T) {
		r := newTTLRig(t)
		r.seedSubmission("s1", "--keep")
		r.store.records["s1"] = map[string]any{"job_id": "s1", "state": "completed"}
		wantKeep(t, r, r.pass(true), "s1", "protected")
	})
	t.Run("unreadable submission", func(t *testing.T) {
		r := newTTLRig(t)
		r.write("s1", "submission.json", []byte("{"))
		r.age("s1")
		r.store.records["s1"] = map[string]any{"job_id": "s1", "state": "completed"}
		wantKeep(t, r, r.pass(true), "s1", "submission-unreadable")
	})
}

// The hub client reads one operation's status from GET /v1/spawns/<id>.
func TestJobDirTTLHubSpawnStatus(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/spawns/op-failed":
			fmt.Fprint(w, `{"status":"failed","result":{"rc":75,"reason":"x"}}`)
		case "/v1/spawns/op-garbled":
			fmt.Fprint(w, `{"status":`)
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer srv.Close()
	hub := &hubHTTP{base: srv.URL, token: "t"}
	if got, err := hub.SpawnStatus(context.Background(), "op-failed"); err != nil || got != "failed" {
		t.Fatalf("status = %q, %v; want failed", got, err)
	}
	for _, id := range []string{"op-garbled", "op-missing"} {
		if got, err := hub.SpawnStatus(context.Background(), id); err == nil {
			t.Fatalf("%s: status %q, want an error", id, got)
		}
	}
}

// A symlink in the jobs root is never followed: one that resolves outside
// the root is refused as outside-root, one that resolves inside as symlink.
func TestJobDirTTLRefusesSymlinkAndOutsideRoot(t *testing.T) {
	r := newTTLRig(t)
	r.seedJob("j1")
	// An eligible-looking job dir outside the jobs root.
	outside := filepath.Join(r.a.Home, "outside")
	r.seedJob("evil")
	if err := os.Rename(r.dir("evil"), outside); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(outside, r.dir("evil")); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(r.dir("j1"), r.dir("alias")); err != nil {
		t.Fatal(err)
	}
	r.store.records["alias"] = map[string]any{"job_id": "alias", "state": "completed"}
	out := r.pass(true)
	wantKeep(t, r, out, "evil", "outside-root")
	wantKeep(t, r, out, "alias", "symlink")
	if _, err := os.Stat(filepath.Join(outside, "report.md")); err != nil {
		t.Fatalf("the dir outside the root lost its files: %v", err)
	}
	if len(r.events) != 1 || r.events[0].Job != "j1" {
		t.Fatalf("fleet events = %+v, want only j1 deleted", r.events)
	}
}

// The gate is decided again under the job's operation lock right before the
// delete; a dir that changed between the scan and the delete stays.
func TestJobDirTTLLockRecheckCatchesChange(t *testing.T) {
	cases := []struct {
		name, reason string
		change       func(r *ttlRig)
	}{
		{"file added with an old mtime", "changed-during-pass", func(r *ttlRig) {
			r.write("j1", "notes.txt", []byte("written mid-pass\n"))
			_ = os.Chtimes(filepath.Join(r.dir("j1"), "notes.txt"), r.base, r.base)
		}},
		{"pane appeared", "pane-live", func(r *ttlRig) {
			r.agents = `{"result":{"agents":[{"pane_id":"w:j1","tab_id":"w:t1","name":"j1","agent_status":"working"}]}}`
		}},
		{"record reopened", "hk-not-terminal", func(r *ttlRig) { r.store.records["j1"]["state"] = "spawned" }},
		{"swapped for a symlink out of the root", "outside-root", func(r *ttlRig) {
			moved := filepath.Join(r.a.Home, "moved-j1")
			if err := os.Rename(r.dir("j1"), moved); err != nil {
				r.t.Fatal(err)
			}
			if err := os.Symlink(moved, r.dir("j1")); err != nil {
				r.t.Fatal(err)
			}
		}},
		{"operation lock held", "locked", func(r *ttlRig) {
			f, err := os.OpenFile(filepath.Join(r.dir("j1"), ".panewire-operation.lock"), os.O_CREATE|os.O_RDWR, 0600)
			if err != nil {
				r.t.Fatal(err)
			}
			r.t.Cleanup(func() { f.Close() })
			if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX); err != nil {
				r.t.Fatal(err)
			}
		}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			r := newTTLRig(t)
			r.seedJob("j1")
			scanned := false
			out, err := r.passWith(&JobDirTTL{TTL: DefaultJobDirTTL, Apply: true, beforeRecheck: func(id string) {
				if id == "j1" {
					scanned = true
					c.change(r)
				}
			}})
			if err != nil {
				t.Fatal(err)
			}
			if !scanned {
				t.Fatal("the pass never reached the locked re-check")
			}
			wantKeep(t, r, out, "j1", c.reason)
			if moved := filepath.Join(r.a.Home, "moved-j1"); c.reason == "outside-root" {
				if _, err := os.Stat(filepath.Join(moved, "report.md")); err != nil {
					t.Fatalf("the moved dir lost its files: %v", err)
				}
			}
		})
	}
}

func TestJobDirTTLKeepsWhatIsNotAJob(t *testing.T) {
	r := newTTLRig(t)
	r.write("lead-scratch", "notes.md", []byte("x\n"))
	r.write("_reaped", "old.txt", []byte("x\n"))
	r.write("broken", "events/0000-job.claim.json", []byte("{"))
	for _, id := range []string{"lead-scratch", "_reaped", "broken"} {
		r.age(id)
	}
	if err := os.WriteFile(filepath.Join(r.a.Jobs, "stray.txt"), []byte("x"), 0600); err != nil {
		t.Fatal(err)
	}
	out := r.pass(true)
	wantKeep(t, r, out, "lead-scratch", "not-a-job")
	wantKeep(t, r, out, "_reaped", "invalid-id")
	wantKeep(t, r, out, "broken", "events-unreadable")
	if line(out, "stray.txt") != "" {
		t.Fatalf("a plain file in the root was listed:\n%s", out)
	}
}

func TestJobDirTTLRefusesATTLBelowTheFloor(t *testing.T) {
	r := newTTLRig(t)
	r.seedJob("j1")
	if _, err := r.passWith(&JobDirTTL{TTL: time.Hour, Apply: true}); err == nil || !r.exists("j1") {
		t.Fatalf("a 1h TTL ran: err=%v exists=%t", err, r.exists("j1"))
	}
}

func TestJobDirTTLConfigKey(t *testing.T) {
	dir := t.TempDir()
	load := func(body string) (Config, error) {
		path := filepath.Join(dir, "hosts.toml")
		if err := os.WriteFile(path, []byte(body), 0600); err != nil {
			t.Fatal(err)
		}
		return LoadConfig(path)
	}
	c, err := load("")
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := ConfiguredJobDirTTL(c); ok {
		t.Fatal("an empty config set a job dir TTL")
	}
	for body, want := range map[string]time.Duration{"[reap]\njob_dir_ttl = \"72h\"\n": 72 * time.Hour, "[reap]\njob_dir_ttl = \"4d\"\n": 96 * time.Hour} {
		c, err = load(body)
		if err != nil {
			t.Fatal(err)
		}
		if got, ok := ConfiguredJobDirTTL(c); !ok || got != want {
			t.Fatalf("%q: ttl=%v ok=%t, want %v", body, got, ok, want)
		}
	}
	for _, body := range []string{"[reap]\njob_dir_ttl = \"1h\"\n", "[reap]\njob_dir_ttl = 72\n", "[reap]\njob_dir_ttl = \"soon\"\n"} {
		if _, err = load(body); code(err) != exitConfig {
			t.Fatalf("%q: %v, want a config error", body, err)
		}
	}
}

// A plain shell pane is in `herdr pane list` and not in `herdr agent list`:
// a job whose agent exited while its shell pane stays keeps its dir.
func TestJobDirTTLShellPaneKeeps(t *testing.T) {
	for name, panes := range map[string]string{
		"pane id":    `{"result":{"panes":[{"pane_id":"w:j1","tab_id":"w:t1","agent_status":"","cwd":"/work/a"}]}}`,
		"pane label": `{"result":{"panes":[{"pane_id":"w:other","tab_id":"w:t9","label":"j1","agent_status":"","cwd":"/work/a"}]}}`,
	} {
		t.Run(name, func(t *testing.T) {
			r := newTTLRig(t)
			r.seedJob("j1")
			r.panes = panes
			wantKeep(t, r, r.pass(true), "j1", "pane-live")
		})
	}
}

// A pane list herdr cannot answer, or answers in a shape panewire cannot
// read, keeps the dir even when the agent list is empty.
func TestJobDirTTLPaneListUnreadableKeeps(t *testing.T) {
	cases := map[string]func(r *ttlRig){
		"pane list fails":       func(r *ttlRig) { r.paneRC = 1 },
		"pane list has no rows": func(r *ttlRig) { r.panes = `{"result":{}}` },
		"pane list garbled":     func(r *ttlRig) { r.panes = `{"result":` },
	}
	for name, break_ := range cases {
		t.Run(name, func(t *testing.T) {
			r := newTTLRig(t)
			r.seedJob("j1")
			break_(r)
			wantKeep(t, r, r.pass(true), "j1", "pane-unreadable")
		})
	}
}

// The pane inventory is read in the herdr session the receipt recorded.
func TestJobDirTTLPaneInRecordedSessionKeeps(t *testing.T) {
	r := newTTLRig(t)
	r.seedJob("j1")
	putEvent(t, r.a, "j1", "job.spawned", map[string]any{"pane_id": "w:j1", "tab_id": "w:t1", "label": "j1", "herdr_session": "other"})
	r.age("j1")
	r.a.HerdrSession = "default"
	r.a.Runner = &fakeRunner{Do: func(c Command) Result {
		if c.Name == "herdr" && strings.Join(c.Args, " ") == "pane list" {
			if slices.Contains(c.Env, "HERDR_SESSION=other") {
				return Result{Out: `{"result":{"panes":[{"pane_id":"w:j1","tab_id":"w:t1","agent_status":""}]}}`}
			}
			return Result{Out: `{"result":{"panes":[]}}`}
		}
		if c.Name == "herdr" && strings.Join(c.Args, " ") == "agent list" {
			return Result{Out: `{"result":{"agents":[]}}`}
		}
		return Result{}
	}}
	wantKeep(t, r, r.pass(true), "j1", "pane-live")
}

// A node-side dir whose receipt names no herdr session (one written before
// the receipt recorded it) keeps: its pane cannot be looked up for sure.
func TestJobDirTTLReceiptWithoutSessionKeeps(t *testing.T) {
	r := newTTLRig(t)
	r.seedJob("j1")
	putEvent(t, r.a, "j1", "job.spawned", map[string]any{"pane_id": "w:j1", "tab_id": "w:t1", "label": "j1"})
	r.age("j1")
	wantKeep(t, r, r.pass(true), "j1", "pane-session-unknown")
}

// Every report path the receipt or a completion recorded, inside the dir,
// needs hk's confirmation as a top-level report*.md does.
func TestJobDirTTLRecordedReportPaths(t *testing.T) {
	cases := []struct {
		name, action, reason string
		setup                func(r *ttlRig)
	}{
		{"completion summary without a doc", "keep", "report-unconfirmed", func(r *ttlRig) {
			r.write("j1", "sub/summary.md", []byte("summary\n"))
			putEvent(r.t, r.a, "j1", "job.completed", map[string]any{"report": filepath.Join(r.dir("j1"), "sub", "summary.md")})
		}},
		{"spawn report without a doc", "keep", "report-unconfirmed", func(r *ttlRig) {
			r.write("j1", "out/result.md", []byte("result\n"))
			putEvent(r.t, r.a, "j1", "job.spawned", map[string]any{"pane_id": "w:j1", "tab_id": "w:t1", "herdr_session": "default", "label": "j1", "report": filepath.Join(r.dir("j1"), "out", "result.md")})
		}},
		{"completion summary whose doc differs", "keep", "report-sha-mismatch", func(r *ttlRig) {
			r.write("j1", "sub/summary.md", []byte("summary\n"))
			r.doc("reports/j1/summary.md", []byte("other summary\n"), "")
			putEvent(r.t, r.a, "j1", "job.completed", map[string]any{"report": filepath.Join(r.dir("j1"), "sub", "summary.md")})
		}},
		{"relative recorded path", "keep", "report-path-unreadable", func(r *ttlRig) {
			putEvent(r.t, r.a, "j1", "job.completed", map[string]any{"report": "summary.md"})
		}},
		{"recorded path not a string", "keep", "report-path-unreadable", func(r *ttlRig) {
			putEvent(r.t, r.a, "j1", "job.completed", map[string]any{"report": 7})
		}},
		{"completion summary with its doc", "delete", "confirmed", func(r *ttlRig) {
			r.write("j1", "sub/summary.md", []byte("summary\n"))
			r.doc("reports/j1/summary.md", []byte("summary\n"), "")
			putEvent(r.t, r.a, "j1", "job.completed", map[string]any{"report": filepath.Join(r.dir("j1"), "sub", "summary.md")})
		}},
		{"recorded path no longer present", "delete", "confirmed", func(r *ttlRig) {
			putEvent(r.t, r.a, "j1", "job.completed", map[string]any{"report": filepath.Join(r.dir("j1"), "sub", "gone.md")})
		}},
		{"recorded path in the parent job's dir", "delete", "confirmed", func(r *ttlRig) {
			r.write("parent", "summary.md", []byte("parent summary\n"))
			r.age("parent")
			putEvent(r.t, r.a, "j1", "job.completed", map[string]any{"report": filepath.Join(r.dir("parent"), "summary.md")})
		}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			r := newTTLRig(t)
			r.seedJob("j1")
			c.setup(r)
			r.age("j1")
			out := r.pass(true)
			if c.action == "delete" {
				wantDelete(t, r, out, "j1", c.reason)
			} else {
				wantKeep(t, r, out, "j1", c.reason)
			}
		})
	}
}

// A writer that lands during the locked re-check's own herdr read (after the
// re-check read the dir, before the delete) keeps the dir (hk 3624 N1).
func TestJobDirTTLWriteDuringLockedRecheckKeeps(t *testing.T) {
	r := newTTLRig(t)
	r.seedJob("j1")
	calls := 0
	base := r.a.Runner.(*fakeRunner).Do
	r.a.Runner = &fakeRunner{Do: func(c Command) Result {
		if c.Name == "herdr" && strings.Join(c.Args, " ") == "agent list" {
			calls++
			if calls == 2 { // the locked re-check's fresh herdr read
				if err := os.WriteFile(filepath.Join(r.dir("j1"), "late-report.md"), []byte("written during the re-check\n"), 0600); err != nil {
					t.Error(err)
				}
			}
		}
		return base(c)
	}}
	out := r.pass(true)
	if calls < 2 {
		t.Fatalf("the locked re-check read herdr %d times, want a second read", calls)
	}
	wantKeep(t, r, out, "j1", "changed-during-pass")
	if _, err := os.Stat(filepath.Join(r.dir("j1"), "late-report.md")); err != nil {
		t.Fatalf("the late write is gone: %v", err)
	}
}

// tombstones lists the .expiring-* entries of the jobs root.
func (r *ttlRig) tombstones() []string {
	r.t.Helper()
	entries, err := os.ReadDir(r.a.Jobs)
	if err != nil {
		r.t.Fatal(err)
	}
	var out []string
	for _, entry := range entries {
		if strings.HasPrefix(entry.Name(), ".expiring-") {
			out = append(out, entry.Name())
		}
	}
	return out
}

// A file that lands after the final gate, right before or right after the
// dir is renamed to its tombstone, keeps the dir: it is renamed back intact.
func TestJobDirTTLWriteAroundTheRenameKeeps(t *testing.T) {
	for _, when := range []string{"before the rename", "after the rename"} {
		t.Run(when, func(t *testing.T) {
			r := newTTLRig(t)
			r.seedJob("j1")
			before, after := false, false
			p := &JobDirTTL{TTL: DefaultJobDirTTL, Apply: true}
			p.beforeRename = func(id string) {
				before = true
				if when == "before the rename" {
					r.write(id, "findings.json", []byte(`{"findings":[]}`))
				}
			}
			p.afterRename = func(id, tombstone string) {
				after = true
				if filepath.Dir(tombstone) != r.a.Jobs || !strings.HasPrefix(filepath.Base(tombstone), ".expiring-"+id+"-") || validID(filepath.Base(tombstone)) {
					t.Errorf("tombstone %s is not a non-job name in the jobs root", tombstone)
				}
				if r.exists(id) {
					t.Errorf("%s still exists under its name after the rename", id)
				}
				if when == "after the rename" {
					if err := os.WriteFile(filepath.Join(tombstone, "findings.json"), []byte(`{"findings":[]}`), 0600); err != nil {
						t.Error(err)
					}
				}
			}
			out, err := r.passWith(p)
			if err != nil {
				t.Fatal(err)
			}
			wantKeep(t, r, out, "j1", "changed-during-pass")
			if !before || !after {
				t.Fatalf("hooks ran before=%t after=%t, want both", before, after)
			}
			for _, name := range []string{"report.md", "brief.md", "findings.json", "events"} {
				if _, err := os.Lstat(filepath.Join(r.dir("j1"), name)); err != nil {
					t.Fatalf("the renamed-back dir lacks %s: %v", name, err)
				}
			}
			if left := r.tombstones(); len(left) != 0 {
				t.Fatalf("tombstones left behind: %v", left)
			}
			if len(r.events) != 0 {
				t.Fatalf("a kept dir recorded fleet events %+v", r.events)
			}
		})
	}
}

// When the tombstone changed and cannot be renamed back, it stays where it
// is, is never deleted, and the pass records it.
func TestJobDirTTLTombstoneThatCannotGoBackStays(t *testing.T) {
	r := newTTLRig(t)
	r.seedJob("j1")
	var tomb string
	p := &JobDirTTL{TTL: DefaultJobDirTTL, Apply: true}
	p.afterRename = func(id, tombstone string) {
		tomb = tombstone
		if err := os.WriteFile(filepath.Join(tombstone, "findings.json"), []byte(`{"findings":[]}`), 0600); err != nil {
			t.Error(err)
		}
		// A new dir under the job's name holds the way back.
		r.write(id, "spawn.out", []byte("new\n"))
	}
	out, err := r.passWith(p)
	if err != nil {
		t.Fatal(err)
	}
	wantKeep(t, r, out, "j1", "tombstone-left")
	for _, name := range []string{"report.md", "findings.json", "events"} {
		if _, err := os.Lstat(filepath.Join(tomb, name)); err != nil {
			t.Fatalf("the tombstone lost %s: %v", name, err)
		}
	}
	if len(r.events) != 1 || r.events[0].Kind != "job_dir_expired" || r.events[0].Job != "j1" || r.events[0].Outcome != "tombstone_left" {
		t.Fatalf("fleet events = %+v, want one tombstone_left for j1", r.events)
	}
}

// A tombstone left by an earlier pass is listed and never deleted or
// restored.
func TestJobDirTTLLeftoverTombstoneIsReportedNeverTouched(t *testing.T) {
	r := newTTLRig(t)
	r.seedJob("j1")
	r.seedJob("j2")
	tomb := ".expiring-j2-1767225600000000000"
	if err := os.Rename(r.dir("j2"), filepath.Join(r.a.Jobs, tomb)); err != nil {
		t.Fatal(err)
	}
	if validID(tomb) {
		t.Fatalf("tombstone name %s is a valid job id", tomb)
	}
	out := r.pass(true)
	if got := line(out, tomb); !strings.HasPrefix(got, "job="+tomb+" action=keep reason=tombstone ") {
		t.Fatalf("line for the tombstone = %q\n%s", got, out)
	}
	wantDelete(t, r, out, "j1", "confirmed")
	if _, err := os.Stat(filepath.Join(r.a.Jobs, tomb, "report.md")); err != nil {
		t.Fatalf("the leftover tombstone was touched: %v", err)
	}
	if r.exists("j2") {
		t.Fatal("the leftover tombstone was restored")
	}
	if !strings.Contains(out, "tombstone:1") {
		t.Fatalf("summary does not count the tombstone:\n%s", out)
	}
}

// ttlUploadedEvidence answers hk with an artifact record of j1 that lists
// rel (under the job dir) as uploaded with Mirror=false, its archive not
// drive-confirmed.
func (r *ttlRig) ttlUploadedEvidence(rel string, body []byte) {
	r.t.Helper()
	r.write("j1", rel, body)
	r.age("j1")
	src := filepath.Join(r.dir("j1"), filepath.Dir(rel))
	rec := ArtifactRecord{Schema: "fleet.artifact.v2", Name: "run", Host: r.a.machine(), Job: "j1", Task: "42", Archive: Archive{Source: src, State: "uploaded", Contents: []Content{{Path: filepath.Base(rel), Bytes: int64(len(body)), SHA: sha(body), Mirror: false, UploadedAt: "2026-01-01T00:00:00Z"}}}}
	raw, _ := json.Marshal(rec)
	key := "artifacts/proj/42/j1/run"
	r.hk.SetTask("42", hkTaskJSON(42, "merged", "t", "", "proj", "{}"))
	r.hk.SetDoc(key, string(raw))
	r.hk.Handle(func(req hkRequest) (int, string, bool) {
		return http.StatusOK, `{"documents":[{"key":"` + key + `"}]}`, req.Method == http.MethodGet && req.Path == "/v1/documents"
	})
	r.a.Config.Artifacts = map[string]any{"local_delete_at": "uploaded"}
}

// Under a jobs root the artifact policy's root does not match (/srv/jobs),
// evidence/** has no policy class, so no mirror requirement applies to it:
// an uploaded Mirror=false row must not let it go.
func TestJobDirTTLUnclassifiedEvidenceKeeps(t *testing.T) {
	r := newTTLRig(t)
	r.a.Jobs = filepath.Join(r.a.Home, "srv", "jobs")
	if err := os.MkdirAll(r.a.Jobs, 0700); err != nil {
		t.Fatal(err)
	}
	r.seedJob("j1")
	r.ttlUploadedEvidence("evidence/run.log", []byte("log\n"))
	wantKeep(t, r, r.pass(true), "j1", "evidence-unclassified")
}

// Under the policy's own root, evidence is mirror-required: the uploaded
// hatch never lets it go before the drive confirms it.
func TestJobDirTTLUploadedEvidenceNeedsTheDrive(t *testing.T) {
	r := newTTLRig(t)
	r.seedJob("j1")
	r.ttlUploadedEvidence("evidence/run.log", []byte("log\n"))
	wantKeep(t, r, r.pass(true), "j1", "evidence-unconfirmed")
}

// --lane filters the pane reap; the job dir TTL has no lane filter, so the
// two together are a usage error, refused before anything runs.
func TestJobDirTTLReapRefusesLane(t *testing.T) {
	r := newTTLRig(t)
	r.seedJob("j1")
	r.now = r.base.Add(73 * time.Hour)
	var out bytes.Buffer
	r.a.Out = &out
	err := r.a.hygiene(context.Background(), "reap", []string{"--lane", "A", "--apply", "--expire-job-dirs"})
	if code(err) != exitUsage || !r.exists("j1") {
		t.Fatalf("reap --lane A --apply --expire-job-dirs: err=%v exists=%t\n%s", err, r.exists("j1"), out.String())
	}
}

// A recorded report path is judged by the file it really names. One that is
// not absolute, or that filepath.Clean would change, keeps the dir; so does
// one a symlink resolves to another path. Cleaning link/../summary.md
// lexically names a file that is absent, while the path really names the
// nested sub/summary.md hk never confirmed (hk 3624 fix 2).
func TestJobDirTTLRecordedReportPathResolved(t *testing.T) {
	// link is j1/link -> j1/sub/deep, with an unconfirmed j1/sub/summary.md.
	link := func(r *ttlRig) {
		r.write("j1", "sub/deep/note.txt", []byte("note\n"))
		r.write("j1", "sub/summary.md", []byte("nested summary\n"))
		if err := os.Symlink(filepath.Join(r.dir("j1"), "sub", "deep"), filepath.Join(r.dir("j1"), "link")); err != nil {
			r.t.Fatal(err)
		}
	}
	record := func(r *ttlRig, path string) {
		putEvent(r.t, r.a, "j1", "job.completed", map[string]any{"report": path})
	}
	sep := string(filepath.Separator)
	cases := []struct {
		name, action, reason string
		setup                func(r *ttlRig)
	}{
		{"link then dotdot names a nested report", "keep", "report-path-unreadable", func(r *ttlRig) {
			link(r)
			record(r, r.dir("j1")+sep+"link"+sep+".."+sep+"summary.md")
		}},
		{"relative path", "keep", "report-path-unreadable", func(r *ttlRig) {
			r.write("j1", "sub/summary.md", []byte("nested summary\n"))
			record(r, "sub/summary.md")
		}},
		{"dotdot without a symlink", "keep", "report-path-unreadable", func(r *ttlRig) {
			r.write("j1", "sub/note.txt", []byte("note\n"))
			record(r, r.dir("j1")+sep+"sub"+sep+".."+sep+"report.md")
		}},
		{"dot segment", "keep", "report-path-unreadable", func(r *ttlRig) {
			record(r, r.dir("j1")+sep+"."+sep+"report.md")
		}},
		{"doubled separator", "keep", "report-path-unreadable", func(r *ttlRig) {
			record(r, r.dir("j1")+sep+sep+"report.md")
		}},
		{"report through a symlink in the dir", "keep", "report-path-unreadable", func(r *ttlRig) {
			link(r)
			r.write("j1", "sub/deep/summary.md", []byte("deep summary\n"))
			record(r, filepath.Join(r.dir("j1"), "link", "summary.md"))
		}},
		{"missing report under a symlink in the dir", "keep", "report-path-unreadable", func(r *ttlRig) {
			link(r)
			record(r, filepath.Join(r.dir("j1"), "link", "gone", "summary.md"))
		}},
		{"alias outside the root then dotdot", "keep", "report-path-unreadable", func(r *ttlRig) {
			link(r)
			alias := filepath.Join(r.a.Home, "alias")
			if err := os.Symlink(filepath.Join(r.dir("j1"), "sub", "deep"), alias); err != nil {
				r.t.Fatal(err)
			}
			record(r, alias+sep+".."+sep+"summary.md")
		}},
		{"alias outside the root", "keep", "report-path-unreadable", func(r *ttlRig) {
			link(r)
			alias := filepath.Join(r.a.Home, "alias")
			if err := os.Symlink(filepath.Join(r.dir("j1"), "sub"), alias); err != nil {
				r.t.Fatal(err)
			}
			record(r, filepath.Join(alias, "summary.md"))
		}},
		{"plain nested report with its doc", "delete", "confirmed", func(r *ttlRig) {
			r.write("j1", "sub/summary.md", []byte("nested summary\n"))
			r.doc("reports/j1/summary.md", []byte("nested summary\n"), "")
			record(r, filepath.Join(r.dir("j1"), "sub", "summary.md"))
		}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			r := newTTLRig(t)
			r.seedJob("j1")
			c.setup(r)
			r.age("j1")
			if _, err := os.Lstat(filepath.Join(r.dir("j1"), "link")); err == nil {
				// age cannot reach the link's own mtime, the wall clock's:
				// the pass runs a TTL past the wall clock.
				r.now = time.Now().Add(73 * time.Hour)
			}
			out := r.pass(true)
			if c.action == "delete" {
				wantDelete(t, r, out, "j1", c.reason)
			} else {
				wantKeep(t, r, out, "j1", c.reason)
				if _, err := os.Stat(filepath.Join(r.dir("j1"), "sub")); c.name != "relative path" && c.name != "dot segment" && c.name != "doubled separator" && err != nil {
					t.Fatalf("kept dir lost sub/: %v", err)
				}
			}
		})
	}
}

// The pass resolves an aliased jobs root itself, so a report path recorded
// through that alias is no doubt: a submitter-side dir (node-side dirs
// under an aliased root keep for their events) whose --report names the
// alias goes once hk holds the report.
func TestJobDirTTLRecordedReportThroughRootAliasDeletes(t *testing.T) {
	r := newTTLRig(t)
	r.a.Hub = &ttlHub{}
	alias := filepath.Join(r.a.Home, "jobs-alias")
	if err := os.Symlink(r.a.Jobs, alias); err != nil {
		t.Fatal(err)
	}
	r.seedSubmission("s1", "--report", filepath.Join(alias, "s1", "sub", "summary.md"))
	r.write("s1", "sub/summary.md", []byte("nested summary\n"))
	r.doc("reports/s1/summary.md", []byte("nested summary\n"), "")
	r.store.records["s1"] = map[string]any{"job_id": "s1", "state": "completed", "report_doc": "reports/s1/summary.md"}
	r.age("s1")
	r.a.Jobs = alias
	out := r.pass(true)
	if got := line(out, "s1"); !strings.HasPrefix(got, "job=s1 action=delete ") {
		t.Fatalf("line for s1 = %q, want action=delete\n%s", got, out)
	}
	if r.exists("s1") {
		t.Fatal("s1 still exists after an apply pass")
	}
}

// A pane list row panewire cannot read (null, not an object, or without a
// pane id) makes the whole inventory unreadable, as does an explicit error
// envelope: with an empty agent list such a row must not read as "no pane"
// (hk 3624 fix 2).
func TestJobDirTTLUnreadablePaneRowKeeps(t *testing.T) {
	for name, panes := range map[string]string{
		"null row":                   `{"result":{"panes":[null]}}`,
		"empty row":                  `{"result":{"panes":[{}]}}`,
		"row without a pane id":      `{"result":{"panes":[{"label":"x"}]}}`,
		"blank pane id":              `{"result":{"panes":[{"pane_id":"  ","label":"x"}]}}`,
		"null beside a readable row": `{"result":{"panes":[{"pane_id":"w:other"},null]}}`,
		"row not an object":          `{"result":{"panes":["w:j1"]}}`,
		"error envelope":             `{"error":{"code":"internal","message":"pane list failed"},"result":{"panes":[]}}`,
		"error in the result":        `{"result":{"panes":[],"error":{"code":"internal"}}}`,
		"ok false":                   `{"ok":false,"result":{"panes":[]}}`,
		"bare error envelope":        `{"error":{"code":"internal","message":"pane list failed"},"panes":[]}`,
	} {
		t.Run(name, func(t *testing.T) {
			r := newTTLRig(t)
			r.seedJob("j1")
			r.panes = panes
			wantKeep(t, r, r.pass(true), "j1", "pane-unreadable")
		})
	}
	// Readable rows that name other panes are evidence of absence.
	r := newTTLRig(t)
	r.seedJob("j1")
	r.panes = `{"ok":true,"result":{"panes":[{"pane_id":"w:other","label":"x"}]}}`
	wantDelete(t, r, r.pass(true), "j1", "confirmed")
}
