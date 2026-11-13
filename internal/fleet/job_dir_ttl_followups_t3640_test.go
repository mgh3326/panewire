package fleet

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// hk 3640: a submitter-side dir's --report-relative is recorded as written,
// joined to the clean jobs root without filepath.Join, so the recorded-path
// check (absolute, clean, no NUL, symlinks resolve to itself) judges the
// path the receipt names at its normal place in the gate order. Every
// malformed value here is a literal: filepath.Join in a fixture would erase
// the defect. The case numbers are those of the hk 3640 brief.

// seedRelative builds the submitter dir s1 whose receipt carries rel, as
// written, as --report-relative, with a completed hk record for s1.
func (r *ttlRig) seedRelative(rel string, extra ...string) {
	r.t.Helper()
	r.a.Hub = &ttlHub{}
	r.seedSubmission("s1", append([]string{"--report-relative", rel}, extra...)...)
	r.store.records["s1"] = map[string]any{"job_id": "s1", "state": "completed"}
}

// pin makes body the report hk confirms for job id: hk holds a document at
// the key `done` publishes body to, content-addressed by its bytes. The job
// record names no pin: a report is confirmed by its bytes alone.
func (r *ttlRig) pin(id, _ string, body []byte) {
	r.doc(reportDocumentKey(id, sha(body)), body, "")
}

// linkDeep makes <id>/link -> sub/deep beside an unconfirmed
// <id>/sub/summary.md, the file link/../summary.md really names. age cannot
// reach the link's own mtime, the wall clock's, so the pass runs a TTL past
// the wall clock.
func (r *ttlRig) linkDeep(id string) {
	r.t.Helper()
	r.write(id, "sub/deep/note.txt", []byte("note\n"))
	r.write(id, "sub/summary.md", []byte("nested summary\n"))
	if err := os.Symlink("sub/deep", filepath.Join(r.dir(id), "link")); err != nil {
		r.t.Fatal(err)
	}
	r.age(id)
	r.now = time.Now().Add(73 * time.Hour)
}

func (r *ttlRig) present(id, rel string) bool {
	_, err := os.Lstat(filepath.Join(r.dir(id), rel))
	return err == nil
}

// Case 1: a .. after a symlink names another file than the cleaned path;
// a .. anywhere keeps the dir, whatever the cleaned path names.
func TestJobDirTTLReportRelativeDotDotKeeps(t *testing.T) {
	for _, rel := range []string{"s1/link/../summary.md", "s1/sub/../summary.md", "../other/summary.md"} {
		t.Run(rel, func(t *testing.T) {
			r := newTTLRig(t)
			r.seedRelative(rel)
			r.linkDeep("s1")
			out := r.pass(true)
			wantKeep(t, r, out, "s1", "report-path-unreadable")
			if !r.present("s1", "sub/summary.md") {
				t.Fatal("the unconfirmed sub/summary.md is gone")
			}
		})
	}
}

// Case 2: the locked re-check reads submission.json again through the same
// code, so a receipt swapped to a malformed path between the scan and the
// delete keeps the dir. The swap keeps the receipt's size and mtimes, so
// only the re-check's own path check can catch it.
func TestJobDirTTLReportRelativeLockedRecheckKeeps(t *testing.T) {
	const clean, malformed = "s1/sub/out/summary.md", "s1/link/../summary.md"
	if len(clean) != len(malformed) {
		t.Fatalf("the swap must keep the receipt's size: %d != %d", len(clean), len(malformed))
	}
	r := newTTLRig(t)
	body := []byte("out summary\n")
	r.write("s1", "sub/out/summary.md", body)
	r.seedRelative(clean)
	r.pin("s1", "summary.md", body)
	r.linkDeep("s1")
	swapped := false
	out, err := r.passWith(&JobDirTTL{TTL: DefaultJobDirTTL, Apply: true, beforeRecheck: func(id string) {
		if id != "s1" {
			return
		}
		path := filepath.Join(r.dir("s1"), "submission.json")
		raw, err := os.ReadFile(path)
		if err != nil {
			t.Error(err)
			return
		}
		next := bytes.Replace(raw, []byte(clean), []byte(malformed), 1)
		if bytes.Equal(next, raw) {
			t.Errorf("submission.json does not carry %q:\n%s", clean, raw)
			return
		}
		if err := os.WriteFile(path, next, 0600); err != nil {
			t.Error(err)
			return
		}
		r.age("s1")
		swapped = true
	}})
	if err != nil {
		t.Fatal(err)
	}
	if !swapped {
		t.Fatalf("the pass never reached the locked re-check:\n%s", out)
	}
	wantKeep(t, r, out, "s1", "report-path-unreadable")
	if left := r.tombstones(); len(left) != 0 {
		t.Fatalf("tombstones left behind: %v", left)
	}
	if len(r.events) != 0 {
		t.Fatalf("a kept dir recorded fleet events %+v", r.events)
	}
	if !r.present("s1", "sub/summary.md") {
		t.Fatal("the unconfirmed sub/summary.md is gone")
	}
}

// Case 3: --report and --report-relative are separate flags, and each path
// they record needs hk's confirmation.
func TestJobDirTTLReportAndReportRelativeEachConfirmed(t *testing.T) {
	confirmed, unconfirmed := []byte("confirmed\n"), []byte("unconfirmed\n")
	t.Run("report-relative confirmed, report not", func(t *testing.T) {
		r := newTTLRig(t)
		r.write("s1", "a/summary.md", confirmed)
		r.write("s1", "b/notes.md", unconfirmed)
		r.seedRelative("s1/a/summary.md", "--report", r.dir("s1")+"/b/notes.md")
		r.pin("s1", "summary.md", confirmed)
		wantKeep(t, r, r.pass(true), "s1", "report-unconfirmed")
	})
	t.Run("report confirmed, report-relative not", func(t *testing.T) {
		r := newTTLRig(t)
		r.write("s1", "a/summary.md", unconfirmed)
		r.write("s1", "b/notes.md", confirmed)
		r.seedRelative("s1/a/summary.md", "--report", r.dir("s1")+"/b/notes.md")
		r.pin("s1", "notes.md", confirmed)
		wantKeep(t, r, r.pass(true), "s1", "report-unconfirmed")
	})
}

// Case 4: an empty --report-relative records nothing.
func TestJobDirTTLReportRelativeEmptyIsExempt(t *testing.T) {
	t.Run("terminal record", func(t *testing.T) {
		r := newTTLRig(t)
		r.seedRelative("")
		wantDelete(t, r, r.pass(true), "s1", "submission-terminal")
	})
	t.Run("hk missing, spawn failed", func(t *testing.T) {
		r := newTTLRig(t)
		r.seedRelative("")
		delete(r.store.records, "s1")
		r.a.Hub = &ttlHub{status: "failed"}
		wantDelete(t, r, r.pass(true), "s1", "submission-spawn-failed")
	})
}

// Case 5: a malformed --report-relative is judged at the artifact gate, so
// an earlier gate still names the reason.
func TestJobDirTTLReportRelativeKeepsGateOrder(t *testing.T) {
	const rel = "s1/./summary.md"
	cases := []struct {
		name, reason string
		extra        []string
		break_       func(r *ttlRig)
	}{
		{"keep", "protected", []string{"--keep"}, func(*ttlRig) {}},
		{"watcher", "watcher-active", nil, func(r *ttlRig) {
			r.write("s1", "completion-sentinel.pid", []byte("123\n"))
			r.age("s1")
		}},
		{"too young", "too-young", nil, func(r *ttlRig) { r.now = r.base.Add(71 * time.Hour) }},
		{"hk not terminal", "hk-not-terminal", nil, func(r *ttlRig) { r.store.records["s1"]["state"] = "spawned" }},
		{"hk unreadable", "hk-unreadable", nil, func(r *ttlRig) { r.store.getErr = errors.New("connection refused") }},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			r := newTTLRig(t)
			r.seedRelative(rel, c.extra...)
			c.break_(r)
			wantKeep(t, r, r.pass(true), "s1", c.reason)
		})
	}
}

// Case 8: a value that is absolute, not clean or holds a NUL keeps the dir.
// An absolute value joins to <root>//..., which is not clean.
func TestJobDirTTLReportRelativeNotCleanKeeps(t *testing.T) {
	for _, rel := range []string{
		"/absolute/report.md",
		"//host/report.md",
		".",
		"..",
		"./s1/summary.md",
		"s1/./summary.md",
		"s1/../summary.md",
		"s1//summary.md",
		"s1/summary.md/",
		"s1/sub/summary.md\x00",
	} {
		t.Run(strings.ReplaceAll(rel, "\x00", "NUL"), func(t *testing.T) {
			r := newTTLRig(t)
			r.seedRelative(rel)
			wantKeep(t, r, r.pass(true), "s1", "report-path-unreadable")
		})
	}
}

// Cases 6, 7, 9, 10 and 11: a clean --report-relative is judged by the file
// it names. The check is by path segment, so a dot inside a name is not a
// dot segment; a symlink still keeps the dir; a path outside the dir is
// skipped and its file left alone.
func TestJobDirTTLReportRelativeCleanPaths(t *testing.T) {
	body := []byte("summary\n")
	cases := []struct {
		name, rel, action, reason string
		// setup runs before s1 is aged; after runs after.
		setup, after func(r *ttlRig)
		// check runs after the pass.
		check func(r *ttlRig) string
	}{
		// 6: nested paths.
		{name: "nested with its doc", rel: "s1/out/review/summary.md", action: "delete", reason: "submission-terminal", setup: func(r *ttlRig) {
			r.write("s1", "out/review/summary.md", body)
			r.pin("s1", "summary.md", body)
		}},
		{name: "nested without its doc", rel: "s1/out/review/summary.md", action: "keep", reason: "report-unconfirmed", setup: func(r *ttlRig) {
			r.write("s1", "out/review/summary.md", body)
		}},
		{name: "nested with another hash", rel: "s1/out/review/summary.md", action: "keep", reason: "report-sha-mismatch", setup: func(r *ttlRig) {
			r.write("s1", "out/review/summary.md", body)
			r.doc("reports/s1/summary.md", body, sha([]byte("other bytes\n")))
		}},
		{name: "nested, absent", rel: "s1/out/review/summary.md", action: "delete", reason: "submission-terminal"},
		// 7: dot substrings are not dot segments.
		{name: "dots inside a name", rel: "s1/report..md", action: "delete", reason: "submission-terminal", setup: func(r *ttlRig) {
			r.write("s1", "report..md", body)
			r.pin("s1", "report..md", body)
		}},
		{name: "hidden dir", rel: "s1/.hidden/summary.md", action: "delete", reason: "submission-terminal", setup: func(r *ttlRig) {
			r.write("s1", ".hidden/summary.md", body)
			r.pin("s1", "summary.md", body)
		}},
		// 9: symlink validation stays.
		{name: "through a symlink", rel: "s1/link/summary.md", action: "keep", reason: "report-path-unreadable", setup: func(r *ttlRig) {
			r.write("s1", "sub/deep/summary.md", body)
			r.linkDeep("s1")
		}},
		{name: "missing leaf through a symlink", rel: "s1/link/gone.md", action: "keep", reason: "report-path-unreadable", setup: func(r *ttlRig) {
			r.linkDeep("s1")
		}},
		{name: "dangling symlink", rel: "s1/dangle/summary.md", action: "keep", reason: "report-path-unreadable", after: func(r *ttlRig) {
			// Made after s1 is aged: age would follow it to nothing.
			if err := os.Symlink("absent-target", filepath.Join(r.dir("s1"), "dangle")); err != nil {
				r.t.Fatal(err)
			}
			r.now = time.Now().Add(73 * time.Hour)
		}},
		{name: "ordinary missing path", rel: "s1/sub/gone.md", action: "delete", reason: "submission-terminal", setup: func(r *ttlRig) {
			r.write("s1", "sub/note.txt", []byte("note\n"))
		}},
		// 10: a report outside this dir is not this pass's to delete.
		{name: "report in another dir", rel: "parent/sub/summary.md", action: "delete", reason: "submission-terminal", setup: func(r *ttlRig) {
			r.write("parent", "sub/summary.md", []byte("parent summary\n"))
			r.age("parent")
		}, check: func(r *ttlRig) string {
			if !r.present("parent", "sub/summary.md") {
				return "the parent's report is gone"
			}
			return ""
		}},
		{name: "report in the jobs root", rel: "summary.md", action: "delete", reason: "submission-terminal", setup: func(r *ttlRig) {
			if err := os.WriteFile(filepath.Join(r.a.Jobs, "summary.md"), []byte("root summary\n"), 0600); err != nil {
				r.t.Fatal(err)
			}
		}, check: func(r *ttlRig) string {
			if _, err := os.Lstat(filepath.Join(r.a.Jobs, "summary.md")); err != nil {
				return "the jobs root's summary.md is gone"
			}
			return ""
		}},
		// 11: the receipt is parsed, not admitted again.
		{name: "minimal receipt", rel: "s1/sub/summary.md", action: "delete", reason: "submission-terminal", setup: func(r *ttlRig) {
			r.write("s1", "sub/summary.md", body)
			r.pin("s1", "summary.md", body)
		}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			r := newTTLRig(t)
			r.seedRelative(c.rel)
			if c.setup != nil {
				c.setup(r)
			}
			r.age("s1")
			if c.after != nil {
				c.after(r)
			}
			out := r.pass(true)
			if c.action == "delete" {
				wantDelete(t, r, out, "s1", c.reason)
			} else {
				wantKeep(t, r, out, "s1", c.reason)
			}
			if c.check != nil {
				if msg := c.check(r); msg != "" {
					t.Fatal(msg)
				}
			}
		})
	}
}

// Case 12: the --report-relative check is the submitter branch's; a dir
// that ran here reads its events and never its submission.json.
func TestJobDirTTLReportRelativeOnlyForSubmitterDirs(t *testing.T) {
	t.Run("malformed receipt in an executing dir", func(t *testing.T) {
		r := newTTLRig(t)
		r.seedJob("j1")
		raw := []byte(`{"request_id":"0f8fad5b-d9cb-469f-a165-70867728950e","args":["--job","j1","--report-relative","j1/link/../summary.md"]}`)
		r.write("j1", "submission.json", raw)
		r.age("j1")
		wantDelete(t, r, r.pass(true), "j1", "confirmed")
	})
	t.Run("unclean absolute event report", func(t *testing.T) {
		r := newTTLRig(t)
		r.seedJob("j1")
		putEvent(t, r.a, "j1", "job.completed", map[string]any{"report": r.dir("j1") + "/./report.md"})
		r.age("j1")
		wantKeep(t, r, r.pass(true), "j1", "report-path-unreadable")
	})
}

// Case 13: the pass applies no hub charset rule and no Windows separator
// rule; a backslash or a non-ASCII name is an ordinary file name here.
func TestJobDirTTLReportNamesNeedNoPlatformRule(t *testing.T) {
	names := map[string]string{
		"backslash":         "sub\\summary.md",
		"hangul":            "요약.md",
		"hangul NFD":        "\u110b\u116d\u110b\u1163\u11a8.md",
		"latin NFC":         "r\u00e9sum\u00e9.md",
		"latin NFD":         "re\u0301sume\u0301.md",
		"dotdot backslash":  "..\\summary.md",
		"dotdot backslash2": "sub\\..\\summary.md",
	}
	body := []byte("summary\n")
	write := func(t *testing.T, r *ttlRig, id, name string) {
		t.Helper()
		if err := os.WriteFile(filepath.Join(r.dir(id), name), body, 0600); err != nil {
			t.Skipf("this filesystem cannot hold the file name %q: %v", name, err)
		}
	}
	for label, name := range names {
		t.Run("submitter "+label, func(t *testing.T) {
			r := newTTLRig(t)
			r.seedRelative("s1/" + name)
			write(t, r, "s1", name)
			r.pin("s1", name, body)
			r.age("s1")
			wantDelete(t, r, r.pass(true), "s1", "submission-terminal")
		})
		t.Run("executing "+label, func(t *testing.T) {
			r := newTTLRig(t)
			r.seedJob("j1")
			if err := os.Remove(filepath.Join(r.dir("j1"), "report.md")); err != nil {
				t.Fatal(err)
			}
			write(t, r, "j1", name)
			putEvent(t, r.a, "j1", "job.completed", map[string]any{"report": r.dir("j1") + "/" + name})
			r.pin("j1", name, body)
			r.age("j1")
			wantDelete(t, r, r.pass(true), "j1", "confirmed")
		})
	}
}

// Case 14: the pass resolves the jobs root itself, so a report under the
// root's own alias is read under the real root; any other symlink in a
// recorded path, such as a /tmp-like alias of a canonical root, keeps the
// dir.
func TestJobDirTTLReportThroughAnotherAlias(t *testing.T) {
	body := []byte("nested summary\n")
	for _, leaf := range []string{"summary.md", "gone.md"} {
		t.Run("canonical root, report through an alias, "+leaf, func(t *testing.T) {
			r := newTTLRig(t)
			alias := filepath.Join(r.a.Home, "tmp-alias")
			if err := os.Symlink(r.a.Jobs, alias); err != nil {
				t.Fatal(err)
			}
			r.write("s1", "sub/summary.md", body)
			r.a.Hub = &ttlHub{}
			r.seedSubmission("s1", "--report", alias+"/s1/sub/"+leaf)
			r.store.records["s1"] = map[string]any{"job_id": "s1", "state": "completed"}
			r.pin("s1", "summary.md", body)
			wantKeep(t, r, r.pass(true), "s1", "report-path-unreadable")
		})
	}
	for _, suffix := range []string{"", "/"} {
		t.Run("jobs root is the alias"+suffix, func(t *testing.T) {
			r := newTTLRig(t)
			alias := filepath.Join(r.a.Home, "jobs-alias")
			if err := os.Symlink(r.a.Jobs, alias); err != nil {
				t.Fatal(err)
			}
			r.write("s1", "sub/summary.md", body)
			r.seedRelative("s1/sub/summary.md")
			r.pin("s1", "summary.md", body)
			r.a.Jobs = alias + suffix
			wantDelete(t, r, r.pass(true), "s1", "submission-terminal")
		})
	}
}

// Case 15: a writer that runs MkdirAll after the rename makes a fresh dir
// under the job's name. An empty one survives while the tombstone is
// unchanged. When the tombstone changed, any fresh dir under the job's name,
// empty or not, makes the rename back fail (os.Rename refuses an existing
// directory target) and the tombstone stays.
func TestJobDirTTLFreshDirAfterRename(t *testing.T) {
	t.Run("empty fresh dir, unchanged tombstone", func(t *testing.T) {
		r := newTTLRig(t)
		r.seedJob("j1")
		p := &JobDirTTL{TTL: DefaultJobDirTTL, Apply: true}
		p.afterRename = func(id, _ string) {
			if err := os.Mkdir(r.dir(id), 0700); err != nil {
				t.Error(err)
			}
		}
		out, err := r.passWith(p)
		if err != nil {
			t.Fatal(err)
		}
		if got := line(out, "j1"); !strings.HasPrefix(got, "job=j1 action=delete reason=confirmed ") {
			t.Fatalf("line for j1 = %q, want action=delete reason=confirmed\n%s", got, out)
		}
		entries, err := os.ReadDir(r.dir("j1"))
		if err != nil || len(entries) != 0 {
			t.Fatalf("the fresh dir = %v, %v; want it kept and empty", entries, err)
		}
		if left := r.tombstones(); len(left) != 0 {
			t.Fatalf("tombstones left behind: %v", left)
		}
	})
	t.Run("empty fresh dir, changed tombstone", func(t *testing.T) {
		r := newTTLRig(t)
		r.seedJob("j1")
		p := &JobDirTTL{TTL: DefaultJobDirTTL, Apply: true}
		p.afterRename = func(id, tombstone string) {
			if err := os.WriteFile(filepath.Join(tombstone, "findings.json"), []byte(`{"findings":[]}`), 0600); err != nil {
				t.Error(err)
			}
			if err := os.Mkdir(r.dir(id), 0700); err != nil {
				t.Error(err)
			}
		}
		out, err := r.passWith(p)
		if err != nil {
			t.Fatal(err)
		}
		wantKeep(t, r, out, "j1", "tombstone-left")
		left := r.tombstones()
		if len(left) != 1 {
			t.Fatalf("tombstones = %v, want exactly one", left)
		}
		for _, name := range []string{"report.md", "brief.md", "findings.json", "events"} {
			if _, err := os.Lstat(filepath.Join(r.a.Jobs, left[0], name)); err != nil {
				t.Fatalf("the tombstone lost %s: %v", name, err)
			}
		}
		entries, err := os.ReadDir(r.dir("j1"))
		if err != nil || len(entries) != 0 {
			t.Fatalf("the fresh dir = %v, %v; want it kept and empty", entries, err)
		}
		for _, e := range r.events {
			if e.Outcome == "deleted" {
				t.Fatalf("a deletion event was recorded: %+v", e)
			}
		}
		if len(r.events) != 1 || r.events[0].Kind != "job_dir_expired" || r.events[0].Outcome != "tombstone_left" {
			t.Fatalf("fleet events = %+v, want one tombstone_left", r.events)
		}
	})
	t.Run("non-empty fresh dir, changed tombstone", func(t *testing.T) {
		r := newTTLRig(t)
		r.seedJob("j1")
		var tomb string
		p := &JobDirTTL{TTL: DefaultJobDirTTL, Apply: true}
		p.afterRename = func(id, tombstone string) {
			tomb = tombstone
			if err := os.WriteFile(filepath.Join(tombstone, "findings.json"), []byte(`{"findings":[]}`), 0600); err != nil {
				t.Error(err)
			}
			r.write(id, "spawn.out", []byte("new\n"))
		}
		out, err := r.passWith(p)
		if err != nil {
			t.Fatal(err)
		}
		wantKeep(t, r, out, "j1", "tombstone-left")
		if _, err := os.Lstat(filepath.Join(tomb, "report.md")); err != nil {
			t.Fatalf("the tombstone lost report.md: %v", err)
		}
		if !r.present("j1", "spawn.out") {
			t.Fatal("the fresh dir lost spawn.out")
		}
	})
}

// Case 16: only the pane list is hardened against rows it cannot read. The
// agent list still vetoes: a matching agent is a live pane and an answer
// that cannot be decoded keeps the dir, while null or empty agent rows, or
// an error member beside an agents array, are read as no agent.
func TestJobDirTTLAgentListVetoes(t *testing.T) {
	cases := []struct {
		name, panes, agents, action, reason string
	}{
		{"null and empty agent rows", `{"result":{"panes":[]}}`, `{"result":{"agents":[null,{}]}}`, "delete", "confirmed"},
		{"agent error envelope with an empty list", `{"result":{"panes":[]}}`, `{"error":{"code":"internal"},"result":{"agents":[]}}`, "delete", "confirmed"},
		{"matching agent", `{"result":{"panes":[]}}`, `{"result":{"agents":[{"pane_id":"w:j1","tab_id":"w:t1","name":"x"}]}}`, "keep", "pane-live"},
		{"agents array missing", `{"result":{"panes":[]}}`, `{"result":{}}`, "keep", "pane-unreadable"},
		{"agent list garbled", `{"result":{"panes":[]}}`, `{"result":`, "keep", "pane-unreadable"},
		{"pane row unreadable, agents empty", `{"result":{"panes":[null]}}`, `{"result":{"agents":[]}}`, "keep", "pane-unreadable"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			r := newTTLRig(t)
			r.seedJob("j1")
			r.panes, r.agents = c.panes, c.agents
			out := r.pass(true)
			if c.action == "delete" {
				wantDelete(t, r, out, "j1", c.reason)
			} else {
				wantKeep(t, r, out, "j1", c.reason)
			}
		})
	}
}

// Case 17: a headless receipt never expires; a submitter-side dir skips the
// pane check, so a missing herdr_session does not keep it.
func TestJobDirTTLHeadlessReceipts(t *testing.T) {
	t.Run("label without a session", func(t *testing.T) {
		r := newTTLRig(t)
		r.seedJob("j1")
		putEvent(t, r.a, "j1", "job.spawned", map[string]any{"label": "j1", "profile": "cmd-dsflash41"})
		r.age("j1")
		wantKeep(t, r, r.pass(true), "j1", "pane-session-unknown")
	})
	t.Run("no pane, no label", func(t *testing.T) {
		r := newTTLRig(t)
		r.seedJob("j1")
		putEvent(t, r.a, "j1", "job.spawned", map[string]any{"herdr_session": "default", "profile": "cmd-dsflash41"})
		r.age("j1")
		wantKeep(t, r, r.pass(true), "j1", "pane-unknown")
	})
	t.Run("submitter dir without a session", func(t *testing.T) {
		r := newTTLRig(t)
		r.seedRelative("")
		wantDelete(t, r, r.pass(true), "s1", "submission-terminal")
	})
}
