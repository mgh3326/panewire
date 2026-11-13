package fleet

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"
)

// hk 3607: the hk calls left on the CLI after hk 3591 — `panewire work`'s task
// read, the artifact record writes and the sweep's task, list and document
// reads, the lifecycle report upload and task comment — go over hk HTTP with
// the svc-panewire credential. A fake hk records each request; a runner fails
// the test if any of these sites runs `handoffkeep`.

// noHKCLI makes f fail the test whenever `handoffkeep` runs; other commands go
// to f's handler.
func noHKCLI(t *testing.T, f *fakeRunner) {
	t.Helper()
	next := f.Do
	f.Do = func(c Command) Result {
		if filepath.Base(c.Name) == "handoffkeep" {
			t.Errorf("ran the hk CLI: handoffkeep %s", strings.Join(c.Args, " "))
			return Result{Code: 1, Err: "handoffkeep: hk CLI must not run"}
		}
		if next != nil {
			return next(c)
		}
		return Result{}
	}
}

// hkCommentAnswer answers POST /v1/tasks/{id}/comments as hk does.
func hkCommentAnswer(r hkRequest) (int, string, bool) {
	if r.Method != http.MethodPost || !strings.HasSuffix(r.Path, "/comments") {
		return 0, "", false
	}
	return http.StatusCreated, `{"id":5,"task_id":42,"body":"x","author":"svc-panewire","created_at":"2026-11-08T08:00:00Z"}`, true
}

// hkJSON decodes a request's JSON body into a map.
func hkJSON(t *testing.T, r hkRequest) map[string]any {
	t.Helper()
	var out map[string]any
	if e := json.Unmarshal(r.Body, &out); e != nil {
		t.Fatalf("%s: body %q is not a JSON object: %v", r.Route(), r.Body, e)
	}
	return out
}

// hkQuery parses a request's query.
func hkQuery(t *testing.T, r hkRequest) url.Values {
	t.Helper()
	q, e := url.ParseQuery(r.Query)
	if e != nil {
		t.Fatalf("%s: query %q: %v", r.Route(), r.Query, e)
	}
	return q
}

// hkAnswers serves fixed answers by route ("METHOD /escaped/path", the query
// left out) ahead of the fake's maps.
func hkAnswers(answers map[string]hkAnswerFixture) func(hkRequest) (int, string, bool) {
	return func(r hkRequest) (int, string, bool) {
		a, ok := answers[r.Route()]
		if !ok {
			return 0, "", false
		}
		return a.status, a.body, true
	}
}

type hkAnswerFixture struct {
	status int
	body   string
}

func hkOK(body string) hkAnswerFixture { return hkAnswerFixture{http.StatusOK, body} }

var (
	hkNotFound = hkAnswerFixture{http.StatusNotFound, `{"error":"not_found"}`}
	hkOutage   = hkAnswerFixture{http.StatusInternalServerError, `{"error":"internal"}`}
)

// cliOut is what the CLI path read: the hk CLI's exit status and stdout. The
// hk CLI printed hk's answer on success and exited 1, printing nothing, on any
// hk error.
type cliOut struct {
	code int
	out  string
}

// asCLI is the hk CLI's output for one hk answer.
func asCLI(a hkAnswerFixture) cliOut {
	if a.status < 200 || a.status > 299 {
		return cliOut{code: 1}
	}
	return cliOut{out: a.body}
}

// seedDoneJob is job "j" on task 42, running, with its hk job record
// claimed.
func seedDoneJob(t *testing.T, a *App) {
	t.Helper()
	seedJob(t, a, "j", "worker")
	if e := a.JobStore.(*fakeJobStore).claimSpawned(context.Background(), "j", "42", a.machine()); e != nil {
		t.Fatal(e)
	}
}

// sweepFixture is a terminal job "j" on task 42 with one final build file,
// and the artifact record that names it.
type sweepFixture struct {
	a      *App
	f      *fakeRunner
	hk     *spawnHK
	path   string
	record string
}

func newSweepFixture(t *testing.T) sweepFixture {
	t.Helper()
	a := testApp(t)
	f := ledgerRunner(t, a)
	hk := useFakeHK(t)
	noHKCLI(t, f)
	seedJob(t, a, "j", "worker")
	putEvent(t, a, "j", "job.completed", map[string]any{})
	src := filepath.Join(a.Jobs, "j", "final")
	if e := os.MkdirAll(src, 0700); e != nil {
		t.Fatal(e)
	}
	path := filepath.Join(src, "build.bin")
	if e := os.WriteFile(path, []byte("artifact"), 0600); e != nil {
		t.Fatal(e)
	}
	n, h, e := hashFile(path)
	if e != nil {
		t.Fatal(e)
	}
	rec := ArtifactRecord{Schema: "fleet.artifact.v2", Host: "local", Job: "j", Task: "42", Archive: Archive{Source: src, State: "drive_confirmed", Contents: []Content{{Path: "build.bin", Bytes: n, SHA: h}}}}
	body, _ := json.Marshal(rec)
	a.Now = func() time.Time { return time.Date(2027, 1, 1, 0, 0, 0, 0, time.UTC) }
	return sweepFixture{a: a, f: f, hk: hk, path: path, record: string(body)}
}

// docAnswer is hk's GET /v1/documents/{key} answer for a document whose body
// is body.
func docAnswer(key, kind, body string) string {
	b, _ := json.Marshal(map[string]any{"id": 9, "key": key, "kind": kind, "session": "", "job": "", "body": body, "sha256": "ab", "created_by": "svc-panewire", "created_at": "2026-11-08T08:00:00Z", "updated_at": "2026-11-08T08:00:00Z"})
	return string(b)
}

// docList is hk's GET /v1/documents answer listing keys (no bodies).
func docList(keys ...string) string {
	docs := []map[string]any{}
	for i, k := range keys {
		docs = append(docs, map[string]any{"id": i + 1, "key": k, "kind": "evidence", "session": "", "job": "", "sha256": "ab", "created_by": "svc-panewire", "created_at": "2026-11-08T08:00:00Z", "updated_at": "2026-11-08T08:00:00Z"})
	}
	b, _ := json.Marshal(map[string]any{"documents": docs})
	return string(b)
}

const sweepPrefix = "artifacts/Example/42/j"

func docRoute(method, key string) string { return method + " /v1/documents/" + url.PathEscape(key) }

func TestHKSitesGoOverHTTPWithSvcToken(t *testing.T) {
	ctx := context.Background()

	t.Run("work preset", func(t *testing.T) {
		a, hk := testApp(t), useFakeHK(t)
		f := &fakeRunner{}
		a.Runner = f
		noHKCLI(t, f)
		hk.SetTask("42", hkTaskJSON(42, "ready", "change (grade A, T1)", "", "panewire", `{"job_id":"job-77","report_path":"/r/report.md"}`))
		s, e := a.workPreset(ctx, []string{"42", "--dry-run"})
		if e != nil {
			t.Fatal(e)
		}
		if s.Job != "job-77" || s.Label != "job-77" || s.Report != "/r/report.md" || s.Task != "42" || s.Prompt != filepath.Join(a.Jobs, "job-77", "brief.md") {
			t.Fatalf("preset=%+v", s)
		}
		hkOnly(t, hk, "GET /v1/tasks/42")
	})

	t.Run("artifact sweep", func(t *testing.T) {
		fx := newSweepFixture(t)
		key := sweepPrefix + "/dataset"
		fx.hk.SetTask("42", hkTaskJSON(42, "merged", "change (grade A, T1)", "", "Example", `{}`)).
			Handle(hkAnswers(map[string]hkAnswerFixture{
				"GET /v1/documents":           hkOK(docList(key)),
				docRoute(http.MethodGet, key): hkOK(docAnswer(key, "evidence", fx.record)),
			}))
		if e := fx.a.artifactSweep(ctx, []string{"--job", "j", "--age", "0s", "--apply"}); e != nil {
			t.Fatal(e)
		}
		if _, e := os.Stat(fx.path); !os.IsNotExist(e) {
			t.Fatalf("confirmed artifact kept: %v", e)
		}
		reqs := hkOnly(t, fx.hk, "GET /v1/tasks/42", "GET /v1/documents", docRoute(http.MethodGet, key), docRoute(http.MethodPut, key), "GET /v1/tasks/42")
		if q := hkQuery(t, reqs[1]); q.Get("prefix") != sweepPrefix || q.Get("limit") != "100" || !q.Has("kind") || q.Get("kind") != "" || q.Get("session") != "" {
			t.Fatalf("list query=%q, want the CLI's doc list: the prefix, limit 100, no kind or session", reqs[1].Query)
		}
		put := hkJSON(t, reqs[3])
		if put["key"] != key || put["kind"] != "evidence" || put["job"] != "" {
			t.Fatalf("artifact put=%v, want the key, kind evidence and no job", put)
		}
		var rec ArtifactRecord
		if e := json.Unmarshal([]byte(put["body"].(string)), &rec); e != nil || len(rec.Deleted) != 1 || rec.Deleted[0].Path != "build.bin" {
			t.Fatalf("artifact put body=%q err=%v, want the record with the deletion", put["body"], e)
		}
		if want, _ := encode(rec); put["body"] != string(want) {
			t.Fatalf("artifact put body=%q, want encode(record)=%q as the CLI's --file carried it", put["body"], want)
		}
	})

	t.Run("lifecycle done", func(t *testing.T) {
		a := testApp(t)
		f := ledgerRunner(t, a)
		hk := useFakeHK(t)
		noHKCLI(t, f)
		hk.Handle(hkCommentAnswer)
		seedDoneJob(t, a)
		path := filepath.Join(a.Jobs, "j", "report.md")
		report := "first line\n" + strings.Repeat("길이 ", 1<<18) + "\nlast line\n"
		if e := os.WriteFile(path, []byte(report), 0600); e != nil {
			t.Fatal(e)
		}
		if e := a.lifecycle(ctx, "done", []string{"j", "--report", path}); e != nil {
			t.Fatal(e)
		}
		key := reportDocumentKey("j", reportDigest([]byte(report)))
		reqs := hkOnly(t, hk, docRoute(http.MethodGet, key), docRoute(http.MethodPut, key), "POST /v1/tasks/42/comments")
		put := hkJSON(t, reqs[1])
		if put["key"] != key || put["kind"] != "report" || put["job"] != "j" || put["body"] != report {
			t.Fatalf("report put key=%v kind=%v job=%v body bytes=%d, want the CLI's doc put: the key, kind report, the job and the whole file (%d bytes)", put["key"], put["kind"], put["job"], len(put["body"].(string)), len(report))
		}
		if got := hkJSON(t, reqs[2]); len(got) != 1 || got["body"] != "panewire job.completed job=j report="+path {
			t.Fatalf("comment body=%v", got)
		}
		if out := a.Out.(*bytes.Buffer).String(); !strings.Contains(out, "OK job=j report="+path+" kind=job.completed") {
			t.Fatalf("out=%q", out)
		}
		if errs := a.Err.(*bytes.Buffer).String(); errs != "" {
			t.Fatalf("warnings=%q", errs)
		}
		rec := a.JobStore.(*fakeJobStore).record(t, "j")
		if str(rec, "report_doc") != key {
			t.Fatalf("record=%v, want report_doc %s", rec, key)
		}
	})
}

// The failure handling stays the CLI path's: a failed report upload or task
// comment is a warning and the job still completes; a failed artifact record
// write or sweep recheck ends the sweep with the CLI's exit status and text.
func TestHKSiteFailuresKeepCLIHandling(t *testing.T) {
	ctx := context.Background()

	t.Run("report upload and comment refused", func(t *testing.T) {
		a := testApp(t)
		f := ledgerRunner(t, a)
		hk := useFakeHK(t)
		noHKCLI(t, f)
		hk.Handle(hkFails("PUT /v1/documents/", http.StatusRequestEntityTooLarge, `{"error":"too_large"}`)).
			Handle(hkFails("POST /v1/tasks/42/comments", http.StatusBadRequest, `{"error":"invalid_comment"}`))
		seedDoneJob(t, a)
		path := filepath.Join(a.Jobs, "j", "report.md")
		if e := os.WriteFile(path, []byte("done\n"), 0600); e != nil {
			t.Fatal(e)
		}
		wantNotInHK(t, a.lifecycle(ctx, "done", []string{"j", "--report", path}))
		errs := a.Err.(*bytes.Buffer).String()
		if !strings.Contains(errs, "panewire: warning: report upload failed; local report retained\n") {
			t.Fatalf("warnings=%q", errs)
		}
		if !strings.Contains(errs, "panewire: warning: task comment failed: handoffkeep: handoffkeep: ") || !strings.Contains(errs, "invalid_comment") {
			t.Fatalf("warnings=%q, want the CLI path's text: check(r, \"handoffkeep\") over the CLI's \"handoffkeep: <error>\"", errs)
		}
		if out := a.Out.(*bytes.Buffer).String(); !strings.Contains(out, "OK job=j ") {
			t.Fatalf("out=%q", out)
		}
		if rec := a.JobStore.(*fakeJobStore).record(t, "j"); rec["report_doc"] != nil {
			t.Fatalf("record=%v, want no report_doc", rec)
		}
		hkOnly(t, hk, docRoute(http.MethodGet, reportDocumentKey("j", reportDigest([]byte("done\n")))), docRoute(http.MethodPut, reportDocumentKey("j", reportDigest([]byte("done\n")))), "POST /v1/tasks/42/comments")
	})

	t.Run("artifact record write refused", func(t *testing.T) {
		fx := newSweepFixture(t)
		key := sweepPrefix + "/dataset"
		fx.hk.SetTask("42", hkTaskJSON(42, "merged", "change (grade A, T1)", "", "Example", `{}`)).
			Handle(hkAnswers(map[string]hkAnswerFixture{
				"GET /v1/documents":           hkOK(docList(key)),
				docRoute(http.MethodGet, key): hkOK(docAnswer(key, "evidence", fx.record)),
				docRoute(http.MethodPut, key): {http.StatusConflict, `{"error":"conflict","reason":"stale"}`},
			}))
		e := fx.a.artifactSweep(ctx, []string{"--job", "j", "--age", "0s", "--apply"})
		if exitCode(e) != 1 || e == nil || !strings.HasPrefix(e.Error(), "handoffkeep: handoffkeep: ") || !strings.Contains(e.Error(), "conflict") {
			t.Fatalf("sweep=%v exit=%d, want check(r, \"handoffkeep\")'s text and the CLI's exit 1", e, exitCode(e))
		}
		if _, e := os.Stat(fx.path); e != nil {
			t.Fatalf("artifact deleted after a failed record write: %v", e)
		}
	})

	t.Run("sweep recheck refused", func(t *testing.T) {
		fx := newSweepFixture(t)
		key := sweepPrefix + "/dataset"
		reads := 0
		fx.hk.Handle(func(r hkRequest) (int, string, bool) {
			if r.Route() != "GET /v1/tasks/42" {
				return 0, "", false
			}
			if reads++; reads > 1 {
				return http.StatusInternalServerError, `{"error":"internal"}`, true
			}
			return http.StatusOK, hkTaskJSON(42, "merged", "change (grade A, T1)", "", "Example", `{}`), true
		}).Handle(hkAnswers(map[string]hkAnswerFixture{
			"GET /v1/documents":           hkOK(docList(key)),
			docRoute(http.MethodGet, key): hkOK(docAnswer(key, "evidence", fx.record)),
		}))
		e := fx.a.artifactSweep(ctx, []string{"--job", "j", "--age", "0s", "--apply"})
		if exitCode(e) != 1 || e == nil || !strings.HasPrefix(e.Error(), "sweep task recheck: handoffkeep: ") {
			t.Fatalf("sweep=%v exit=%d, want check(r, \"sweep task recheck\")'s text and exit 1", e, exitCode(e))
		}
		if _, e := os.Stat(fx.path); e != nil {
			t.Fatalf("artifact deleted after a failed recheck: %v", e)
		}
	})
}

// With no svc-panewire credential every site behaves as the CLI path did when
// the hk CLI failed, runs no subprocess and sends nothing; the error the site
// reports is the credential's unconfigured error.
func TestHKSitesUnconfigured(t *testing.T) {
	ctx := context.Background()
	unconfigured := "hk service credential unconfigured"

	t.Run("work preset", func(t *testing.T) {
		a := testApp(t)
		f := &fakeRunner{}
		a.Runner = f
		noHKCLI(t, f)
		_, e := a.workPreset(ctx, []string{"42"})
		if exitCode(e) != exitConfig || e == nil || e.Error() != "cannot read task 42; run handoffkeep tasks show 42 and restore task access" {
			t.Fatalf("preset=%v", e)
		}
	})

	t.Run("lifecycle done", func(t *testing.T) {
		a := testApp(t)
		f := ledgerRunner(t, a)
		noHKCLI(t, f)
		seedDoneJob(t, a)
		path := filepath.Join(a.Jobs, "j", "report.md")
		if e := os.WriteFile(path, []byte("done\n"), 0600); e != nil {
			t.Fatal(e)
		}
		wantNotInHK(t, a.lifecycle(ctx, "done", []string{"j", "--report", path}))
		errs := a.Err.(*bytes.Buffer).String()
		if !strings.Contains(errs, "report upload failed; local report retained") || !strings.Contains(errs, "task comment failed: handoffkeep: handoffkeep: "+unconfigured) {
			t.Fatalf("warnings=%q", errs)
		}
	})

	t.Run("artifact sweep", func(t *testing.T) {
		fx := newSweepFixture(t)
		fx.hk.Close()
		t.Setenv("HANDOFFKEEP_URL", "")
		t.Setenv("HANDOFFKEEP_TOKEN_SVC_PANEWIRE", "")
		if e := fx.a.artifactSweep(ctx, []string{"--job", "j", "--age", "0s", "--apply"}); e != nil {
			t.Fatalf("sweep=%v, want the task skipped as an unreadable task was", e)
		}
		if _, e := os.Stat(fx.path); e != nil {
			t.Fatalf("artifact deleted without hk: %v", e)
		}
	})
}

// cliWorkPresetTask is workPreset's read of `handoffkeep tasks show ID` at
// ab2e296, copied: the CLI printed hk's GET /v1/tasks/{id} answer.
func cliWorkPresetTask(r cliOut, id string) (Task, bool) {
	var task Task
	if r.code != 0 || json.Unmarshal([]byte(r.out), &task) != nil || scalar(task.ID) != id {
		return Task{}, false
	}
	return task, true
}

// The preset reads its task as the CLI path did: the same hk answer gives the
// same refusal or the same job, brief, report and working directory over
// HTTP as the copied CLI parser gives on the CLI's output.
func TestWorkPresetCLIHTTPParity(t *testing.T) {
	ctx := context.Background()
	wd, e := os.Getwd()
	if e != nil {
		t.Fatal(e)
	}
	marked := "change (grade A, T1)"
	for _, tc := range []struct {
		name   string
		answer hkAnswerFixture
		// refused is the exit status of a refusal, 0 when the preset reads.
		refused int
	}{
		{"hk task with refs", hkOK(hkTaskJSON(42, "ready", marked, "", "panewire", `{"job_id":"job-77","report_path":"/r/report.md","pr":"https://example.invalid/pr/1"}`)), 0},
		{"no refs", hkOK(hkTaskJSON(42, "ready", marked, "", "", `{}`)), 0},
		{"null refs", hkOK(`{"id":42,"title":"` + marked + `","refs":null}`), 0},
		{"string id", hkOK(`{"id":"42","title":"` + marked + `","refs":{"job_id":"job-s"}}`), 0},
		{"no id", hkOK(`{"title":"` + marked + `","refs":{}}`), exitConfig},
		{"null id", hkOK(`{"id":null,"title":"` + marked + `"}`), exitConfig},
		{"other id", hkOK(hkTaskJSON(43, "ready", marked, "", "", `{}`)), exitConfig},
		{"id 0", hkOK(`{"id":0,"title":"` + marked + `"}`), exitConfig},
		{"fractional id", hkOK(`{"id":42.5,"title":"` + marked + `"}`), exitConfig},
		{"malformed", hkOK(`{"id":42,`), exitConfig},
		{"empty 2xx", hkOK(``), exitConfig},
		{"not an object", hkOK(`[42]`), exitConfig},
		{"non-string title", hkOK(`{"id":42,"title":7}`), exitConfig},
		{"non-object refs", hkOK(`{"id":42,"title":"` + marked + `","refs":"x"}`), exitConfig},
		{"no marker", hkOK(hkTaskJSON(42, "ready", "change", "", "", `{}`)), exitSeatRefused},
		{"string priority, bad timestamp", hkOK(`{"id":42,"title":"` + marked + `","priority":"2","created_at":"yesterday","refs":{"job_id":"job-p"}}`), 0},
		{"non-string refs", hkOK(`{"id":42,"title":"` + marked + `","refs":{"job_id":7,"report_path":["x"],"brief_path":{}}}`), 0},
		{"dead refs: worktree object", hkOK(`{"id":42,"title":"` + marked + `","refs":{"worktree":{"path":"/w/tree"},"cwd":"/w/cwd","brief_path":"/b/brief.md"}}`), 0},
		{"dead refs: worktree string", hkOK(`{"id":42,"title":"` + marked + `","refs":{"worktree":"/w/tree"}}`), 0},
		{"dead refs: cwd", hkOK(`{"id":42,"title":"` + marked + `","refs":{"cwd":"/w/cwd"}}`), 0},
		{"not found", hkNotFound, exitConfig},
		{"outage", hkOutage, exitConfig},
		{"refused", hkAnswerFixture{http.StatusForbidden, `{"error":"forbidden"}`}, exitConfig},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// The CLI path, copied.
			task, ok := cliWorkPresetTask(asCLI(tc.answer), "42")
			cliRefused := 0
			switch {
			case !ok:
				cliRefused = exitConfig
			case taskMarker.FindStringSubmatch(task.Title) == nil:
				cliRefused = exitSeatRefused
			}
			if cliRefused != tc.refused {
				t.Fatalf("the CLI parser gives refusal %d, the table %d", cliRefused, tc.refused)
			}
			// The HTTP path.
			a, hk := testApp(t), useFakeHK(t)
			f := &fakeRunner{}
			a.Runner = f
			noHKCLI(t, f)
			hk.Handle(hkAnswers(map[string]hkAnswerFixture{"GET /v1/tasks/42": tc.answer}))
			s, e := a.workPreset(ctx, []string{"42", "--dry-run"})
			hkOnly(t, hk, "GET /v1/tasks/42")
			if tc.refused != 0 {
				want := "cannot read task 42; run handoffkeep tasks show 42 and restore task access"
				if tc.refused == exitSeatRefused {
					want = "task 42 needs a (grade X, Tn) title marker; update the task title before panewire work"
				}
				if exitCode(e) != tc.refused || e == nil || e.Error() != want {
					t.Fatalf("preset=%v exit=%d, want %d %q", e, exitCode(e), tc.refused, want)
				}
				return
			}
			if e != nil {
				t.Fatal(e)
			}
			job := firstNonempty(str(task.Refs, "job_id"), "task42-work")
			cwd := wd
			if ref, ok := task.Refs["worktree"].(map[string]any); ok {
				cwd = firstNonempty(str(ref, "path"), cwd)
			} else {
				cwd = firstNonempty(str(task.Refs, "worktree"), str(task.Refs, "cwd"), cwd)
			}
			prompt := firstNonempty(str(task.Refs, "brief_path"), filepath.Join(a.Jobs, job, "brief.md"))
			report := firstNonempty(str(task.Refs, "report_path"), filepath.Join(a.Jobs, job, "report.md"))
			if s.Job != job || s.Label != job || s.CWD != cwd || s.Prompt != prompt || s.Report != report || s.T != "T1" || s.Task != "42" {
				t.Fatalf("HTTP preset job=%q cwd=%q prompt=%q report=%q t=%q, CLI job=%q cwd=%q prompt=%q report=%q", s.Job, s.CWD, s.Prompt, s.Report, s.T, job, cwd, prompt, report)
			}
		})
	}
}

// cliSweepRead is artifactSweep's read of one job's task, document listing
// and documents at ab2e296, copied: it gives the keys it read with `doc get`
// and those whose record the sweep goes on to judge. The CLI printed hk's task
// and document answers, and the listing's documents array.
func cliSweepRead(task, list cliOut, docs map[string]cliOut, prefix, machine, job, taskID string) (read, judged []string, err error) {
	if task.code != 0 {
		return nil, nil, nil
	}
	var tk struct{ State, Project string }
	if json.Unmarshal([]byte(task.out), &tk) != nil || !slices.Contains([]string{"merged", "dropped"}, tk.State) || tk.Project == "" {
		return nil, nil, nil
	}
	if list.code != 0 {
		return nil, nil, nil
	}
	var ds []struct{ Key string }
	if json.Unmarshal([]byte(list.out), &ds) != nil {
		return nil, nil, fail(exitConfig, "invalid artifact doc listing")
	}
	for _, doc := range ds {
		if doc.Key != prefix && !strings.HasPrefix(doc.Key, prefix+"/") {
			continue
		}
		read = append(read, doc.Key)
		r := docs[doc.Key]
		if r.code != 0 {
			continue
		}
		var d struct{ Body string }
		if json.Unmarshal([]byte(r.out), &d) != nil {
			continue
		}
		var rec ArtifactRecord
		if json.Unmarshal([]byte(d.Body), &rec) != nil || rec.Schema != "fleet.artifact.v2" || rec.Host != machine || rec.Job != "" && rec.Job != job || rec.Task != "" && rec.Task != taskID {
			continue
		}
		judged = append(judged, doc.Key)
	}
	return read, judged, nil
}

// listAsCLI is the hk CLI's `doc list` output for one hk answer: the
// documents array it printed. A 2xx answer whose documents cannot be picked
// out is what the CLI parser cannot read.
func listAsCLI(a hkAnswerFixture) cliOut {
	r := asCLI(a)
	if r.code != 0 {
		return r
	}
	var v struct {
		Documents json.RawMessage `json:"documents"`
	}
	if json.Unmarshal([]byte(r.out), &v) != nil {
		return r
	}
	if v.Documents == nil {
		return cliOut{out: "null"}
	}
	return cliOut{out: string(v.Documents)}
}

// The sweep reads the task, the listing and each document as the CLI path
// did: the same hk answers make it read the same keys, judge the same records
// and stop with the same error.
func TestArtifactSweepCLIHTTPParity(t *testing.T) {
	ctx := context.Background()
	key := sweepPrefix + "/dataset"
	merged := hkOK(hkTaskJSON(42, "merged", "change (grade A, T1)", "", "Example", `{}`))
	for _, tc := range []struct {
		name       string
		task, list hkAnswerFixture
		// docs maps a key to its answer; recordDoc stands for the fixture's
		// record.
		docs   map[string]hkAnswerFixture
		judged int
		code   int
	}{
		{name: "merged", task: merged, list: hkOK(docList(key)), docs: map[string]hkAnswerFixture{key: recordDoc}, judged: 1},
		{name: "dropped", task: hkOK(hkTaskJSON(42, "dropped", "x", "", "Example", `{}`)), list: hkOK(docList(key)), docs: map[string]hkAnswerFixture{key: recordDoc}, judged: 1},
		{name: "task reopened", task: hkOK(hkTaskJSON(42, "reopened", "x", "", "Example", `{}`)), list: hkOK(docList(key))},
		{name: "task without project", task: hkOK(hkTaskJSON(42, "merged", "x", "", "", `{}`)), list: hkOK(docList(key))},
		{name: "task state not a string", task: hkOK(`{"id":42,"state":7,"project":"Example"}`), list: hkOK(docList(key))},
		{name: "task without id", task: hkOK(`{"state":"merged","project":"Example"}`), list: hkOK(docList(key)), docs: map[string]hkAnswerFixture{key: recordDoc}, judged: 1},
		{name: "task malformed", task: hkOK(`{"state":`), list: hkOK(docList(key))},
		{name: "task empty 2xx", task: hkOK(``), list: hkOK(docList(key))},
		{name: "task not found", task: hkNotFound, list: hkOK(docList(key))},
		{name: "task outage", task: hkOutage, list: hkOK(docList(key))},
		{name: "LIKE wildcard keys", task: merged, list: hkOK(docList(sweepPrefix, sweepPrefix+"X/dataset", "artifacts/Example/42/jj", key)), docs: map[string]hkAnswerFixture{sweepPrefix: recordDoc, key: recordDoc, sweepPrefix + "X/dataset": recordDoc}, judged: 2},
		{name: "empty listing", task: merged, list: hkOK(`{"documents":[]}`)},
		{name: "listing without documents", task: merged, list: hkOK(`{}`)},
		{name: "listing malformed", task: merged, list: hkOK(`{"documents":[`), code: exitConfig},
		{name: "listing documents not an array", task: merged, list: hkOK(`{"documents":"x"}`), code: exitConfig},
		{name: "listing key not a string", task: merged, list: hkOK(`{"documents":[{"key":7}]}`), code: exitConfig},
		{name: "listing outage", task: merged, list: hkOutage},
		{name: "document not found", task: merged, list: hkOK(docList(key)), docs: map[string]hkAnswerFixture{key: hkNotFound}},
		{name: "document outage", task: merged, list: hkOK(docList(key)), docs: map[string]hkAnswerFixture{key: hkOutage}},
		{name: "document malformed", task: merged, list: hkOK(docList(key)), docs: map[string]hkAnswerFixture{key: hkOK(`{"body":`)}},
		{name: "document body not a string", task: merged, list: hkOK(docList(key)), docs: map[string]hkAnswerFixture{key: hkOK(`{"key":"k","body":{}}`)}},
		{name: "document body not a record", task: merged, list: hkOK(docList(key)), docs: map[string]hkAnswerFixture{key: hkOK(docAnswer(key, "evidence", "notes"))}},
		{name: "document of another host", task: merged, list: hkOK(docList(key)), docs: map[string]hkAnswerFixture{key: hkOK(docAnswer(key, "evidence", `{"schema":"fleet.artifact.v2","host":"other"}`))}},
		{name: "document with a bad timestamp", task: merged, list: hkOK(docList(key)), docs: map[string]hkAnswerFixture{key: {http.StatusOK, "record:" + `{"created_at":"yesterday"}`}}, judged: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fx := newSweepFixture(t)
			answers := map[string]hkAnswerFixture{"GET /v1/tasks/42": tc.task, "GET /v1/documents": tc.list}
			cliDocs := map[string]cliOut{}
			for k, a := range tc.docs {
				if a == recordDoc {
					a = hkOK(docAnswer(k, "evidence", fx.record))
				} else if strings.HasPrefix(a.body, "record:") {
					var extra map[string]any
					_ = json.Unmarshal([]byte(strings.TrimPrefix(a.body, "record:")), &extra)
					var doc map[string]any
					_ = json.Unmarshal([]byte(docAnswer(k, "evidence", fx.record)), &doc)
					for x, v := range extra {
						doc[x] = v
					}
					b, _ := json.Marshal(doc)
					a = hkOK(string(b))
				}
				answers[docRoute(http.MethodGet, k)] = a
				cliDocs[k] = asCLI(a)
			}
			cliRead, cliJudged, cliErr := cliSweepRead(asCLI(tc.task), listAsCLI(tc.list), cliDocs, sweepPrefix, fx.a.machine(), "j", "42")
			if len(cliJudged) != tc.judged || exitCode(cliErr) != tc.code {
				t.Fatalf("the CLI parser judges %q and stops with %v; the table says %d and exit %d", cliJudged, cliErr, tc.judged, tc.code)
			}
			fx.hk.Handle(hkAnswers(answers))
			e := fx.a.artifactSweep(ctx, []string{"--job", "j", "--age", "0s"})
			if exitCode(e) != tc.code || (cliErr == nil) != (e == nil) || e != nil && e.Error() != cliErr.Error() {
				t.Fatalf("HTTP sweep=%v (exit %d), CLI %v", e, exitCode(e), cliErr)
			}
			var read []string
			for _, r := range fx.hk.Requests() {
				if r.Method == http.MethodGet && strings.HasPrefix(r.Path, "/v1/documents/") {
					k, _ := url.PathUnescape(strings.TrimPrefix(r.Path, "/v1/documents/"))
					read = append(read, k)
				}
			}
			if !slices.Equal(read, cliRead) {
				t.Fatalf("HTTP read documents %q, CLI %q", read, cliRead)
			}
			judged := strings.Count(fx.a.Out.(*bytes.Buffer).String(), "artifact-sweep job=j ")
			if judged != len(cliJudged) {
				t.Fatalf("HTTP judged %d records (out %q), CLI %q", judged, fx.a.Out.(*bytes.Buffer).String(), cliJudged)
			}
		})
	}
}

// recordDoc stands, in a parity row, for the sweep fixture's own record.
var recordDoc = hkAnswerFixture{status: -1}

// A report upload and an artifact record write get the upload bound, as the
// brief upload does; the task comment, the reads and a note get the call
// bound. Shrunk bounds and an hk slower than the call bound prove which one
// each uses.
func TestHKSiteUploadsUseTheUploadBound(t *testing.T) {
	call, upload := hkCallTimeout, hkUploadTimeout
	t.Cleanup(func() { hkCallTimeout, hkUploadTimeout = call, upload })
	hkCallTimeout, hkUploadTimeout = 150*time.Millisecond, 20*time.Second
	ctx := context.Background()

	a := testApp(t)
	f := ledgerRunner(t, a)
	hk := useFakeHK(t)
	noHKCLI(t, f)
	hk.SetTask("42", hkTaskJSON(42, "merged", "change (grade A, T1)", "", "Example", `{}`)).
		Handle(func(hkRequest) (int, string, bool) {
			time.Sleep(600 * time.Millisecond)
			return 0, "", false
		})
	seedDoneJob(t, a)
	path := filepath.Join(a.Jobs, "j", "report.md")
	if e := os.WriteFile(path, []byte("done\n"), 0600); e != nil {
		t.Fatal(e)
	}
	if e := a.lifecycle(ctx, "done", []string{"j", "--report", path}); e != nil {
		t.Fatal(e)
	}
	if rec := a.JobStore.(*fakeJobStore).record(t, "j"); str(rec, "report_doc") != reportDocumentKey("j", reportDigest([]byte("done\n"))) {
		t.Fatalf("record=%v, want the report uploaded under the upload bound", rec)
	}
	if errs := a.Err.(*bytes.Buffer).String(); !strings.Contains(errs, "task comment failed: ") {
		t.Fatalf("warnings=%q, want the comment timed out at the call bound", errs)
	}
	if e := a.putRecord(ctx, sweepPrefix+"/dataset", map[string]any{"schema": "fleet.artifact.v2"}); e != nil {
		t.Fatalf("artifact record write=%v, want it under the upload bound", e)
	}
	if e := PutHKDocument(ctx, "quota-stop/k", "note", "j", "note\n"); e == nil {
		t.Fatal("a note write outlived the call bound")
	}
	if _, e := hkTaskBody(ctx, "42"); e == nil {
		t.Fatal("a task read outlived the call bound")
	}
}

// An artifact record write carries a kind hk accepts: the fake refuses the
// kinds hk's documentKinds lacks, as hk answers 400 invalid_context.
func TestArtifactRecordWriteUsesKindHKAccepts(t *testing.T) {
	a, hk := testApp(t), useFakeHK(t)
	if e := a.putRecord(context.Background(), sweepPrefix+"/dataset", ArtifactRecord{}); e != nil {
		t.Fatalf("putRecord: %v", e)
	}
	hkOnly(t, hk, docRoute(http.MethodPut, sweepPrefix+"/dataset"))
}
