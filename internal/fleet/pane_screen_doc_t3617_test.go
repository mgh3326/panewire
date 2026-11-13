package fleet

import (
	"bytes"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"unicode/utf8"
)

// hk 3617: the task pane sweep uploads the screen it saves to hk as the
// evidence document screens/<job>/pane-screen.txt, linked to the job and its
// task. The local file is written exactly as before, and a failed upload
// never keeps the pane open: one warning, and the close goes on.

const paneScreenUploadWarning = "pane screen upload failed"

// screenDocPut is the fake hk's PUT of job's screen document, or nil.
func screenDocPut(t *testing.T, hk *spawnHK, job string) map[string]any {
	t.Helper()
	for _, r := range hk.Requests() {
		if r.Route() == docRoute(http.MethodPut, "screens/"+job+"/pane-screen.txt") {
			return hkJSON(t, r)
		}
	}
	return nil
}

func (f *paneSweepFixture) screen(job string) string {
	f.t.Helper()
	b, err := os.ReadFile(filepath.Join(f.a.Jobs, job, PaneScreenFile))
	if err != nil {
		f.t.Fatalf("local screen: %v", err)
	}
	return string(b)
}

func TestPaneSweepUploadsTheScreenAsEvidence(t *testing.T) {
	for name, task := range map[string]string{"task job": "42", "non-numeric task": "hk-abc"} {
		t.Run(name, func(t *testing.T) {
			f := newPaneSweepFixture(t)
			hk := useFakeHK(t)
			pane := f.job("j", "implementer", task)
			f.tasks[task] = "merged"
			f.at(0)
			f.at(11)
			if got := f.closed(); !slices.Equal(got, []string{pane}) {
				t.Fatalf("closed panes = %v, want [%s]", got, pane)
			}
			put := screenDocPut(t, hk, "j")
			if put == nil {
				t.Fatalf("hk routes = %q, want the screen document PUT", hk.Routes())
			}
			if put["key"] != "screens/j/pane-screen.txt" || put["kind"] != "evidence" || put["job"] != "j" || put["origin_class"] != "job" {
				t.Fatalf("screen put = %v, want key screens/j/pane-screen.txt, kind evidence, job j, origin_class job", put)
			}
			// hk 3702: a stored upload writes no local screen; the document is the copy.
			if body, _ := put["body"].(string); body != "last screen of "+pane {
				t.Fatalf("screen put body = %q, want the pane's screen", body)
			}
			if f.localScreenFile("j") {
				t.Fatalf("a stored screen left %s in the job dir", PaneScreenFile)
			}
			taskID, linked := put["task_id"]
			switch {
			case task == "42" && taskID != float64(42):
				t.Fatalf("screen put task_id = %v, want 42", taskID)
			case task != "42" && linked:
				t.Fatalf("screen put task_id = %v, want it omitted for task %q", taskID, task)
			}
			if errs := f.a.Err.(*bytes.Buffer).String(); strings.Contains(errs, paneScreenUploadWarning) {
				t.Fatalf("warnings = %q, want none after a stored upload", errs)
			}
		})
	}
}

// Whatever hk does with the upload — refuse it (a secret-like screen, a body
// over its cap), fail, or not be configured on this node at all — the sweep
// closes the pane with the local screen saved, after one warning.
func TestPaneSweepClosesThePaneWhenTheScreenUploadFails(t *testing.T) {
	cases := map[string]func(t *testing.T){
		"hk refuses": func(t *testing.T) {
			useFakeHK(t).Handle(hkFails("PUT /v1/documents/", http.StatusBadRequest, `{"error":"invalid_context","reason":"secret-like content"}`))
		},
		"hk fails": func(t *testing.T) {
			useFakeHK(t).Handle(hkFails("PUT /v1/documents/", http.StatusInternalServerError, `{"error":"internal_error"}`))
		},
		"hk unconfigured": func(*testing.T) {},
	}
	for name, setup := range cases {
		t.Run(name, func(t *testing.T) {
			f := newPaneSweepFixture(t)
			setup(t)
			pane := f.job("j", "implementer", "42")
			f.tasks["42"] = "merged"
			f.at(0)
			f.at(11)
			if got := f.closed(); !slices.Equal(got, []string{pane}) {
				t.Fatalf("closed panes = %v, want [%s]", got, pane)
			}
			if f.screen("j") != "last screen of "+pane {
				t.Fatalf("local screen = %q", f.screen("j"))
			}
			if payload := f.reaped("j"); payload["reason"] != "task_merged" {
				t.Fatalf("job.reaped payload = %v", payload)
			}
			if errs := f.a.Err.(*bytes.Buffer).String(); strings.Count(errs, paneScreenUploadWarning) != 1 {
				t.Fatalf("warnings = %q, want one %q", errs, paneScreenUploadWarning)
			}
		})
	}
}

// A screen over hk's document cap is uploaded as its tail, cut on a rune
// boundary, under a first line that says it was cut; the local file keeps
// the whole screen.
func TestPaneSweepUploadsTheTailOfAnOversizedScreen(t *testing.T) {
	f := newPaneSweepFixture(t)
	hk := useFakeHK(t)
	pane := f.job("j", "implementer", "42")
	f.tasks["42"] = "merged"
	full := "first line\n" + strings.Repeat("화면 ", 120<<10) + "\nlast line\n"
	base := f.runner.Do
	f.runner.Do = func(c Command) Result {
		if c.Name == "herdr" && strings.Join(c.Args, " ") == "pane read "+pane {
			return Result{Out: full}
		}
		return base(c)
	}
	f.at(0)
	f.at(11)
	if got := f.closed(); !slices.Equal(got, []string{pane}) {
		t.Fatalf("closed panes = %v, want [%s]", got, pane)
	}
	// hk 3702: the document keeps only the tail, so the whole screen stays locally.
	if f.screen("j") != full {
		t.Fatalf("local screen is %d bytes, want the whole %d", len(f.screen("j")), len(full))
	}
	put := screenDocPut(t, hk, "j")
	if put == nil {
		t.Fatalf("hk routes = %q, want the screen document PUT", hk.Routes())
	}
	body, _ := put["body"].(string)
	header, tail, found := strings.Cut(body, "\n")
	if !found || len(body) > 512<<10 || !utf8.ValidString(body) {
		t.Fatalf("uploaded body is %d bytes (valid UTF-8 %v), want at most %d", len(body), utf8.ValidString(body), 512<<10)
	}
	if !strings.Contains(header, "cut") || strings.Contains(full, header) {
		t.Fatalf("first line = %q, want a header that says the screen was cut", header)
	}
	if !strings.HasSuffix(full, tail) || !strings.HasSuffix(tail, "\nlast line\n") || len(tail) < (512<<10)-len(header)-8 {
		t.Fatalf("uploaded tail is %d bytes, want the screen's last bytes up to the cap", len(tail))
	}
	if errs := f.a.Err.(*bytes.Buffer).String(); strings.Contains(errs, paneScreenUploadWarning) {
		t.Fatalf("warnings = %q, want none after a stored upload", errs)
	}
}
