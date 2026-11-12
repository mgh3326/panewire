package fleet

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// hk 3615: `panewire done` links the report document it uploads to the job's
// hk task (task_id, origin_class job), and the job record update that names
// the report also pins its sha256, so hk confirms the stored bytes. A failed
// upload is still a warning: the local report stays, no pin is written and
// the relay still goes out.

// doneReportLink runs `done --report` for job "j" seeded with task, which
// "" leaves without a task, and returns the fake hk's report PUT body, the
// job record and the job.completed event's report_sha256.
func doneReportLink(t *testing.T, task string) (put, rec map[string]any, eventSHA, reportSHA string) {
	t.Helper()
	ctx := context.Background()
	a := testApp(t)
	f := ledgerRunner(t, a)
	hk := useFakeHK(t)
	noHKCLI(t, f)
	hk.Handle(hkCommentAnswer)
	claim := map[string]any{"owner_lane": "owner", "parent_lane": "parent", "t_level": "T1", "role": "worker"}
	if task != "" {
		claim["task_id"] = task
	}
	putEvent(t, a, "j", "job.claim", claim)
	putEvent(t, a, "j", "job.spawned", map[string]any{"pane_id": "w:p1", "tab_id": "w:t1", "label": "agent-a", "profile": "cmd-dsflash41", "family": "deepseek", "keep": false})
	if e := a.JobStore.(*fakeJobStore).claimSpawned(ctx, "j", task, a.machine()); e != nil {
		t.Fatal(e)
	}
	path := filepath.Join(a.Jobs, "j", "report.md")
	report := []byte("first line\nREADY: done\n")
	if e := os.WriteFile(path, report, 0600); e != nil {
		t.Fatal(e)
	}
	if e := a.lifecycle(ctx, "done", []string{"j", "--report", path}); e != nil {
		t.Fatal(e)
	}
	for _, r := range hk.Requests() {
		if r.Route() == docRoute(http.MethodPut, reportDocumentKey("j", reportDigest(report))) {
			put = hkJSON(t, r)
		}
	}
	if put == nil {
		t.Fatalf("hk routes=%q, want the report PUT", hk.Routes())
	}
	j, e := a.job("j")
	if e != nil {
		t.Fatal(e)
	}
	for _, ev := range j.Events {
		if ev.Kind == "job.completed" {
			eventSHA = str(ev.Payload, "report_sha256")
		}
	}
	sum := sha256.Sum256(report)
	return put, a.JobStore.(*fakeJobStore).record(t, "j"), eventSHA, hex.EncodeToString(sum[:])
}

func TestDoneReportDocLinksTaskAndPinsSHA(t *testing.T) {
	t.Run("task job", func(t *testing.T) {
		put, rec, eventSHA, reportSHA := doneReportLink(t, "42")
		if put["task_id"] != float64(42) || put["origin_class"] != "job" {
			t.Errorf("report put task_id=%v origin_class=%v, want 42 and job", put["task_id"], put["origin_class"])
		}
		if put["kind"] != "report" || put["job"] != "j" {
			t.Fatalf("report put=%v, want kind report and job j", put)
		}
		body, _ := put["body"].(string)
		sum := sha256.Sum256([]byte(body))
		if str(rec, "report_doc") != reportDocumentKey("j", reportDigest([]byte(body))) || str(rec, "report_sha256") != hex.EncodeToString(sum[:]) {
			t.Errorf("record=%v, want report_doc and report_sha256 of the uploaded body", rec)
		}
		if str(rec, "report_sha256") != reportSHA || eventSHA != reportSHA {
			t.Fatalf("record sha=%q event sha=%q, want both the report's %q", str(rec, "report_sha256"), eventSHA, reportSHA)
		}
	})

	for name, task := range map[string]string{"job without task": "", "non-numeric task": "hk-abc"} {
		t.Run(name, func(t *testing.T) {
			put, rec, _, reportSHA := doneReportLink(t, task)
			if _, ok := put["task_id"]; ok {
				t.Fatalf("report put task_id=%v, want it omitted", put["task_id"])
			}
			if put["origin_class"] != "job" {
				t.Errorf("report put origin_class=%v, want job", put["origin_class"])
			}
			if str(rec, "report_sha256") != reportSHA {
				t.Fatalf("record=%v, want report_sha256 %s", rec, reportSHA)
			}
		})
	}
}

// A refused upload keeps the existing failure handling: the warning, the
// local report, the relay and the event digest, and the record names neither
// a report doc nor a pin (hk refuses a pin without a report).
func TestDoneReportUploadFailureWritesNoPin(t *testing.T) {
	ctx := context.Background()
	a := testApp(t)
	f := ledgerRunner(t, a)
	hk := useFakeHK(t)
	noHKCLI(t, f)
	hk.Handle(hkFails("PUT /v1/documents/", http.StatusInternalServerError, `{"error":"internal_error"}`)).Handle(hkCommentAnswer)
	seedDoneJob(t, a)
	path := filepath.Join(a.Jobs, "j", "report.md")
	report := []byte("done\n")
	if e := os.WriteFile(path, report, 0600); e != nil {
		t.Fatal(e)
	}
	if e := a.lifecycle(ctx, "done", []string{"j", "--report", path}); e != nil {
		t.Fatal(e)
	}
	if errs := a.Err.(*bytes.Buffer).String(); !strings.Contains(errs, "panewire: warning: report upload failed; local report retained\n") {
		t.Fatalf("warnings=%q", errs)
	}
	if b, e := os.ReadFile(path); e != nil || !bytes.Equal(b, report) {
		t.Fatalf("local report=%q err=%v, want it retained", b, e)
	}
	rec := a.JobStore.(*fakeJobStore).record(t, "j")
	if _, ok := rec["report_doc"]; ok {
		t.Fatalf("record=%v, want no report_doc", rec)
	}
	if _, ok := rec["report_sha256"]; ok {
		t.Fatalf("record=%v, want no report_sha256", rec)
	}
	if str(rec, "state") != "completed" {
		t.Fatalf("record=%v, want state completed", rec)
	}
	relayed := false
	for _, c := range f.Calls {
		if c.Name == "panewire" && len(c.Args) > 0 && c.Args[0] == "emit" {
			relayed = true
		}
	}
	if !relayed {
		t.Fatalf("calls=%v, want the relay emit", f.Calls)
	}
	j, e := a.job("j")
	if e != nil {
		t.Fatal(e)
	}
	sum := sha256.Sum256(report)
	for _, ev := range j.Events {
		if ev.Kind == "job.completed" && (str(ev.Payload, "report_sha256") != hex.EncodeToString(sum[:]) || str(ev.Payload, "document") != "") {
			t.Fatalf("event=%v, want the local digest and no document", ev.Payload)
		}
	}
	if out := a.Out.(*bytes.Buffer).String(); !strings.Contains(out, "OK job=j ") {
		t.Fatalf("out=%q", out)
	}
}
