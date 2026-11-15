package panewire

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// hk 3685 item 2/4: `done` deletes the local report once hk holds it and the
// record pins it. The node's job.result answers from the completion receipt,
// a reaper's job.done from the done marker, and the stall detector records the
// hk document instead of a missing local file.

func TestNodeJobResultAnswersADeletedReportFromTheLedger(t *testing.T) {
	client, report := nodeReapJobDir(t, "job-a", nodeJobReportFile)
	dir := filepath.Dir(report)
	sha := strings.Repeat("a", 64)
	events := filepath.Join(dir, "events")
	if err := os.MkdirAll(events, 0700); err != nil {
		t.Fatal(err)
	}
	raw := `{"kind":"job.completed","payload":{"report":"` + report + `","report_sha256":"` + sha + `","document":"reports/job-a/sha256/` + sha + `.md"}}`
	if err := os.WriteFile(filepath.Join(events, "00001-job.completed.json"), []byte(raw), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(report); err != nil {
		t.Fatal(err)
	}
	got, code := client.nodeJobResult(nodeJobResultArgs{Job: "job-a"})
	if code != "" {
		t.Fatal(code)
	}
	result := got.(nodeJobResult)
	if !result.Completed || !result.Report.Present || result.Report.SHA256 != sha {
		t.Fatalf("%+v, want the ledger's report sha", result)
	}
	// The bytes and last line were local facts of a file that is gone.
	if result.Report.Bytes != 0 || result.Report.LastLine != "" {
		t.Fatalf("report=%+v, want no local byte count or last line", result.Report)
	}
}

// A completion whose upload failed names no document: the absent file is still
// an absent report.
func TestNodeJobResultKeepsAMissingReportWithoutADocument(t *testing.T) {
	client, report := nodeReapJobDir(t, "job-a", nodeJobReportFile)
	dir := filepath.Dir(report)
	events := filepath.Join(dir, "events")
	if err := os.MkdirAll(events, 0700); err != nil {
		t.Fatal(err)
	}
	raw := `{"kind":"job.completed","payload":{"report":"` + report + `","report_sha256":""}}`
	if err := os.WriteFile(filepath.Join(events, "00001-job.completed.json"), []byte(raw), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(report); err != nil {
		t.Fatal(err)
	}
	got, code := client.nodeJobResult(nodeJobResultArgs{Job: "job-a"})
	if code != "" {
		t.Fatal(code)
	}
	if result := got.(nodeJobResult); result.Report.Present {
		t.Fatalf("%+v, want no report", result)
	}
}

func TestNodeJobDoneAnswersAlreadyDoneAfterTheReportIsDeleted(t *testing.T) {
	runs, _, lifecycle := nodeReapLifecycle(t, "", false)
	client, report := nodeReapJobDir(t, "job-a", nodeJobReportFile)
	client.nodeLifecycle = lifecycle
	args := nodeJobDoneArgs{Job: "job-a", Report: report}
	want := "OK job=job-a report=" + report + " kind=job.completed"

	result, code := nodeRun(t, client, nodeCommandJobDone, args)
	if code != "" || result["outcome"] != nodeJobDoneOK || result["line"] != want {
		t.Fatalf("code=%q result=%v", code, result)
	}
	// `done` deleted the report after the pin; the marker is what is left.
	if err := os.Remove(report); err != nil {
		t.Fatal(err)
	}
	result, code = nodeRun(t, client, nodeCommandJobDone, args)
	if code != "" || result["outcome"] != nodeJobAlreadyDone || result["line"] != want {
		t.Fatalf("after delete code=%q result=%v", code, result)
	}
	if runs() != 1 {
		t.Fatalf("panewire ran %d times, want one", runs())
	}
}

// A released job id claimed again is a new round: the old round's marker must
// not answer for it. Every kind the fleet's foldJob treats as a revive starts
// one.
func TestNodeJobDoneIgnoresAMarkerBeforeTheLastNewRound(t *testing.T) {
	for _, kind := range []string{"job.claim", "job.claimed", "job.reclaim", "job.spawned", "job.reprompted"} {
		t.Run(kind, func(t *testing.T) {
			runs, _, lifecycle := nodeReapLifecycle(t, "", false)
			client, report := nodeReapJobDir(t, "job-a", nodeJobReportFile)
			client.nodeLifecycle = lifecycle
			args := nodeJobDoneArgs{Job: "job-a", Report: report}
			if _, code := nodeRun(t, client, nodeCommandJobDone, args); code != "" {
				t.Fatalf("first run code=%q", code)
			}
			if err := os.Remove(report); err != nil {
				t.Fatal(err)
			}
			events := filepath.Join(filepath.Dir(report), "events")
			if err := os.MkdirAll(events, 0700); err != nil {
				t.Fatal(err)
			}
			record := filepath.Join(events, "00001-"+kind+".json")
			if err := os.WriteFile(record, []byte(`{"kind":"`+kind+`"}`), 0600); err != nil {
				t.Fatal(err)
			}
			later := time.Now().Add(time.Second)
			if err := os.Chtimes(record, later, later); err != nil {
				t.Fatal(err)
			}
			result, code := nodeRun(t, client, nodeCommandJobDone, args)
			if code != "" || result["outcome"] != nodeJobReportInvalid {
				t.Fatalf("%s after the marker code=%q result=%v, want report-invalid", kind, code, result)
			}
			if runs() != 1 {
				t.Fatalf("panewire ran %d times, want one", runs())
			}
		})
	}
}

func TestStallObservesAPinnedReportInHK(t *testing.T) {
	fx := newStallFixture(t, false)
	claimJob(t, fx, "job-a", "w1:p1", nil, fx.now.Add(-time.Hour))
	writeRawJobEvent(t, fx.inbox, "job-a", "00004-job.completed.json",
		`{"kind":"job.completed","job_id":"job-a","report_path":"reports/job-a/sha256/abc.md","payload":{"document":"reports/job-a/sha256/abc.md","report_sha256":"abc"},"seq":4,"created_at":"2026-09-17T09:00:00Z"}`)
	fx.scan()
	row, found, err := fx.store.stallReport(context.Background(), "job-a", 1)
	if err != nil || !found {
		t.Fatalf("report row not found: found=%v err=%v", found, err)
	}
	if row.RefKind != "doc_key" || row.RefValue != "reports/job-a/sha256/abc.md" || row.State != "in_hk" {
		t.Fatalf("pinned report row=%+v, want the hk document recorded in_hk", row)
	}
	if !strings.Contains(row.Note, "reports/job-a/sha256/abc.md") {
		t.Fatalf("note=%q, want the document key named", row.Note)
	}
}
