package fleet

import (
	"crypto/sha256"
	"encoding/hex"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// briefSentFixture is the record of a pane the node could not confirm (rc 76):
// claimed in hk, no job.spawned, the brief file on disk. It returns the
// job.brief_sent payload so a case can break one field of it.
func briefSentPayload(t *testing.T, a *App, brief string) map[string]any {
	t.Helper()
	dir := filepath.Join(a.Jobs, "j")
	if e := os.MkdirAll(dir, 0700); e != nil {
		t.Fatal(e)
	}
	path := filepath.Join(dir, "brief-delivered.md")
	if e := os.WriteFile(path, []byte(brief), 0600); e != nil {
		t.Fatal(e)
	}
	sum := sha256.Sum256([]byte(brief))
	return map[string]any{"pane_id": "w:p1", "label": "agent-a", "profile": "cmd-dsflash41", "family": "deepseek", "role": "reviewer", "owner_lane": "owner", "parent_lane": "parent", "brief_path": path, "brief_sha256": hex.EncodeToString(sum[:])}
}

// The pane ran after the node reported rc 76. Its done is accepted on the
// node's own receipt that the brief reached the pane.
func TestDoneAcceptsBriefSentReceiptWhenSubmitWasUnconfirmed(t *testing.T) {
	a, h, report, result := strictClaimedWithoutReceiptFixture(t)
	putEvent(t, a, "j", eventBriefSent, briefSentPayload(t, a, "the brief\n"))
	putEvent(t, a, "j", "job.spawn_retryable", map[string]any{"retry": "repeat the same spawn command on the same host"})
	if err := strictDone(t, a, report, result); err != nil {
		t.Fatalf("done refused although the brief was delivered: %v", err)
	}
	if str(h.record(t, "j"), "state") != "completed" {
		t.Fatalf("record=%v", h.record(t, "j"))
	}
	repaired := false
	for _, body := range h.putBodies {
		if body["state"] == "spawned" && body["pane_id"] == "w:p1" {
			repaired = true
		}
	}
	if !repaired {
		t.Fatalf("no spawned repair from the brief receipt: %v", h.putBodies)
	}
}

// A pane that never received the brief has no receipt, so done is still refused.
func TestDoneRefusesPaneWithoutBriefReceipt(t *testing.T) {
	a, h, report, result := strictClaimedWithoutReceiptFixture(t)
	// The brief file exists (the node writes it before sending) but nothing
	// says it reached a pane.
	briefSentPayload(t, a, "the brief\n")
	err := strictDone(t, a, report, result)
	assertStrictFailure(t, a, err, exitStateConflict, "no_spawn_evidence")
	for _, body := range h.putBodies {
		if body["state"] == "spawned" {
			t.Fatalf("unexpected spawned repair PUT: %v", body)
		}
	}
}

// A receipt that does not agree with the job's own brief file, or that belongs
// to an earlier attempt, is not evidence.
func TestDoneRefusesBriefReceiptThatIsNotThisAttemptsOwn(t *testing.T) {
	for _, tc := range []struct {
		name  string
		setup func(t *testing.T, a *App)
	}{
		{"brief file changed", func(t *testing.T, a *App) {
			putEvent(t, a, "j", eventBriefSent, briefSentPayload(t, a, "the brief\n"))
			if e := os.WriteFile(filepath.Join(a.Jobs, "j", "brief-delivered.md"), []byte("another brief\n"), 0600); e != nil {
				t.Fatal(e)
			}
		}},
		{"brief file gone", func(t *testing.T, a *App) {
			putEvent(t, a, "j", eventBriefSent, briefSentPayload(t, a, "the brief\n"))
			if e := os.Remove(filepath.Join(a.Jobs, "j", "brief-delivered.md")); e != nil {
				t.Fatal(e)
			}
		}},
		{"another job's brief path", func(t *testing.T, a *App) {
			p := briefSentPayload(t, a, "the brief\n")
			p["brief_path"] = filepath.Join(a.Jobs, "other", "brief-delivered.md")
			putEvent(t, a, "j", eventBriefSent, p)
		}},
		{"no pane", func(t *testing.T, a *App) {
			p := briefSentPayload(t, a, "the brief\n")
			delete(p, "pane_id")
			putEvent(t, a, "j", eventBriefSent, p)
		}},
		{"reclaimed after", func(t *testing.T, a *App) {
			putEvent(t, a, "j", eventBriefSent, briefSentPayload(t, a, "the brief\n"))
			putEvent(t, a, "j", "job.reclaim", map[string]any{"owner_lane": "owner", "parent_lane": "parent", "task_id": "42", "role": "reviewer"})
		}},
		{"lost after", func(t *testing.T, a *App) {
			putEvent(t, a, "j", eventBriefSent, briefSentPayload(t, a, "the brief\n"))
			putEvent(t, a, "j", "job.lost", map[string]any{})
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			a, h, report, result := strictClaimedWithoutReceiptFixture(t)
			tc.setup(t, a)
			err := strictDone(t, a, report, result)
			if code(err) != exitStateConflict || !strings.Contains(errText(err), "reason=no_spawn_evidence") {
				t.Fatalf("done = %v, want no_spawn_evidence", err)
			}
			for _, body := range h.putBodies {
				if body["state"] == "spawned" {
					t.Fatalf("unexpected spawned repair PUT: %v", body)
				}
			}
		})
	}
}

// The late pane completes as a spawned one: the job takes the spawn receipt, so
// the completion names its pane and label and the pane sweep can find it.
func TestDoneAdoptsBriefReceiptAsSpawnReceipt(t *testing.T) {
	a, _, report, result := strictClaimedWithoutReceiptFixture(t)
	payload := briefSentPayload(t, a, "the brief\n")
	payload["cwd"], payload["herdr_session"], payload["expires_at"] = "/work/tree", "sess", "2026-11-18T08:00:00Z"
	putEvent(t, a, "j", eventBriefSent, payload)
	if err := strictDone(t, a, report, result); err != nil {
		t.Fatal(err)
	}
	j, err := a.job("j")
	if err != nil {
		t.Fatal(err)
	}
	if j.Pane != "w:p1" || j.Label != "agent-a" || str(j.Receipt, "cwd") != "/work/tree" || str(j.Receipt, "herdr_session") != "sess" || str(j.Receipt, "expires_at") == "" {
		t.Fatalf("job did not take the spawn receipt: pane=%q label=%q receipt=%v", j.Pane, j.Label, j.Receipt)
	}
	if str(j.Receipt, "brief_sha256") != "" || str(j.Receipt, "adopted_from") != eventBriefSent {
		t.Fatalf("receipt=%v", j.Receipt)
	}
	completed := 0
	for _, ev := range mustEvents(t, a, "j") {
		if ev.Kind == "job.completed" {
			completed++
			if str(ev.Payload, "pane_id") != "w:p1" || str(ev.Payload, "label") != "agent-a" {
				t.Fatalf("completion names pane=%q label=%q", str(ev.Payload, "pane_id"), str(ev.Payload, "label"))
			}
		}
	}
	if completed != 1 {
		t.Fatalf("job.completed events=%d", completed)
	}
}

// Nothing is adopted without a receipt: no job.spawned appears.
func TestDoneAdoptsNothingWithoutBriefReceipt(t *testing.T) {
	a, _, report, result := strictClaimedWithoutReceiptFixture(t)
	_ = strictDone(t, a, report, result)
	for _, ev := range mustEvents(t, a, "j") {
		if ev.Kind == "job.spawned" {
			t.Fatal("job.spawned written without a delivered-brief receipt")
		}
	}
}
