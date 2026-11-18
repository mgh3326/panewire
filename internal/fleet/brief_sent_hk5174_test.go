package fleet

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
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

// spawnFailed is what Spawn's failure path writes last: the spawn is over.
func spawnFailed(t *testing.T, a *App) {
	t.Helper()
	putEvent(t, a, "j", "job.spawn_retryable", map[string]any{"retry": "repeat the same spawn command on the same host"})
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
			spawnFailed(t, a)
			if e := os.WriteFile(filepath.Join(a.Jobs, "j", "brief-delivered.md"), []byte("another brief\n"), 0600); e != nil {
				t.Fatal(e)
			}
		}},
		{"brief file gone", func(t *testing.T, a *App) {
			putEvent(t, a, "j", eventBriefSent, briefSentPayload(t, a, "the brief\n"))
			spawnFailed(t, a)
			if e := os.Remove(filepath.Join(a.Jobs, "j", "brief-delivered.md")); e != nil {
				t.Fatal(e)
			}
		}},
		{"another job's brief path", func(t *testing.T, a *App) {
			p := briefSentPayload(t, a, "the brief\n")
			p["brief_path"] = filepath.Join(a.Jobs, "other", "brief-delivered.md")
			putEvent(t, a, "j", eventBriefSent, p)
			spawnFailed(t, a)
		}},
		{"no pane", func(t *testing.T, a *App) {
			p := briefSentPayload(t, a, "the brief\n")
			delete(p, "pane_id")
			putEvent(t, a, "j", eventBriefSent, p)
			spawnFailed(t, a)
		}},
		{"reclaimed after", func(t *testing.T, a *App) {
			putEvent(t, a, "j", eventBriefSent, briefSentPayload(t, a, "the brief\n"))
			spawnFailed(t, a)
			putEvent(t, a, "j", "job.reclaim", map[string]any{"owner_lane": "owner", "parent_lane": "parent", "task_id": "42", "role": "reviewer"})
		}},
		{"lost after", func(t *testing.T, a *App) {
			putEvent(t, a, "j", eventBriefSent, briefSentPayload(t, a, "the brief\n"))
			spawnFailed(t, a)
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
	spawnFailed(t, a)
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

// The receipt exists from the send on, while Spawn may still confirm the brief
// and write its own job.spawned. A done in that interval adopts nothing: the
// spawn's later job.spawned would revive the completed job.
func TestDoneDoesNotAdoptWhileSpawnIsStillConfirming(t *testing.T) {
	a, h, report, result := strictClaimedWithoutReceiptFixture(t)
	putEvent(t, a, "j", eventBriefSent, briefSentPayload(t, a, "the brief\n"))
	err := strictDone(t, a, report, result)
	assertStrictFailure(t, a, err, exitStateConflict, "no_spawn_evidence")
	for _, ev := range mustEvents(t, a, "j") {
		if ev.Kind == "job.spawned" {
			t.Fatal("job.spawned adopted while the spawn is in flight")
		}
	}
	for _, body := range h.putBodies {
		if body["state"] == "spawned" {
			t.Fatalf("unexpected spawned repair PUT: %v", body)
		}
	}
}

// A spawn whose pane cleanup was refused ends with job.cleanup_pending; that
// pane is the one still running, and its done is accepted.
func TestDoneAcceptsBriefReceiptAfterRefusedCleanup(t *testing.T) {
	a, _, report, result := strictClaimedWithoutReceiptFixture(t)
	putEvent(t, a, "j", eventBriefSent, briefSentPayload(t, a, "the brief\n"))
	putEvent(t, a, "j", "job.cleanup_pending", map[string]any{"reason": "pane_or_resources_unverified"})
	if err := strictDone(t, a, report, result); err != nil {
		t.Fatal(err)
	}
}

// The adopted receipt is not a new claim: a hub revocation stands whether it
// was written before the adoption or after it.
func TestAdoptedSpawnReceiptDoesNotClearHubRevocation(t *testing.T) {
	for _, tc := range []struct {
		name  string
		order func(t *testing.T, a *App)
	}{
		{"revoked after adoption", func(t *testing.T, a *App) {
			a2 := adoptFixture(t, a)
			_ = a2
			writeHubRevocationRecord(t, a, "j")
		}},
		{"revoked before adoption", func(t *testing.T, a *App) {
			writeHubRevocationRecord(t, a, "j")
			adoptFixture(t, a)
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			a, _, _, _ := strictClaimedWithoutReceiptFixture(t)
			tc.order(t, a)
			j, err := a.job("j")
			if err != nil {
				t.Fatal(err)
			}
			if !j.Reassigned || !a.hubReassigned(j) {
				t.Fatal("adoption cleared the hub's revocation")
			}
		})
	}
}

// adoptFixture records the brief receipt and the spawn's end, then adopts it as
// `done` would, writing the job.spawned directly.
func adoptFixture(t *testing.T, a *App) Job {
	t.Helper()
	putEvent(t, a, "j", eventBriefSent, briefSentPayload(t, a, "the brief\n"))
	spawnFailed(t, a)
	j, err := a.job("j")
	if err != nil {
		t.Fatal(err)
	}
	receipt := a.briefSentReceipt(j.Events, "j")
	if receipt == nil {
		t.Fatal("no receipt to adopt")
	}
	putEvent(t, a, "j", "job.spawned", map[string]any{"pane_id": "w:p1", "adopted_from": eventBriefSent})
	adopted, err := a.job("j")
	if err != nil {
		t.Fatal(err)
	}
	return adopted
}

// putEventAt writes the next ledger event with an explicit timestamp, so a test
// can step the node clock back between two attempts of one job.
func putEventAt(t *testing.T, a *App, job, kind string, p map[string]any, at time.Time) {
	t.Helper()
	dir := filepath.Join(a.Jobs, job, "events")
	files, _ := os.ReadDir(dir)
	b, _ := json.Marshal(map[string]any{"kind": kind, "payload": p, "created_at": at.Format(time.RFC3339Nano)})
	if e := os.WriteFile(filepath.Join(dir, fmt.Sprintf("%05d-%s.json", len(files)+1, kind)), b, 0600); e != nil {
		t.Fatal(e)
	}
}

// The node clock stepped back before a same-job retry: by timestamp the retry's
// claim and receipt sort before the first attempt's receipt and failure marker,
// but in ledger order the retry is the newest attempt and is still confirming.
// The first attempt's closed pane must not be adopted for it.
func TestBriefReceiptFollowsLedgerOrderAcrossClockStepBack(t *testing.T) {
	a, _, report, result := strictClaimedWithoutReceiptFixture(t)
	base := a.Now()
	first := briefSentPayload(t, a, "the brief\n")
	first["pane_id"] = "w:old"
	second := briefSentPayload(t, a, "the brief\n")
	second["pane_id"] = "w:new"
	claim := map[string]any{"owner_lane": "owner", "parent_lane": "parent", "task_id": "42", "role": "reviewer"}
	putEventAt(t, a, "j", eventBriefSent, first, base.Add(10*time.Minute))
	putEventAt(t, a, "j", "job.spawn_retryable", map[string]any{}, base.Add(11*time.Minute))
	// The retry, after the clock stepped back an hour.
	putEventAt(t, a, "j", "job.reclaim", claim, base.Add(-50*time.Minute))
	putEventAt(t, a, "j", eventBriefSent, second, base.Add(-49*time.Minute))
	events, err := a.events("j")
	if err != nil {
		t.Fatal(err)
	}
	if got := a.briefSentReceipt(events, "j"); got != nil {
		t.Fatalf("adopted pane %q while the retry is still confirming", str(got, "pane_id"))
	}
	err = strictDone(t, a, report, result)
	assertStrictFailure(t, a, err, exitStateConflict, "no_spawn_evidence")
	for _, ev := range mustEvents(t, a, "j") {
		if ev.Kind == "job.spawned" {
			t.Fatal("job.spawned adopted from the first attempt")
		}
	}
}
