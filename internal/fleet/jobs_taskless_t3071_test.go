package fleet

import (
	"context"
	"slices"
	"testing"
	"time"
)

// A task-less job (a reviewer or consult run with no --task) claims its hk
// record with a null task_id. hk accepts that claim (docs/jobs.md: "Claims
// may omit task_id or send null"), so the claim must reach the wire rather
// than fail locally.
func TestJobClaimTaskLessSendsNullTaskID(t *testing.T) {
	h := newFakeHK(t)
	if e := h.client().Claim(context.Background(), "j", "", "test-host"); e != nil {
		t.Fatalf("task-less claim refused locally: %v", e)
	}
	rec := h.record(t, "j")
	if rec["task_id"] != nil {
		t.Fatalf("task_id=%#v, want null", rec["task_id"])
	}
	if str(rec, "host") != "test-host" || str(rec, "state") != "claimed" {
		t.Fatalf("record=%v", rec)
	}
}

// hk stores the client time of a claim as claimed_at (docs/jobs.md), so the
// placement time reflects when panewire claimed, not when hk received it.
func TestJobClaimStampsClientTime(t *testing.T) {
	h := newFakeHK(t)
	at := time.Date(2026, 11, 2, 9, 30, 0, 0, time.UTC)
	store := newHKJobs(&hkQuota{base: h.srv.URL, token: "test-token"}, func() time.Time { return at })
	if e := store.Claim(context.Background(), "j", "42", "test-host"); e != nil {
		t.Fatal(e)
	}
	if got, want := str(h.record(t, "j"), "claimed_at"), at.Format(time.RFC3339Nano); got != want {
		t.Fatalf("claimed_at=%q, want %q", got, want)
	}
}

// The spawned transition's spawned_at must come from the App clock. The
// fixture clock advances per call, so the test records every value the clock
// returns during Spawn and requires spawned_at to be one of them. The lost
// transition carries ended_at from a fixed clock, since MarkJobLost does no
// waiting.
func TestSpawnedAndEndedRecordsCarryClientTime(t *testing.T) {
	a, s, _, _ := cmdFixture(t)
	h := newFakeHK(t)
	a.JobStore = h.client()
	base := a.Now
	var seen []time.Time
	a.Now = func() time.Time {
		now := base()
		seen = append(seen, now)
		return now
	}
	if e := a.Spawn(context.Background(), s); e != nil {
		t.Fatal(e)
	}
	spawnedAt, e := time.Parse(time.RFC3339Nano, str(h.record(t, s.Job), "spawned_at"))
	if e != nil {
		t.Fatalf("spawned_at unparsable: %v", e)
	}
	if !slices.ContainsFunc(seen, func(v time.Time) bool { return v.Equal(spawnedAt) }) {
		t.Fatalf("spawned_at=%v is not a value the App clock returned", spawnedAt)
	}
	endedAt := time.Date(2026, 11, 2, 9, 55, 0, 0, time.UTC)
	a.Now = func() time.Time { return endedAt }
	if e := a.MarkJobLost(context.Background(), s.Job); e != nil {
		t.Fatal(e)
	}
	rec := h.record(t, s.Job)
	if str(rec, "state") != "lost" {
		t.Fatalf("state=%v, want lost", rec["state"])
	}
	if got, want := str(rec, "ended_at"), endedAt.Format(time.RFC3339Nano); got != want {
		t.Fatalf("ended_at=%q, want %q", got, want)
	}
}
