package panewire

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/mgh3326/panewire/internal/fleet"
)

// hk 6702: a parked request's reason is the one its tried hosts' refusal codes
// name, never a word of the error text; branch_in_use parks on its timer, and
// the parked notice names the holding worktree.

// parkTriedDesk is the production shape the desk reported: three hosts refused
// the branch as held, two are not accepting and one is excluded. The only
// "quota" in the error text is the name of a host that is not accepting.
const parkTriedDesk = "local spawns are disabled on this host; no remote candidate completed (tried: mac-a checkout_refused:branch_in_use; mac-b checkout_refused:branch_in_use; mac-c checkout_refused:branch_in_use; mac-d not_accepting; quota-lab not_accepting; mac-f excluded_host)"

func parkTriedDeskRecords(holder string) []fleet.PlacementCandidateRecord {
	refused := "hub_checkout_refused:branch_in_use:_worktree_" + holder + "_retry_in_600s"
	return []fleet.PlacementCandidateRecord{
		{Host: "mac-a", Outcome: "skipped", Reason: refused},
		{Host: "mac-b", Outcome: "skipped", Reason: refused},
		{Host: "mac-c", Outcome: "skipped", Reason: refused},
		{Host: "mac-d", Outcome: "skipped", Reason: "not_accepting"},
		{Host: "quota-lab", Outcome: "skipped", Reason: "not_accepting"},
		{Host: "mac-f", Outcome: "skipped", Reason: "excluded_host"},
	}
}

// RED on a0df3182 by the text mutant (spawnParkReasonFor reads spawnParkReason
// of the text): the "quota" host name selects quota. The holder path variant
// is RED by the substring mutant (a record matched with strings.Contains).
func TestParkReasonNamesTheRefusalsNotAWord_t6702(t *testing.T) {
	if got := spawnParkReasonFor(80, parkTriedDesk, parkTriedDeskRecords("work_repo.parent")); got != spawnParkBranchInUse {
		t.Fatalf("reason = %q, want %q: every candidate refused the branch or is not a candidate", got, spawnParkBranchInUse)
	}
	holderWord := parkTriedDeskRecords("work_quota-lab")
	if got := spawnParkReasonFor(80, parkTriedDesk, holderWord); got != spawnParkBranchInUse {
		t.Fatalf("reason = %q with a holder path naming quota, want %q", got, spawnParkBranchInUse)
	}
	none := []fleet.PlacementCandidateRecord{{Host: "mac-d", Outcome: "skipped", Reason: "not_accepting"}, {Host: "mac-f", Outcome: "skipped", Reason: "excluded_host"}}
	if got := spawnParkReasonFor(80, "local spawns are disabled on this host; no remote candidate completed (tried: mac-d not_accepting; mac-f excluded_host)", none); got != spawnParkNoEligibleHost {
		t.Fatalf("reason = %q, want %q: no tried host refused for any park reason", got, spawnParkNoEligibleHost)
	}
}

// RED on a0df3182 by the text mutant: the decoy host "launch_backoff-1" names a
// reason no host gave, so the text reads quota,launch_backoff. A real quota
// refusal on one tried host still parks as quota alone.
func TestRealQuotaRefusalStillParksAsQuota_t6702(t *testing.T) {
	tried := []fleet.PlacementCandidateRecord{
		{Host: "launch_backoff-1", Outcome: "skipped", Reason: "not_accepting"},
		{Host: "mac-a", Outcome: "skipped", Reason: "quota_exhausted"},
	}
	text := "local spawns are disabled on this host; no remote candidate completed (tried: launch_backoff-1 not_accepting; mac-a quota_exhausted)"
	if got := spawnParkReasonFor(80, text, tried); got != spawnParkQuota {
		t.Fatalf("reason = %q, want %q", got, spawnParkQuota)
	}
	// A pass that recorded no candidates keeps reading the text as it did.
	if got, want := spawnParkReasonFor(80, text, nil), spawnParkReason(80, text); got != want {
		t.Fatalf("no-records reason = %q, want the text's %q", got, want)
	}
}

// branch_in_use is timer-only (no event counts for it) and a parked row reads
// parked with that reason through the status API. RED on a0df3182 by the
// timer mutant (markSpawnWakeLocked without the branch_in_use clause).
func TestBranchInUseParksOnItsTimerOnly_t6702(t *testing.T) {
	h := asyncSpawnHub(t)
	clock := expiryClock(h, expiryNow())
	task := claimParkRequest(t, h, 1, *clock)
	if !parkNow(t, h, task, spawnParkBranchInUse) {
		t.Fatal("park was not stored")
	}
	h.noteSpawnWake(spawnParkBranchInUse)
	h.spawnWakeMu.Lock()
	pending := h.spawnWakes[spawnParkBranchInUse]
	h.spawnWakeMu.Unlock()
	if pending || h.spawnWakeSnapshot()[spawnParkBranchInUse] != 0 {
		t.Fatalf("an event woke branch_in_use (pending=%v); it is timer-only", pending)
	}
	server := httptest.NewServer(h.Handler())
	defer server.Close()
	code, _, body := t14HTTP(t, http.MethodGet, server.URL+"/v1/spawns/"+task.Request.RequestID, t14OperatorToken, nil)
	var answer struct{ Status, Reason string }
	if code != http.StatusOK || json.Unmarshal(body, &answer) != nil || answer.Status != "parked" || answer.Reason != spawnParkBranchInUse {
		t.Fatalf("status read %d %s, want parked %s", code, body, spawnParkBranchInUse)
	}
}

// The parked notice names the worktree the latest branch refusal named, and
// only for a branch_in_use park. RED on a0df3182 by a notice without the
// holder (spawnParkHolder returning "").
func TestParkNoticeNamesTheBranchHolder_t6702(t *testing.T) {
	task := hubSpawnTask{
		Job:        "park-1",
		ParkReason: spawnParkBranchInUse,
		Exclusions: []hubSpawnExclusion{
			{Machine: "mac-a", Reason: "checkout_refused:branch_in_use: worktree=/work/repo.parent"},
			{Machine: "mac-b", Reason: "checkout_refused:branch_in_use: worktree=/work/repo.other"},
		},
	}
	if text := spawnParkNoticeText(task); !strings.HasSuffix(text, " holder=/work/repo.other") || !strings.Contains(text, "reason=branch_in_use") {
		t.Fatalf("notice = %q, want the latest holder named", text)
	}
	task.ParkReason = spawnParkQuota
	if text := spawnParkNoticeText(task); strings.Contains(text, "holder=") {
		t.Fatalf("a quota park names a holder: %q", text)
	}
}
