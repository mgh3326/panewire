package panewire

import (
	"strings"
	"testing"
)

func TestParkNoticeKeepsLongHolderPath_t6715(t *testing.T) {
	path := "/Users/robin/work/mwr-914-8362387p1t3los9j-checkout"
	task := hubSpawnTask{
		Job:        "park-1",
		ParkReason: spawnParkBranchInUse,
		Exclusions: []hubSpawnExclusion{{Reason: "checkout_refused:branch_in_use: worktree=" + path}},
	}
	if got := spawnParkNoticeText(task); !strings.HasSuffix(got, " holder="+path) {
		t.Fatalf("notice = %q, want full holder path %q", got, path)
	}
}

func TestParkNoticeRedactsLongHolderPathSegment_t6715(t *testing.T) {
	secret := "tokensegment0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMN"
	path := "/work/" + secret + "/checkout"
	task := hubSpawnTask{
		Job:        "park-1",
		ParkReason: spawnParkBranchInUse,
		Exclusions: []hubSpawnExclusion{{Reason: "checkout_refused:branch_in_use: worktree=" + path}},
	}
	got := spawnParkNoticeText(task)
	if !strings.HasSuffix(got, " holder=/work/[redacted]/checkout") {
		t.Fatalf("notice = %q, want token segment redacted", got)
	}
}
