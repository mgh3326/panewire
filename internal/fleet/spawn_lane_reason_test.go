package fleet

import (
	"context"
	"strings"
	"testing"
)

// A failed lane registration keeps its reason: the node discards the spawn's
// stderr, so the receipt and the LANE_UNREGISTERED notice are the only record.
func TestLaneRegistrationFailureRecordsReason(t *testing.T) {
	fail := Result{Code: 4, Err: "lanes add: hub answered 403 lane_not_owned token=private-value"}
	a, s, _, emitted, _ := laneRegisterFixture(t, fail)
	if err := a.Spawn(context.Background(), s); err != nil {
		t.Fatalf("landed spawn failed: %v", err)
	}
	reason, _ := spawnedReceipt(t, a, s.Job)["lane_error"].(string)
	if !strings.Contains(reason, "403 lane_not_owned") {
		t.Fatalf("receipt lane_error=%q", reason)
	}
	notices := 0
	for _, c := range *emitted {
		text := flagValue(c.Args, "--text")
		if !strings.Contains(text, "LANE_UNREGISTERED") {
			continue
		}
		notices++
		if !strings.Contains(text, "reason="+reason) {
			t.Fatalf("notice without the reason: %s", text)
		}
	}
	if notices != 1 {
		t.Fatalf("LANE_UNREGISTERED notices=%d", notices)
	}
	if strings.Contains(reason, "private-value") {
		t.Fatalf("reason leaked a credential: %q", reason)
	}
	for _, c := range *emitted {
		if strings.Contains(strings.Join(c.Args, " "), "private-value") {
			t.Fatal("notice leaked a credential")
		}
	}
}

func TestLaneRegistrationSuccessRecordsNoReason(t *testing.T) {
	a, s, _, emitted, _ := laneRegisterFixture(t, Result{})
	if err := a.Spawn(context.Background(), s); err != nil {
		t.Fatal(err)
	}
	if _, ok := spawnedReceipt(t, a, s.Job)["lane_error"]; ok {
		t.Fatal("lane_error recorded on success")
	}
	for _, c := range *emitted {
		if strings.Contains(flagValue(c.Args, "--text"), "LANE_UNREGISTERED") {
			t.Fatal("notice on success")
		}
	}
}

func TestLaneFailureReasonIsBoundedAndSingleLine(t *testing.T) {
	long := strings.Repeat("é", 400) + "\nsecond line"
	got := laneFailureReason(laneTestError(long))
	if strings.Contains(got, "\n") || len(got) > 240+len("…") || !strings.HasPrefix(got, "é") {
		t.Fatalf("reason=%q", got)
	}
}

type laneTestError string

func (e laneTestError) Error() string { return string(e) }

func TestLaneRegistrationParentMismatchReason(t *testing.T) {
	reason := "lanes rejected by hub: lane_machine_mismatch (check that parent lane p is registered on the hub)"
	a, s, _, emitted, _ := laneRegisterFixture(t, Result{Code: 4, Err: reason})
	s.Owner = "p"
	if err := a.Spawn(context.Background(), s); err != nil {
		t.Fatal(err)
	}
	got, _ := spawnedReceipt(t, a, s.Job)["lane_error"].(string)
	if !strings.Contains(got, "check that parent lane p is registered") {
		t.Fatalf("lane_error=%q", got)
	}
	for _, c := range *emitted {
		if strings.Contains(flagValue(c.Args, "--text"), "LANE_UNREGISTERED") && strings.Contains(flagValue(c.Args, "--text"), got) {
			return
		}
	}
	t.Fatal("notice did not include lane_error")
}
