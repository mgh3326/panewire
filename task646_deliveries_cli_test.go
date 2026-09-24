package panewire

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// #646 AC4: panewire deliveries reads the delivery audit after the fact,
// read-only, by id prefix or as a newest-first list.
func TestTask646DeliveriesShowAndList(t *testing.T) {
	path := filepath.Join(t.TempDir(), "panewire.sqlite3")
	store, err := OpenStore(path)
	if err != nil {
		t.Fatal(err)
	}
	for i, d := range []Delivery{
		{DeliveryID: "aaaa1111", Sender: "b623", TargetInput: "t623-verify", ResolvedPaneID: "w1:p5", RequestedAtMS: 1000, CompletedAtMS: 8557, PreflightResult: "passed", HerdrAcceptance: "accepted", SubmissionResult: "marker_observed", SubmissionEvidence: "recent_unwrapped:marker_echo", UptakeMode: "status-transition", UptakeResult: "confirmed"},
		{DeliveryID: "aaaa2222", Sender: "wrk-spawn:x", TargetInput: "other", ResolvedPaneID: "w1:p6", RequestedAtMS: 2000, CompletedAtMS: 2500, PreflightResult: "passed", SubmissionResult: "unproven", ErrorCode: "delivery_failure"},
	} {
		if ok, err := store.InsertDelivery(t.Context(), d, ""); err != nil || !ok {
			t.Fatalf("insert %d: ok=%v err=%v", i, ok, err)
		}
	}
	_ = store.Close()
	before, _ := os.ReadFile(path)

	run := func(args ...string) (int, string, string) {
		var out, errOut bytes.Buffer
		code := runDeliveriesCLI(append(args, "--db", path), &out, &errOut)
		return code, out.String(), errOut.String()
	}

	code, out, _ := run("show", "aaaa1")
	var view map[string]any
	if code != ExitOK || json.Unmarshal([]byte(out), &view) != nil {
		t.Fatalf("show exit=%d out=%q", code, out)
	}
	if view["delivery_id"] != "aaaa1111" || view["submission_evidence"] != "recent_unwrapped:marker_echo" || view["uptake_result"] != "confirmed" || view["elapsed_ms"] != float64(7557) {
		t.Fatalf("show view %v", view)
	}
	if code, _, errOut := run("show", "aaaa"); code != ExitConditionInvalid || !strings.Contains(errOut, "2 matches") {
		t.Fatalf("ambiguous prefix exit=%d err=%q", code, errOut)
	}
	if code, _, _ := run("show", "zz"); code != ExitConditionInvalid {
		t.Fatalf("missing id exit=%d", code)
	}

	code, out, _ = run("list", "--to", "t623-verify")
	lines := strings.Split(strings.TrimSpace(out), "\n")
	if code != ExitOK || len(lines) != 2 || !strings.HasPrefix(lines[1], "aaaa1111\t") || !strings.Contains(lines[1], "status-transition:confirmed") {
		t.Fatalf("list --to exit=%d out=%q", code, out)
	}
	code, out, _ = run("list")
	lines = strings.Split(strings.TrimSpace(out), "\n")
	if code != ExitOK || len(lines) != 3 || !strings.HasPrefix(lines[1], "aaaa2222\t") {
		t.Fatalf("list newest first exit=%d out=%q", code, out)
	}

	if after, _ := os.ReadFile(path); !bytes.Equal(before, after) {
		t.Fatal("deliveries changed the database file")
	}
	missing := filepath.Join(t.TempDir(), "missing.sqlite3")
	if code := runDeliveriesCLI([]string{"show", "x", "--db", missing}, &bytes.Buffer{}, &bytes.Buffer{}); code != ExitDaemonUnavailable {
		t.Fatalf("missing db exit=%d", code)
	}
	if _, err := os.Stat(missing); err == nil {
		t.Fatal("a missing database must not be created")
	}
}
