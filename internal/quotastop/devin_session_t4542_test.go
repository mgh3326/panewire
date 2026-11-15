package quotastop

import (
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"
)

// hk 4542: a devin pane is classified as free-limit-stopped from the devin
// CLI's own session record, not from the screen text.

func readFixture(t *testing.T, name string) []byte {
	t.Helper()
	body, err := os.ReadFile(filepath.Join("testdata", name))
	if err != nil {
		t.Fatal(err)
	}
	return body
}

func TestDevinSessionLogFreeLimitStopT4542(t *testing.T) {
	body := readFixture(t, "devin_session_free_limit.log")
	got := parseDevinSessionLog(body, "rich-noodle", false, time.UTC)
	if got.State != DevinSessionFreeLimit {
		t.Fatalf("evidence = %+v, want the free-limit stop", got)
	}
	wantAt := time.Date(2026, 10, 20, 9, 59, 12, 948962000, time.UTC)
	if !got.At.Equal(wantAt) {
		t.Fatalf("At = %v, want %v", got.At, wantAt)
	}
	if want := wantAt.Add(47 * time.Second); !got.Reset.Equal(want) {
		t.Fatalf("Reset = %v, want %v (47 seconds after the failed turn)", got.Reset, want)
	}
}

// A prompt after the failing turn means the session is running again: the
// pane is not stopped however the screen reads.
func TestDevinSessionLogResumedT4542(t *testing.T) {
	body := readFixture(t, "devin_session_resumed.log")
	got := parseDevinSessionLog(body, "rich-noodle", false, time.UTC)
	if got.State != DevinSessionRunning {
		t.Fatalf("evidence = %+v, want a running session", got)
	}
}

// A log that does not name the session is not that session's record, however
// its name lines up with the lock pid (a recycled pid).
func TestDevinSessionLogForeignSessionT4542(t *testing.T) {
	body := readFixture(t, "devin_session_free_limit.log")
	if got := parseDevinSessionLog(body, "other-session", false, time.UTC); got.State != DevinSessionUnknown {
		t.Fatalf("evidence = %+v, want no evidence for a foreign session", got)
	}
}

// A truncated window that holds no turn outcome is not evidence either way.
func TestDevinSessionLogTruncatedWithoutOutcomeT4542(t *testing.T) {
	body := []byte("2026-10-20T09:59:14.714658Z  INFO run_acp_server: session_db_job: Saved 1 message nodes for session rich-noodle\n")
	if got := parseDevinSessionLog(body, "rich-noodle", true, time.UTC); got.State != DevinSessionUnknown {
		t.Fatalf("evidence = %+v, want unknown for a truncated window", got)
	}
}

func writeDevinSessionLog(t *testing.T, cli, session string, pid int, start time.Time, body []byte, lockAt time.Time) {
	t.Helper()
	logs := filepath.Join(cli, "logs")
	locks := filepath.Join(cli, "session_locks")
	for _, dir := range []string{logs, locks} {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatal(err)
		}
	}
	name := "devin_" + start.Local().Format("20060102-150405") + "_" + strconv.Itoa(pid) + ".log"
	if err := os.WriteFile(filepath.Join(logs, name), body, 0o600); err != nil {
		t.Fatal(err)
	}
	lock := filepath.Join(locks, session+".lock")
	if err := os.WriteFile(lock, []byte(strconv.Itoa(pid)+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Chtimes(lock, lockAt, lockAt); err != nil {
		t.Fatal(err)
	}
}

func TestReadDevinSessionEvidenceT4542(t *testing.T) {
	cli := t.TempDir()
	start := time.Date(2026, 10, 20, 9, 59, 0, 0, time.UTC)
	writeDevinSessionLog(t, cli, "rich-noodle", 4242, start, readFixture(t, "devin_session_free_limit.log"), start.Add(time.Second))
	got := ReadDevinSessionEvidence(cli, "rich-noodle", time.UTC)
	if got.State != DevinSessionFreeLimit || got.Reset.IsZero() {
		t.Fatalf("evidence = %+v, want the free-limit stop", got)
	}
}

// The lock's time need not name the log exactly: the pid lookup still finds
// the session's log, which is the path a second-boundary miss takes.
func TestReadDevinSessionEvidenceByPIDT4542(t *testing.T) {
	cli := t.TempDir()
	start := time.Date(2026, 10, 20, 9, 59, 0, 0, time.UTC)
	writeDevinSessionLog(t, cli, "rich-noodle", 4343, start, readFixture(t, "devin_session_free_limit.log"), start.Add(3*time.Hour))
	got := ReadDevinSessionEvidence(cli, "rich-noodle", time.UTC)
	if got.State != DevinSessionFreeLimit {
		t.Fatalf("evidence = %+v, want the free-limit stop", got)
	}
}

func TestReadDevinSessionEvidenceMissingLockT4542(t *testing.T) {
	if got := ReadDevinSessionEvidence(t.TempDir(), "rich-noodle", time.UTC); got.State != DevinSessionUnknown {
		t.Fatalf("evidence = %+v, want no evidence without a session lock", got)
	}
}

// A screen with no limit text at all: only the session record can report the
// stop. A detector that ignores the record returns nothing here.
func TestDetectStructuredStopWithoutScreenTextT4542(t *testing.T) {
	now := time.Date(2026, 10, 20, 10, 0, 0, 0, time.UTC)
	evidence := DevinSessionEvidence{State: DevinSessionFreeLimit, At: now.Add(-time.Minute), Reset: now.Add(30 * time.Minute)}
	screen := "⏺ Done. The review is written.\n\n❭\n────\n"
	stop, basis, ok := Detect(screen, "devin", "", evidence, now, time.UTC)
	if !ok || basis != BasisStructured || stop.Pool != "devin-free" || !stop.Reset.Equal(evidence.Reset) {
		t.Fatalf("Detect = (%+v, %q, %v), want a structured devin-free stop", stop, basis, ok)
	}
}

// The hk 4507 hazard: a banner quoted inside a multi-row injected directive
// reads as a stop from the screen alone. The session record says the turn
// ended otherwise, so no stop.
func TestDetectStructuredRunningVetoesScreenStopT4542(t *testing.T) {
	screen := string(readFixture(t, "devin_quoted_banner_in_directive_t4542.txt"))
	now := time.Date(2026, 11, 16, 12, 0, 0, 0, time.UTC)
	if _, ok := ClassifyAgent(screen, "devin", now, time.UTC); !ok {
		t.Fatal("fixture no longer reads as a stop from the screen alone; the veto case would prove nothing")
	}
	evidence := DevinSessionEvidence{State: DevinSessionRunning}
	stop, basis, ok := Detect(screen, "devin", "", evidence, now, time.UTC)
	if ok || basis != BasisStructured {
		t.Fatalf("Detect = (%+v, %q, %v), want the session record to refuse the stop", stop, basis, ok)
	}
}

// No session record: the screen classifier decides, as it did before hk 4542.
func TestDetectNoEvidenceFallsBackToScreenT4542(t *testing.T) {
	screen := string(readFixture(t, "devin_quoted_banner_in_directive_t4542.txt"))
	now := time.Date(2026, 11, 16, 12, 0, 0, 0, time.UTC)
	stop, basis, ok := Detect(screen, "devin", "", DevinSessionEvidence{}, now, time.UTC)
	if !ok || basis != BasisClassifier || stop.Pool != "devin-free" {
		t.Fatalf("Detect = (%+v, %q, %v), want the classifier's stop", stop, basis, ok)
	}
}

// The record belongs to a devin pane: for another agent kind it is ignored and
// the screen classifier decides alone.
func TestDetectIgnoresEvidenceForOtherAgentsT4542(t *testing.T) {
	now := time.Date(2026, 10, 20, 10, 0, 0, 0, time.UTC)
	evidence := DevinSessionEvidence{State: DevinSessionFreeLimit, Reset: now.Add(30 * time.Minute)}
	screen := "⏺ Done.\n\n❭\n────\n"
	if _, basis, ok := Detect(screen, "claude", "", evidence, now, time.UTC); ok || basis != BasisClassifier {
		t.Fatalf("Detect = (%q, %v), want the classifier for a claude pane", basis, ok)
	}
}

// A free-limit record says nothing about a request-rate-limit banner: that is
// a real stop of the same pane, and the classifier must keep reading it.
func TestDetectRunningKeepsRequestLimitScreenStopT4542(t *testing.T) {
	screen := " ▲ Something went wrong\n   Reached request rate limit. Please switch to a different model or contact support. Your limit will\n   reset in 6 seconds (at 10:57 UTC). Send a message to retry\n\n ● ~/work/mwr-4552\n────\n❭ Ask Devin to build features, fix bugs, or work on your code\n────\nSWE-2 Flash High · bypass                          Use /bug to report a bug to the Devin CLI developers\n"
	now := time.Date(2026, 11, 16, 12, 0, 0, 0, time.UTC)
	stop, basis, ok := Detect(screen, "devin", "", DevinSessionEvidence{State: DevinSessionRunning}, now, time.UTC)
	if !ok || basis != BasisClassifier || stop.Pool != "devin" {
		t.Fatalf("Detect = (%+v, %q, %v), want the classifier's request-limit stop", stop, basis, ok)
	}
}

// A turn that failed for another reason is not free-limit evidence either
// way: the screen classifier still reads that pane's own banner.
func TestDevinSessionLogOtherFailureIsUnknownT4542(t *testing.T) {
	body := []byte(`2026-11-16T10:00:00.000000Z  INFO run_acp_server:session_db_call:session_db_job: chisel_agent::session_db: Saved 1 message nodes for session rich-noodle
2026-11-16T10:00:05.000000Z  WARN run_acp_server: agent_client_protocol::jsonrpc::outgoing_actor: Sending error response id=Str("x") method=session/prompt error=Error { code: -32010: Unknown error, message: "Reached request rate limit. Please switch to a different model or contact support. Your limit will reset in 6 seconds (at 10:57 UTC).", data: Some(Object {"cognition.ai/errorKind": String("unavailable")}) }
`)
	if got := parseDevinSessionLog(body, "rich-noodle", false, time.UTC); got.State != DevinSessionUnknown {
		t.Fatalf("evidence = %+v, want unknown for a non-free-limit failure", got)
	}
}

// The session id comes from herdr's output and names a file: only devin's own
// id alphabet may pass.
func TestDevinSessionIDRejectsPathsT4542(t *testing.T) {
	for _, id := range []string{"", ".", "..", "../x", "a/b", `a\b`, "a b", "a\nb"} {
		if validDevinSessionID(id) {
			t.Errorf("validDevinSessionID(%q) accepted an unsafe id", id)
		}
	}
	for _, id := range []string{"foul-poison", "rich_noodle", "a.b"} {
		if !validDevinSessionID(id) {
			t.Errorf("validDevinSessionID(%q) refused a devin id", id)
		}
	}
}
