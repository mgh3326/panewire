package panewire

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// R1: classifySubmission has exactly four possible outputs. This table pins
// an expectation for every one of them so the enumeration stays closed.
func TestClassifySubmissionAllFourValues(t *testing.T) {
	cases := []struct {
		name    string
		harness string
		screen  string
		marker  string
		want    string
	}{
		{
			name:    "paste chip anywhere is composer_residue regardless of harness",
			harness: "",
			screen:  "prompt\n[Pasted text #1]\n",
			marker:  "",
			want:    "composer_residue",
		},
		{
			name:    "claude echo between the last two dividers is composer_residue",
			harness: "claude",
			screen:  "───────\nhello world\n───────\n",
			marker:  "hello world",
			want:    "composer_residue",
		},
		{
			name:    "claude queued banner is queued",
			harness: "claude",
			screen:  "Press up to edit queued messages",
			marker:  "",
			want:    "queued",
		},
		{
			name:    "codex queued banner is queued",
			harness: "codex",
			screen:  "Press up to edit queued messages",
			marker:  "",
			want:    "queued",
		},
		{
			name:    "claude screen carrying the marker is marker_observed",
			harness: "claude",
			screen:  "prefix hello world suffix",
			marker:  "hello world",
			want:    "marker_observed",
		},
		{
			name:    "codex screen carrying the marker is marker_observed",
			harness: "codex",
			screen:  "hello world",
			marker:  "hello world",
			want:    "marker_observed",
		},
		{
			name:    "claude screen missing the marker is unproven",
			harness: "claude",
			screen:  "nothing relevant here",
			marker:  "hello world",
			want:    "unproven",
		},
		{
			name:    "marker present but harness is neither claude nor codex is unproven",
			harness: "some-other-harness",
			screen:  "hello world",
			marker:  "hello world",
			want:    "unproven",
		},
		{
			name:    "empty screen and marker is unproven",
			harness: "claude",
			screen:  "",
			marker:  "",
			want:    "unproven",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := classifySubmission(tc.harness, tc.screen, tc.marker); got != tc.want {
				t.Fatalf("classifySubmission(%q, %q, %q) = %q, want %q", tc.harness, tc.screen, tc.marker, got, tc.want)
			}
		})
	}
}

// unprovenFakeHerdr writes a herdr shim on PATH whose `agent read` output cycles
// through reads (one entry per call, the last entry repeats after that) and
// whose `agent send-keys` calls are recorded but otherwise inert. It never
// touches the workstation's real herdr binary.
func unprovenFakeHerdr(t *testing.T, log string, reads []string) string {
	t.Helper()
	dir := t.TempDir()
	binary := filepath.Join(dir, "herdr")
	count := filepath.Join(dir, "count")
	var caseLines strings.Builder
	for i, r := range reads {
		fmt.Fprintf(&caseLines, "      %d) printf '%%s' %q ;;\n", i+1, r)
	}
	if len(reads) > 0 {
		fmt.Fprintf(&caseLines, "      *) printf '%%s' %q ;;\n", reads[len(reads)-1])
	}
	script := "#!/bin/sh\n" +
		fmt.Sprintf("echo \"$2\" >> %q\n", log) +
		"case \"$2\" in\n" +
		"  read)\n" +
		fmt.Sprintf("    n=$(cat %q 2>/dev/null || echo 0)\n", count) +
		"    n=$((n+1))\n" +
		fmt.Sprintf("    echo $n > %q\n", count) +
		"    case $n in\n" +
		caseLines.String() +
		"    esac\n" +
		"    ;;\n" +
		"  send-keys) : ;;\n" +
		"esac\n"
	if err := os.WriteFile(binary, []byte(script), 0700); err != nil {
		t.Fatal(err)
	}
	return dir
}

func unprovenSetupHerdr(t *testing.T, reads []string) (log string, calls func() []string) {
	t.Helper()
	dir := t.TempDir()
	log = filepath.Join(dir, "herdr.log")
	binDir := unprovenFakeHerdr(t, log, reads)
	t.Setenv("PATH", binDir+string(os.PathListSeparator)+os.Getenv("PATH"))
	return log, func() []string {
		b, err := os.ReadFile(log)
		if err != nil {
			if os.IsNotExist(err) {
				return nil
			}
			t.Fatal(err)
		}
		return strings.Fields(string(b))
	}
}

// R1 defect ②: a directly unproven submission (no composer residue, no
// queued banner, no marker) must not be reported delivered.
func TestRelayInjectVerifySubmissionUnprovenDirectIsNotDelivered(t *testing.T) {
	_, calls := unprovenSetupHerdr(t, []string{"nothing useful on screen"})
	if relayInjectVerifySubmission(context.Background(), "test-pane", "claude", "one line") {
		t.Fatal("unproven submission (no return pressed) reported delivered")
	}
	if got := calls(); strings.Join(got, ",") != "read" {
		t.Fatalf("unexpected herdr calls: %q", got)
	}
}

// R1 defect ①: this is the previously untested path. composer_residue is
// seen first (the return-once contract fires), and the re-read after that
// return keypress classifies as unproven rather than marker_observed or
// composer_residue/queued. That must still not be reported delivered.
func TestRelayInjectVerifySubmissionComposerResidueThenUnprovenIsNotDelivered(t *testing.T) {
	_, calls := unprovenSetupHerdr(t, []string{"[Pasted text #1]", "still nothing useful"})
	if relayInjectVerifySubmission(context.Background(), "test-pane", "claude", "one line") {
		t.Fatal("unproven submission after one return keypress reported delivered")
	}
	if got := calls(); strings.Join(got, ",") != "read,send-keys,read" {
		t.Fatalf("unexpected herdr calls: %q", got)
	}
}

// Only a proven marker_observed classification reports delivered, on the
// direct path.
func TestRelayInjectVerifySubmissionMarkerObservedDirectIsDelivered(t *testing.T) {
	_, calls := unprovenSetupHerdr(t, []string{"prefix one line suffix"})
	if !relayInjectVerifySubmission(context.Background(), "test-pane", "claude", "one line") {
		t.Fatal("marker_observed submission (no return pressed) reported unconfirmed")
	}
	if got := calls(); strings.Join(got, ",") != "read" {
		t.Fatalf("unexpected herdr calls: %q", got)
	}
}

// Regression guard for the return-once contract: composer_residue on the
// first read, followed by a proven marker_observed re-read, must still
// report delivered after the fix.
func TestRelayInjectVerifySubmissionComposerResidueThenMarkerObservedIsDelivered(t *testing.T) {
	_, calls := unprovenSetupHerdr(t, []string{"[Pasted text #1]", "prefix one line suffix"})
	if !relayInjectVerifySubmission(context.Background(), "test-pane", "claude", "one line") {
		t.Fatal("marker_observed submission after one return keypress reported unconfirmed")
	}
	if got := calls(); strings.Join(got, ",") != "read,send-keys,read" {
		t.Fatalf("unexpected herdr calls: %q", got)
	}
}

// The composer_residue/queued arm's own behavior (press return exactly once,
// then treat a still-residue/queued re-read as unconfirmed) must be
// unchanged by the R1 fix.
func TestRelayInjectVerifySubmissionComposerResidueArmUnchanged(t *testing.T) {
	_, calls := unprovenSetupHerdr(t, []string{"[Pasted text #1]", "[Pasted text #1]"})
	if relayInjectVerifySubmission(context.Background(), "test-pane", "claude", "one line") {
		t.Fatal("still-residue submission reported delivered")
	}
	if got := calls(); strings.Join(got, ",") != "read,send-keys,read" {
		t.Fatalf("unexpected herdr calls: %q", got)
	}
}

// Same as above for the queued arm on a codex harness.
func TestRelayInjectVerifySubmissionQueuedArmUnchanged(t *testing.T) {
	_, calls := unprovenSetupHerdr(t, []string{"Press up to edit queued messages", "Press up to edit queued messages"})
	if relayInjectVerifySubmission(context.Background(), "test-pane", "codex", "one line") {
		t.Fatal("still-queued submission reported delivered")
	}
	if got := calls(); strings.Join(got, ",") != "read,send-keys,read" {
		t.Fatalf("unexpected herdr calls: %q", got)
	}
}
