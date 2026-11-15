package panewire

import (
	"bytes"
	"strings"
	"testing"

	"github.com/mgh3326/panewire/internal/fleet"
)

// hk 4824: a job id names a directory, and the node/checkout/legacy paths
// accept only the portable ASCII grammar. Non-ASCII is refused rather than
// normalised: macOS maps an NFC and an NFD spelling of one name to a single
// inode, so the two spellings would share one directory just as two letter
// cases do.
func TestNodeJobNameIsPortableASCII(t *testing.T) {
	for _, name := range []string{"job-a", "Review-A", "review-a", "J1", "a.b_c-d", "x", "build..42", "a."} {
		if !validNodeJobName(name) {
			t.Fatalf("portable name %q refused", name)
		}
	}
	nfc := "caf\u00e9-review"
	nfd := "cafe\u0301-review"
	for _, name := range []string{nfc, nfd, "job a", "jobs/a", "jobs\\a", "..x", "_job", "-job", ""} {
		if validNodeJobName(name) {
			t.Fatalf("non-portable name %q accepted", name)
		}
	}
}

// The legacy completion CLI names the jobs-inbox directory from its job
// argument, so it refuses a spelling no launcher can create.
func TestJobCLIRefusesNonPortableJobIDs(t *testing.T) {
	for _, id := range []string{"caf\u00e9-review", "cafe\u0301-review"} {
		for _, args := range [][]string{
			{"done", id},
			{"escalate", id, "--question", "q"},
			{"joined", id, "--pr", "https://example/pull/1", "--head", "abc1234", "--report", "/tmp/report.md"},
		} {
			var stdout, stderr bytes.Buffer
			if rc := runJobCLI(args, &stdout, &stderr, CLIConfig{}); rc != ExitUsage {
				t.Fatalf("%v rc=%d want %d stderr=%q", args, rc, ExitUsage, stderr.String())
			}
		}
	}
}

// An id the spawn gate accepts is an id the legacy CLI accepts, including one
// with ".." inside its single component; and the frozen golden's id outside
// the relay pattern ("jw+odd") still completes.
func TestJobCLIAcceptsSpawnIDsAndLegacyIDs(t *testing.T) {
	for _, id := range []string{"build..42", "a.b"} {
		if !fleet.ValidJobID(id) || !validNodeJobName(id) || !validLegacyJobID(id) {
			t.Fatalf("%q: spawn=%t node=%t legacy=%t", id, fleet.ValidJobID(id), validNodeJobName(id), validLegacyJobID(id))
		}
	}
	if fleet.ValidJobID("jw+odd") || validNodeJobName("jw+odd") || !validLegacyJobID("jw+odd") {
		t.Fatal("jw+odd must be legacy-only: the hub relay pattern refuses it, the completion CLI accepts it")
	}
	t.Setenv("ARBITER_INBOX_ROOT", t.TempDir())
	for _, id := range []string{"build..42", "a.b", "jw+odd"} {
		var stdout, stderr bytes.Buffer
		rc := runJobCLI([]string{"done", id}, &stdout, &stderr, CLIConfig{})
		if strings.Contains(stderr.String(), "invalid job id") {
			t.Fatalf("done %q refused at the id gate: %s", id, stderr.String())
		}
		if rc == ExitOK || !strings.Contains(stderr.String(), id) {
			t.Fatalf("done %q: rc=%d stderr=%q, want the later missing-metadata refusal naming it", id, rc, stderr.String())
		}
	}
	options, err := parseJobCloseArgs([]string{"build..42", "--lane", "lane-a", "--outcome", "completed", "--reason", "done"})
	if err != nil || options.job != "build..42" {
		t.Fatalf("close parser refused build..42: %+v %v", options, err)
	}
	if _, err := parseJobCloseArgs([]string{"caf\u00e9", "--lane", "lane-a", "--outcome", "completed", "--reason", "done"}); err == nil {
		t.Fatal("close accepted a non-ASCII id")
	}
}
