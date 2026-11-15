package panewire

import (
	"bytes"
	"testing"
)

// hk 4824: a job id names a directory, and the node/checkout/legacy paths
// accept only the portable ASCII grammar. Non-ASCII is refused rather than
// normalised: macOS maps an NFC and an NFD spelling of one name to a single
// inode, so the two spellings would share one directory just as two letter
// cases do.
func TestNodeJobNameIsPortableASCII(t *testing.T) {
	for _, name := range []string{"job-a", "Review-A", "review-a", "J1", "a.b_c-d", "x"} {
		if !validNodeJobName(name) {
			t.Fatalf("portable name %q refused", name)
		}
	}
	nfc := "caf\u00e9-review"
	nfd := "cafe\u0301-review"
	for _, name := range []string{nfc, nfd, "job a", "jobs/a", "jobs\\a", "..x", "_job", "-job", "", "job..a"} {
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
