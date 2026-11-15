package panewire

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
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

// A pane label is not a job id: the hub label grammar admits ':', and pane
// close/capture/read must keep accepting such a label (hk 4824 review).
func TestNodePaneNameKeepsLabelPunctuation(t *testing.T) {
	if !validNodePaneName("lane:worker") || validNodeJobName("lane:worker") {
		t.Fatal("a pane label with ':' must be a pane name, not a job id")
	}
	for _, tc := range []struct {
		op   string
		args map[string]any
	}{
		{nodeCommandPaneClose, map[string]any{"pane_id": "w1:p1", "name": "lane:worker", "agent_status": "idle", "cwd": "/work/a"}},
		{nodeCommandPaneCapture, map[string]any{"pane_id": "w1:p1", "name": "lane:worker", "cwd": "/work/a", "merged_after": 0, "write": false}},
		{nodeCommandPaneRead, map[string]any{"pane_id": "w1:p1", "name": "lane:worker", "cwd": "/work/a", "tail_lines": 5}},
	} {
		raw, _ := json.Marshal(tc.args)
		if _, ok := decodeNodeCommandArgs(tc.op, raw); !ok {
			t.Fatalf("%s refused a pane label with ':': %s", tc.op, raw)
		}
	}
	args := closeOpArgs("op-1")
	args["name"] = "lane:worker"
	raw, _ := json.Marshal(args)
	if _, ok := decodeNodeCommandArgs(nodeCommandPaneCloseOp, raw); !ok {
		t.Fatal("pane.close_op refused a pane label with ':'")
	}
}

// On a case-insensitive volume the wrong spelling resolves to the existing job
// dir; the legacy CLI refuses it instead of appending one job's record to
// another's events (hk 4824 review).
func TestJobCLIRefusesCaseVariantOfExistingJobDir(t *testing.T) {
	root := t.TempDir()
	jobs := filepath.Join(root, "jobs")
	events := filepath.Join(jobs, "build", "events")
	if err := os.MkdirAll(events, 0o755); err != nil {
		t.Fatal(err)
	}
	for name, body := range map[string]string{
		"00001-job.claim.json":   `{"kind":"job.claim","payload":{"agent_label":"build-label","owner_lane":"owner-lane","parent_lane":"parent-lane","pane_id":"w3:p7","role":"builder"}}`,
		"00002-job.spawned.json": `{"kind":"job.spawned","payload":{"label":"build","owner_lane":"owner-lane","pane_id":"w3:p7"}}`,
	} {
		if err := os.WriteFile(filepath.Join(events, name), []byte(body), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	report := filepath.Join(root, "report.md")
	if err := os.WriteFile(report, []byte("done\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("ARBITER_INBOX_ROOT", jobs)
	t.Setenv("HOSTNAME", "fixture-host")
	t.Setenv("HANDOFFKEEP_BIN", filepath.Join(root, "absent"))
	t.Setenv("HANDOFFKEEP_URL", "")
	t.Setenv("HANDOFFKEEP_TOKEN_SVC_PANEWIRE", "")
	t.Setenv("PANEWIRE_HK_ENV", filepath.Join(root, "absent-hk.env"))
	t.Setenv("PANEWIRE_SOCKET", filepath.Join(root, "absent.sock"))

	before, _ := os.ReadDir(events)
	for _, args := range [][]string{
		{"done", "Build", "--report", report},
		{"escalate", "Build", "--question", "q"},
		{"joined", "Build", "--pr", "https://example/pull/1", "--head", "abc1234", "--report", report},
		{"close", "Build", "--lane", "lane-a", "--outcome", "completed", "--reason", "done"},
	} {
		var stdout, stderr bytes.Buffer
		if rc := runJobCLI(args, &stdout, &stderr, CLIConfig{}); rc != ExitUsage {
			t.Fatalf("%v rc=%d want %d stderr=%q", args, rc, ExitUsage, stderr.String())
		}
		for _, want := range []string{"Build", "build"} {
			if !strings.Contains(stderr.String(), want) {
				t.Fatalf("%v refusal %q does not name %q", args, stderr.String(), want)
			}
		}
	}
	after, _ := os.ReadDir(events)
	if len(after) != len(before) {
		t.Fatalf("a refused spelling appended to build/events: before=%d after=%d", len(before), len(after))
	}
	// The exact spelling still completes.
	var stdout, stderr bytes.Buffer
	if rc := runJobCLI([]string{"done", "build", "--report", report}, &stdout, &stderr, CLIConfig{}); rc != ExitOK {
		t.Fatalf("exact spelling refused: rc=%d stderr=%q", rc, stderr.String())
	}
	if !strings.Contains(stdout.String(), "OK job=build") {
		t.Fatalf("stdout=%q", stdout.String())
	}

	// No local events dir means a remote close writes nothing here, so a
	// case-variant entry must not refuse it.
	remote := t.TempDir()
	if err := os.MkdirAll(filepath.Join(remote, "build", "events-elsewhere"), 0o700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("ARBITER_INBOX_ROOT", remote)
	stdout.Reset()
	stderr.Reset()
	rc := runJobCLI([]string{"close", "Build", "--lane", "lane-a", "--outcome", "completed", "--reason", "done"}, &stdout, &stderr, CLIConfig{SocketPath: filepath.Join(root, "absent.sock")})
	if strings.Contains(stderr.String(), "differs only in letter case") {
		t.Fatalf("a remote close was refused by the local case guard: %s", stderr.String())
	}
	if rc == ExitOK {
		t.Fatalf("a remote close without a daemon reported success: %s", stdout.String())
	}
}

func caseInsensitiveVolume(t *testing.T, dir string) bool {
	t.Helper()
	probe := filepath.Join(dir, "CaseProbe")
	if err := os.MkdirAll(probe, 0o700); err != nil {
		t.Fatal(err)
	}
	defer os.Remove(probe)
	_, err := os.Stat(filepath.Join(dir, "caseprobe"))
	return err == nil
}

// The legacy guard only considers directories, and an exact directory settles
// the name: a file must not block its case variant, and on a case-sensitive
// volume coexisting spellings keep their own directories (hk 4824 review).
func TestJobDirCaseVariantRules(t *testing.T) {
	fileRoot := t.TempDir()
	if err := os.WriteFile(filepath.Join(fileRoot, "Build"), []byte("not a dir"), 0o600); err != nil {
		t.Fatal(err)
	}
	if got := jobDirCaseVariant(fileRoot, "Build"); got != "" {
		t.Fatalf("a file named Build counted as a variant: %q", got)
	}

	root := t.TempDir()
	if err := os.MkdirAll(filepath.Join(root, "build"), 0o700); err != nil {
		t.Fatal(err)
	}
	if got := jobDirCaseVariant(root, "Build"); got != "build" {
		t.Fatalf("variant=%q, want build", got)
	}
	if got := jobDirCaseVariant(root, "build"); got != "" {
		t.Fatalf("an exact directory did not settle the name: variant %q", got)
	}
	if caseInsensitiveVolume(t, root) {
		return
	}
	if err := os.MkdirAll(filepath.Join(root, "Build"), 0o700); err != nil {
		t.Fatal(err)
	}
	if got := jobDirCaseVariant(root, "build"); got != "" {
		t.Fatalf("coexisting exact directory was overridden by its variant: %q", got)
	}
}
