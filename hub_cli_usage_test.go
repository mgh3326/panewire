package panewire

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestLanesUsageErrorsExplainSyntax(t *testing.T) {
	cases := []struct {
		name string
		args []string
		want string
	}{
		{"missing command", nil, "command is required"},
		{"unknown command", []string{"get"}, "unknown lanes command"},
		{"list suggestion", []string{"list"}, "did you mean ls?"},
		{"remove suggestion", []string{"remove"}, "did you mean rm?"},
		{"unknown flag", []string{"ls", "--token-env", "x"}, "unknown lanes flag"},
		{"grouped flags", []string{"ls", "--hub-url https://hub.invalid --hub-token-env /tmp/token"}, "unknown lanes flag"},
		{"duplicate flag", []string{"ls", "--hub-url", "https://hub.invalid", "--hub-url", "https://hub.invalid"}, "duplicate lanes flag"},
		{"missing value at end", []string{"ls", "--hub-url"}, "flag value is required"},
		{"missing value before flag", []string{"ls", "--hub-url", "--hub-token-env", "/tmp/token"}, "flag value is required"},
		{"missing credentials", []string{"ls"}, "--hub-url and --hub-token-env are required"},
		{"missing lane", []string{"rm", "--hub-url", "https://hub.invalid", "--hub-token-env", "/tmp/token"}, "lane is required"},
		{"missing route", []string{"add", "lane-a", "--hub-url", "https://hub.invalid", "--hub-token-env", "/tmp/token"}, "route flags are required"},
		{"invalid bool", []string{"add", "lane-a", "--sink=maybe"}, "invalid sink value"},
		{"wrong command flag", []string{"ls", "--lane", "lane-a"}, "invalid self-check flag"},
		{"extra positional", []string{"ls", "extra"}, "invalid lanes list flags"},
		{"invalid epoch", []string{"self-check", "--expect-epoch", "wrong"}, "invalid expected epoch"},
		{"missing self-check flags", []string{"self-check"}, "self-check flags are required"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var stdout, stderr bytes.Buffer
			if rc := runLanesCLI(tc.args, &stdout, &stderr, hubCLIDeps{}); rc != ExitUsage {
				t.Fatalf("rc=%d, want %d", rc, ExitUsage)
			}
			if stdout.Len() != 0 || !strings.Contains(stderr.String(), tc.want) || !strings.Contains(stderr.String(), "panewire lanes ls") || !strings.Contains(stderr.String(), "panewire lanes rm") {
				t.Fatalf("stdout=%q stderr=%q", stdout.String(), stderr.String())
			}
			if strings.Contains(stderr.String(), "fixture-secret-token") {
				t.Fatal("credential leaked in diagnostic")
			}
		})
	}
}

func TestLanesHelpShowsCommandSyntax(t *testing.T) {
	for _, flag := range []string{"-h", "--help"} {
		var stdout, stderr bytes.Buffer
		if rc := runLanesCLI([]string{flag}, &stdout, &stderr, hubCLIDeps{}); rc != ExitOK || stderr.Len() != 0 || !strings.Contains(stdout.String(), "panewire lanes self-check") || !strings.Contains(stdout.String(), "--hub-token-env FILE") {
			t.Fatalf("flag=%q rc=%d stdout=%q stderr=%q", flag, rc, stdout.String(), stderr.String())
		}
	}
}

func TestLanesInvalidTokenFileDoesNotExposeContents(t *testing.T) {
	path := filepath.Join(t.TempDir(), "operator.env")
	const secret = "fixture-secret-token"
	if err := os.WriteFile(path, []byte("HUB_MACHINE_ID=invalid machine\nHUB_TOKEN="+secret+"\n"), 0600); err != nil {
		t.Fatal(err)
	}
	var stdout, stderr bytes.Buffer
	if rc := runLanesCLI([]string{"ls", "--hub-url", "https://hub.invalid", "--hub-token-env", path}, &stdout, &stderr, hubCLIDeps{}); rc != ExitConditionInvalid {
		t.Fatalf("rc=%d stdout=%q stderr=%q", rc, stdout.String(), stderr.String())
	}
	if stdout.Len() != 0 || !strings.Contains(stderr.String(), "invalid operator token env") || strings.Contains(stderr.String(), secret) {
		t.Fatalf("stdout=%q stderr=%q", stdout.String(), stderr.String())
	}
}

func TestOtherHubCLIUsageErrorsExplainSyntax(t *testing.T) {
	type runner func([]string, *bytes.Buffer, *bytes.Buffer, hubCLIDeps) int
	cases := []struct {
		name  string
		run   runner
		args  []string
		usage string
	}{
		{"jobs missing command", func(a []string, o, e *bytes.Buffer, d hubCLIDeps) int { return runJobsCLI(a, o, e, d) }, nil, "panewire jobs jobs"},
		{"jobs unknown flag", func(a []string, o, e *bytes.Buffer, d hubCLIDeps) int { return runJobsCLI(a, o, e, d) }, []string{"jobs", "--wrong"}, "panewire jobs jobs"},
		{"jobs required flags", func(a []string, o, e *bytes.Buffer, d hubCLIDeps) int { return runJobsCLI(a, o, e, d) }, []string{"jobs"}, "panewire jobs jobs"},
		{"sessions missing command", func(a []string, o, e *bytes.Buffer, d hubCLIDeps) int { return runSessionsCLI(a, o, e, d) }, nil, "panewire sessions find"},
		{"sessions missing label", func(a []string, o, e *bytes.Buffer, d hubCLIDeps) int { return runSessionsCLI(a, o, e, d) }, []string{"find"}, "panewire sessions find"},
		{"sessions invalid machine", func(a []string, o, e *bytes.Buffer, d hubCLIDeps) int { return runSessionsCLI(a, o, e, d) }, []string{"find", "label", "--hub-url", "https://hub.invalid", "--hub-token-env", "/tmp/token", "--machine", "BAD!"}, "panewire sessions find"},
		{"fleet unknown flag", func(a []string, o, e *bytes.Buffer, d hubCLIDeps) int { return runFleetCensusCLI(a, o, e, d) }, []string{"--wrong"}, "panewire fleet-census"},
		{"fleet missing value", func(a []string, o, e *bytes.Buffer, d hubCLIDeps) int { return runFleetCensusCLI(a, o, e, d) }, []string{"--grace"}, "panewire fleet-census"},
		{"reap duplicate flag", func(a []string, o, e *bytes.Buffer, d hubCLIDeps) int { return runSessionReapCLI(a, o, e, d) }, []string{"--json", "--json"}, "panewire session-reap"},
		{"reap missing value", func(a []string, o, e *bytes.Buffer, d hubCLIDeps) int { return runSessionReapCLI(a, o, e, d) }, []string{"--grace", "--json"}, "panewire session-reap"},
		{"audit missing credentials", func(a []string, o, e *bytes.Buffer, d hubCLIDeps) int { return runLanesAuditCLI(a, o, e, d) }, nil, "panewire lanes-audit"},
		{"audit unknown flag", func(a []string, o, e *bytes.Buffer, d hubCLIDeps) int { return runLanesAuditCLI(a, o, e, d) }, []string{"--wrong"}, "panewire lanes-audit"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var stdout, stderr bytes.Buffer
			if rc := tc.run(tc.args, &stdout, &stderr, hubCLIDeps{}); rc != ExitUsage {
				t.Fatalf("rc=%d, want %d", rc, ExitUsage)
			}
			lines := strings.Split(strings.TrimSuffix(stderr.String(), "\n"), "\n")
			if stdout.Len() != 0 || len(lines) < 2 || !strings.Contains(stderr.String(), tc.usage) || !strings.Contains(lines[0], ":") {
				t.Fatalf("stdout=%q stderr=%q", stdout.String(), stderr.String())
			}
		})
	}
}
