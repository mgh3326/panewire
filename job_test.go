package panewire

import (
	"bytes"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"unicode/utf8"
)

func TestJobProbeAnswersTheDelegationToken(t *testing.T) {
	var stdout, stderr bytes.Buffer
	if rc := runJobCLI([]string{"probe"}, &stdout, &stderr, CLIConfig{}); rc != ExitOK {
		t.Fatalf("probe rc=%d stderr=%q", rc, stderr.String())
	}
	// wrk delegates only on this exact line; anything else keeps its own path.
	if stdout.String() != "panewire-job/1\n" {
		t.Fatalf("probe stdout = %q", stdout.String())
	}
	stdout.Reset()
	if rc := runJobCLI([]string{"probe", "extra"}, &stdout, &stderr, CLIConfig{}); rc != ExitUsage || stdout.Len() != 0 {
		t.Fatalf("probe with an argument: rc=%d stdout=%q", rc, stdout.String())
	}
	if rc := RunCLI([]string{"job", "probe"}, CLIConfig{}); rc != ExitOK {
		t.Fatalf("RunCLI job probe rc=%d", rc)
	}
}

// C-3: relay text is normalized once, on the node. `panewire job` hands the
// escalation question to the record and the socket untouched; the 240-rune
// bound and newline removal appear only in the node's wire form.
func TestJobEscalateLeavesQuestionNormalizationToTheNode(t *testing.T) {
	root, err := os.MkdirTemp("/tmp", "pwjobq")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(root)
	jobs := filepath.Join(root, "jobs")
	events := filepath.Join(jobs, "jq", "events")
	if err := os.MkdirAll(events, 0o755); err != nil {
		t.Fatal(err)
	}
	claim := `{"job_id":"jq","kind":"job.claim","payload":{"agent_label":"jq","owner_lane":"lane-q","parent_lane":"parent-q","role":"builder","pane_id":"w1:p1"}}`
	if err := os.WriteFile(filepath.Join(events, "00001-job.claim.json"), []byte(claim), 0o644); err != nil {
		t.Fatal(err)
	}
	question := strings.Repeat("질문 🤔 이모지 ", 30) + "\n다음 줄"
	if utf8.RuneCountInString(question) <= 241 {
		t.Fatalf("fixture question must exceed the bound: %d runes", utf8.RuneCountInString(question))
	}
	t.Setenv("ARBITER_INBOX_ROOT", jobs)
	t.Setenv("HOSTNAME", "fixture-host")
	t.Setenv("HANDOFFKEEP_BIN", filepath.Join(root, "absent"))
	socket := filepath.Join(root, "s")
	t.Setenv("PANEWIRE_SOCKET", socket)
	server := startJobFakeSocket(t, socket, "ok")
	var stdout, stderr bytes.Buffer
	if rc := runJobCLI([]string{"escalate", "jq", "--question", question}, &stdout, &stderr, CLIConfig{}); rc != ExitOK {
		t.Fatalf("escalate rc=%d stderr=%q", rc, stderr.String())
	}
	lines := server.stop()
	if len(lines) != 1 {
		t.Fatalf("socket requests = %d, want 1", len(lines))
	}
	var request localRequest
	if err := json.Unmarshal([]byte(lines[0]), &request); err != nil {
		t.Fatal(err)
	}
	if request.Question != question {
		t.Fatalf("pushed question was altered before the node: %d runes, want %d", utf8.RuneCountInString(request.Question), utf8.RuneCountInString(question))
	}
	var record map[string]any
	contents, err := os.ReadFile(filepath.Join(events, "00002-job.escalate.json"))
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(contents, &record); err != nil {
		t.Fatal(err)
	}
	if record["question"] != question {
		t.Fatalf("recorded question was altered before the node")
	}
	scanned := scanHubRelayEvents(root)
	var found *hubScannedRelayEvent
	for i := range scanned {
		if scanned[i].JobID == "jq" && scanned[i].Kind == "job.escalate" {
			found = &scanned[i]
		}
	}
	if found == nil {
		t.Fatalf("node did not scan the escalation: %+v", scanned)
	}
	wire := relayEventWireForm(*found)
	want := string([]rune(strings.ReplaceAll(question, "\n", " "))[:240])
	if wire.Question != want {
		t.Fatalf("node wire question = %d runes %q, want the first 240 runes once", utf8.RuneCountInString(wire.Question), wire.Question)
	}
	if again := relayEventWireForm(wire); again.Question != wire.Question {
		t.Fatalf("wire form is not stable under a second pass")
	}
}

// The done record carries no reason key at all; the builder records always
// carry one. This is what the node keys job.completed by (an empty reason).
func TestJobDoneRecordOmitsReason(t *testing.T) {
	root, err := os.MkdirTemp("/tmp", "pwjobr")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(root)
	jobs := filepath.Join(root, "jobs")
	events := filepath.Join(jobs, "jd", "events")
	if err := os.MkdirAll(events, 0o755); err != nil {
		t.Fatal(err)
	}
	claim := `{"job_id":"jd","kind":"job.claim","payload":{"agent_label":"jd","owner_lane":"lane-d","pane_id":"w1:p2"}}`
	if err := os.WriteFile(filepath.Join(events, "00001-job.claim.json"), []byte(claim), 0o644); err != nil {
		t.Fatal(err)
	}
	report := filepath.Join(root, "report.md")
	if err := os.WriteFile(report, []byte("done line\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	t.Setenv("ARBITER_INBOX_ROOT", jobs)
	t.Setenv("HANDOFFKEEP_BIN", filepath.Join(root, "absent"))
	socket := filepath.Join(root, "s")
	t.Setenv("PANEWIRE_SOCKET", socket)
	server := startJobFakeSocket(t, socket, "ok")
	var stdout, stderr bytes.Buffer
	if rc := runJobCLI([]string{"done", "jd", "--report", report}, &stdout, &stderr, CLIConfig{}); rc != ExitOK {
		t.Fatalf("done rc=%d stderr=%q", rc, stderr.String())
	}
	lines := server.stop()
	contents, err := os.ReadFile(filepath.Join(events, "00002-job.completed.json"))
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(contents, []byte(`"reason"`)) {
		t.Fatalf("job.completed record carries a reason key: %s", contents)
	}
	if len(lines) != 1 || strings.Contains(lines[0], `"reason"`) {
		t.Fatalf("job.completed request carries a reason: %q", lines)
	}
}

// The record encoder must be Python's json.dumps(ensure_ascii=True) for a str
// decoded from argv with surrogateescape. Checked against python3 itself when
// it is available.
func TestPythonJSONStringMatchesPython(t *testing.T) {
	inputs := []string{
		"", "plain", `quote " backslash \ slash /`, "tab\tnl\ncr\rbs\bff\f", "\x01\x1f\x7f",
		"한글 🚀 é ü   �", "bad \xff\xfe bytes \xe4\xb8 end", "\xed\xa0\x80 surrogate bytes", "<>&",
	}
	for _, input := range inputs {
		got := pythonJSONString(input)
		if !utf8.ValidString(got) || strings.ContainsFunc(got, func(r rune) bool { return r >= 0x7f }) {
			t.Fatalf("%q encoded to non-ASCII %q", input, got)
		}
	}
	python, err := exec.LookPath("python3")
	if err != nil {
		t.Skip("python3 not available")
	}
	for _, input := range inputs {
		command := exec.Command(python, "-c", "import json,sys; sys.stdout.write(json.dumps(sys.argv[1]))", input)
		want, err := command.Output()
		if err != nil {
			t.Fatalf("python3 on %q: %v", input, err)
		}
		if got := pythonJSONString(input); got != string(want) {
			t.Errorf("pythonJSONString(%q) = %s, python3 = %s", input, got, want)
		}
	}
}

func TestBashReadTabMatchesBash(t *testing.T) {
	lines := []string{
		"o\tl\tp\tparent\tbuilder", "o\tl\tp\t\tworker", "\to\tl\tp", "o\t\t\tl\tp\tx\ty\t\t",
		"o l\tl\tp", "o\tl\tp\npast newline", "only", "", "a\tb\tc\td\te\tf\tg",
	}
	bash, err := exec.LookPath("bash")
	if err != nil {
		t.Skip("bash not available")
	}
	for _, count := range []int{4, 5} {
		names := []string{"a", "b", "c", "d", "e"}[:count]
		script := `IFS=$'\t' read -r ` + strings.Join(names, " ") + ` <<<"$1"; printf '%s\037' "$` + strings.Join(names, `" "$`) + `"`
		for _, line := range lines {
			out, err := exec.Command(bash, "-c", script, "bash", line).Output()
			if err != nil {
				t.Fatal(err)
			}
			want := strings.Split(strings.TrimSuffix(string(out), "\037"), "\037")
			got := bashReadTab(line, count)
			if strings.Join(got, "\037") != strings.Join(want, "\037") {
				t.Errorf("read %d of %q = %q, bash = %q", count, line, got, want)
			}
		}
	}
}

func TestPosixDirnameMatchesDirname(t *testing.T) {
	for _, path := range []string{"/a/b", "/a/b/", "a", "a/", "/", "//", "/a", "a/b//c//", "./x", "../x/y"} {
		want, err := exec.Command("dirname", path).Output()
		if err != nil {
			t.Fatal(err)
		}
		if got := posixDirname(path); got != strings.TrimRight(string(want), "\n") {
			t.Errorf("posixDirname(%q) = %q, dirname = %q", path, got, want)
		}
	}
}

func TestPythonIntMatchesPython(t *testing.T) {
	inputs := []string{"00003", "12", " 7 ", "+4", "-5", "1_000", "1__0", "_1", "1_", "", "x", "3a", "++1", ".completion", "99999999999999999999"}
	python, err := exec.LookPath("python3")
	if err != nil {
		t.Skip("python3 not available")
	}
	for _, input := range inputs {
		out, err := exec.Command(python, "-c", "import sys\ntry: print(int(sys.argv[1]))\nexcept ValueError: print('ValueError')", input).Output()
		if err != nil {
			t.Fatal(err)
		}
		want := strings.TrimSpace(string(out))
		got := "ValueError"
		if value, ok := pythonInt(input); ok {
			got = value.String()
		}
		if got != want {
			t.Errorf("pythonInt(%q) = %s, python3 = %s", input, got, want)
		}
	}
}
