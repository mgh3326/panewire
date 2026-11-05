package panewire

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func TestNodeJobResultFiles(t *testing.T) {
	client, report := nodeReapJobDir(t, "job-a", "report.md")
	dir := filepath.Dir(report)
	cases := []struct{ name, body, reason string }{
		{"valid", `{"findings":[],"note":"ok"}`, ""},
		{"large", strings.Repeat("x", nodeJobFindingsLimit+1), "too_large"},
		{"json", "{", "not_json"},
		{"shape", `{"findings":null}`, "schema"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if err := os.WriteFile(filepath.Join(dir, "findings.json"), []byte(tc.body), 0600); err != nil {
				t.Fatal(err)
			}
			got, code := client.nodeJobResult(nodeJobResultArgs{Job: "job-a"})
			if code != "" {
				t.Fatal(code)
			}
			result := got.(nodeJobResult)
			if result.FindingsError != tc.reason || (result.Findings != nil) != (tc.reason == "") {
				t.Fatalf("%+v", result)
			}
		})
	}
	if err := os.Remove(filepath.Join(dir, "findings.json")); err != nil {
		t.Fatal(err)
	}
	got, _ := client.nodeJobResult(nodeJobResultArgs{Job: "job-a"})
	if got.(nodeJobResult).FindingsError != "missing" {
		t.Fatal(got)
	}
	outside := filepath.Join(t.TempDir(), "secret")
	if err := os.WriteFile(outside, []byte(`{"findings":["secret"]}`), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(outside, filepath.Join(dir, "findings.json")); err != nil {
		t.Fatal(err)
	}
	got, code := client.nodeJobResult(nodeJobResultArgs{Job: "job-a"})
	if code != "" {
		t.Fatalf("symlink: expected missing, got error %q", code)
	}
	if got.(nodeJobResult).FindingsError != "missing" {
		t.Fatal(got)
	}
	got, _ = client.nodeJobResult(nodeJobResultArgs{Job: "absent"})
	if got.(nodeJobResult).Found {
		t.Fatal(got)
	}
	data := []byte("secret report body\nlast line\n")
	if err := os.WriteFile(report, data, 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(filepath.Join(dir, "events"), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "events", "00001-job.completed.json"), []byte(`{"kind":"job.completed"}`), 0600); err != nil {
		t.Fatal(err)
	}
	got, _ = client.nodeJobResult(nodeJobResultArgs{Job: "job-a"})
	result := got.(nodeJobResult)
	sum := sha256.Sum256(data)
	if !result.Completed || !result.Report.Present || result.Report.Bytes != int64(len(data)) || result.Report.SHA256 != hex.EncodeToString(sum[:]) || result.Report.LastLine != "last line" {
		t.Fatalf("%+v", result)
	}
	encoded, _ := json.Marshal(result)
	if bytes.Contains(encoded, []byte("secret report body")) {
		t.Fatal(string(encoded))
	}
}

func TestNodeJobResultValidation(t *testing.T) {
	for _, raw := range []string{`{"job":"../x"}`, `{"job":".x"}`, `{"job":"-x"}`, `{"job":"x/y"}`, `{"job":"x","extra":1}`} {
		if _, ok := decodeNodeCommandArgs(nodeCommandJobResult, json.RawMessage(raw)); ok {
			t.Fatal(raw)
		}
	}
}

func TestNodeJobResultCLI(t *testing.T) {
	fixture := nodeReapSetup(t, map[string]string{"svc-inspect": "node.command:worktree.inspect@node-a", "svc-other": "node.command:worktree.inspect@node-b"})
	dir := filepath.Dir(fixture.report)
	data := `{"findings":[],"value":"` + strings.Repeat("<", nodeJobFindingsLimit-len(`{"findings":[],"value":""}`)) + `"}`
	if len(data) != nodeJobFindingsLimit {
		t.Fatal(len(data))
	}
	if err := os.WriteFile(filepath.Join(dir, "findings.json"), []byte(data), 0600); err != nil {
		t.Fatal(err)
	}
	credential := filepath.Join(t.TempDir(), "service.env")
	if err := os.WriteFile(credential, []byte("HUB_MACHINE_ID=svc-inspect\nHUB_TOKEN="+pw2ServiceToken("svc-inspect")+"\n"), 0600); err != nil {
		t.Fatal(err)
	}
	args := []string{"job-result", "--hub-url", fixture.server.URL, "--hub-token-env", credential, "--host", "node-a", "--job", "job-a"}
	var out, errout bytes.Buffer
	if code := runNodeCLI(args, &out, &errout, hubCLIDeps{AllowInsecureForTests: true}); code != ExitOK {
		t.Fatalf("code=%d stderr=%s", code, errout.String())
	}
	var result nodeJobResult
	if err := json.Unmarshal(out.Bytes(), &result); err != nil || !result.Found || result.FindingsError != "" {
		t.Fatalf("result=%+v err=%v", result, err)
	}
	if len(out.Bytes()) > nodeCommandMaxResultBytes {
		t.Fatalf("size=%d", out.Len())
	}
	args[6] = "node-b"
	out.Reset()
	errout.Reset()
	if code := runNodeCLI(args, &out, &errout, hubCLIDeps{AllowInsecureForTests: true}); code != ExitConditionInvalid {
		t.Fatalf("code=%d stderr=%s", code, errout.String())
	}
}

func TestNodeJobResultEncoding(t *testing.T) {
	for _, fill := range []string{"<", "\u2028", "\\u2028"} {
		body, err := json.Marshal(map[string]any{"findings": []any{}, "value": strings.Repeat(fill, 3000)})
		if err != nil {
			t.Fatal(err)
		}
		var object any
		if err := json.Unmarshal(body, &object); err != nil {
			t.Fatal(err)
		}
		raw, err := marshalNodeCommandJSON(object)
		if err != nil {
			t.Fatal(err)
		}
		var decoded any
		if err := json.Unmarshal(raw, &decoded); err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(object, decoded) {
			t.Fatal("JSON changed during encoding")
		}
	}
	missing := nodeJobResult{Found: true, FindingsError: "missing"}
	raw, err := json.Marshal(missing)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := canonicalNodeCommandResult(nodeCommandJobResult, raw); !ok {
		t.Fatalf("missing findings rejected: %s", raw)
	}
}

func TestNodeJobResultRejectsCombinedFindingErrors(t *testing.T) {
	for _, reason := range []string{"missing|too_large", "too_large|not_json", ""} {
		raw, err := json.Marshal(nodeJobResult{Found: true, FindingsError: reason})
		if err != nil {
			t.Fatal(err)
		}
		if _, ok := canonicalNodeCommandResult(nodeCommandJobResult, raw); ok {
			t.Errorf("hub accepted findings_error %q", reason)
		}
	}
}

// activity_events is what lets the hub tell a pane that never worked from one
// that did (hk 3179): only the spawner's and the quota gate's own records are
// bookkeeping; any other event, or a name that does not parse, is activity.
func TestNodeJobResultActivityEvents(t *testing.T) {
	client, report := nodeReapJobDir(t, "job-a", "report.md")
	dir := filepath.Dir(report)
	count := func(t *testing.T) int {
		t.Helper()
		got, code := client.nodeJobResult(nodeJobResultArgs{Job: "job-a"})
		result, _ := got.(nodeJobResult)
		if code != "" || result.ActivityEvents == nil {
			t.Fatalf("code=%q result=%+v", code, result)
		}
		return *result.ActivityEvents
	}
	if got := count(t); got != 0 {
		t.Fatalf("no events directory: %d", got)
	}
	events := filepath.Join(dir, "events")
	if err := os.Mkdir(events, 0700); err != nil {
		t.Fatal(err)
	}
	write := func(name string) {
		t.Helper()
		if err := os.WriteFile(filepath.Join(events, name), []byte(`{}`), 0600); err != nil {
			t.Fatal(err)
		}
	}
	for i, name := range []string{"00001-job.claim.json", "00002-job.placement.json", "00003-lease.acquire.json", "00004-quota_pool.record.json", "00005-job.task_link.json"} {
		write(name)
		if got := count(t); got != 0 {
			t.Fatalf("bookkeeping event %d %s counted as activity: %d", i, name, got)
		}
	}
	for want, name := range []string{"00006-lane.event.json", "00007-job.spawned.json", "00008-job.escalate.json", "stray.json", "00010-job.claim.txt"} {
		write(name)
		if got := count(t); got != want+1 {
			t.Fatalf("%s: activity %d, want %d", name, got, want+1)
		}
	}
}

func TestNodeJobResultActivityEventsValidation(t *testing.T) {
	negative := -1
	if validNodeJobResult(nodeJobResult{Found: true, FindingsError: "missing", ActivityEvents: &negative}) {
		t.Fatal("negative count accepted")
	}
	zero := 0
	if !validNodeJobResult(nodeJobResult{Found: true, FindingsError: "missing", ActivityEvents: &zero}) || !validNodeJobResult(nodeJobResult{Found: true, FindingsError: "missing"}) {
		t.Fatal("count rejected")
	}
}
