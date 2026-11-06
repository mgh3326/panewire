package panewire

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"
)

// prStatusFixture is the hk 3185 fixture from mergewake-design: the JSON
// `mergewake pr-status --json` prints, serialised from mergewake#152 head
// 1c4231f with sample values.
const prStatusFixture = `{
  "repo": "owner/name",
  "pr": 152,
  "head": "1c4231f99c574ac6da029dd038756a9190e6a3cf",
  "gate_head": "1c4231f99c574ac6da029dd038756a9190e6a3cf",
  "state": "blocked",
  "reasons": ["threads_open:1"],
  "checks": [{"name": "test", "state": "green"}, {"name": "vet", "state": "green"}],
  "review": {"passes": [{"pass": 1, "lens": "correctness", "family": "openai"}], "done": true},
  "threads": {"blocking": 1, "other": 1, "unresolved": 2},
  "findings": [
    {"id": "PRRT_kwDOexample1", "location": "internal/prstatus/handler.go:42", "severity": "high", "blocking": true,
     "summary": "fresh read overwrites a newer gate decision", "url": "https://github.com/owner/name/pull/152#discussion_r1", "author_replied": false},
    {"id": "PRRT_kwDOexample2", "location": "docs/pr-status.md:10", "severity": "low", "blocking": false,
     "summary": "typo in field table", "url": "https://github.com/owner/name/pull/152#discussion_r2", "author_replied": true}
  ],
  "base_fresh": true,
  "observed_at": "2026-11-06T00:12:06Z",
  "fresh": null,
  "fresh_at": null,
  "source": "last_pass"
}`

// prStatusFakeMergewake installs a fake mergewake that records its argv and
// environment, then runs body. The recording files are returned.
type prStatusFake struct {
	argv, env string
}

func installFakeMergewake(t *testing.T, body string) prStatusFake {
	t.Helper()
	if runtime.GOOS == "windows" {
		t.Skip("fake mergewake is a POSIX shell script")
	}
	dir := t.TempDir()
	fake := prStatusFake{argv: filepath.Join(dir, "argv"), env: filepath.Join(dir, "env")}
	script := "#!/bin/sh\nprintf '%s\\n' \"$@\" > '" + fake.argv + "'\nenv > '" + fake.env + "'\n" + body + "\n"
	path := filepath.Join(dir, "mergewake")
	if err := os.WriteFile(path, []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	previous := nodeMergewakeExecutable
	nodeMergewakeExecutable = path
	t.Cleanup(func() { nodeMergewakeExecutable = previous })
	return fake
}

func fixtureFile(t *testing.T, contents string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "snapshot.json")
	if err := os.WriteFile(path, []byte(contents), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func (fake prStatusFake) read(t *testing.T, name string) string {
	t.Helper()
	contents, err := os.ReadFile(map[string]string{"argv": fake.argv, "env": fake.env}[name])
	if err != nil {
		t.Fatal(err)
	}
	return string(contents)
}

func prStatusArgsJSON(repo string, pr int, fresh bool) map[string]any {
	return map[string]any{"repo": repo, "pr": pr, "fresh": fresh}
}

// prStatusSpawnConfig writes a spawn.json whose handoffkeep_env points at a
// mode-0600 env file for the hk server, and returns a node client using it.
func prStatusNodeClient(t *testing.T, hkURL string) *HubClient {
	t.Helper()
	dir := t.TempDir()
	config := `{"cwd_map":{},"workspace":"w","herdr_session":"s","enabled":true`
	if hkURL != "" {
		env := writeMode0600(t, dir, "hk.env", "HANDOFFKEEP_URL="+hkURL+"\nHANDOFFKEEP_TOKEN=hk-secret-token-1\n")
		config += `,"handoffkeep_env":"` + env + `"`
	}
	config += "}"
	return &HubClient{spawnConfigPath: writeMode0600(t, dir, "spawn.json", config)}
}

func TestPRStatusNodeMergewakeFixture(t *testing.T) {
	t.Setenv("GITHUB_TOKEN", "ghp_leak_canary_value")
	t.Setenv("HANDOFFKEEP_TOKEN", "hk_leak_canary_value")
	fake := installFakeMergewake(t, "cat '"+fixtureFile(t, prStatusFixture)+"'")
	client := prStatusNodeClient(t, "")
	result, code := nodeRun(t, client, nodeCommandPRStatus, prStatusArgsJSON("owner/name", 152, false))
	if code != "" {
		t.Fatal(code)
	}
	merge := result["mergewake"].(map[string]any)
	snapshot := merge["snapshot"].(map[string]any)
	if merge["outcome"] != "ok" || snapshot["state"] != "blocked" || snapshot["head"] != "1c4231f99c574ac6da029dd038756a9190e6a3cf" || len(snapshot["findings"].([]any)) != 2 || len(snapshot["checks"].([]any)) != 2 {
		t.Fatalf("snapshot %v", merge)
	}
	finding := snapshot["findings"].([]any)[0].(map[string]any)
	if finding["id"] != "PRRT_kwDOexample1" || finding["location"] != "internal/prstatus/handler.go:42" || finding["blocking"] != true || finding["author_replied"] != false {
		t.Fatalf("finding %v", finding)
	}
	if result["hk"].(map[string]any)["outcome"] != "unconfigured" {
		t.Fatalf("hk %v", result["hk"])
	}
	if argv := fake.read(t, "argv"); argv != "pr-status\n--repo\nowner/name\n--pr\n152\n--json\n" {
		t.Fatalf("argv %q", argv)
	}
	// The child gets a closed environment: neither the GitHub token nor the hk
	// token of the node's own process reaches it, and nothing credential-shaped
	// is in argv.
	env := fake.read(t, "env")
	for _, secret := range []string{"ghp_leak_canary_value", "hk_leak_canary_value", "GITHUB_TOKEN", "HANDOFFKEEP_TOKEN"} {
		if strings.Contains(env, secret) || strings.Contains(fake.read(t, "argv"), secret) {
			t.Fatalf("child saw %s", secret)
		}
	}
	if !strings.Contains(env, "PATH=") {
		t.Fatalf("child lost PATH: %s", env)
	}

	if _, code := nodeRun(t, client, nodeCommandPRStatus, prStatusArgsJSON("owner/name", 152, true)); code != "" {
		t.Fatal(code)
	}
	if argv := fake.read(t, "argv"); argv != "pr-status\n--repo\nowner/name\n--pr\n152\n--json\n--fresh\n" {
		t.Fatalf("fresh argv %q", argv)
	}
}

func TestPRStatusNodeArgsAreStrict(t *testing.T) {
	good := `{"repo":"owner/name","pr":152,"fresh":false}`
	if _, valid := decodeNodeCommandArgs(nodeCommandPRStatus, json.RawMessage(good)); !valid {
		t.Fatal("valid args refused")
	}
	for name, raw := range map[string]string{
		"missing fresh":     `{"repo":"owner/name","pr":152}`,
		"null fresh":        `{"repo":"owner/name","pr":152,"fresh":null}`,
		"unknown field":     `{"repo":"owner/name","pr":152,"fresh":false,"config":"/etc/x"}`,
		"string pr":         `{"repo":"owner/name","pr":"152","fresh":false}`,
		"float pr":          `{"repo":"owner/name","pr":1.5,"fresh":false}`,
		"zero pr":           `{"repo":"owner/name","pr":0,"fresh":false}`,
		"negative pr":       `{"repo":"owner/name","pr":-1,"fresh":false}`,
		"huge pr":           `{"repo":"owner/name","pr":2147483648,"fresh":false}`,
		"no owner":          `{"repo":"name","pr":1,"fresh":false}`,
		"three segments":    `{"repo":"a/b/c","pr":1,"fresh":false}`,
		"dotdot name":       `{"repo":"owner/..","pr":1,"fresh":false}`,
		"dot name":          `{"repo":"owner/.","pr":1,"fresh":false}`,
		"flag-shaped":       `{"repo":"--owner/name","pr":1,"fresh":false}`,
		"space":             `{"repo":"owner/na me","pr":1,"fresh":false}`,
		"newline":           `{"repo":"owner/name\n","pr":1,"fresh":false}`,
		"dot git":           `{"repo":"owner/name.git","pr":1,"fresh":false}`,
		"trailing object":   good + `{}`,
		"array":             `[]`,
		"empty":             ``,
		"oversized segment": `{"repo":"owner/` + strings.Repeat("a", 101) + `","pr":1,"fresh":false}`,
	} {
		if _, valid := decodeNodeCommandArgs(nodeCommandPRStatus, json.RawMessage(raw)); valid {
			t.Errorf("%s accepted: %s", name, raw)
		}
	}
	// The node refuses the same inputs with invalid_args before running anything.
	installFakeMergewake(t, "echo should-not-run > /dev/null; exit 99")
	if _, code := (&HubClient{}).runNodeCommand(t.Context(), nodeCommandPRStatus, json.RawMessage(`{"repo":"owner/name","pr":1}`)); code != "invalid_args" {
		t.Fatalf("code %q", code)
	}
}

func TestPRStatusNodeSourceFailuresAreOutcomesNotErrors(t *testing.T) {
	cases := map[string]struct {
		body    string
		outcome string
		reason  string
	}{
		"no snapshot":    {"echo 'no status snapshot for owner/name#152' >&2; exit 1", "not_found", ""},
		"other failure":  {"echo 'token ghp_secretvalue rejected' >&2; exit 1", "failed", "exit_status"},
		"garbage":        {"echo 'not json'", "failed", "unreadable_output"},
		"other pr":       {`echo '{"repo":"owner/name","pr":9,"state":"ready","observed_at":"2026-11-06T00:12:06Z"}'`, "failed", "unreadable_output"},
		"other repo":     {`echo '{"repo":"owner/other","pr":152,"state":"ready","observed_at":"2026-11-06T00:12:06Z"}'`, "failed", "unreadable_output"},
		"no state":       {`echo '{"repo":"owner/name","pr":152,"observed_at":"2026-11-06T00:12:06Z"}'`, "failed", "unreadable_output"},
		"no observed_at": {`echo '{"repo":"owner/name","pr":152,"state":"ready"}'`, "failed", "unreadable_output"},
		"oversized":      {"yes '{\"padding\":1}' | head -c 70000", "failed", "output_too_large"},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			installFakeMergewake(t, tc.body)
			result, code := nodeRun(t, prStatusNodeClient(t, ""), nodeCommandPRStatus, prStatusArgsJSON("owner/name", 152, false))
			if code != "" {
				t.Fatalf("op error %q: a failed source must not fail the op", code)
			}
			merge := result["mergewake"].(map[string]any)
			if merge["outcome"] != tc.outcome || merge["snapshot"] != nil {
				t.Fatalf("mergewake %v", merge)
			}
			if tc.reason != "" && merge["reason"] != tc.reason {
				t.Fatalf("mergewake %v", merge)
			}
			if encoded, _ := json.Marshal(result); strings.Contains(string(encoded), "ghp_secretvalue") || strings.Contains(string(encoded), "rejected") {
				t.Fatalf("child stderr reached the result: %s", encoded)
			}
		})
	}
}

func TestPRStatusNodeMissingBinaryIsUnavailable(t *testing.T) {
	previous := nodeMergewakeExecutable
	nodeMergewakeExecutable = filepath.Join(t.TempDir(), "absent-mergewake")
	t.Cleanup(func() { nodeMergewakeExecutable = previous })
	result, code := nodeRun(t, prStatusNodeClient(t, ""), nodeCommandPRStatus, prStatusArgsJSON("owner/name", 152, false))
	merge := result["mergewake"].(map[string]any)
	if code != "" || merge["outcome"] != "unavailable" || merge["reason"] != "not_installed" {
		t.Fatalf("code=%q mergewake=%v", code, merge)
	}
}

func TestPRStatusNodeMergewakeTimeout(t *testing.T) {
	installFakeMergewake(t, "exec sleep 30")
	previous := prStatusMergewakeTimeout
	prStatusMergewakeTimeout = 200 * time.Millisecond
	t.Cleanup(func() { prStatusMergewakeTimeout = previous })
	start := time.Now()
	result, code := nodeRun(t, prStatusNodeClient(t, ""), nodeCommandPRStatus, prStatusArgsJSON("owner/name", 152, false))
	merge := result["mergewake"].(map[string]any)
	if code != "" || merge["outcome"] != "failed" || merge["reason"] != "timeout" {
		t.Fatalf("code=%q mergewake=%v", code, merge)
	}
	if elapsed := time.Since(start); elapsed > 5*time.Second {
		t.Fatalf("timeout took %s", elapsed)
	}
}

func prStatusBigSnapshot(findings, checks, reasons, passes, width int) PRStatusSnapshot {
	snapshot := PRStatusSnapshot{Repo: "owner/name", PR: 152, Head: strings.Repeat("a", 40), GateHead: strings.Repeat("a", 40), State: "blocked", ObservedAt: "2026-11-06T00:12:06Z", Source: "last_pass", Fresh: json.RawMessage("null")}
	for i := 0; i < findings; i++ {
		snapshot.Findings = append(snapshot.Findings, PRStatusFinding{
			ID: fmt.Sprintf("PRRT_%0*d", width, i), Location: strings.Repeat("d/", width/2) + "f:1", Severity: "high", Blocking: true,
			Summary: strings.Repeat("지적", width), URL: "https://github.com/owner/name/pull/152#" + strings.Repeat("x", width),
		})
	}
	for i := 0; i < checks; i++ {
		snapshot.Checks = append(snapshot.Checks, PRStatusCheck{Name: strings.Repeat("c", width) + fmt.Sprint(i), State: "green"})
	}
	for i := 0; i < reasons; i++ {
		snapshot.Reasons = append(snapshot.Reasons, strings.Repeat("r", width))
	}
	for i := 0; i < passes; i++ {
		snapshot.Review.Passes = append(snapshot.Review.Passes, PRStatusPass{Pass: i + 1, Lens: strings.Repeat("l", width), Family: strings.Repeat("f", width)})
	}
	return snapshot
}

// Whatever mergewake sends, every list and field is bounded on the node, so
// the widest possible snapshot still fits the node reply cap: the node drops
// findings from the tail and counts them instead of failing result_too_large.
func TestPRStatusNodeWidestSnapshotFitsReplyCap(t *testing.T) {
	// Far wider than mergewake's own 64 KiB bound allows, to prove the node's
	// caps alone are enough.
	snapshot := boundPRStatusSnapshot(prStatusBigSnapshot(500, 500, 500, 500, 400))
	result := fitPRStatusResult(nodePRStatusResult{
		Mergewake: nodePRStatusMergewake{Outcome: prStatusOK, Snapshot: &snapshot},
		HK:        nodePRStatusHK{Outcome: prStatusOK, Status: 200, Change: json.RawMessage(`{"pad":"` + strings.Repeat("h", prStatusMaxHKChange-20) + `"}`)},
	})
	encoded, err := marshalNodeCommandJSON(result)
	if err != nil {
		t.Fatal(err)
	}
	if len(encoded) > nodeCommandMaxResultBytes {
		t.Fatalf("reply %d bytes exceeds the %d byte node reply cap", len(encoded), nodeCommandMaxResultBytes)
	}
	if _, valid := canonicalNodeCommandResult(nodeCommandPRStatus, encoded); !valid {
		t.Fatal("widest reply is outside the documented shape")
	}
	if len(snapshot.Findings) == 0 || snapshot.FindingsOmitted != prStatusMaxFindings-len(snapshot.Findings) {
		t.Fatalf("kept %d findings, omitted %d of %d", len(snapshot.Findings), snapshot.FindingsOmitted, prStatusMaxFindings)
	}
	if len(snapshot.Checks) != prStatusMaxChecks || len(snapshot.Reasons) != prStatusMaxReasons || len(snapshot.Review.Passes) != prStatusMaxPasses {
		t.Fatalf("caps not applied: %d checks %d reasons %d passes", len(snapshot.Checks), len(snapshot.Reasons), len(snapshot.Review.Passes))
	}
}

// A realistic worst case through the real op: mergewake's own caps (50
// findings of 200 multi-byte characters) still reach the CLI as many findings
// as fit, with the rest counted.
func TestPRStatusNodeLargeMergewakeSnapshotThroughOp(t *testing.T) {
	huge, err := json.Marshal(prStatusBigSnapshot(50, 100, 50, 20, 60))
	if err != nil {
		t.Fatal(err)
	}
	if len(huge) >= prStatusMaxMergewakeOutput {
		t.Fatalf("fixture is %d bytes, over mergewake's own bound", len(huge))
	}
	installFakeMergewake(t, "cat '"+fixtureFile(t, string(huge))+"'")
	raw, _ := json.Marshal(prStatusArgsJSON("owner/name", 152, false))
	reply, code := prStatusNodeClient(t, "").runNodeCommand(t.Context(), nodeCommandPRStatus, raw)
	if code != "" {
		t.Fatal(code)
	}
	encoded, err := marshalNodeCommandJSON(reply)
	if err != nil {
		t.Fatal(err)
	}
	canonical, valid := canonicalNodeCommandResult(nodeCommandPRStatus, encoded)
	if len(encoded) > nodeCommandMaxResultBytes || !valid {
		t.Fatalf("reply %d bytes valid=%v", len(encoded), valid)
	}
	var decoded nodePRStatusResult
	if err := json.Unmarshal(canonical, &decoded); err != nil {
		t.Fatal(err)
	}
	snapshot := decoded.Mergewake.Snapshot
	if snapshot == nil || snapshot.FindingsOmitted == 0 || len(snapshot.Findings)+snapshot.FindingsOmitted != 50 {
		t.Fatalf("snapshot %+v", snapshot)
	}
}

// prStatusHKServer answers GET /v1/changes and records each request.
type prStatusHKServer struct {
	*httptest.Server
	mu       sync.Mutex
	requests []*http.Request
}

func newPRStatusHKServer(t *testing.T, status int, body string) *prStatusHKServer {
	t.Helper()
	hk := &prStatusHKServer{}
	hk.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hk.mu.Lock()
		hk.requests = append(hk.requests, r)
		hk.mu.Unlock()
		w.WriteHeader(status)
		_, _ = w.Write([]byte(body))
	}))
	t.Cleanup(hk.Close)
	return hk
}

func TestPRStatusNodeHandoffkeepOutcomes(t *testing.T) {
	installFakeMergewake(t, "cat '"+fixtureFile(t, prStatusFixture)+"'")
	cases := map[string]struct {
		status  int
		body    string
		outcome string
		reason  string
	}{
		"record":        {200, `{"change":{"pr":"x","verdict":"approve"}}`, "ok", ""},
		"not deployed":  {404, `{"error":"not found"}`, "not_found", ""},
		"server error":  {500, `boom`, "failed", "http_status"},
		"forbidden":     {403, `{}`, "failed", "http_status"},
		"not an object": {200, `[1,2]`, "failed", "unreadable_body"},
		"not json":      {200, `<html>`, "failed", "unreadable_body"},
		"too large":     {200, `{"pad":"` + strings.Repeat("x", prStatusMaxHKChange) + `"}`, "too_large", ""},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			hk := newPRStatusHKServer(t, tc.status, tc.body)
			result, code := nodeRun(t, prStatusNodeClient(t, hk.URL), nodeCommandPRStatus, prStatusArgsJSON("owner/name", 152, false))
			if code != "" {
				t.Fatalf("op error %q: a failed hk must not fail the op", code)
			}
			source := result["hk"].(map[string]any)
			if source["outcome"] != tc.outcome || (tc.reason != "" && source["reason"] != tc.reason) {
				t.Fatalf("hk %v", source)
			}
			if result["mergewake"].(map[string]any)["outcome"] != "ok" {
				t.Fatalf("mergewake lost with hk %s: %v", name, result["mergewake"])
			}
			if len(hk.requests) != 1 {
				t.Fatalf("hk requests %d", len(hk.requests))
			}
			request := hk.requests[0]
			if request.Method != http.MethodGet || request.URL.Path != "/v1/changes" || request.URL.Query().Get("pr") != "https://github.com/owner/name/pull/152" || request.Header.Get("Authorization") != "Bearer hk-secret-token-1" {
				t.Fatalf("hk request %s %s auth=%q", request.Method, request.URL.String(), request.Header.Get("Authorization"))
			}
			// hk's token authenticates the node's own call and never travels back.
			if encoded, _ := json.Marshal(result); strings.Contains(string(encoded), "hk-secret-token-1") {
				t.Fatalf("hk token in result: %s", encoded)
			}
			if tc.outcome == "ok" {
				change := source["change"].(map[string]any)["change"].(map[string]any)
				if change["verdict"] != "approve" {
					t.Fatalf("change %v", source["change"])
				}
			}
		})
	}
}

func TestPRStatusNodeHandoffkeepUnreachableIsAnOutcome(t *testing.T) {
	installFakeMergewake(t, "cat '"+fixtureFile(t, prStatusFixture)+"'")
	hk := newPRStatusHKServer(t, 200, `{}`)
	url := hk.URL
	hk.Close()
	result, code := nodeRun(t, prStatusNodeClient(t, url), nodeCommandPRStatus, prStatusArgsJSON("owner/name", 152, false))
	source := result["hk"].(map[string]any)
	if code != "" || source["outcome"] != "failed" || source["reason"] != "request_failed" {
		t.Fatalf("code=%q hk=%v", code, source)
	}
}

// The op is read-only: it must not take the acting-command lock, so a pane
// close in progress never delays a status read.
func TestPRStatusNodeDoesNotTakeActingLock(t *testing.T) {
	installFakeMergewake(t, "cat '"+fixtureFile(t, prStatusFixture)+"'")
	client := prStatusNodeClient(t, "")
	client.nodeCommandMu.Lock()
	defer client.nodeCommandMu.Unlock()
	done := make(chan struct{})
	go func() {
		defer close(done)
		_, _ = nodeRun(t, client, nodeCommandPRStatus, prStatusArgsJSON("owner/name", 152, false))
	}()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("pr.status waited for the acting-command lock")
	}
}
