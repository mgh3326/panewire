package panewire

import (
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/mgh3326/panewire/internal/fleet"
	"github.com/mgh3326/panewire/internal/hkcred"
	"github.com/mgh3326/panewire/internal/quota/codex"
)

// TestMain points every test's fleet events at a temporary state dir, so no
// test appends to the developer's real ~/.local/state/panewire (which Alloy
// ships to Loki). It also points the claude and codex logins at an empty
// directory, so a test that reaches the node's native quota measurement
// without a fixture finds no credential instead of the developer's own login.
func TestMain(m *testing.M) {
	state, err := os.MkdirTemp("", "panewire-test-state-")
	if err != nil {
		panic(err)
	}
	os.Setenv("CODEX_HOME", filepath.Join(state, "no-codex-login"))
	os.Setenv("CLAUDE_CONFIG_DIR", filepath.Join(state, "no-claude-login"))
	os.Unsetenv("CLAUDE_SECURESTORAGE_CONFIG_DIR")
	os.Unsetenv(codex.BinaryOverrideEnv)
	// The crash-output child keeps the state dir its parent test chose.
	if os.Getenv("PANEWIRE_CRASH_CHILD") == "" {
		os.Setenv("XDG_STATE_HOME", state)
	}
	// The bench catalog cache must not land in the developer's real cache.
	os.Setenv("XDG_CACHE_HOME", filepath.Join(state, "cache"))
	// No test may reach hk with the developer's own credential (hk 3570): the
	// svc-panewire variables are cleared and the default file points at a
	// path that does not exist. A test that wants one sets it with t.Setenv.
	for _, key := range []string{hkcred.URLKey, hkcred.TokenKey, hkcred.CFIDKey, hkcred.CFSecretKey} {
		os.Unsetenv(key)
	}
	os.Setenv(hkcred.PathEnv, filepath.Join(state, "no-hk-svc.env"))
	// No test may read the developer's real files through a default path
	// under HOME (spawn.json, hk-svc.env, the herdr socket; hk 3570): HOME is
	// a private directory for the whole package. The go toolchain keeps the
	// launch HOME's caches and env file, so a test that runs go stays warm and
	// offline. A test that needs another HOME sets it with t.Setenv.
	testLaunchHome = os.Getenv("HOME")
	pinGoToolchainEnv()
	home := filepath.Join(state, "home")
	if err := os.MkdirAll(home, 0o700); err != nil {
		panic(err)
	}
	os.Setenv("HOME", home)
	code := m.Run()
	os.RemoveAll(state)
	os.Exit(code)
}

// testLaunchHome is the HOME the test binary was started with, before
// TestMain moved it.
var testLaunchHome string

// pinGoToolchainEnv fixes GOCACHE, GOMODCACHE, GOPATH and GOENV to what they
// resolve to under the launch HOME, before TestMain moves HOME. Without go on
// PATH nothing is pinned: no test can run go then either.
func pinGoToolchainEnv() {
	out, err := exec.Command("go", "env", "-json", "GOCACHE", "GOMODCACHE", "GOPATH", "GOENV").Output()
	if err != nil {
		return
	}
	var values map[string]string
	if json.Unmarshal(out, &values) != nil {
		return
	}
	for key, value := range values {
		if value != "" && os.Getenv(key) == "" {
			os.Setenv(key, value)
		}
	}
}

var fleetEventFakeSecrets = []string{"ghp_FAKEfakeFAKEfakeFAKEfake0123456789", "hunter2pass", "tok3nVALUEfake99"}

const fleetEventFakeSecretText = "token=tok3nVALUEfake99 ghp_FAKEfakeFAKEfakeFAKEfake0123456789 https://ops:hunter2pass@quota.invalid/v1"

// readFleetEventsFile parses the panewire fleet-events file under a fresh
// state dir and checks every line has exactly the v1 fields.
func readFleetEventsFile(t *testing.T) (string, []map[string]string) {
	t.Helper()
	raw, err := os.ReadFile(fleetEventsPath())
	if os.IsNotExist(err) {
		return "", nil
	}
	if err != nil {
		t.Fatal(err)
	}
	var events []map[string]string
	for _, line := range strings.Split(strings.TrimSuffix(string(raw), "\n"), "\n") {
		var fields map[string]any
		if err := json.Unmarshal([]byte(line), &fields); err != nil {
			t.Fatalf("event line is not JSON: %q", line)
		}
		keys := []string{"ts", "host", "tool", "kind", "job", "outcome", "reason", "detail"}
		if _, ok := fields["request_id"]; ok {
			keys = append(keys, "request_id")
		}
		// A successful node_spawn also carries its profile and duration_ms.
		if _, ok := fields["profile"]; ok {
			keys = append(keys, "profile")
		}
		if _, ok := fields["duration_ms"].(float64); ok {
			if len(fields) != len(keys)+1 {
				t.Fatalf("event is not the v1 shape: %q", line)
			}
			event := map[string]string{"duration_ms": "set"}
			for _, key := range keys {
				value, ok := fields[key].(string)
				if !ok {
					t.Fatalf("event field %q missing: %q", key, line)
				}
				event[key] = value
			}
			events = append(events, event)
			continue
		}
		if len(fields) != len(keys) {
			t.Fatalf("event is not the v1 shape: %q", line)
		}
		event := map[string]string{}
		for _, key := range keys {
			value, ok := fields[key].(string)
			if !ok {
				t.Fatalf("event field %q missing: %q", key, line)
			}
			event[key] = value
		}
		events = append(events, event)
	}
	return string(raw), events
}

func TestHubSpawnUppercaseCWDKeyNamesTheRule(t *testing.T) {
	t.Setenv("XDG_STATE_HOME", t.TempDir())
	path := filepath.Join(t.TempDir(), "spawn.json")
	if err := os.WriteFile(path, []byte(`{"cwd_map":{"Repo-A":"/srv/repo-a"},"workspace":"worker","herdr_session":"worker","enabled":true}`), 0600); err != nil {
		t.Fatal(err)
	}
	message := hubOutboundMessage{RequestID: t14RequestID(90), CWDKey: "repo-a", BriefInline: "brief", WaitSeconds: 1, Args: []string{"--job", "job-upper"}}
	result := (&HubClient{spawnConfigPath: path}).runHubSpawn(t.Context(), message)
	// The wire error stays spawn_disabled for older hubs; the rule rides in the tail.
	if result.RC != 2 || result.Error != "spawn_disabled" {
		t.Fatalf("result=%+v", result)
	}
	if !strings.Contains(result.StdoutTail, "cwd_map_key_pattern") || !strings.Contains(result.StdoutTail, `"Repo-A"`) {
		t.Fatalf("refusal must name the rule and key: %q", result.StdoutTail)
	}
	if _, ok := parseHubSpawnResult(hubSpawnResultWire(t, result)); !ok {
		t.Fatal("the hub must still accept the result")
	}
	raw, events := readFleetEventsFile(t)
	if len(events) != 1 {
		t.Fatalf("want one event: %s", raw)
	}
	event := events[0]
	if event["tool"] != "panewire" || event["kind"] != "node_spawn" || event["job"] != "job-upper" || event["outcome"] != "refused" || event["reason"] != "cwd_map_key_pattern" {
		t.Fatalf("event=%+v", event)
	}
	if !strings.Contains(event["detail"], `key \"Repo-A\"`) && !strings.Contains(event["detail"], `key "Repo-A"`) {
		t.Fatalf("detail=%q", event["detail"])
	}
}

func TestHubSpawnConfigRulesAreNamed(t *testing.T) {
	directory := t.TempDir()
	cases := map[string]string{
		`{"cwd_map":{},"workspace":"w","herdr_session":"s","enabled":false}`:              "enabled_false",
		`{"cwd_map":{},"workspace":"","herdr_session":"s","enabled":true}`:                "workspace_required",
		`{"cwd_map":{"a":"relative"},"workspace":"w","herdr_session":"s","enabled":true}`: "cwd_map_dir_absolute",
		`{"cwd_map":"x","workspace":"w","herdr_session":"s","enabled":true}`:              "config_json_invalid",
		`{"cwd_map":{},"workspace":"w","herdr_session":"s","enabled":true} {}`:            "config_trailing_data",
	}
	for contents, rule := range cases {
		path := filepath.Join(directory, rule+".json")
		if err := os.WriteFile(path, []byte(contents), 0600); err != nil {
			t.Fatal(err)
		}
		_, err := loadHubSpawnConfig(path)
		if configErr, ok := err.(*hubSpawnConfigError); !ok || configErr.Rule != rule {
			t.Fatalf("%s: got %v, want rule %s", contents, err, rule)
		}
	}
	if _, err := loadHubSpawnConfig(filepath.Join(directory, "missing.json")); err == nil || !strings.Contains(err.Error(), "config_missing") {
		t.Fatalf("missing file: %v", err)
	}
}

func TestHubSpawnFailingEngineStderrReachesResultBoundedAndRedacted(t *testing.T) {
	t.Setenv("XDG_STATE_HOME", t.TempDir())
	root := t.TempDir()
	// 8 KiB of stderr noise, then the line that explains the refusal.

	message := hubOutboundMessage{RequestID: t14RequestID(91), CWDKey: "repo-a", BriefInline: "brief", WaitSeconds: 5, Args: []string{"--job", "job-stderr"}}
	result := (&HubClient{spawnConfigPath: t14SpawnConfig(t, root), spawnEngine: func(ctx context.Context, cwd, brief, workspace, session string, args []string) (*hubSpawnOutput, fleet.Execution) {
		output := &hubSpawnOutput{}
		_, _ = output.stderr.Write([]byte(strings.Repeat("noise\n", 1500) + "panewire: quota gate refused: " + fleetEventFakeSecretText))
		return output, fleet.Execution{RC: 70, Stage: "placement"}
	}}).runHubSpawn(t.Context(), message)
	if result.RC != 70 || result.Error != "spawn_failed" {
		t.Fatalf("result=%+v", result)
	}
	if !strings.Contains(result.StdoutTail, "panewire: quota gate refused:") {
		t.Fatalf("engine's stderr tail must reach the result: %q", result.StdoutTail)
	}
	if spawnResultFix(result.StdoutTail) == "" || strings.Count(result.StdoutTail, "stderr: ") != 1 {
		t.Fatalf("refusal and actionable fix must share the bounded stderr section: %q", result.StdoutTail)
	}
	if len(result.StdoutTail) > hubSpawnResultTail || len(result.StdoutTail) > len("stderr: ")+hubSpawnStderrTail {
		t.Fatalf("tail not bounded: %d bytes", len(result.StdoutTail))
	}
	if _, ok := parseHubSpawnResult(hubSpawnResultWire(t, result)); !ok {
		t.Fatal("the hub must accept the result")
	}
	raw, events := readFleetEventsFile(t)
	if len(events) != 1 || events[0]["outcome"] != "failed" || events[0]["reason"] != "spawn_failed" || !strings.HasPrefix(events[0]["detail"], "spawn_failed rc=70 stderr: ") {
		t.Fatalf("events: %s", raw)
	}
	for _, secret := range fleetEventFakeSecrets {
		if strings.Contains(result.StdoutTail, secret) || strings.Contains(raw, secret) {
			t.Fatalf("secret %q leaked (result %q, events %s)", secret, result.StdoutTail, raw)
		}
	}
}

func TestHubSpawnTailWithStderrKeepsTheWireBound(t *testing.T) {
	stdout := strings.Repeat("o", hubSpawnResultTail)
	stderr := strings.Repeat("é", hubSpawnStderrTail)
	got := hubSpawnTailWithStderr(stdout, stderr)
	if len(got) > hubSpawnResultTail || !strings.HasSuffix(got, "é") || !strings.Contains(got, "\nstderr: ") {
		t.Fatalf("len=%d", len(got))
	}
	if got := hubSpawnTailWithStderr("", "boom"); got != "stderr: boom" {
		t.Fatalf("empty stdout: %q", got)
	}
}

func TestFleetEventRedactsAndBounds(t *testing.T) {
	path := filepath.Join(t.TempDir(), "events", fleetEventFileName)
	ok := appendFleetEvent(path, fleetEventMaxBytes, fleetEvent{TS: time.Unix(0, 0).UTC().Format(time.RFC3339Nano), Host: "Node-A", Tool: "panewire", Kind: "Node Spawn", Outcome: "REFUSED", Reason: "x", Detail: fleetEventFakeSecretText + strings.Repeat("z", 4096)})
	if !ok {
		t.Fatal("append failed")
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var event fleetEvent
	if err := json.Unmarshal(raw, &event); err != nil {
		t.Fatal(err)
	}
	if event.Host != "node-a" || event.Kind != "node_spawn" || event.Outcome != "refused" || len(event.Detail) > fleetEventMaxDetail {
		t.Fatalf("event=%+v", event)
	}
	for _, secret := range fleetEventFakeSecrets {
		if strings.Contains(string(raw), secret) {
			t.Fatalf("secret %q leaked: %s", secret, raw)
		}
	}
	if info, err := os.Stat(path); err != nil || info.Mode().Perm() != 0o640 {
		t.Fatalf("mode: %v %v", info, err)
	}
}

// hubSpawnResultWire renders a node result the way handleHubSpawn sends it.
func hubSpawnResultWire(t *testing.T, result hubSpawnResult) []byte {
	t.Helper()
	wire := map[string]any{"type": "job.spawn.result", "request_id": result.RequestID, "rc": result.RC, "job_id": result.JobID, "pane": result.Pane, "stdout_tail": result.StdoutTail}
	if result.Error != "" {
		wire["error"] = result.Error
	}
	raw, err := json.Marshal(wire)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func TestFleetEventRedactCoversKeyAssignmentsPEMBlobsAndEmails(t *testing.T) {
	pem := "-----BEGIN RSA PRIVATE KEY-----\nMIIEowIBAAKCAQEAuFAKEKEYbodyFAKEKEYbodyFAKEKEYbody0123456789abcdefABCDEF\nQWERTYfakefakefake\n-----END RSA PRIVATE KEY-----"
	cases := map[string]string{
		"AWS_SECRET_ACCESS_KEY=wJalrXUtnFEMIK7MDENGbPxRfiCYEXAMPLEKEY":                "wJalrXUtnFEMI",
		`"client_secret": "s3cr3tvalue99"`:                                            "s3cr3tvalue99",
		"before " + pem + " after":                                                    "FAKEKEYbody",
		"truncated tail: uFAKEKEYbodyFAKEKEYbodyFAKEKEYbody0123456789abcdefABCDEFxyz": "FAKEKEYbody",
		"author alice@example.org pushed":                                             "alice@example.org",
	}
	for input, secret := range cases {
		if got := fleetEventRedact(input); strings.Contains(got, secret) {
			t.Fatalf("%q kept %q: %q", input, secret, got)
		}
	}
	sha := "2cdf695b2e8f19ce4fda2f7fad4236125453c22c"
	if got := fleetEventRedact("head " + sha + " exit=3"); got != "head "+sha+" exit=3" {
		t.Fatalf("a commit SHA and plain fields must survive: %q", got)
	}
}

// hk's secret_like_content refusal names the guard pattern, never the matched
// text; the spawn outcome reason carries it, so redaction keeps exactly the
// code:pattern form. hk's free-form "code: reason" and out-of-charset patterns
// still meet the credential rules.
func TestFleetEventRedactKeepsHKGuardPatternOnly(t *testing.T) {
	for _, input := range []string{
		"brief upload to briefs/job-a.md failed: handoffkeep: secret_like_content:private_key",
		"handoffkeep: secret_like_content:github_token and handoffkeep: secret_like_content:aws_key",
	} {
		got := fleetEventRedact(input)
		if !strings.Contains(got, "secret_like_content:") || strings.Contains(got, "[redacted]") {
			t.Fatalf("%q lost the guard pattern: %q", input, got)
		}
	}
	for input, secret := range map[string]string{
		"handoffkeep: secret_like_content: hunter2 real reason":       "hunter2",
		"handoffkeep: secret_like_content:../x":                       "../x",
		"handoffkeep: secret_like_content:" + strings.Repeat("a", 41): strings.Repeat("a", 41),
	} {
		if got := fleetEventRedact(input); strings.Contains(got, secret) {
			t.Fatalf("%q kept %q: %q", input, secret, got)
		}
	}
}

func TestFleetEventRedactQuotedSpacedAndShortCredentialValues(t *testing.T) {
	cases := map[string]string{
		`password="abc def"`:                   "abc def",
		`password="abcdefghij klmnop"`:         "klmnop", // the stall set alone would stop at the space
		`password="abc def`:                    "abc def",
		`token='open sesame now'`:              "open sesame",
		`secret="a \"quoted\" word"`:           "quoted",
		`password=abc`:                         "abc",
		`api_key: "k"`:                         `"k"`,
		`{"client_secret": "s3 cr3t"}`:         "s3 cr3t",
		`DB_PASSWORD = "two words" next=field`: "two words",
	}
	for input, secret := range cases {
		got := fleetEventRedact(input)
		if strings.Contains(got, secret) || !strings.Contains(got, "[redacted]") {
			t.Fatalf("%s kept %q: %s", input, secret, got)
		}
	}
	if got := fleetEventRedact(`DB_PASSWORD = "two words" next=field`); !strings.Contains(got, "next=field") {
		t.Fatalf("redaction must stop at the closing quote: %s", got)
	}
}

func TestFleetEventModesSurviveARestrictiveUmask(t *testing.T) {
	old := syscall.Umask(0o077)
	defer syscall.Umask(old)
	path := filepath.Join(t.TempDir(), "events", fleetEventFileName)
	if !appendFleetEvent(path, fleetEventMaxBytes, fleetEvent{Kind: "k", Outcome: "o"}) {
		t.Fatal("append failed")
	}
	for target, want := range map[string]os.FileMode{path: 0o640, filepath.Dir(path): 0o750} {
		if info, err := os.Stat(target); err != nil || info.Mode().Perm() != want {
			t.Fatalf("%s: %v %v, want %o", target, info.Mode().Perm(), err, want)
		}
	}
}

func TestUTF8SuffixWithinClampsAndStaysValid(t *testing.T) {
	if got := utf8SuffixWithin("abc", -5); got != "" {
		t.Fatalf("negative limit: %q", got)
	}
	if got := utf8SuffixWithin("ok\xffend", 100); got != "okend" {
		t.Fatalf("invalid bytes must go: %q", got)
	}
	if got := hubSpawnTailWithStderr(strings.Repeat("o", 10), strings.Repeat("e", hubSpawnResultTail*2)); len(got) > hubSpawnResultTail {
		t.Fatalf("tail exceeded the wire bound: %d", len(got))
	}
}
