package panewire

import (
	"bytes"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestHubSpawnReportRelativeArgValidation(t *testing.T) {
	if !validHubSpawnArgs([]string{"--report-relative", "reviews/x/report.md"}) {
		t.Fatal("--report-relative reviews/x/report.md refused")
	}
	for _, value := range []string{"/abs/report.md", "../x", "a/../../b", "a/..", "", "./x", "a//b", "a/", "two words"} {
		if validHubSpawnArgs([]string{"--report-relative", value}) {
			t.Errorf("--report-relative %q accepted", value)
		}
	}
}

func TestHubSpawnReportRelativeHTTPValidation(t *testing.T) {
	_, server := t14Hub(t)
	for _, value := range []string{"/abs/report.md", "../x", "a/../../b", ""} {
		body := t14SpawnBody(t14RequestID(120))
		body["args"] = []string{"--report-relative", value}
		status, _, response := t14HTTP(t, http.MethodPost, server.URL+"/v1/spawn", t14OperatorToken, body)
		if status != http.StatusBadRequest || !bytes.Contains(response, []byte(`"invalid_request"`)) {
			t.Errorf("--report-relative %q: status=%d body=%s", value, status, response)
		}
	}
}

func TestHubSpawnReportRelativeReachesNodeRuntugVerbatim(t *testing.T) {
	root := t.TempDir()
	argsPath := filepath.Join(root, "args")
	t.Setenv("T14_ARGS_FILE", argsPath)
	t14FakeRuntug(t, `printf '%s\000' "$@" > "$T14_ARGS_FILE"
printf 'OK pane=pane-a model=m label=l status=s landed=y job=job-a quota_record=q\n'`)

	const report = "reviews/x/report.md"
	raw := `{"type":"job.spawn","request_id":"` + t14RequestID(121) + `","cwd_key":"repo-a","brief":{"inline":"brief"},"args":["-l","label-a","--report-relative","` + report + `"],"wait_seconds":1}`
	message, ok := parseHubOutbound([]byte(raw))
	if !ok {
		t.Fatal("node refused a --report-relative spawn directive")
	}
	client := &HubClient{spawnConfigPath: t14SpawnConfig(t, root), spawnTimeoutExtra: 20 * time.Millisecond}
	if result := client.runHubSpawn(t.Context(), message); result.RC != 0 {
		t.Fatalf("result=%+v", result)
	}
	got, err := os.ReadFile(argsPath)
	if err != nil {
		t.Fatal(err)
	}
	argv := strings.Split(strings.TrimSuffix(string(got), "\x00"), "\x00")
	want := []string{"-l", "label-a", "--report-relative", report, "--override-reason", hubSpawnOverrideReason}
	if tail := argv[len(argv)-len(want):]; strings.Join(tail, "\x00") != strings.Join(want, "\x00") {
		t.Fatalf("argv=%q want tail %q", argv, want)
	}
}
