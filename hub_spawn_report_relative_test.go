package panewire

import (
	"bytes"
	"context"
	"net/http"
	"strings"
	"testing"

	"github.com/mgh3326/panewire/internal/fleet"
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

func TestHubSpawnReportRelativeReachesNodeEngineVerbatim(t *testing.T) {
	root := t.TempDir()
	const report = "reviews/x/report.md"
	raw := `{"type":"job.spawn","request_id":"` + t14RequestID(121) + `","cwd_key":"repo-a","brief":{"inline":"brief"},"args":["-l","label-a","--report-relative","` + report + `"],"wait_seconds":1}`
	message, ok := parseHubOutbound([]byte(raw))
	if !ok {
		t.Fatal("node rejected report-relative")
	}
	var got []string
	client := &HubClient{spawnConfigPath: t14SpawnConfig(t, root), spawnEngine: func(ctx context.Context, cwd, brief, workspace, session string, args []string) (*hubSpawnOutput, fleet.Execution) {
		got = append([]string(nil), args...)
		return t14EngineOK()
	}}
	result := client.runHubSpawn(t.Context(), message)
	if result.RC != 0 || strings.Join(got, "\x00") != strings.Join(message.Args, "\x00") {
		t.Fatalf("result=%+v args=%v", result, got)
	}
}
