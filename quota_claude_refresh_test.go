package panewire

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/mgh3326/panewire/internal/fleet"
)

const claudeRefreshedSecret = "sk-ant-oat01-refreshed-loop-token-must-not-leak"

// A claude login whose stored token expired, where the CLI fallback failed
// after its run refreshed the store to a current token, is measured and
// published; it is not reported as a missing login with token_expired (hk 3783).
func TestFleetQuotaClaudeRefreshedByFailingCLIIsNotLoginMissing(t *testing.T) {
	hk := newClaudeHK(t)
	client := quotaClient(t)
	client.spawnConfigPath = claudeStore(t, hk.server.URL)
	files := claudeLoginFiles(claudeSecret, claudeTestNow.Add(-time.Hour))
	apiCalls := 0
	api := claudeAPIFunc(func(r *http.Request) (*http.Response, error) {
		apiCalls++
		return claudeAPIReply(200, claudeAPIBody)(r)
	})
	src := claudeTestSource(client.spawnConfigPath, files, api)
	cliCalls := 0
	src.Claude.RunCLI = func(context.Context, string, string, string) (string, error) {
		cliCalls++
		files["/home/u/.claude/.credentials.json"] = claudeLoginFiles(claudeRefreshedSecret, claudeTestNow.Add(time.Hour))["/home/u/.claude/.credentials.json"]
		return "", errors.New("usage command failed")
	}
	client.nativeQuota = map[string]*nativeQuotaSource{"claude": src}
	var warnings []string
	client.warn = func(m string) { warnings = append(warnings, m) }

	client.refreshFleetQuota(t.Context(), "claude", map[string]fleetQuotaSent{})

	if cliCalls != 1 || apiCalls != 1 {
		t.Fatalf("cli calls = %d, API calls = %d, want 1 and 1", cliCalls, apiCalls)
	}
	if got := poolLogin(t, client, "claude").Login; got != fleet.ReadinessLoginPresent {
		t.Fatalf("login = %q, want present", got)
	}
	for _, w := range warnings {
		if strings.Contains(w, "token_expired") {
			t.Fatalf("warning %q names token_expired", w)
		}
	}
	if hk.published() != 1 {
		t.Fatalf("published %d observations, want 1", hk.published())
	}
}
