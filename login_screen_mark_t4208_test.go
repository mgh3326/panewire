package panewire

import (
	"testing"
	"time"

	"github.com/mgh3326/panewire/internal/fleet"
)

func TestHubQuotaRefreshUnshareableLoginStaysUnknownT4208(t *testing.T) {
	files := claudeLoginFiles(claudeSecret, claudeTestNow.Add(time.Hour))
	files["/vault/.credentials.json"] = files["/home/u/.claude/.credentials.json"]
	files["/cfg/a/.claude.json"] = files["/home/u/.claude.json"]
	source := claudeTestSource("", files, claudeAPIReply(200, claudeAPIBody))
	source.Claude.LookupEnv = func(name string) (string, bool) {
		switch name {
		case "CLAUDE_CONFIG_DIR":
			return "/cfg/a", true
		case "CLAUDE_SECURESTORAGE_CONFIG_DIR":
			return "/vault", true
		default:
			return "", false
		}
	}
	client := quotaClient(t)
	client.nativeQuota = map[string]*nativeQuotaSource{"claude": source}

	if _, failure := client.nodeQuotaRefresh(t.Context(), nodeQuotaRefreshArgs{Pool: "claude"}); failure != "" {
		t.Fatalf("quota.refresh failed: %q", failure)
	}
	if got := poolLogin(t, client, "claude").Login; got != fleet.ReadinessLoginUnknown {
		t.Fatalf("unshareable successful measurement login = %q, want unknown", got)
	}
}
