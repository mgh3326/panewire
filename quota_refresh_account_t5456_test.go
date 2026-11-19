package panewire

import (
	"strings"
	"testing"
	"time"
)

// hk 5456: a refresh that names an account is answered only when the node's
// readings are for that account. A host that measured another account answers
// account_mismatch, so the hub records a named failure instead of a silent
// success for a reading it cannot use.
func TestNodeQuotaRefreshAnswersAccountMismatch_t5456(t *testing.T) {
	hk := newClaudeHK(t)
	client := &HubClient{}
	client.spawnConfigPath = claudeStore(t, hk.server.URL)
	client.nativeQuota = map[string]*nativeQuotaSource{"claude": claudeTestSource(client.spawnConfigPath, claudeLoginFiles(claudeSecret, claudeTestNow.Add(time.Hour)), claudeAPIReply(200, claudeAPIBody))}
	// The first refresh publishes this host's own account; the store path
	// names it, which is the key the hub asks for.
	if result, code := nodeRun(t, client, nodeCommandQuotaRefresh, map[string]any{"pool": "claude"}); code != "" || result["outcome"] != nodeQuotaRefreshed {
		t.Fatalf("refresh without an account: result=%v code=%q", result, code)
	}
	key := claudeMeasuredKey(t, hk)
	if result, code := nodeRun(t, client, nodeCommandQuotaRefresh, map[string]any{"pool": "claude", "account_key": key}); code != "" || result["outcome"] != nodeQuotaRefreshed {
		t.Fatalf("refresh for %s: result=%v code=%q, want refreshed", key, result, code)
	}
	if result, code := nodeRun(t, client, nodeCommandQuotaRefresh, map[string]any{"pool": "claude", "account_key": "claude:0000000000000000"}); code != "account_mismatch" || result != nil {
		t.Fatalf("refresh for another account: result=%v code=%q, want account_mismatch", result, code)
	}
}

// claudeMeasuredKey is the account key the fake store was asked about.
func claudeMeasuredKey(t *testing.T, hk *claudeHK) string {
	t.Helper()
	hk.mu.Lock()
	defer hk.mu.Unlock()
	const prefix = "/v1/quota/accounts/"
	for _, call := range hk.calls {
		if i := strings.Index(call, prefix); i >= 0 {
			rest := call[i+len(prefix):]
			if key, _, ok := strings.Cut(rest, "/"); ok && strings.HasPrefix(key, "claude:") {
				return key
			}
		}
	}
	t.Fatalf("the refresh did not name an account in its store calls: %v", hk.calls)
	return ""
}
