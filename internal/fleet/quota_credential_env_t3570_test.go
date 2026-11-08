package fleet

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

const credentialEnvRetiredWarning = "credential_env is retired; panewire reads HANDOFFKEEP_TOKEN_SVC_PANEWIRE or ~/.config/panewire/hk-svc.env"

func newWithHostsConfig(t *testing.T, body string) string {
	t.Helper()
	isolateSvcCred(t)
	path := filepath.Join(t.TempDir(), "hosts.toml")
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("FLEET_HOSTS_CONFIG", path)
	var errw bytes.Buffer
	if _, err := New(&bytes.Buffer{}, &errw); err != nil {
		t.Fatalf("New: %v", err)
	}
	return errw.String()
}

// A hosts.toml that still sets [quota] credential_env loads, and says once
// that the key no longer does anything, without echoing its value (hk 3570).
func TestQuotaCredentialEnvWarnsItIsRetired(t *testing.T) {
	const value = "HK_PRIVATE_TOKEN_VAR_3570"
	stderr := newWithHostsConfig(t, "[quota]\ncredential_env = \""+value+"\"\n")
	if got := strings.Count(stderr, credentialEnvRetiredWarning); got != 1 {
		t.Fatalf("warning printed %d times, want once; stderr=%q", got, stderr)
	}
	if strings.Contains(stderr, value) {
		t.Fatalf("warning echoes the configured value: %q", stderr)
	}
}

func TestQuotaWithoutCredentialEnvDoesNotWarn(t *testing.T) {
	stderr := newWithHostsConfig(t, "[quota]\nurl = \"https://hk.example\"\n")
	if strings.Contains(stderr, "credential_env") {
		t.Fatalf("warning without credential_env: %q", stderr)
	}
}

// The preflight's transport check compares URLs only: a host whose
// hk-svc.env carries the URL but not yet the token still has a URL to
// compare against, while no token leaves Load.
func TestQuotaURLFromAFileWithoutAToken(t *testing.T) {
	home := isolateSvcCred(t)
	writeSvcCredFile(t, filepath.Join(home, ".config", "panewire", "hk-svc.env"), "HANDOFFKEEP_URL=https://hk.example\n", 0o600)
	a := &App{}
	if got := a.quotaURL(); got != "https://hk.example" {
		t.Fatalf("quotaURL=%q, want the file's URL", got)
	}
}
