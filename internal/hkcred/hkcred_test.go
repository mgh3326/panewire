package hkcred

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func isolate(t *testing.T) string {
	t.Helper()
	home := t.TempDir()
	t.Setenv("HOME", home)
	for _, key := range []string{URLKey, TokenKey, CFIDKey, CFSecretKey, PathEnv, explicitFileLegacyTokenKey} {
		t.Setenv(key, "")
	}
	return home
}

func TestLoadEnvironmentNeedsBothKeys(t *testing.T) {
	home := isolate(t)
	t.Setenv(URLKey, "https://hk.example")
	_, err := Load()
	var unconfigured *UnconfiguredError
	if !errors.As(err, &unconfigured) || !unconfigured.Absent || unconfigured.Path != filepath.Join(home, ".config", "panewire", "hk-svc.env") {
		t.Fatalf("URL alone must fall through to the file: %v", err)
	}
	t.Setenv(TokenKey, "svc")
	creds, err := Load()
	if err != nil || creds != (Credentials{URL: "https://hk.example", Token: "svc"}) {
		t.Fatalf("creds=%+v err=%v", creds, err)
	}
}

func TestLoadRefusesHalfACFPair(t *testing.T) {
	isolate(t)
	t.Setenv(URLKey, "https://hk.example")
	t.Setenv(TokenKey, "svc")
	t.Setenv(CFIDKey, "cf-id-value")
	_, err := Load()
	if err == nil || !strings.Contains(err.Error(), CFSecretKey) || strings.Contains(err.Error(), "cf-id-value") {
		t.Fatalf("half CF pair: %v", err)
	}
}

func TestLoadNoHomeNamesTheKeys(t *testing.T) {
	isolate(t)
	t.Setenv("HOME", "")
	_, err := Load()
	if err == nil || !strings.Contains(err.Error(), TokenKey) || !strings.Contains(err.Error(), PathEnv) {
		t.Fatalf("no home: %v", err)
	}
}

func TestLoadFileIgnoresLegacyKey(t *testing.T) {
	home := isolate(t)
	path := filepath.Join(home, ".config", "panewire", "hk-svc.env")
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte("HANDOFFKEEP_URL=https://hk.example\nHANDOFFKEEP_TOKEN=legacy-value\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	_, err := Load()
	if err == nil || !strings.Contains(err.Error(), TokenKey) || strings.Contains(err.Error(), "legacy-value") {
		t.Fatalf("the default file must not accept the legacy key: %v", err)
	}
}

func TestExplicitFileToken(t *testing.T) {
	for _, tc := range []struct {
		values map[string]string
		want   string
	}{
		{map[string]string{TokenKey: "svc", explicitFileLegacyTokenKey: "legacy"}, "svc"},
		{map[string]string{explicitFileLegacyTokenKey: "legacy"}, "legacy"},
		{map[string]string{}, ""},
	} {
		if got := ExplicitFileToken(tc.values); got != tc.want {
			t.Errorf("ExplicitFileToken(%v)=%q, want %q", tc.values, got, tc.want)
		}
	}
}
