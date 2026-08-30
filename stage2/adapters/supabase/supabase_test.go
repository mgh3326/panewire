package supabase

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestR4RefreshPersistsClientEnvAtomically(t *testing.T) {
	root := t.TempDir()
	envPath := filepath.Join(root, "client.env")
	initial := "# keep this comment and ordering\n" +
		"export PANEWIRE_SUPABASE_URL=\"https://fixture.invalid\"\n" +
		"PANEWIRE_MACHINE_ID=fixture-a\n" +
		"PANEWIRE_SUPABASE_ACCESS_TOKEN=\"expired-access\" # keep access style\n" +
		"PANEWIRE_SUPABASE_REFRESH_TOKEN=expired-refresh\n" +
		"PANEWIRE_SUPABASE_PUBLISHABLE_KEY=fixture-public\n" +
		"UNRELATED_SETTING=preserved\n"
	if err := os.WriteFile(envPath, []byte(initial), 0600); err != nil {
		t.Fatal(err)
	}

	server := newR4RefreshServer(t, "expired-access", "fresh-access", "expired-refresh", "fresh-refresh")
	defer server.Close()
	adapter, err := New(Config{
		BaseURL: server.URL, AccessToken: "expired-access", RefreshToken: "expired-refresh", APIKey: "fixture-public",
		ClientEnvPath: envPath, HTTPClient: server.Client(), AllowInsecureForTests: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	var temporaryDir string
	adapter.createTemp = func(dir, pattern string) (*os.File, error) {
		temporaryDir = dir
		return os.CreateTemp(dir, pattern)
	}

	if health, err := adapter.Health(t.Context()); err != nil || !health.Healthy {
		t.Fatalf("health=%+v err=%v", health, err)
	}
	if temporaryDir != root {
		t.Fatalf("temporary directory=%q, want same directory as env %q", temporaryDir, root)
	}
	if remnants, err := filepath.Glob(filepath.Join(root, ".panewire-client-env-*")); err != nil || len(remnants) != 0 {
		t.Fatalf("temporary remnants=%v err=%v", remnants, err)
	}
	info, err := os.Stat(envPath)
	if err != nil || info.Mode().Perm() != 0600 {
		t.Fatalf("env mode=%v err=%v", info.Mode(), err)
	}
	got, err := os.ReadFile(envPath)
	if err != nil {
		t.Fatal(err)
	}
	want := "# keep this comment and ordering\n" +
		"export PANEWIRE_SUPABASE_URL=\"https://fixture.invalid\"\n" +
		"PANEWIRE_MACHINE_ID=fixture-a\n" +
		"PANEWIRE_SUPABASE_ACCESS_TOKEN=\"fresh-access\" # keep access style\n" +
		"PANEWIRE_SUPABASE_REFRESH_TOKEN=fresh-refresh\n" +
		"PANEWIRE_SUPABASE_PUBLISHABLE_KEY=fixture-public\n" +
		"UNRELATED_SETTING=preserved\n"
	if string(got) != want {
		t.Fatalf("rewritten env changed unrelated content or formatting:\n got=%q\nwant=%q", got, want)
	}
}

func TestR4RefreshPersistenceFailureKeepsTransportWorkingAndWarns(t *testing.T) {
	server := newR4RefreshServer(t, "expired-access", "fresh-access", "expired-refresh", "fresh-refresh")
	defer server.Close()
	adapter, err := New(Config{
		BaseURL: server.URL, AccessToken: "expired-access", RefreshToken: "expired-refresh", APIKey: "fixture-public",
		ClientEnvPath: filepath.Join(t.TempDir(), "client.env"), HTTPClient: server.Client(), AllowInsecureForTests: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	adapter.persistClientEnv = func(string, string, string) error { return errors.New("fixture disk failure") }
	var warnings []string
	adapter.warn = func(message string, args ...any) {
		warnings = append(warnings, message+" "+fmt.Sprint(args...))
	}

	if health, err := adapter.Health(t.Context()); err != nil || !health.Healthy {
		t.Fatalf("health after persistence failure=%+v err=%v", health, err)
	}
	if health, err := adapter.Health(t.Context()); err != nil || !health.Healthy {
		t.Fatalf("second health must use the in-memory refreshed token: health=%+v err=%v", health, err)
	}
	if len(warnings) != 1 || !strings.Contains(warnings[0], "credential persistence failed") {
		t.Fatalf("warnings=%q", warnings)
	}
	for _, secret := range []string{"expired-access", "fresh-access", "expired-refresh", "fresh-refresh"} {
		if strings.Contains(strings.Join(warnings, "\n"), secret) {
			t.Fatalf("warning leaked credential %q: %q", secret, warnings)
		}
	}
}

func newR4RefreshServer(t *testing.T, expiredAccess, freshAccess, oldRefresh, freshRefresh string) *httptest.Server {
	t.Helper()
	var refreshCalls, freshCalls int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/rest/v1/" && r.Header.Get("Authorization") == "Bearer "+expiredAccess:
			http.Error(w, "expired", http.StatusUnauthorized)
		case r.URL.Path == "/auth/v1/token" && r.URL.Query().Get("grant_type") == "refresh_token":
			var request struct {
				RefreshToken string `json:"refresh_token"`
			}
			if r.Header.Get("apikey") != "fixture-public" || json.NewDecoder(r.Body).Decode(&request) != nil || request.RefreshToken != oldRefresh {
				http.Error(w, "bad refresh", http.StatusBadRequest)
				return
			}
			refreshCalls++
			_ = json.NewEncoder(w).Encode(map[string]string{"access_token": freshAccess, "refresh_token": freshRefresh})
		case r.URL.Path == "/rest/v1/" && r.Header.Get("Authorization") == "Bearer "+freshAccess:
			freshCalls++
			w.WriteHeader(http.StatusNoContent)
		default:
			http.Error(w, "unexpected", http.StatusBadRequest)
		}
	}))
	t.Cleanup(func() {
		if refreshCalls != 1 || freshCalls < 1 {
			t.Errorf("refresh=%d fresh=%d", refreshCalls, freshCalls)
		}
	})
	return server
}
