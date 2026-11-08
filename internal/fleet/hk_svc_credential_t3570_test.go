package fleet

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
)

// hk 3570: every panewire hk HTTP path that is not handed an explicit file
// authenticates as svc-panewire. The credential comes from HANDOFFKEEP_URL +
// HANDOFFKEEP_TOKEN_SVC_PANEWIRE in the environment, else the mode-0600
// ~/.config/panewire/hk-svc.env (PANEWIRE_HK_ENV overrides the path). The hk
// CLI's config.env and HANDOFFKEEP_TOKEN are never read.

const (
	svcCredToken    = "svc-panewire-token-3570"
	svcCredCLIToken = "cli-config-env-token-3570"
	svcCredLegacy   = "legacy-env-token-3570"
	svcCredCFSecret = "cf-secret-value-3570"
)

// svcCredHK records the bearer each hk route received.
type svcCredHK struct {
	mu   sync.Mutex
	seen map[string][]string
}

func (s *svcCredHK) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	s.seen[r.URL.Path] = append(s.seen[r.URL.Path], r.Header.Get("Authorization"))
	s.mu.Unlock()
	switch {
	case r.URL.Path == "/v1/bench/catalog":
		_, _ = io.WriteString(w, catalogFixture)
	case r.URL.Path == "/v1/quota/presence":
		_, _ = io.WriteString(w, `[]`)
	default:
		_, _ = io.WriteString(w, `{}`)
	}
}

func (s *svcCredHK) bearers(path string) []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.seen[path]...)
}

func (s *svcCredHK) total() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := 0
	for _, v := range s.seen {
		n += len(v)
	}
	return n
}

func newSvcCredHK(t *testing.T) (*svcCredHK, string) {
	t.Helper()
	state := &svcCredHK{seen: map[string][]string{}}
	server := httptest.NewServer(state)
	t.Cleanup(server.Close)
	return state, server.URL
}

// isolateSvcCred gives the test a private HOME and clears every variable the
// old and new credential paths consult.
func isolateSvcCred(t *testing.T) string {
	t.Helper()
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("XDG_CONFIG_HOME", "")
	t.Setenv("XDG_CACHE_HOME", t.TempDir())
	for _, key := range []string{"HANDOFFKEEP_URL", "HANDOFFKEEP_TOKEN", "HANDOFFKEEP_TOKEN_SVC_PANEWIRE", "HANDOFFKEEP_CF_ACCESS_CLIENT_ID", "HANDOFFKEEP_CF_ACCESS_CLIENT_SECRET", "HANDOFFKEEP_CONFIG", "PANEWIRE_HK_ENV"} {
		t.Setenv(key, "")
	}
	return home
}

func writeSvcCredFile(t *testing.T, path, body string, mode os.FileMode) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(body), mode); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(path, mode); err != nil {
		t.Fatal(err)
	}
}

// writeCLIConfigEnv plants the hk CLI's own credential file, which no
// panewire service path may read.
func writeCLIConfigEnv(t *testing.T, home, url string) {
	t.Helper()
	writeSvcCredFile(t, filepath.Join(home, ".config", "handoffkeep", "config.env"), "HANDOFFKEEP_URL="+url+"\nHANDOFFKEEP_TOKEN="+svcCredCLIToken+"\n", 0o600)
}

// svcCredCaller is one switched hk reader: build drives it end to end and
// returns the first error (construction or request).
type svcCredCaller struct {
	name, route string
	call        func(ctx context.Context, t *testing.T) error
}

func svcCredCallers() []svcCredCaller {
	app := func() *App { return &App{Config: Config{}} }
	return []svcCredCaller{
		{name: "catalog", route: "/v1/bench/catalog", call: func(ctx context.Context, t *testing.T) error {
			client, err := NewCatalogClient(CatalogClientConfig{CacheDir: t.TempDir()})
			if err != nil {
				return err
			}
			_, err = client.Read(ctx, false)
			return err
		}},
		{name: "jobs", route: "/v1/jobs/job-3570", call: func(ctx context.Context, t *testing.T) error {
			store, err := app().jobStore()
			if err != nil {
				return err
			}
			_, err = store.Get(ctx, "job-3570")
			return err
		}},
		{name: "bench", route: "/v1/bench/reps", call: func(ctx context.Context, t *testing.T) error {
			store, err := app().benchStore()
			if err != nil {
				return err
			}
			_, _, err = store.ReadReps(ctx, "")
			return err
		}},
		{name: "presence", route: "/v1/quota/presence", call: func(ctx context.Context, t *testing.T) error {
			_, err := fleetSource{app()}.Presence(ctx)
			return err
		}},
		{name: "shared-latest", route: "/v1/quota/accounts/claude:0123456789abcdef/latest", call: func(ctx context.Context, t *testing.T) error {
			_, err := app().sharedLatest(ctx, "claude:0123456789abcdef")
			return err
		}},
	}
}

func TestHKSvcCredentialIgnoresCLIConfigEnv(t *testing.T) {
	for _, caller := range svcCredCallers() {
		t.Run(caller.name, func(t *testing.T) {
			home := isolateSvcCred(t)
			hk, url := newSvcCredHK(t)
			writeCLIConfigEnv(t, home, url)
			err := caller.call(t.Context(), t)
			if err == nil || hk.total() != 0 {
				t.Fatalf("hk CLI config.env was used: err=%v requests=%d", err, hk.total())
			}
		})
	}
}

func TestHKSvcCredentialIgnoresLegacyTokenEnv(t *testing.T) {
	for _, caller := range svcCredCallers() {
		t.Run(caller.name, func(t *testing.T) {
			isolateSvcCred(t)
			hk, url := newSvcCredHK(t)
			t.Setenv("HANDOFFKEEP_URL", url)
			t.Setenv("HANDOFFKEEP_TOKEN", svcCredLegacy)
			err := caller.call(t.Context(), t)
			if err == nil || hk.total() != 0 {
				t.Fatalf("HANDOFFKEEP_TOKEN was used: err=%v requests=%d", err, hk.total())
			}
		})
	}
}

func TestHKSvcCredentialFromEnvironment(t *testing.T) {
	for _, caller := range svcCredCallers() {
		t.Run(caller.name, func(t *testing.T) {
			home := isolateSvcCred(t)
			hk, url := newSvcCredHK(t)
			writeCLIConfigEnv(t, home, url)
			// A panewire file with another token must lose to the environment.
			writeSvcCredFile(t, filepath.Join(home, ".config", "panewire", "hk-svc.env"), "HANDOFFKEEP_URL="+url+"\nHANDOFFKEEP_TOKEN_SVC_PANEWIRE=file-token\n", 0o600)
			t.Setenv("HANDOFFKEEP_URL", url)
			t.Setenv("HANDOFFKEEP_TOKEN", svcCredLegacy)
			t.Setenv("HANDOFFKEEP_TOKEN_SVC_PANEWIRE", svcCredToken)
			if err := caller.call(t.Context(), t); err != nil && hk.total() == 0 {
				t.Fatalf("no request: %v", err)
			}
			if got := hk.bearers(caller.route); len(got) == 0 || got[0] != "Bearer "+svcCredToken {
				t.Fatalf("%s bearers=%q, want the svc token from the environment", caller.route, got)
			}
		})
	}
}

func TestHKSvcCredentialFromPanewireFile(t *testing.T) {
	for _, caller := range svcCredCallers() {
		t.Run(caller.name, func(t *testing.T) {
			home := isolateSvcCred(t)
			hk, url := newSvcCredHK(t)
			writeCLIConfigEnv(t, home, url)
			writeSvcCredFile(t, filepath.Join(home, ".config", "panewire", "hk-svc.env"), "HANDOFFKEEP_URL="+url+"\nHANDOFFKEEP_TOKEN_SVC_PANEWIRE="+svcCredToken+"\nHANDOFFKEEP_TOKEN="+svcCredLegacy+"\n", 0o600)
			if err := caller.call(t.Context(), t); err != nil && hk.total() == 0 {
				t.Fatalf("no request: %v", err)
			}
			if got := hk.bearers(caller.route); len(got) == 0 || got[0] != "Bearer "+svcCredToken {
				t.Fatalf("%s bearers=%q, want the svc token from hk-svc.env", caller.route, got)
			}
		})
	}
}

func TestHKSvcCredentialPathOverride(t *testing.T) {
	for _, caller := range svcCredCallers() {
		t.Run(caller.name, func(t *testing.T) {
			home := isolateSvcCred(t)
			hk, url := newSvcCredHK(t)
			writeSvcCredFile(t, filepath.Join(home, ".config", "panewire", "hk-svc.env"), "HANDOFFKEEP_URL="+url+"\nHANDOFFKEEP_TOKEN_SVC_PANEWIRE=default-path-token\n", 0o600)
			override := filepath.Join(t.TempDir(), "elsewhere.env")
			writeSvcCredFile(t, override, "HANDOFFKEEP_URL="+url+"\nHANDOFFKEEP_TOKEN_SVC_PANEWIRE="+svcCredToken+"\n", 0o600)
			t.Setenv("PANEWIRE_HK_ENV", override)
			if err := caller.call(t.Context(), t); err != nil && hk.total() == 0 {
				t.Fatalf("no request: %v", err)
			}
			if got := hk.bearers(caller.route); len(got) == 0 || got[0] != "Bearer "+svcCredToken {
				t.Fatalf("%s bearers=%q, want the token from PANEWIRE_HK_ENV", caller.route, got)
			}
		})
	}
}

func TestHKSvcCredentialRefusesLooseFile(t *testing.T) {
	for _, caller := range svcCredCallers() {
		t.Run(caller.name, func(t *testing.T) {
			home := isolateSvcCred(t)
			hk, url := newSvcCredHK(t)
			writeSvcCredFile(t, filepath.Join(home, ".config", "panewire", "hk-svc.env"), "HANDOFFKEEP_URL="+url+"\nHANDOFFKEEP_TOKEN_SVC_PANEWIRE="+svcCredToken+"\n", 0o644)
			err := caller.call(t.Context(), t)
			if err == nil || hk.total() != 0 {
				t.Fatalf("mode-0644 file was used: err=%v requests=%d", err, hk.total())
			}
			if !strings.Contains(err.Error(), "hk-svc.env") || !strings.Contains(err.Error(), "0600") {
				t.Fatalf("refusal does not name the file and its mode rule: %v", err)
			}
		})
	}
}

func TestHKSvcCredentialUnconfiguredErrorNamesKeysOnly(t *testing.T) {
	for _, caller := range svcCredCallers() {
		t.Run(caller.name+"/nothing", func(t *testing.T) {
			home := isolateSvcCred(t)
			hk, url := newSvcCredHK(t)
			writeCLIConfigEnv(t, home, url)
			t.Setenv("HANDOFFKEEP_TOKEN", svcCredLegacy)
			err := caller.call(t.Context(), t)
			if err == nil || hk.total() != 0 {
				t.Fatalf("unconfigured caller sent a request: err=%v requests=%d", err, hk.total())
			}
			msg := err.Error()
			if !strings.Contains(msg, "HANDOFFKEEP_TOKEN_SVC_PANEWIRE") || !strings.Contains(msg, "hk-svc.env") || strings.Contains(msg, "\n") {
				t.Fatalf("error does not name the key and file in one line: %q", msg)
			}
			for _, value := range []string{svcCredCLIToken, svcCredLegacy, url} {
				if strings.Contains(msg, value) {
					t.Fatalf("error leaks a value: %q", msg)
				}
			}
		})
		t.Run(caller.name+"/partial-file", func(t *testing.T) {
			home := isolateSvcCred(t)
			hk, url := newSvcCredHK(t)
			writeSvcCredFile(t, filepath.Join(home, ".config", "panewire", "hk-svc.env"), "HANDOFFKEEP_URL="+url+"\nHANDOFFKEEP_CF_ACCESS_CLIENT_ID=cf-id-3570\nHANDOFFKEEP_CF_ACCESS_CLIENT_SECRET="+svcCredCFSecret+"\n", 0o600)
			err := caller.call(t.Context(), t)
			if err == nil || hk.total() != 0 {
				t.Fatalf("file without a token sent a request: err=%v requests=%d", err, hk.total())
			}
			msg := err.Error()
			if !strings.Contains(msg, "HANDOFFKEEP_TOKEN_SVC_PANEWIRE") {
				t.Fatalf("error does not name the missing key: %q", msg)
			}
			for _, value := range []string{url, "cf-id-3570", svcCredCFSecret} {
				if strings.Contains(msg, value) {
					t.Fatalf("error leaks a value: %q", msg)
				}
			}
		})
	}
}
