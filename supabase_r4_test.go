package panewire

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/mgh3326/panewire/stage2/adapters/supabase"
)

func TestR4RefreshPersistenceSurvivesAdapterRestart(t *testing.T) {
	root := t.TempDir()
	envPath := filepath.Join(root, "client.env")
	if err := writeClientCredentialEnv(envPath, clientCredentialEnv{
		URL: "http://placeholder.invalid", MachineID: "fixture-a", AccessToken: "expired-access", RefreshToken: "old-refresh", PublishableKey: "fixture-public",
	}); err != nil {
		t.Fatal(err)
	}
	var mu sync.Mutex
	var refreshCalls, restartedFreshCalls int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		switch {
		case r.URL.Path == "/rest/v1/" && r.Header.Get("Authorization") == "Bearer expired-access":
			http.Error(w, "expired", http.StatusUnauthorized)
		case r.URL.Path == "/auth/v1/token" && r.URL.Query().Get("grant_type") == "refresh_token":
			var body struct {
				RefreshToken string `json:"refresh_token"`
			}
			if r.Header.Get("apikey") != "fixture-public" || json.NewDecoder(r.Body).Decode(&body) != nil || body.RefreshToken != "old-refresh" {
				http.Error(w, "bad refresh", http.StatusBadRequest)
				return
			}
			refreshCalls++
			_ = json.NewEncoder(w).Encode(map[string]string{"access_token": "new-access", "refresh_token": "new-refresh"})
		case r.URL.Path == "/rest/v1/" && r.Header.Get("Authorization") == "Bearer new-access":
			restartedFreshCalls++
			w.WriteHeader(http.StatusNoContent)
		default:
			http.Error(w, "unexpected", http.StatusBadRequest)
		}
	}))
	defer server.Close()
	credential, err := loadStage2ClientEnv(envPath)
	if err != nil {
		t.Fatal(err)
	}
	credential.URL = server.URL
	first, err := supabase.New(supabase.Config{
		BaseURL: server.URL, AccessToken: credential.AccessToken, RefreshToken: credential.RefreshToken, APIKey: credential.PublishableKey,
		ClientEnvPath: envPath, HTTPClient: server.Client(), AllowInsecureForTests: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if health, err := first.Health(t.Context()); err != nil || !health.Healthy {
		t.Fatalf("first adapter health=%+v err=%v", health, err)
	}

	// A new daemon assembly reloads the shared env file. The old refresh token
	// is deliberately rejected by the fake, so this succeeds only if L1 wrote
	// the rotated pair before the simulated restart.
	restarted, err := loadStage2ClientEnv(envPath)
	if err != nil {
		t.Fatal(err)
	}
	if restarted.AccessToken != "new-access" || restarted.RefreshToken != "new-refresh" {
		t.Fatalf("restarted credential=%+v", restarted)
	}
	second, err := supabase.New(supabase.Config{
		BaseURL: server.URL, AccessToken: restarted.AccessToken, RefreshToken: restarted.RefreshToken, APIKey: restarted.PublishableKey,
		ClientEnvPath: envPath, HTTPClient: server.Client(), AllowInsecureForTests: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if health, err := second.Health(t.Context()); err != nil || !health.Healthy {
		t.Fatalf("restarted adapter health=%+v err=%v", health, err)
	}
	mu.Lock()
	defer mu.Unlock()
	if refreshCalls != 1 || restartedFreshCalls != 2 {
		t.Fatalf("refresh=%d fresh=%d", refreshCalls, restartedFreshCalls)
	}
}

func TestR4ReEnrollmentMatchesLiveShapesAndUpsertsExistingRegistry(t *testing.T) {
	fixture := newR4EnrollmentFixture("fixture-existing", true)
	server := httptest.NewServer(fixture)
	defer server.Close()
	root := t.TempDir()
	out := filepath.Join(root, "client.env")
	code, stdout, stderr := runR4Enrollment(t, server, "fixture-existing", out)
	if code != ExitOK || stderr != "" {
		t.Fatalf("code=%d stdout=%q stderr=%q", code, stdout, stderr)
	}
	if !strings.Contains(stdout, "CONFIRMED: enrolled") {
		t.Fatalf("success output=%q", stdout)
	}
	credential, err := loadMode0600Env(out)
	if err != nil {
		t.Fatal(err)
	}
	if got := credential["PANEWIRE_SUPABASE_REFRESH_TOKEN"]; got != "fake" || len(got) != 4 {
		t.Fatalf("opaque refresh token=%q len=%d", got, len(got))
	}
	if state := fixture.snapshot(); state.rotations != 1 || state.registryUpserts != 1 || !state.publishablePasswordGrant || !state.mergeDuplicates {
		t.Fatalf("fixture state=%+v", state)
	}
	for _, marker := range []string{"fixture-service-key", "fixture-access-token", "fake"} {
		if strings.Contains(stdout+stderr, marker) {
			t.Fatalf("credential marker %q leaked to output %q / %q", marker, stdout, stderr)
		}
	}
}

func TestR4ReEnrollmentTagsEachStageAndWarnsAfterRotation(t *testing.T) {
	cases := []struct {
		name        string
		existing    bool
		failure     string
		invalidID   bool
		wantStep    string
		wantHTTP    int
		wantRotate  int
		wantWarning bool
	}{
		{name: "admin lookup http failure", existing: true, failure: "lookup", wantStep: "admin_user_lookup", wantHTTP: http.StatusServiceUnavailable},
		{name: "admin lookup parse failure before rotation", existing: true, invalidID: true, wantStep: "admin_user_lookup", wantHTTP: http.StatusOK},
		{name: "admin create", existing: false, failure: "create", wantStep: "admin_user_create", wantHTTP: http.StatusBadGateway},
		{name: "admin rotate", existing: true, failure: "rotate", wantStep: "admin_user_rotate", wantHTTP: http.StatusBadGateway, wantRotate: 1, wantWarning: true},
		{name: "password session", existing: true, failure: "session", wantStep: "session_create", wantHTTP: http.StatusServiceUnavailable, wantRotate: 1, wantWarning: true},
		{name: "registry upsert", existing: true, failure: "registry", wantStep: "registry_upsert", wantHTTP: http.StatusConflict, wantRotate: 1, wantWarning: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fixture := newR4EnrollmentFixture("fixture-existing", tc.existing)
			fixture.failure, fixture.invalidExistingID = tc.failure, tc.invalidID
			server := httptest.NewServer(fixture)
			defer server.Close()
			code, stdout, stderr := runR4Enrollment(t, server, "fixture-existing", filepath.Join(t.TempDir(), "client.env"))
			if code != ExitInternal || stdout != "" {
				t.Fatalf("code=%d stdout=%q stderr=%q", code, stdout, stderr)
			}
			want := "enrollment failed (step=" + tc.wantStep + " http=" + strconvItoa(tc.wantHTTP) + ")"
			if !strings.Contains(stderr, want) {
				t.Fatalf("stderr=%q, want %q", stderr, want)
			}
			if got := strings.Contains(stderr, "existing sessions may have been invalidated"); got != tc.wantWarning {
				t.Fatalf("warning=%v want=%v stderr=%q", got, tc.wantWarning, stderr)
			}
			if got := fixture.snapshot().rotations; got != tc.wantRotate {
				t.Fatalf("rotations=%d want=%d", got, tc.wantRotate)
			}
		})
	}
}

func TestR4CredentialWriteFailureWarnsAfterExistingRotation(t *testing.T) {
	fixture := newR4EnrollmentFixture("fixture-existing", true)
	server := httptest.NewServer(fixture)
	defer server.Close()
	root := t.TempDir()
	// Passing a directory as --out makes the final credential install fail only
	// after the existing user's password was rotated and the session/upsert work
	// completed.
	code, stdout, stderr := runR4Enrollment(t, server, "fixture-existing", root)
	if code != ExitInternal || stdout != "" || !strings.Contains(stderr, "step=client_credential_write") || !strings.Contains(stderr, "existing sessions may have been invalidated") {
		t.Fatalf("code=%d stdout=%q stderr=%q", code, stdout, stderr)
	}
	if state := fixture.snapshot(); state.rotations != 1 || state.registryUpserts != 1 {
		t.Fatalf("fixture state=%+v", state)
	}
}

func runR4Enrollment(t *testing.T, server *httptest.Server, machineID, output string) (int, string, string) {
	t.Helper()
	root := t.TempDir()
	adminEnv := filepath.Join(root, "admin.env")
	if err := os.WriteFile(adminEnv, []byte("SUPABASE_URL="+server.URL+"\nSUPABASE_SECRET_KEY=fixture-service-key\nSUPABASE_PUBLISHABLE_KEY=fixture-publishable\n"), 0600); err != nil {
		t.Fatal(err)
	}
	var stdout, stderr bytes.Buffer
	code := runEnrollMachineCLI([]string{"--admin-env", adminEnv, "--machine-id", machineID, "--out", output, "--confirm"}, &stdout, &stderr, enrollDeps{
		HTTPClient: server.Client(), AllowInsecureForTests: true, Random: bytes.NewReader(bytes.Repeat([]byte{7}, 64)),
	})
	return code, stdout.String(), stderr.String()
}

type r4EnrollmentFixture struct {
	mu                       sync.Mutex
	machineID                string
	existing                 bool
	failure                  string
	invalidExistingID        bool
	rotations                int
	registryUpserts          int
	publishablePasswordGrant bool
	mergeDuplicates          bool
}

type r4EnrollmentSnapshot struct {
	rotations                int
	registryUpserts          int
	publishablePasswordGrant bool
	mergeDuplicates          bool
}

func newR4EnrollmentFixture(machineID string, existing bool) *r4EnrollmentFixture {
	return &r4EnrollmentFixture{machineID: machineID, existing: existing}
}

func (f *r4EnrollmentFixture) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()
	w.Header().Set("Content-Type", "application/json")
	switch {
	case r.Method == http.MethodGet && r.URL.Path == "/auth/v1/admin/users":
		if r.URL.Query().Get("per_page") != "50" {
			http.Error(w, "unexpected admin page size", http.StatusBadRequest)
			return
		}
		if f.failure == "lookup" {
			http.Error(w, "lookup failure", http.StatusServiceUnavailable)
			return
		}
		users := []authAdminUser{}
		if f.existing {
			id := "11111111-1111-1111-1111-111111111111"
			if f.invalidExistingID {
				id = "not-a-uuid"
			}
			users = append(users, authAdminUser{ID: id, Email: machineEmail(f.machineID)})
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"users": users})
	case r.Method == http.MethodPost && r.URL.Path == "/auth/v1/admin/users":
		if f.failure == "create" {
			http.Error(w, "create failure", http.StatusBadGateway)
			return
		}
		_ = json.NewEncoder(w).Encode(authAdminUser{ID: "11111111-1111-1111-1111-111111111111", Email: machineEmail(f.machineID)})
	case r.Method == http.MethodPut && strings.HasPrefix(r.URL.Path, "/auth/v1/admin/users/"):
		f.rotations++
		if f.failure == "rotate" {
			http.Error(w, "rotate failure", http.StatusBadGateway)
			return
		}
		w.WriteHeader(http.StatusOK)
	case r.Method == http.MethodPost && r.URL.Path == "/auth/v1/token":
		if r.URL.Query().Get("grant_type") != "password" || r.Header.Get("apikey") != "fixture-publishable" || r.Header.Get("Authorization") != "" {
			http.Error(w, "publishable password grant required", http.StatusUnauthorized)
			return
		}
		f.publishablePasswordGrant = true
		if f.failure == "session" {
			http.Error(w, "session failure", http.StatusServiceUnavailable)
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]string{"access_token": "fixture-access-token", "refresh_token": "fake"})
	case r.Method == http.MethodPost && r.URL.Path == "/rest/v1/machine_registry":
		if !strings.Contains(r.Header.Get("Prefer"), "resolution=merge-duplicates") {
			http.Error(w, "upsert preference required", http.StatusConflict)
			return
		}
		f.mergeDuplicates = true
		if f.failure == "registry" {
			http.Error(w, "registry conflict", http.StatusConflict)
			return
		}
		f.registryUpserts++
		w.WriteHeader(http.StatusNoContent)
	default:
		http.NotFound(w, r)
	}
}

func (f *r4EnrollmentFixture) snapshot() r4EnrollmentSnapshot {
	f.mu.Lock()
	defer f.mu.Unlock()
	return r4EnrollmentSnapshot{
		rotations: f.rotations, registryUpserts: f.registryUpserts,
		publishablePasswordGrant: f.publishablePasswordGrant, mergeDuplicates: f.mergeDuplicates,
	}
}

func strconvItoa(value int) string {
	if value == 0 {
		return "0"
	}
	var digits [16]byte
	index := len(digits)
	for value > 0 {
		index--
		digits[index] = byte('0' + value%10)
		value /= 10
	}
	return string(digits[index:])
}
