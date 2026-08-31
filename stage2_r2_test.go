package panewire

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/mgh3326/panewire/stage2/core"
)

func TestR2EnrollmentDryRunMakesNoHTTPMutation(t *testing.T) {
	fixture := newR2EnrollmentFixture()
	server := httptest.NewServer(fixture)
	defer server.Close()
	var stdout, stderr bytes.Buffer
	code := runEnrollMachineCLI([]string{
		"--admin-env", filepath.Join(t.TempDir(), "missing.env"),
		"--machine-id", "fixture-a", "--out", filepath.Join(t.TempDir(), "client.env"),
	}, &stdout, &stderr, enrollDeps{HTTPClient: server.Client(), AllowInsecureForTests: true})
	if code != ExitOK || !strings.Contains(stdout.String(), "DRY-RUN") || stderr.Len() != 0 {
		t.Fatalf("dry run code=%d stdout=%q stderr=%q", code, stdout.String(), stderr.String())
	}
	if fixture.callCount() != 0 || fixture.mutationCount() != 0 {
		t.Fatalf("dry run made HTTP calls=%d mutations=%d", fixture.callCount(), fixture.mutationCount())
	}
	t.Logf("dry-run output:\n%s", stdout.String())
}

func TestR2EnrollmentAndIdempotentRevokeUseFixtureOnly(t *testing.T) {
	fixture := newR2EnrollmentFixture()
	server := httptest.NewServer(fixture)
	defer server.Close()
	root := t.TempDir()
	adminEnv := filepath.Join(root, "admin.env")
	secretMarker := "fixture-service-key-not-for-output"
	if err := os.WriteFile(adminEnv, []byte("SUPABASE_URL="+server.URL+"\nSUPABASE_SECRET_KEY="+secretMarker+"\nSUPABASE_PUBLISHABLE_KEY=fixture-public-key\n"), 0600); err != nil {
		t.Fatal(err)
	}
	out := filepath.Join(root, "machine-a.env")
	deps := enrollDeps{
		HTTPClient:            server.Client(),
		AllowInsecureForTests: true,
		Now:                   func() time.Time { return time.Date(2026, 8, 29, 12, 0, 0, 0, time.UTC) },
		Random:                bytes.NewReader(bytes.Repeat([]byte{7}, 64)),
	}
	var stdout, stderr bytes.Buffer
	code := runEnrollMachineCLI([]string{"--admin-env", adminEnv, "--machine-id", "fixture-a", "--out", out, "--confirm"}, &stdout, &stderr, deps)
	if code != ExitOK || stderr.Len() != 0 {
		t.Fatalf("enroll code=%d stdout=%q stderr=%q", code, stdout.String(), stderr.String())
	}
	if strings.Contains(stdout.String(), secretMarker) || strings.Contains(stdout.String(), "fixture-access-token") {
		t.Fatalf("credential value leaked to enrollment output: %q", stdout.String())
	}
	info, err := os.Stat(out)
	if err != nil || info.Mode().Perm() != 0600 {
		t.Fatalf("credential output mode=%v err=%v", info.Mode(), err)
	}
	credentials, err := loadMode0600Env(out)
	if err != nil || credentials["PANEWIRE_MACHINE_ID"] != "fixture-a" || credentials["PANEWIRE_SUPABASE_ACCESS_TOKEN"] == "" {
		t.Fatalf("credential output invalid: keys=%d err=%v", len(credentials), err)
	}
	if fixture.registryState("fixture-a") != "active" {
		t.Fatalf("registry was not active")
	}

	for i := 0; i < 2; i++ {
		stdout.Reset()
		stderr.Reset()
		code = runEnrollMachineCLI([]string{"--admin-env", adminEnv, "--machine-id", "fixture-a", "--revoke", "--confirm"}, &stdout, &stderr, deps)
		if code != ExitOK || stderr.Len() != 0 || strings.Contains(stdout.String(), secretMarker) {
			t.Fatalf("revoke iteration=%d code=%d stdout=%q stderr=%q", i, code, stdout.String(), stderr.String())
		}
	}
	if fixture.registryState("fixture-a") != "revoked" || fixture.disabledCount() != 2 {
		t.Fatalf("revoke was not idempotently applied: registry=%q disabled=%d", fixture.registryState("fixture-a"), fixture.disabledCount())
	}
}

func TestR2SmokeDryRunDoesNotReadCredentials(t *testing.T) {
	var stdout, stderr bytes.Buffer
	code := runSmokeSupabaseCLI([]string{
		"--client-env-a", filepath.Join(t.TempDir(), "missing-a.env"),
		"--client-env-b", filepath.Join(t.TempDir(), "missing-b.env"),
		"--inbox-root", filepath.Join(t.TempDir(), "inbox"),
	}, &stdout, &stderr, smokeDeps{})
	if code != ExitOK || !strings.Contains(stdout.String(), "DRY-RUN smoke plan") || stderr.Len() != 0 {
		t.Fatalf("dry run code=%d stdout=%q stderr=%q", code, stdout.String(), stderr.String())
	}
}

func TestR2SmokeFixtureRoundTripAndRLS(t *testing.T) {
	fixture := newR2SmokeFixture(map[string]string{
		"fixture-access-a": "fixture-a",
		"fixture-access-b": "fixture-b",
	})
	server := httptest.NewServer(fixture)
	defer server.Close()
	root := t.TempDir()
	aEnv, bEnv := filepath.Join(root, "a.env"), filepath.Join(root, "b.env")
	for path, machine := range map[string]string{aEnv: "fixture-a", bEnv: "fixture-b"} {
		if err := writeClientCredentialEnv(path, clientCredentialEnv{
			URL: server.URL, MachineID: machine, AccessToken: tokenForFixture(machine), RefreshToken: "fixture-refresh-" + machine, PublishableKey: "fixture-public-key",
		}); err != nil {
			t.Fatal(err)
		}
	}
	var stdout, stderr bytes.Buffer
	code := runSmokeSupabaseCLI([]string{
		"--client-env-a", aEnv, "--client-env-b", bEnv,
		"--inbox-root", filepath.Join(root, "inbox"), "--confirm",
	}, &stdout, &stderr, smokeDeps{
		HTTPClient: server.Client(), AllowInsecureForTests: true,
	})
	if code != ExitOK || stderr.Len() != 0 || strings.Contains(stdout.String(), "FAIL") {
		t.Fatalf("smoke code=%d stdout=%q stderr=%q", code, stdout.String(), stderr.String())
	}
	for _, required := range []string{"A publish", "RLS: A claim of B destination", "RLS: publishable-key select", "ack body erasure", "completion"} {
		if !strings.Contains(stdout.String(), required) {
			t.Fatalf("smoke output is missing %q: %q", required, stdout.String())
		}
	}
	for _, forbidden := range []string{"fixture-access-a", "fixture-access-b", "fixture-public-key"} {
		if strings.Contains(stdout.String(), forbidden) {
			t.Fatalf("fixture credential leaked to smoke output: %q", stdout.String())
		}
	}
	if !fixture.allBodiesErased() {
		t.Fatal("smoke fixture retained an acknowledged transport body")
	}
	t.Logf("fixture smoke output:\n%s", stdout.String())
}

func tokenForFixture(machine string) string {
	if machine == "fixture-a" {
		return "fixture-access-a"
	}
	return "fixture-access-b"
}

type r2EnrollmentFixture struct {
	mu        sync.Mutex
	users     map[string]authAdminUser
	registry  map[string]string
	calls     int
	mutations int
	disabled  int
}

func newR2EnrollmentFixture() *r2EnrollmentFixture {
	return &r2EnrollmentFixture{users: map[string]authAdminUser{}, registry: map[string]string{}}
}

func (f *r2EnrollmentFixture) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	w.Header().Set("Content-Type", "application/json")
	switch {
	case r.Method == http.MethodGet && r.URL.Path == "/auth/v1/admin/users":
		users := make([]authAdminUser, 0, len(f.users))
		for _, user := range f.users {
			users = append(users, user)
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"users": users})
	case r.Method == http.MethodPost && r.URL.Path == "/auth/v1/admin/users":
		var input struct {
			Email string `json:"email"`
		}
		if json.NewDecoder(r.Body).Decode(&input) != nil || input.Email == "" {
			http.Error(w, "bad user", http.StatusBadRequest)
			return
		}
		user := authAdminUser{ID: "11111111-1111-1111-1111-111111111111", Email: input.Email}
		f.users[input.Email] = user
		f.mutations++
		_ = json.NewEncoder(w).Encode(user)
	case r.Method == http.MethodPut && strings.HasPrefix(r.URL.Path, "/auth/v1/admin/users/"):
		var input map[string]any
		_ = json.NewDecoder(r.Body).Decode(&input)
		if input["ban_duration"] == "876000h" {
			f.disabled++
		}
		f.mutations++
		_ = json.NewEncoder(w).Encode(authAdminUser{ID: "11111111-1111-1111-1111-111111111111", Email: machineEmail("fixture-a")})
	case r.Method == http.MethodPost && r.URL.Path == "/auth/v1/token":
		f.mutations++
		_ = json.NewEncoder(w).Encode(map[string]string{"access_token": "fixture-access-token", "refresh_token": "fixture-refresh-token"})
	case r.Method == http.MethodPost && r.URL.Path == "/rest/v1/machine_registry":
		var rows []struct {
			MachineID string `json:"machine_id"`
			State     string `json:"state"`
		}
		if json.NewDecoder(r.Body).Decode(&rows) != nil || len(rows) != 1 {
			http.Error(w, "bad registry", http.StatusBadRequest)
			return
		}
		f.registry[rows[0].MachineID] = rows[0].State
		f.mutations++
		w.WriteHeader(http.StatusNoContent)
	case r.Method == http.MethodPatch && r.URL.Path == "/rest/v1/machine_registry":
		machineID := strings.TrimPrefix(r.URL.Query().Get("machine_id"), "eq.")
		f.registry[machineID] = "revoked"
		f.mutations++
		w.WriteHeader(http.StatusNoContent)
	default:
		http.NotFound(w, r)
	}
}

func (f *r2EnrollmentFixture) callCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls
}
func (f *r2EnrollmentFixture) mutationCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.mutations
}
func (f *r2EnrollmentFixture) registryState(machine string) string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.registry[machine]
}
func (f *r2EnrollmentFixture) disabledCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.disabled
}

type r2SmokeRow struct {
	envelope core.Envelope
	body     []byte
	dest     string
	state    string
	token    string
	claimant string
}

type r2SmokeFixture struct {
	mu         sync.Mutex
	identities map[string]string
	rows       map[string]*r2SmokeRow
	sequence   int
}

func newR2SmokeFixture(identities map[string]string) *r2SmokeFixture {
	return &r2SmokeFixture{identities: identities, rows: map[string]*r2SmokeRow{}}
}

func (f *r2SmokeFixture) machine(r *http.Request) string {
	return f.identities[strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")]
}

func (f *r2SmokeFixture) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	f.mu.Lock()
	defer f.mu.Unlock()
	if r.Method == http.MethodGet && r.URL.Path == "/rest/v1/message_queue" {
		if r.Header.Get("Authorization") != "" || r.Header.Get("apikey") != "fixture-public-key" {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		_ = json.NewEncoder(w).Encode([]any{})
		return
	}
	switch r.URL.Path {
	case "/rest/v1/rpc/panewire_publish":
		machine := f.machine(r)
		var input struct {
			Envelope   core.Envelope `json:"p_envelope"`
			PayloadB64 string        `json:"p_payload_b64"`
		}
		if machine == "" || json.NewDecoder(r.Body).Decode(&input) != nil || input.Envelope.Source.MachineID != machine {
			http.Error(w, "denied", http.StatusForbidden)
			return
		}
		body, err := base64.StdEncoding.DecodeString(input.PayloadB64)
		if err != nil {
			http.Error(w, "bad body", http.StatusBadRequest)
			return
		}
		_, duplicate := f.rows[input.Envelope.DeliveryID]
		if !duplicate {
			f.rows[input.Envelope.DeliveryID] = &r2SmokeRow{envelope: input.Envelope, body: body, dest: input.Envelope.Destination.MachineID, state: "ready"}
		}
		_ = json.NewEncoder(w).Encode([]map[string]any{{"message_id": input.Envelope.MessageID, "delivery_id": input.Envelope.DeliveryID, "accepted_at": time.Now().UTC(), "duplicate": duplicate}})
	case "/rest/v1/rpc/panewire_claim":
		machine := f.machine(r)
		var input struct {
			Destination string `json:"p_destination_machine_id"`
		}
		if json.NewDecoder(r.Body).Decode(&input) != nil || machine == "" || input.Destination != machine {
			http.Error(w, "denied", http.StatusForbidden)
			return
		}
		rows := make([]map[string]any, 0)
		for _, row := range f.rows {
			if row.dest != machine || row.state == "acked" || row.state == "rejected" {
				continue
			}
			f.sequence++
			row.state, row.claimant, row.token = "claimed", machine, "fixture-claim-"+strconv.Itoa(f.sequence)
			rows = append(rows, map[string]any{"token": row.token, "destination_machine_id": row.dest, "visibility_deadline": time.Now().UTC().Add(time.Minute), "envelope": row.envelope})
		}
		_ = json.NewEncoder(w).Encode(rows)
	case "/rest/v1/rpc/panewire_fetch_payload":
		machine := f.machine(r)
		var input struct {
			Token string `json:"p_token"`
		}
		_ = json.NewDecoder(r.Body).Decode(&input)
		for _, row := range f.rows {
			if row.token == input.Token && row.claimant == machine && row.state == "claimed" {
				_ = json.NewEncoder(w).Encode([]map[string]string{{"payload_b64": base64.StdEncoding.EncodeToString(row.body)}})
				return
			}
		}
		http.Error(w, "denied", http.StatusForbidden)
	case "/rest/v1/rpc/panewire_ack":
		machine := f.machine(r)
		var input struct {
			Token       string `json:"p_token"`
			Disposition string `json:"p_disposition"`
		}
		_ = json.NewDecoder(r.Body).Decode(&input)
		for _, row := range f.rows {
			if row.token == input.Token && row.claimant == machine && row.state == "claimed" {
				if input.Disposition == string(core.AckTerminalReject) {
					row.state = "rejected"
				} else {
					row.state = "acked"
				}
				row.body = nil
				w.WriteHeader(http.StatusNoContent)
				return
			}
		}
		http.Error(w, "denied", http.StatusForbidden)
	case "/rest/v1/rpc/panewire_message_status":
		machine := f.machine(r)
		var input struct {
			DeliveryID string `json:"p_delivery_id"`
		}
		_ = json.NewDecoder(r.Body).Decode(&input)
		if row := f.rows[input.DeliveryID]; row != nil && row.dest == machine {
			_ = json.NewEncoder(w).Encode([]map[string]any{{"state": row.state, "body_erased": row.body == nil, "acked_at": time.Now().UTC()}})
			return
		}
		http.Error(w, "denied", http.StatusForbidden)
	default:
		http.NotFound(w, r)
	}
}

func (f *r2SmokeFixture) allBodiesErased() bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, row := range f.rows {
		if row.body != nil {
			return false
		}
	}
	return len(f.rows) > 0
}
