package panewire

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/mgh3326/panewire/stage2/adapters/supabase"
	"github.com/mgh3326/panewire/stage2/core"
)

func TestR3SubmitValidationAndOutboxList(t *testing.T) {
	root := t.TempDir()
	dbPath := filepath.Join(root, "stage2.sqlite3")
	source := filepath.Join(root, "brief.md")
	if err := os.WriteFile(source, []byte("r3 submit fixture"), 0600); err != nil {
		t.Fatal(err)
	}
	base := []string{
		"--db", dbPath, "--file", source, "--from-machine", "sender",
		"--to", "receiver", "--path", "jobs/r3/brief.md",
	}
	var stdout, stderr bytes.Buffer
	if code := runSubmitCLIWithWriters(base, &stdout, &stderr); code != ExitOK {
		t.Fatalf("submit code=%d stdout=%q stderr=%q", code, stdout.String(), stderr.String())
	}
	if strings.TrimSpace(stdout.String()) == "" {
		t.Fatal("submit did not print a message ID")
	}

	store, err := core.OpenMetadataStore(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	records, err := store.ListOutbox(t.Context(), "")
	_ = store.Close()
	if err != nil || len(records) != 1 || records[0].State != core.OutboxSubmitted || records[0].Classification != core.ClassificationPersonalNonCompany {
		t.Fatalf("records=%+v err=%v", records, err)
	}

	spawnDB := filepath.Join(root, "spawn.sqlite3")
	spawnArgs := append([]string{}, base...)
	spawnArgs[1] = spawnDB
	spawnArgs = append(spawnArgs, "--request-wrk", "--wrk-label", "fixture-spawn")
	stdout.Reset()
	stderr.Reset()
	if code := runSubmitCLIWithWriters(spawnArgs, &stdout, &stderr); code != ExitOK {
		t.Fatalf("spawn submit code=%d stdout=%q stderr=%q", code, stdout.String(), stderr.String())
	}
	spawnStore, err := core.OpenMetadataStore(spawnDB)
	if err != nil {
		t.Fatal(err)
	}
	spawnRecords, err := spawnStore.ListOutbox(t.Context(), "")
	_ = spawnStore.Close()
	if err != nil || len(spawnRecords) != 1 || !spawnRecords[0].Spawn.Requested || spawnRecords[0].Spawn.Label != "fixture-spawn" {
		t.Fatalf("spawn records=%+v err=%v", spawnRecords, err)
	}

	stdout.Reset()
	stderr.Reset()
	if code := runOutboxCLIWithWriters([]string{"list", "--db", dbPath}, &stdout, &stderr); code != ExitOK {
		t.Fatalf("outbox list code=%d stderr=%q", code, stderr.String())
	}
	if !strings.Contains(stdout.String(), "STATE") || !strings.Contains(stdout.String(), "SUBMITTED") || !strings.Contains(stdout.String(), "jobs/r3/brief.md") {
		t.Fatalf("outbox list output=%q", stdout.String())
	}

	completionDB := filepath.Join(root, "completion.sqlite3")
	completionBody := filepath.Join(root, "completion.json")
	if err := os.WriteFile(completionBody, []byte(`{"outcome":"received"}`), 0600); err != nil {
		t.Fatal(err)
	}
	stdout.Reset()
	stderr.Reset()
	if code := runSubmitCLIWithWriters([]string{
		"--db", completionDB, "--file", completionBody, "--from-machine", "receiver", "--to", "sender", "--path", "completions/original.json",
		"--kind", string(core.MessageKindCompletion), "--correlation-id", "original-message", "--causation-id", "received",
	}, &stdout, &stderr); code != ExitOK {
		t.Fatalf("completion submit code=%d stdout=%q stderr=%q", code, stdout.String(), stderr.String())
	}
	completionStore, err := core.OpenMetadataStore(completionDB)
	if err != nil {
		t.Fatal(err)
	}
	completionRecords, err := completionStore.ListOutbox(t.Context(), "")
	_ = completionStore.Close()
	if err != nil || len(completionRecords) != 1 || completionRecords[0].MessageKind != core.MessageKindCompletion || completionRecords[0].MessageID != core.CompletionIDFor("original-message", "received", completionRecords[0].SHA256) {
		t.Fatalf("completion records=%+v err=%v", completionRecords, err)
	}

	for _, tc := range []struct {
		name string
		args []string
	}{
		{name: "logical path traversal", args: append(append([]string{}, base...), "--path", "../escape.md")},
		{name: "company classification", args: append(append([]string{}, base...), "--classification", "company")},
		{name: "unknown classification", args: append(append([]string{}, base...), "--classification", "unknown")},
		{name: "spawn missing label", args: append(append([]string{}, base...), "--request-wrk")},
		{name: "spawn label without request", args: append(append([]string{}, base...), "--wrk-label", "fixture-spawn")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var out, errOut bytes.Buffer
			if code := runSubmitCLIWithWriters(tc.args, &out, &errOut); code != ExitConditionInvalid {
				t.Fatalf("code=%d stdout=%q stderr=%q", code, out.String(), errOut.String())
			}
		})
	}

	overCap := filepath.Join(root, "over-cap.md")
	if err := os.WriteFile(overCap, bytes.Repeat([]byte{'x'}, core.MaxInlineBytes+1), 0600); err != nil {
		t.Fatal(err)
	}
	tooLarge := append([]string{}, base...)
	tooLarge[3] = overCap
	var overOut, overErr bytes.Buffer
	if code := runSubmitCLIWithWriters(tooLarge, &overOut, &overErr); code != ExitConditionInvalid {
		t.Fatalf("oversized submit code=%d stdout=%q stderr=%q", code, overOut.String(), overErr.String())
	}
}

func TestR3DaemonClientEnvModesAndDefaultOff(t *testing.T) {
	root := t.TempDir()
	clientEnv := filepath.Join(root, "client.env")
	if err := writeClientCredentialEnv(clientEnv, clientCredentialEnv{
		URL: "https://fixture.invalid", MachineID: "fixture-a", AccessToken: "access", RefreshToken: "refresh", PublishableKey: "public",
	}); err != nil {
		t.Fatal(err)
	}
	args := []string{
		"--socket", filepath.Join(root, "daemon.sock"), "--db", filepath.Join(root, "stage1.sqlite3"),
		"--stage2-db", filepath.Join(root, "stage2.sqlite3"), "--stage2-client-env", clientEnv,
		"--stage2-inbox-root", filepath.Join(root, "inbox"),
	}
	for _, mode := range []os.FileMode{0640, 0644} {
		if err := os.Chmod(clientEnv, mode); err != nil {
			t.Fatal(err)
		}
		d, code, err := newDaemonForCLI(args, daemonCLIDeps{})
		if d != nil || code != ExitConditionInvalid || err == nil {
			t.Fatalf("mode=%o daemon=%v code=%d err=%v", mode, d, code, err)
		}
	}
	if err := os.Chmod(clientEnv, 0600); err != nil {
		t.Fatal(err)
	}
	spawnPolicy := filepath.Join(root, "spawn-policy.json")
	if err := os.WriteFile(spawnPolicy, []byte(`{"rules":[]}`), 0644); err != nil {
		t.Fatal(err)
	}
	if d, code, err := newDaemonForCLI([]string{
		"--socket", filepath.Join(root, "policy.sock"), "--db", filepath.Join(root, "policy.sqlite3"),
		"--stage2-client-env", clientEnv, "--stage2-inbox-root", filepath.Join(root, "policy-inbox"),
		"--stage2-spawn-policy", spawnPolicy,
	}, daemonCLIDeps{}); d != nil || code != ExitConditionInvalid || err == nil {
		t.Fatalf("policy without gate daemon=%v code=%d err=%v", d, code, err)
	}

	legacyInbox := filepath.Join(root, "legacy-inbox")
	d, code, err := newDaemonForCLI([]string{
		"--socket", filepath.Join(root, "legacy.sock"), "--db", filepath.Join(root, "legacy.sqlite3"), "--inbox-root", legacyInbox,
	}, daemonCLIDeps{})
	if err != nil || code != ExitOK || d == nil {
		t.Fatalf("default-off daemon=%v code=%d err=%v", d, code, err)
	}
	if d.cfg.Stage2.Enabled || d.cfg.InboxRoot != legacyInbox {
		t.Fatalf("default-off config=%+v", d.cfg)
	}
}

func TestR3SupabaseRefreshesExpiredAccessToken(t *testing.T) {
	var mu sync.Mutex
	var expiredCalls, refreshedCalls, refreshCalls int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		switch {
		case r.URL.Path == "/rest/v1/" && r.Header.Get("Authorization") == "Bearer expired":
			expiredCalls++
			http.Error(w, "expired", http.StatusUnauthorized)
		case r.URL.Path == "/auth/v1/token" && r.URL.Query().Get("grant_type") == "refresh_token":
			var request struct {
				RefreshToken string `json:"refresh_token"`
			}
			if r.Header.Get("apikey") != "fixture-public" || json.NewDecoder(r.Body).Decode(&request) != nil || request.RefreshToken != "refresh-one" {
				http.Error(w, "bad refresh", http.StatusBadRequest)
				return
			}
			refreshCalls++
			_ = json.NewEncoder(w).Encode(map[string]any{
				"access_token": "fresh", "refresh_token": "refresh-two", "user": map[string]any{"ignored": true},
			})
		case r.URL.Path == "/rest/v1/" && r.Header.Get("Authorization") == "Bearer fresh":
			refreshedCalls++
			w.WriteHeader(http.StatusNoContent)
		default:
			http.Error(w, "unexpected", http.StatusBadRequest)
		}
	}))
	defer server.Close()

	adapter, err := supabase.New(supabase.Config{
		BaseURL: server.URL, AccessToken: "expired", RefreshToken: "refresh-one", APIKey: "fixture-public",
		HTTPClient: server.Client(), AllowInsecureForTests: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if health, err := adapter.Health(t.Context()); err != nil || !health.Healthy {
		t.Fatalf("health=%+v err=%v", health, err)
	}
	if _, err := adapter.Health(t.Context()); err != nil {
		t.Fatalf("second health after refresh: %v", err)
	}
	mu.Lock()
	defer mu.Unlock()
	if expiredCalls != 1 || refreshCalls != 1 || refreshedCalls != 2 {
		t.Fatalf("expired=%d refresh=%d fresh=%d", expiredCalls, refreshCalls, refreshedCalls)
	}
}

func TestR3DaemonStage2E2EWithoutHerdrAndNoWrkGate(t *testing.T) {
	// Keep PATH empty so the stage-1 schema guard proves its existing
	// fail-closed path while the independent stage-2 poll loop continues.
	t.Setenv("PATH", t.TempDir())
	fixture := newR2SmokeFixture(map[string]string{
		"fixture-access-a": "fixture-a",
		"fixture-access-b": "fixture-b",
	})
	server := httptest.NewServer(fixture)
	defer server.Close()

	root := t.TempDir()
	aEnv := filepath.Join(root, "a.env")
	bEnv := filepath.Join(root, "b.env")
	for path, machine := range map[string]string{aEnv: "fixture-a", bEnv: "fixture-b"} {
		if err := writeClientCredentialEnv(path, clientCredentialEnv{
			URL: server.URL, MachineID: machine, AccessToken: tokenForFixture(machine), RefreshToken: "fixture-refresh-" + machine, PublishableKey: "fixture-public-key",
		}); err != nil {
			t.Fatal(err)
		}
	}
	shortRoot, err := os.MkdirTemp("/tmp", "pw-r3-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(shortRoot) })
	logs := &r3LockedBuffer{}
	deps := daemonCLIDeps{
		HTTPClient: server.Client(), AllowInsecureForTests: true,
		Logger: slog.New(slog.NewTextHandler(logs, nil)),
	}
	aStage2 := filepath.Join(root, "a-stage2.sqlite3")
	bStage2 := filepath.Join(root, "b-stage2.sqlite3")
	bInbox := filepath.Join(root, "b-inbox")
	bDaemon, code, err := newDaemonForCLI([]string{
		"--socket", filepath.Join(shortRoot, "b.sock"), "--db", filepath.Join(root, "b-stage1.sqlite3"), "--stage2-db", bStage2,
		"--stage2-client-env", bEnv, "--stage2-inbox-root", bInbox, "--stage2-poll", "10ms",
	}, deps)
	if err != nil || code != ExitOK {
		t.Fatalf("receiver assembly code=%d err=%v", code, err)
	}
	aDaemon, code, err := newDaemonForCLI([]string{
		"--socket", filepath.Join(shortRoot, "a.sock"), "--db", filepath.Join(root, "a-stage1.sqlite3"), "--stage2-db", aStage2,
		"--stage2-client-env", aEnv, "--stage2-inbox-root", filepath.Join(root, "a-inbox"), "--stage2-poll", "10ms",
	}, deps)
	if err != nil || code != ExitOK {
		t.Fatalf("sender assembly code=%d err=%v", code, err)
	}
	ctxB, cancelB := context.WithCancel(t.Context())
	if err := bDaemon.Start(ctxB); err != nil {
		cancelB()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cancelB()
		_ = bDaemon.Stop()
	})
	ctxA, cancelA := context.WithCancel(t.Context())
	if err := aDaemon.Start(ctxA); err != nil {
		cancelA()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cancelA()
		_ = aDaemon.Stop()
	})

	if code := RunCLI([]string{"wait", "--agent", "fixture", "--status", "idle", "--timeout", "100ms"}, CLIConfig{SocketPath: filepath.Join(shortRoot, "b.sock")}); code != ExitDaemonUnavailable {
		t.Fatalf("stage1 capability code=%d, want %d", code, ExitDaemonUnavailable)
	}
	if !strings.Contains(logs.String(), "herdr schema guard failed") {
		t.Fatalf("missing stage1 fail-closed diagnostic: %q", logs.String())
	}

	source := filepath.Join(root, "brief.md")
	body := []byte("stage2 daemon fixture delivery")
	if err := os.WriteFile(source, body, 0600); err != nil {
		t.Fatal(err)
	}
	messageID := r3Submit(t, []string{
		"--db", aStage2, "--file", source, "--from-machine", "fixture-a", "--to", "fixture-b", "--path", "jobs/r3/brief.md",
	})
	finalPath := filepath.Join(bInbox, "jobs", "r3", "brief.md")
	r3Eventually(t, "materialized stage2 delivery", func() bool {
		got, err := os.ReadFile(finalPath)
		return err == nil && bytes.Equal(got, body)
	})
	aStore, err := core.OpenMetadataStore(aStage2)
	if err != nil {
		t.Fatal(err)
	}
	defer aStore.Close()
	bStore, err := core.OpenMetadataStore(bStage2)
	if err != nil {
		t.Fatal(err)
	}
	defer bStore.Close()
	var published core.OutboxRecord
	r3Eventually(t, "sender publish state", func() bool {
		record, found, err := r3OutboxByMessage(t.Context(), aStore, messageID)
		if err != nil || !found || record.State != core.OutboxPublished {
			return false
		}
		published = record
		return true
	})
	r3Eventually(t, "receiver acknowledgement", func() bool {
		record, found, err := bStore.InboxByDelivery(t.Context(), published.DeliveryID)
		return err == nil && found && record.State == core.InboxAcked && record.Acked && record.TerminalReason == ""
	})

	spawnSource := filepath.Join(root, "spawn.md")
	if err := os.WriteFile(spawnSource, []byte("spawn must be rejected without a gate"), 0600); err != nil {
		t.Fatal(err)
	}
	spawnMessageID := r3Submit(t, []string{
		"--db", aStage2, "--file", spawnSource, "--from-machine", "fixture-a", "--to", "fixture-b", "--path", "jobs/r3/spawn.md", "--request-wrk", "--wrk-label", "fixture-spawn",
	})
	var spawnOutbox core.OutboxRecord
	r3Eventually(t, "spawn request publish state", func() bool {
		record, found, err := r3OutboxByMessage(t.Context(), aStore, spawnMessageID)
		if err != nil || !found || record.State != core.OutboxPublished {
			return false
		}
		spawnOutbox = record
		return true
	})
	r3Eventually(t, "gate-not-installed terminal reject", func() bool {
		record, found, err := bStore.InboxByDelivery(t.Context(), spawnOutbox.DeliveryID)
		return err == nil && found && record.State == core.InboxAcked && record.Acked && record.TerminalReason == core.CodeGateNotInstalled
	})
	if !fixture.allBodiesErased() {
		t.Fatal("fixture retained an acknowledged transport body")
	}
}

type r3LockedBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *r3LockedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *r3LockedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

func r3Submit(t *testing.T, args []string) string {
	t.Helper()
	var stdout, stderr bytes.Buffer
	if code := runSubmitCLIWithWriters(args, &stdout, &stderr); code != ExitOK {
		t.Fatalf("submit code=%d stdout=%q stderr=%q", code, stdout.String(), stderr.String())
	}
	messageID := strings.TrimSpace(stdout.String())
	if messageID == "" {
		t.Fatal("submit returned no message ID")
	}
	return messageID
}

func r3OutboxByMessage(ctx context.Context, store *core.MetadataStore, messageID string) (core.OutboxRecord, bool, error) {
	records, err := store.ListOutbox(ctx, "")
	if err != nil {
		return core.OutboxRecord{}, false, err
	}
	for _, record := range records {
		if record.MessageID == messageID {
			return record, true, nil
		}
	}
	return core.OutboxRecord{}, false, nil
}

func r3Eventually(t *testing.T, description string, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", description)
}
