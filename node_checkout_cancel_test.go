package panewire

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

type nodeCheckoutLogCapture struct {
	mu      sync.Mutex
	records []slog.Record
}

func (capture *nodeCheckoutLogCapture) Enabled(context.Context, slog.Level) bool { return true }

func (capture *nodeCheckoutLogCapture) Handle(_ context.Context, record slog.Record) error {
	capture.mu.Lock()
	defer capture.mu.Unlock()
	capture.records = append(capture.records, record.Clone())
	return nil
}

func (capture *nodeCheckoutLogCapture) WithAttrs([]slog.Attr) slog.Handler { return capture }

func (capture *nodeCheckoutLogCapture) WithGroup(string) slog.Handler { return capture }

func (capture *nodeCheckoutLogCapture) find(level slog.Level, msg string) []slog.Record {
	capture.mu.Lock()
	defer capture.mu.Unlock()
	var matched []slog.Record
	for _, r := range capture.records {
		if r.Level == level && strings.Contains(r.Message, msg) {
			matched = append(matched, r)
		}
	}
	return matched
}

func recordAttrsMap(record slog.Record) map[string]any {
	attrs := make(map[string]any)
	record.Attrs(func(attr slog.Attr) bool {
		attrs[attr.Key] = attr.Value.Any()
		return true
	})
	return attrs
}

type fakeGitExitError struct {
	code   int
	stderr []byte
}

func (e *fakeGitExitError) Error() string {
	return fmt.Sprintf("exit status %d", e.code)
}

func (e *fakeGitExitError) ExitCode() int {
	return e.code
}

func (e *fakeGitExitError) Stderr() []byte {
	return e.stderr
}

// (a) a git step that blocks until the context deadline -> outcome cancelled and
// checkout_refused:cancelled through the hub_spawn_node.go path.
func TestNodeCheckoutDeadlineReportsCancelled(t *testing.T) {
	root := t.TempDir()
	workRoot := filepath.Join(root, "work")
	if err := os.MkdirAll(workRoot, 0700); err != nil {
		t.Fatal(err)
	}
	config := filepath.Join(root, "spawn.json")
	configText := fmt.Sprintf(`{"work_root":%q,"cwd_map":{"repo-a":"/old/cwd"},"workspace":"worker","herdr_session":"worker","enabled":true}`, workRoot)
	if err := os.WriteFile(config, []byte(configText), 0600); err != nil {
		t.Fatal(err)
	}

	capture := &nodeCheckoutLogCapture{}
	prev := slog.Default()
	slog.SetDefault(slog.New(capture))
	t.Cleanup(func() { slog.SetDefault(prev) })

	client := &HubClient{
		spawnConfigPath:   config,
		spawnTimeoutExtra: -950 * time.Millisecond,
		nodeGit: func(ctx context.Context, args ...string) ([]byte, error) {
			if len(args) == 1 && (args[0] == "--version" || args[0] == "version") {
				return []byte("git version 2.31.0\n"), nil
			}
			<-ctx.Done()
			return nil, ctx.Err()
		},
	}

	checkout := &hubSpawnCheckout{
		Origin: "https://example.invalid/owner/repo.git",
		Mode:   "branch",
		Branch: "main",
		Dir:    "repo.job",
	}

	result := client.runHubSpawn(t.Context(), hubOutboundMessage{
		RequestID:   "req-test-a",
		CWDKey:      "repo-a",
		BriefInline: "test brief",
		Checkout:    checkout,
		WaitSeconds: 1,
	})

	if result.RC != 81 {
		t.Fatalf("result.RC = %d, want 81 (result=%+v)", result.RC, result)
	}
	if result.Error != "checkout_refused:"+nodeOutcomeCancelled {
		t.Fatalf("result.Error = %q, want %q", result.Error, "checkout_refused:"+nodeOutcomeCancelled)
	}

	// Verify INFO log was written for cancelled outcome
	infoRecords := capture.find(slog.LevelInfo, "cancelled")
	if len(infoRecords) == 0 {
		t.Fatalf("expected slog INFO log on checkout cancelled, found none")
	}
	infoAttrs := recordAttrsMap(infoRecords[0])
	if infoAttrs["step"] == "" {
		t.Errorf("expected non-empty step in INFO log: %+v", infoAttrs)
	}
}

// (b) a git step that exits 128 with stderr -> git-failed, and the WARN record
// carries step, exit and a bounded stderr tail but no origin userinfo.
func TestNodeCheckoutGitFailedLogsWarnWithoutUserinfo(t *testing.T) {
	root := t.TempDir()
	workRoot := filepath.Join(root, "work")
	if err := os.MkdirAll(workRoot, 0700); err != nil {
		t.Fatal(err)
	}
	config := filepath.Join(root, "spawn.json")
	configText := fmt.Sprintf(`{"work_root":%q,"cwd_map":{"repo-a":"/old/cwd"},"workspace":"worker","herdr_session":"worker","enabled":true}`, workRoot)
	if err := os.WriteFile(config, []byte(configText), 0600); err != nil {
		t.Fatal(err)
	}

	capture := &nodeCheckoutLogCapture{}
	prev := slog.Default()
	slog.SetDefault(slog.New(capture))
	t.Cleanup(func() { slog.SetDefault(prev) })

	secretOrigin := "https://user:secret@example.invalid/owner/repo.git"
	stderrText := fmt.Sprintf("fatal: unable to access '%s': could not resolve host", secretOrigin)

	client := &HubClient{
		spawnConfigPath: config,
		nodeGit: func(ctx context.Context, args ...string) ([]byte, error) {
			if len(args) == 1 && (args[0] == "--version" || args[0] == "version") {
				return []byte("git version 2.31.0\n"), nil
			}
			return nil, &fakeGitExitError{code: 128, stderr: []byte(stderrText)}
		},
	}

	// First verify through nodeWorktreePrepare directly
	prepared, code := client.nodeWorktreePrepare(t.Context(), nodeWorktreePrepareArgs{
		Origin: "https://example.invalid/owner/repo.git",
		Mode:   "branch",
		Branch: "main",
		Dir:    "repo.job",
	})
	if code != "" {
		t.Fatalf("unexpected code=%q", code)
	}
	res, ok := prepared.(nodeWorktreePrepareResult)
	if !ok || res.Outcome != nodeOutcomeGitFailed {
		t.Fatalf("prepared outcome=%+v, want %q", prepared, nodeOutcomeGitFailed)
	}

	// Verify through runHubSpawn path
	spawnResult := client.runHubSpawn(t.Context(), hubOutboundMessage{
		RequestID:   "req-test-b",
		CWDKey:      "repo-a",
		BriefInline: "test brief",
		Checkout: &hubSpawnCheckout{
			Origin: "https://example.invalid/owner/repo.git",
			Mode:   "branch",
			Branch: "main",
			Dir:    "repo.job",
		},
		WaitSeconds: 10,
	})
	if spawnResult.RC != 81 || spawnResult.Error != "checkout_refused:"+nodeOutcomeGitFailed {
		t.Fatalf("spawnResult=%+v, want RC=81 Error=%q", spawnResult, "checkout_refused:"+nodeOutcomeGitFailed)
	}

	warnRecords := capture.find(slog.LevelWarn, "git failed")
	if len(warnRecords) == 0 {
		t.Fatalf("expected slog WARN log on git-failed, found none")
	}

	// Check each WARN record: must contain step, subcommand, exit, stderr tail, and NO user:secret@
	for _, record := range warnRecords {
		attrs := recordAttrsMap(record)
		if attrs["step"] != "clone" {
			t.Errorf("record step=%v, want 'clone'", attrs["step"])
		}
		if attrs["subcommand"] != "clone" {
			t.Errorf("record subcommand=%v, want 'clone'", attrs["subcommand"])
		}
		exitVal, ok := attrs["exit"]
		if !ok || (exitVal != 128 && exitVal != int64(128)) {
			t.Errorf("record exit=%v, want 128", exitVal)
		}
		stderrVal, ok := attrs["stderr"].(string)
		if !ok || stderrVal == "" {
			t.Errorf("record stderr empty or not string: %v", attrs["stderr"])
		}
		if strings.Contains(stderrVal, "user:secret@") {
			t.Fatalf("stderr tail leaked secret userinfo: %q", stderrVal)
		}
		if !strings.Contains(stderrVal, "[redacted]@") {
			t.Errorf("stderr tail expected '[redacted]@', got: %q", stderrVal)
		}
	}
}

// (c) a canceled context before the step -> cancelled.
func TestNodeCheckoutPreCancelledContext(t *testing.T) {
	root := t.TempDir()
	workRoot := filepath.Join(root, "work")
	if err := os.MkdirAll(workRoot, 0700); err != nil {
		t.Fatal(err)
	}
	config := filepath.Join(root, "spawn.json")
	configText := fmt.Sprintf(`{"work_root":%q,"cwd_map":{"repo-a":"/old/cwd"},"workspace":"worker","herdr_session":"worker","enabled":true}`, workRoot)
	if err := os.WriteFile(config, []byte(configText), 0600); err != nil {
		t.Fatal(err)
	}

	capture := &nodeCheckoutLogCapture{}
	prev := slog.Default()
	slog.SetDefault(slog.New(capture))
	t.Cleanup(func() { slog.SetDefault(prev) })

	client := &HubClient{
		spawnConfigPath: config,
		nodeGit: func(ctx context.Context, args ...string) ([]byte, error) {
			return nil, ctx.Err()
		},
	}

	ctx, cancel := context.WithCancel(t.Context())
	cancel() // canceled before calling

	prepared, code := client.nodeWorktreePrepare(ctx, nodeWorktreePrepareArgs{
		Origin: "https://example.invalid/owner/repo.git",
		Mode:   "branch",
		Branch: "main",
		Dir:    "repo.job",
	})
	if code != "" {
		t.Fatalf("unexpected code=%q", code)
	}
	res, ok := prepared.(nodeWorktreePrepareResult)
	if !ok || res.Outcome != nodeOutcomeCancelled {
		t.Fatalf("prepared outcome=%+v, want %q", prepared, nodeOutcomeCancelled)
	}

	infoRecords := capture.find(slog.LevelInfo, "cancelled")
	if len(infoRecords) == 0 {
		t.Fatalf("expected slog INFO log on pre-cancelled checkout, found none")
	}
}
