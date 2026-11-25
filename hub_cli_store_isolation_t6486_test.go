package panewire

import (
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"testing"
)

// hk 6486: TestMain clears PANEWIRE_HUB_DATABASE_URL, so a CLI hub in a test
// opens its --inbox-store file even when the run's environment names a live
// hub store. The variable only reaches the test when TestMain fails to clear
// it, so reproduce the case by starting the run with an unreachable
// placeholder:
//
//	PANEWIRE_HUB_DATABASE_URL=postgres://127.0.0.1:1/unreachable go test -run TestHubCLIIgnoresInheritedDatabaseURL_t6486 .
//
// Without the clear, the hub tries that URL and fails here.
func TestHubCLIIgnoresInheritedDatabaseURL_t6486(t *testing.T) {
	if got := os.Getenv(hubDatabaseURLEnv); got != "" {
		t.Fatalf("%s is still set in the test process, so TestMain did not clear it", hubDatabaseURLEnv)
	}
	dir := t.TempDir()
	authPath := filepath.Join(dir, "hub-auth.env")
	if err := writeFile0600(authPath, "HUB_TOKEN_operator=t6486-op-token\nHUB_TOKEN_host-a=t6486-node-token\n"); err != nil {
		t.Fatal(err)
	}
	inbox := filepath.Join(dir, "inbox.db")
	hub, _, code, err := newHubServerForCLI([]string{"--hub-auth", authPath, "--inbox-store", inbox}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil || code != ExitOK {
		t.Fatalf("hub code=%d err=%v", code, err)
	}
	t.Cleanup(func() { _ = hub.Close() })
	if hub.inbox == nil || hub.inbox.store.dialect != dialectSQLite {
		t.Fatal("the CLI hub did not open its SQLite --inbox-store")
	}
	if _, err := os.Stat(inbox); err != nil {
		t.Fatalf("--inbox-store was not created: %v", err)
	}
}
