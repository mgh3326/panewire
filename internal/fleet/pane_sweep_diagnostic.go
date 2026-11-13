package fleet

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"time"

	"github.com/mgh3326/panewire/internal/durable"
)

const paneSweepDiagnosticDir = "pane-sweep-diagnostics"

type paneSweepDiagnostic struct {
	Pane      string `json:"pane"`
	CaptureID string `json:"capture_id"`
	Ownership string `json:"ownership"`
	Captured  string `json:"captured_at"`
	Screen    string `json:"screen"`
}

// spoolPaneDiagnostic keeps a screen as a node-local, explicitly unattributed
// diagnostic. It carries no job or task identity and is never uploaded.
func (a *App) spoolPaneDiagnostic(pane string, screen []byte) error {
	instance := make([]byte, 16)
	if _, err := rand.Read(instance); err != nil {
		return err
	}
	captureID := hex.EncodeToString(instance)
	paneHash := sha256.Sum256([]byte(pane))
	dir := filepath.Join(a.stateDir(), paneSweepDiagnosticDir, hex.EncodeToString(paneHash[:]))
	if err := os.MkdirAll(dir, 0700); err != nil {
		return err
	}
	if err := os.Chmod(dir, 0700); err != nil {
		return err
	}
	record := paneSweepDiagnostic{
		Pane: pane, CaptureID: captureID, Ownership: "unknown",
		Captured: a.Now().UTC().Format(time.RFC3339Nano), Screen: string(screen),
	}
	body, err := json.Marshal(record)
	if err != nil {
		return err
	}
	// Keep only the latest observation per pane so repeated daemon passes do
	// not grow the diagnostic spool without bound. CaptureID distinguishes the
	// pane read represented by this replace.
	path := filepath.Join(dir, "latest.json")
	return durable.ReplaceFileAtomically(path, ".capture-*", body, 0600)
}
