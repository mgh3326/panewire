package fleet

import (
	"encoding/json"
	"os"
	"path/filepath"
	"time"
)

// PoolActionFile is the marker a holder node keeps beside its job ledger while
// the hub owns an exhausted-pool action for the job's attempt. The hub registers
// it before it closes the pane; while it is open and fresh the watcher must not
// write its own terminal record, because the hub's action writes the quota
// reason (and, for a hand-over, the successor pointer) and will not be able to
// rewrite an hk record the watcher made terminal first.
const PoolActionFile = "pool-action.json"

// poolActionMarkerTTL bounds how long the watcher defers to a marker. An action
// finishes in seconds; one whose hub is gone must not keep a dead pane's watch
// (and its job record) alive forever.
const poolActionMarkerTTL = 30 * time.Minute

type poolActionMarker struct {
	ActionID  string    `json:"action_id"`
	EndReason string    `json:"end_reason"`
	At        time.Time `json:"at"`
}

// poolActionOpen reports whether the hub registered an exhausted-pool action
// for this job's attempt that may still be running.
func (a *App) poolActionOpen(id string) bool {
	if a.Jobs == "" || id == "" {
		return false
	}
	raw, err := os.ReadFile(filepath.Join(a.Jobs, id, PoolActionFile))
	if err != nil {
		return false
	}
	var marker poolActionMarker
	if json.Unmarshal(raw, &marker) != nil || marker.ActionID == "" || marker.At.IsZero() {
		return false
	}
	return a.Now().Sub(marker.At) < poolActionMarkerTTL
}
