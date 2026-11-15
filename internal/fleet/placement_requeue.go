package fleet

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"

	"github.com/mgh3326/panewire/internal/textbound"
)

// RequeuedPlacement is one quarantined record's replay outcome.
type RequeuedPlacement struct {
	Name    string
	Job     string
	Attempt int
	// Role is the work class the replay posted: the record's role mapped
	// through PlacementWorkClass.
	Role string
	// Outcome is delivered, duplicate, refused or pending.
	Outcome string
	// Reason is hk's bounded answer for a refused or still-pending record.
	Reason string
}

// PlacementRequeue is one replay pass over the hub's quarantine.
type PlacementRequeue struct {
	Dir     string
	Records []RequeuedPlacement
}

// RequeueRejectedPlacements re-posts every record quarantined under
// placements-rejected after mapping its role to hk's work class. It is the
// operator's one-shot replay for records refused before that mapping existed
// (hk 4728): a refused record's snapshot is sealed, so the outbox never
// revisits it. A record hk takes or already holds leaves the quarantine with
// its .reason sidecar; a refused or unavailable one stays there for the next
// run. The hub itself never calls this.
func (a *App) RequeueRejectedPlacements(ctx context.Context) (PlacementRequeue, error) {
	dir := filepath.Join(a.stateDir(), "placements-rejected")
	result := PlacementRequeue{Dir: dir}
	entries, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return result, nil
		}
		return result, err
	}
	for _, entry := range entries {
		// A .reason sidecar is not a record: it goes with its record.
		if !entry.Type().IsRegular() || !strings.HasSuffix(entry.Name(), ".json") {
			continue
		}
		row := a.requeueRejectedPlacement(ctx, dir, entry.Name())
		result.Records = append(result.Records, row)
	}
	return result, nil
}

func (a *App) requeueRejectedPlacement(ctx context.Context, dir, name string) RequeuedPlacement {
	path := filepath.Join(dir, name)
	row := RequeuedPlacement{Name: name}
	b, err := os.ReadFile(path)
	var p pendingPlacement
	if err != nil || json.Unmarshal(b, &p) != nil || !validID(p.Record.JobID) || p.Record.Attempt < 1 {
		row.Outcome, row.Reason = "unreadable", "record_unreadable"
		return row
	}
	row.Job, row.Attempt = p.Record.JobID, p.Record.Attempt
	p.Record.Role = PlacementWorkClass(p.Record.Role, "")
	row.Role = p.Record.Role
	d, err := a.EnqueuePlacementRecord(ctx, p.Record)
	switch {
	case err != nil:
		row.Outcome = "pending"
		row.Reason = "enqueue_failed: " + textbound.PrefixWithin(strings.Join(strings.Fields(err.Error()), " "), 200)
	case d.Quarantined:
		// The delivery moved the newly spooled copy back under the same name,
		// so the quarantine keeps this record and its refreshed reason.
		row.Outcome, row.Reason = "refused", d.Reason
	case d.Duplicate:
		row.Outcome = "duplicate"
	case d.Delivered:
		row.Outcome = "delivered"
	default:
		row.Outcome, row.Reason = "pending", d.Reason
	}
	if row.Outcome == "delivered" || row.Outcome == "duplicate" {
		if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
			row.Outcome, row.Reason = "pending", "quarantine_remove_failed"
			return row
		}
		_ = os.Remove(path + ".reason")
		return row
	}
	// A record that did not reach hk stays in the quarantine alone: the spool
	// copy this pass wrote is dropped so a later replay posts it once.
	_ = os.Remove(filepath.Join(a.stateDir(), "placements", name))
	return row
}
