package panewire

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"time"

	"github.com/mgh3326/panewire/internal/fleet"
)

const legacyPlacementRecordConflict = "legacy_placement_record_conflict"

// recoverDeferredPlacementSnapshot handles saves from before the deferred
// file carried a base. Their origin is ambiguous, so check the existing hk
// key before merging either trace or sealing the attempt.
func (h *HubServer) recoverDeferredPlacementSnapshot(parent context.Context, d placementDeferredSave) (bool, *placementDeferredBase, error) {
	if d.Base != nil {
		return h.inbox.store.saveDeferredPlacementSnapshot(parent, d)
	}
	ctx, cancel := context.WithTimeout(parent, placementRecordDeliveryBudget)
	defer cancel()
	task, err := h.inbox.store.spawnTask(ctx, d.RequestID)
	if err != nil {
		return false, nil, err
	}
	payload, delivery, recorded, err := h.inbox.store.spawnPlacementSnapshot(ctx, d.RequestID, d.Attempt)
	if err != nil {
		return false, nil, err
	}
	base := &placementDeferredBase{Exists: len(payload) != 0, Payload: payload, Delivery: delivery}
	previousDelivery := delivery
	if d.Delivery != "" {
		delivery = d.Delivery
	}
	// A previous emission may have used a different spool directory. Neither
	// copy may be bypassed, even though the failed emission supplies the final
	// delivery settings once both directories are clear.
	if base.Exists && previousDelivery != delivery {
		previousApp, err := h.placementRecordApp(previousDelivery)
		if err != nil {
			return false, base, err
		}
		if previousApp.PlacementSpooled(task.Job, d.Attempt) {
			return false, base, errors.New("legacy placement already has a spool entry")
		}
	}
	app, err := h.placementRecordApp(delivery)
	if err != nil {
		return false, base, err
	}
	if app.PlacementSpooled(task.Job, d.Attempt) {
		return false, base, errors.New("legacy placement already has a spool entry")
	}
	remote, err := app.LookupPlacementRecord(ctx, task.Job, d.Attempt)
	if err != nil {
		return false, base, err
	}
	var previous, deferred fleet.PlacementTrace
	if len(payload) != 0 {
		if err := json.Unmarshal(payload, &previous); err != nil {
			return false, base, err
		}
	}
	if err := json.Unmarshal(d.Payload, &deferred); err != nil {
		return false, base, err
	}
	merged := continueSpawnPlacementTrace(previous, deferred, task.Exclusions)
	if merged.Record == nil && previous.Record != nil {
		record := *previous.Record
		record.Candidates = merged.Candidates
		merged.Record = &record
	}
	if merged.Reason == "quota_refresh_pending" || merged.Record != nil && merged.Record.Reason == "quota_refresh_pending" {
		merged.Record, merged.Outcome, merged.Reason = nil, "", ""
	}
	if task.Attempt != d.Attempt && merged.Record == nil {
		h.noticeLegacyPlacementConflict(ctx, task, d.Attempt)
		return false, base, errors.New("legacy placement final result is unavailable")
	}
	if remote != nil {
		// A pending operation has no final result to compare, and an old
		// attempt needs a self-contained finished record of its own.
		comparable := task.Attempt == d.Attempt && spawnAttemptTerminal(task.Status) || task.Attempt != d.Attempt && merged.Record != nil
		if !comparable || !sameLegacyPlacementRecord(task, merged, d.Attempt, *remote) {
			h.noticeLegacyPlacementConflict(ctx, task, d.Attempt)
			return false, base, errors.New(legacyPlacementRecordConflict)
		}
	} else if recorded {
		return false, base, errors.New("sealed legacy placement is absent from handoffkeep")
	}
	mergedPayload, err := json.Marshal(merged)
	if err != nil {
		return false, base, err
	}
	// The compare-and-swap protects a newer trace while the hk read was in
	// flight. A crash after this write is safe: the legacy file remains until
	// the merged snapshot (and, when present, the seal) is durable.
	write := placementDeferredSave{RequestID: d.RequestID, Attempt: d.Attempt, Payload: mergedPayload, Delivery: delivery, Base: base}
	_, observed, err := h.inbox.store.saveDeferredPlacementSnapshot(ctx, write)
	if err != nil {
		return false, observed, err
	}
	if observed == nil || !observed.Exists || string(observed.Payload) != string(mergedPayload) || observed.Delivery != delivery {
		return false, observed, errors.New("legacy placement snapshot changed during reconciliation")
	}
	if remote != nil && !recorded {
		if err := h.inbox.store.sealReconciledPlacement(ctx, d.RequestID, d.Attempt, mergedPayload, delivery); err != nil {
			return false, observed, err
		}
	}
	return true, observed, nil
}

// A flat legacy trace never recorded the producer version. Only in that
// case is hk authoritative for the version; a finished local record must
// match in full, including its original version. The terminal operation also
// remains authoritative if a stale finished trace lost a timeout race.
func sameLegacyPlacementRecord(task hubSpawnTask, trace fleet.PlacementTrace, attempt int, remote fleet.PlacementRecord) bool {
	version := remote.ToolVersion
	if trace.Record != nil {
		finished := *trace.Record
		finished.JobID, finished.Attempt = task.Job, attempt
		if !samePlacementRecord(remote, finished) {
			return false
		}
		version = finished.ToolVersion
	}
	expected := hubPlacementRecord(task, trace, attempt)
	expected.ToolVersion = version
	return samePlacementRecord(remote, expected)
}

func (s *laneInboxStore) sealReconciledPlacement(parent context.Context, requestID string, attempt int, payload []byte, delivery string) error {
	ctx, done := s.op(parent)
	defer done()
	result, err := s.db.ExecContext(ctx, s.q(`UPDATE spawn_placement_snapshots SET recorded=1 WHERE request_id=? AND attempt=? AND recorded=0 AND payload=? AND delivery=?`), requestID, attempt, string(payload), delivery)
	if err != nil {
		return err
	}
	if n, err := result.RowsAffected(); err != nil || n == 0 {
		if err != nil {
			return err
		}
		return errors.New("legacy placement snapshot changed before sealing")
	}
	return nil
}

func samePlacementRecord(a, b fleet.PlacementRecord) bool {
	// hk stores and compares decided_at at microsecond precision.
	at, err := time.Parse(time.RFC3339Nano, a.DecidedAt)
	if err != nil {
		return false
	}
	bt, err := time.Parse(time.RFC3339Nano, b.DecidedAt)
	if err != nil {
		return false
	}
	a.DecidedAt = at.UTC().Truncate(time.Microsecond).Format(time.RFC3339Nano)
	b.DecidedAt = bt.UTC().Truncate(time.Microsecond).Format(time.RFC3339Nano)
	// Kind is an in-process receipt tag and is not part of the hk record.
	a.Kind, b.Kind = "", ""
	return reflect.DeepEqual(a, b)
}

// The existing spawn-notification outbox makes the owner notice durable and
// deduplicates repeated recovery attempts without changing the spawn outcome.
func (h *HubServer) noticeLegacyPlacementConflict(ctx context.Context, task hubSpawnTask, attempt int) {
	result := hubSpawnOutcome{RC: 1, Stage: "placement_record", Reason: legacyPlacementRecordConflict}
	raw, err := json.Marshal(result)
	if err != nil {
		return
	}
	store := h.inbox.store
	op, done := store.op(ctx)
	defer done()
	id := fmt.Sprintf("%s:%d:placement_record_conflict", task.Request.RequestID, attempt)
	if _, err := store.db.ExecContext(op, store.q(`INSERT INTO spawn_notifications(event_id,request_id,job,owner,result,created_ms) VALUES(?,?,?,?,?,?) ON CONFLICT DO NOTHING`), id, task.Request.RequestID, task.Job, task.Owner, string(raw), h.now().UnixMilli()); err != nil {
		h.logger.Warn("legacy placement conflict notice could not be saved", "request_id", task.Request.RequestID, "attempt", attempt)
	}
}
