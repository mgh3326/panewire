package fleet

import (
	"context"
	"errors"
	"io/fs"
	"os"
	"slices"
	"time"
)

// The durable hub acknowledgement of a drain-cancelled hub close (hk 5073).
//
// A rollback drain that cancels the intent of a gone or unknown close, whose
// pane is still present, answers `intent_cancelled` to the hub's
// pane.close_status. The hub reads that answer on its own 10 s pass, so the
// local cancel alone does not prove the hub stored it, and a binary rolled
// back right after the drain no longer answers close_status at all. The node
// therefore marks such a cancel as owing an acknowledgement
// (hubCloseAckField), and the hub clears the debt with a pane.close_status
// that carries `ack`, sent only once its guarded action is stored. The node
// records the acknowledgement as a synced job.hub_close_acked event; until
// then PendingHubCloseAckJobs lists the job and the drain does not report
// success. docs/node-commands.md ("Rollback") describes the runbook.
const (
	hubCloseAckKind = "job.hub_close_acked"
	// hubCloseAckField marks a job.hub_close_cancelled event whose hub
	// acknowledgement the drain waits for. Events written before this field
	// existed carry none, so they never hold a drain.
	hubCloseAckField = "hub_ack"
	// hubCloseRollbackUnproven is the cancel reason of a drain that hands a
	// gone or unknown close whose pane is still present back to its watcher.
	hubCloseRollbackUnproven = "rollback_unproven"
)

// RecordHubCloseAck records the hub's acknowledgement that it stored the
// intent_cancelled answer for request id, durably. The caller holds the job's
// end lock.
func RecordHubCloseAck(ctx context.Context, jobs, job, id string) error {
	home, _ := os.UserHomeDir()
	a := &App{Jobs: jobs, Home: home, Now: time.Now}
	return a.eventSynced(ctx, job, hubCloseAckKind, map[string]any{"request_id": id})
}

// unackedHubCloseCancels is the requests of j whose drain cancel still owes
// the hub's acknowledgement, in ledger sequence order. A repeat of a request
// records a new intent and starts over; a settled hub close ends the
// protocol, and pane.close_status answers settled instead of intent_cancelled.
func unackedHubCloseCancels(j Job) []string {
	var owed []string
	for _, ev := range ledgerOrder(j.Events) {
		id := str(ev.Payload, "request_id")
		switch {
		case ev.Kind == hubCloseIntentKind || ev.Kind == hubCloseAckKind:
			owed = slices.DeleteFunc(owed, func(o string) bool { return o == id })
		case ev.Kind == hubCloseCancelKind && boolean(ev.Payload, hubCloseAckField, false):
			if !slices.Contains(owed, id) {
				owed = append(owed, id)
			}
		case hubCloseSettledLoss(ev):
			owed = nil
		}
	}
	return owed
}

// PendingHubCloseAckJobs lists the jobs under the jobs root with a drain
// cancel the hub has not acknowledged, or whose ledger cannot be read. The
// rollback drain reports them as pending after PendingHubCloseJobs is empty.
// It is separate from PendingHubCloseJobs, which the node's settlement and
// watcher fence also use: an unacknowledged cancel needs no settlement.
func PendingHubCloseAckJobs(jobs string) ([]string, error) {
	entries, err := os.ReadDir(jobs)
	if err != nil {
		return nil, err
	}
	a := &App{Jobs: jobs}
	var out []string
	for _, entry := range entries {
		if !entry.IsDir() || !validID(entry.Name()) {
			continue
		}
		evs, err := a.events(entry.Name())
		if errors.Is(err, fs.ErrNotExist) {
			continue
		}
		if err != nil {
			out = append(out, entry.Name())
			continue
		}
		j, err := foldJob(entry.Name(), ledgerOrder(evs))
		if err != nil {
			out = append(out, entry.Name())
			continue
		}
		if len(unackedHubCloseCancels(j)) != 0 {
			out = append(out, entry.Name())
		}
	}
	return out, nil
}
