package fleet

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"os"
	"path/filepath"
)

// eventBriefSent is the receipt a node writes once the brief is in the pane
// (sendBrief returned) and before it waits to see the pane take it up. It is
// the spawn evidence of a pane whose submission the node could not confirm
// (rc 76): the pane may still have run, and `done` then needs a receipt that
// the brief was delivered to that pane (hk 5174).
const eventBriefSent = "job.brief_sent"

// recordBriefSent writes the job.brief_sent receipt. It carries the whole
// spawn receipt (spawnReceipt) the pane would have been recorded with, and the
// digest of the brief file the pane was told to read.
func (a *App) recordBriefSent(ctx context.Context, s Spawn, seat Seat, launch Launch, pane, tab, pool, owner, payload string) error {
	sum := sha256.Sum256([]byte(payload))
	receipt := a.spawnReceipt(ctx, s, seat, launch, pane, tab, pool, owner, a.machine())
	receipt["brief_path"] = filepath.Join(a.Jobs, s.Job, "brief-delivered.md")
	receipt["brief_sha256"] = hex.EncodeToString(sum[:])
	return a.event(ctx, s.Job, eventBriefSent, receipt)
}

// adoptBriefSent gives a job with no job.spawned the spawn receipt its
// validated job.brief_sent stands for, as a job.spawned event, and folds the job
// again. The late pane then completes as a spawned one would: its pane, label
// and session are on the job, so the completion names them, usage is read and
// the pane sweep can reap it. A job that has a spawn receipt, or no valid
// brief receipt, is returned unchanged.
func (a *App) adoptBriefSent(ctx context.Context, j Job) (Job, error) {
	if j.Receipt != nil || j.Pane != "" {
		return j, nil
	}
	sent := a.briefSentReceipt(j.Events, j.ID)
	if sent == nil {
		return j, nil
	}
	receipt := make(map[string]any, len(sent))
	for k, v := range sent {
		if k != "brief_path" && k != "brief_sha256" {
			receipt[k] = v
		}
	}
	receipt["adopted_from"] = eventBriefSent
	// A revocation is written without this job's locks: read the ledger again
	// before the write. The adopted receipt cannot clear one (reassignedInLedger),
	// so a revocation landing after this check still stops every later write.
	if e := a.refuseReassigned(j); e != nil {
		return j, e
	}
	if e := a.event(ctx, j.ID, "job.spawned", receipt); e != nil {
		return j, e
	}
	return a.job(j.ID)
}

// briefSentReceipt is the spawn receipt `done` may adopt when the job has no
// job.spawned event: the newest job.brief_sent of the
// job's current attempt, accepted only when
//   - the spawn that sent it has ended in failure (job.spawn_retryable or
//     job.cleanup_pending after it), so no Spawn is still confirming the brief
//     and about to write its own job.spawned over a completion,
//   - no claim, reclaim, loss, revocation or reap came after it, so it belongs
//     to this attempt's pane and not to an earlier one that was closed, and
//   - the brief file it names still exists in this job's directory with the
//     digest it recorded, so the receipt and the bytes the pane was told to
//     read agree.
//
// A pane that never received the brief has no such receipt: the node writes it
// only after the send, into its own ledger, and no panewire command appends an
// event of this kind.
func (a *App) briefSentReceipt(events []Event, id string) map[string]any {
	var receipt map[string]any
	ended := false
	// Ledger sequence, not timestamp: a node clock stepped back before a retry
	// of the same job would otherwise sort the retry's claim and receipt before
	// the first attempt's, and the closed pane of the first would look current.
	for _, event := range ledgerOrder(events) {
		switch event.Kind {
		case eventBriefSent:
			receipt, ended = event.Payload, false
		case "job.spawn_retryable", "job.cleanup_pending":
			// The spawn's failure path ran to its end (the markers it writes
			// last). Until then Spawn may still confirm the brief and write its
			// own job.spawned, and a completion adopted now would be revived by it.
			ended = receipt != nil
		case "job.claim", "job.claimed", "job.reclaim", "job.lost", "job.revoked", "job.reaped",
			// A --job-dup-ok respawn of the same id begins an attempt with no
			// claim record: job.duplicate_override, then job.placement. Either
			// ends the earlier attempt's receipt.
			"job.duplicate_override", "job.placement":
			receipt, ended = nil, false
		}
	}
	if receipt == nil || !ended || str(receipt, "pane_id") == "" {
		return nil
	}
	path := filepath.Join(a.Jobs, id, "brief-delivered.md")
	if str(receipt, "brief_path") != path {
		return nil
	}
	body, err := os.ReadFile(path)
	if err != nil {
		return nil
	}
	sum := sha256.Sum256(body)
	if hex.EncodeToString(sum[:]) != str(receipt, "brief_sha256") {
		return nil
	}
	return receipt
}
