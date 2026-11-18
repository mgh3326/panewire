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

// recordBriefSent writes the job.brief_sent receipt. It carries the fields of
// the spawn receipt that a `done` repair needs (localSpawnRepair), the pane the
// brief went to, and the digest of the brief file the pane was told to read.
func (a *App) recordBriefSent(ctx context.Context, s Spawn, seat Seat, launch Launch, pane, pool, owner, payload string) error {
	sum := sha256.Sum256([]byte(payload))
	return a.event(ctx, s.Job, eventBriefSent, map[string]any{
		"pane_id": pane, "label": s.Label, "profile": s.Profile, "launch_profile": launchProfile(seat.Canon, launch.Effort),
		"model_id": launch.ModelID, "effort": launch.Effort, "family": seat.Family, "role": s.Role,
		"owner_lane": owner, "parent_lane": s.Parent, "pool": pool,
		"brief_path": filepath.Join(a.Jobs, s.Job, "brief-delivered.md"), "brief_sha256": hex.EncodeToString(sum[:]),
	})
}

// briefSentReceipt is the spawn receipt a `done` may repair a claimed record
// from when the job has no job.spawned event: the newest job.brief_sent of the
// job's current attempt, accepted only when
//   - no claim, reclaim, loss, revocation or reap came after it, so it belongs
//     to this attempt's pane and not to an earlier one that was closed, and
//   - the brief file it names still exists in this job's directory with the
//     digest it recorded, so the receipt and the bytes the pane was told to
//     read agree.
//
// A pane that never received the brief has no such receipt: the node writes it
// only after the send, into its own ledger, and no panewire command appends an
// event of this kind. A job.spawned event, when present, is the better receipt
// and spawnReceiptForRepair returns it first.
func (a *App) briefSentReceipt(events []Event, id string) map[string]any {
	var receipt map[string]any
	for _, event := range events {
		switch event.Kind {
		case eventBriefSent:
			receipt = event.Payload
		case "job.claim", "job.claimed", "job.reclaim", "job.lost", "job.revoked", "job.reaped":
			receipt = nil
		}
	}
	if receipt == nil || str(receipt, "pane_id") == "" {
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
