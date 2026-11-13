package panewire

// Contract: docs/relay-harness-delivery.md (workingUnprovable).
//
// hk 3411: a held row for a harness that cannot prove text typed into a
// working pane (cmd) has no working-wait expiry while its job is live. The
// release used to drop such a row as working_wait_expired once max_wait ran
// out, and a cmd worker that works for an hour never got its findings. The row
// now stays held and the existing wake types it when the pane turns idle or
// done. It is dropped only when the pane is gone or reassigned (the lease
// gate), when the job has ended, or when the relay.cmd_hold_max ceiling passes.

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

const (
	// defaultRelayCmdHoldMax is the relay.cmd_hold_max default.
	defaultRelayCmdHoldMax = 24 * time.Hour
	// relayCmdHoldRecheck is how far a working cmd pane's held row moves its
	// deadline at each expiry. The wake at the new deadline checks the job and
	// the ceiling again and reads the screen for a stuck pane (hk 3517,
	// relay_cmd_stale.go); nothing is typed into a pane not proven stale. It is
	// short so a frozen screen is seen within about relay.cmd_stale_after plus
	// one recheck.
	relayCmdHoldRecheck = 5 * time.Minute
	// relayDroppedJobEnded drops a held row whose job has ended. The durable
	// row stays undelivered, as with every other lease expiry.
	relayDroppedJobEnded = "job_ended"
)

// relayCmdHoldMaxFromEnv reads the relay.cmd_hold_max setting from
// PANEWIRE_RELAY_CMD_HOLD_MAX. A missing or invalid value is the default.
func relayCmdHoldMaxFromEnv() time.Duration {
	if value, err := time.ParseDuration(os.Getenv("PANEWIRE_RELAY_CMD_HOLD_MAX")); err == nil && value > 0 {
		return value
	}
	return defaultRelayCmdHoldMax
}

func (client *HubClient) relayCmdHoldMax() time.Duration {
	if client.relayControl.cmdHoldMax > 0 {
		return client.relayControl.cmdHoldMax
	}
	return defaultRelayCmdHoldMax
}

// keepWorkingCmdHold decides an expired held row whose pane is working and
// cannot be typed into (hk 3411); the release filter has already dropped a row
// whose job ended. It reports true when the row stays held, with its deadline
// moved one recheck later (never past the ceiling). False means the ceiling has
// passed and the caller takes the working_wait_expired drop.
func (manager *relayBusyManager) keepWorkingCmdHold(parent context.Context, item relayHeld) bool {
	now := manager.client.nowUTC()
	ceiling := item.HeldSince.Add(manager.client.relayCmdHoldMax())
	if !now.Before(ceiling) {
		return false
	}
	next := now.Add(relayCmdHoldRecheck)
	if next.After(ceiling) {
		next = ceiling
	}
	// The in-memory deadline is what the next wait is armed with; the store
	// keeps it across a node restart. A failed write only loses that.
	manager.mu.Lock()
	if manager.cmdHoldUntil[item.EventID].Before(next) {
		manager.cmdHoldUntil[item.EventID] = next
	}
	for id, until := range manager.cmdHoldUntil {
		if until.Before(now) {
			delete(manager.cmdHoldUntil, id)
		}
	}
	manager.mu.Unlock()
	if store := manager.client.relayStore(); store != nil && next.After(item.HeldSince.Add(item.MaxWait)) {
		_, _ = store.ExtendRelayHeldMaxWait(parent, item.EventID, next.Sub(item.HeldSince))
	}
	return true
}

// relayLaneJobEnded reports whether this node's own record of the job that
// runs in lane says the job has ended. A worker's job id is its lane. No
// record, an unreadable one, a lane that is not a job id, or a job that was
// never claimed proves nothing, and the row keeps its hold up to the ceiling.
func relayLaneJobEnded(inboxRoot, lane string) bool {
	if inboxRoot == "" || !LaneNamePattern.MatchString(lane) {
		return false
	}
	eventsDir := filepath.Join(inboxRoot, "jobs", lane, "events")
	entries, err := os.ReadDir(eventsDir)
	if err != nil {
		return false
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].Name() < entries[j].Name() })
	claimed, ended := false, false
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".json") {
			continue
		}
		contents, err := os.ReadFile(filepath.Join(eventsDir, entry.Name()))
		if err != nil || len(contents) > 16<<10 {
			continue
		}
		var event hubInboxEvent
		if json.Unmarshal(contents, &event) != nil {
			continue
		}
		switch kind := event.eventKind(); {
		case kind == "job.claimed" || kind == "job.claim":
			claimed, ended = true, false
		case hubJobEndKind(kind):
			ended = claimed
		}
	}
	return ended
}
