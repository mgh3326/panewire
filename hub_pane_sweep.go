package panewire

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"strings"
	"time"
)

const (
	defaultHubPaneSweepInterval = 10 * time.Minute
	defaultHubPaneSweepAge      = 60 * time.Minute
	// hubPaneSweepWait bounds one tick's wait for the nodes' replies. A node that
	// stays silent is counted as node_timeout and does not hold the next tick.
	hubPaneSweepWait = 5 * time.Minute
)

// hubPaneSweepTick is one tick's tally, logged as one line by logPaneSweepTick.
type hubPaneSweepTick struct {
	Nodes  int
	Closed []string // machine:job:pane:reason for each pane a node closed
	Failed []string // machine=code for each node that did not close cleanly
}

// runPaneSweep asks every node to close the panes of jobs that ended at least
// paneSweepAge ago, once per paneSweepInterval, until ctx ends.
// docs/pane-sweep.md describes the rule.
func (h *HubServer) runPaneSweep(ctx context.Context) {
	if h.paneSweepInterval <= 0 {
		return
	}
	ticker := time.NewTicker(h.paneSweepInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			h.logPaneSweepTick(h.sweepEndedPanes(ctx))
		}
	}
}

// sweepEndedPanes sends pane.sweep to each known node in machine order and
// waits for the replies under one shared deadline.
func (h *HubServer) sweepEndedPanes(ctx context.Context) hubPaneSweepTick {
	h.mu.Lock()
	machines := make([]string, 0, len(h.nodes))
	for machine := range h.nodes {
		machines = append(machines, machine)
	}
	h.mu.Unlock()
	slices.Sort(machines)
	raw, _ := json.Marshal(nodePaneSweepArgs{AgeS: int64(h.paneSweepAge / time.Second)})
	tick := hubPaneSweepTick{Nodes: len(machines)}
	type waiting struct {
		machine   string
		requestID string
		pending   *hubNodeCommandPending
	}
	var waits []waiting
	for _, machine := range machines {
		pending, requestID, state, sent := h.sendNodeCommand(machine, nodeCommandPaneSweep, raw)
		if !sent {
			tick.Failed = append(tick.Failed, machine+"="+state)
			continue
		}
		waits = append(waits, waiting{machine: machine, requestID: requestID, pending: pending})
	}
	deadline, cancel := context.WithTimeout(ctx, hubPaneSweepWait)
	defer cancel()
	for _, w := range waits {
		reply := h.awaitPaneSweepReply(deadline, w.requestID, w.pending)
		if reply.Error != "" {
			tick.Failed = append(tick.Failed, w.machine+"="+reply.Error)
			continue
		}
		var swept nodePaneSweepResult
		if err := json.Unmarshal(reply.Result, &swept); err != nil {
			tick.Failed = append(tick.Failed, w.machine+"=bad_result")
			continue
		}
		for _, closed := range swept.Closed {
			tick.Closed = append(tick.Closed, fmt.Sprintf("%s:%s:%s:%s", w.machine, closed.Job, closed.Pane, closed.Reason))
		}
	}
	return tick
}

// awaitPaneSweepReply returns one node's reply. A reply that arrived before the
// deadline counts even if the deadline has passed since.
func (h *HubServer) awaitPaneSweepReply(deadline context.Context, requestID string, pending *hubNodeCommandPending) hubNodeCommandResult {
	select {
	case reply := <-pending.done:
		return reply
	case <-deadline.Done():
	}
	select {
	case reply := <-pending.done:
		return reply
	default:
	}
	h.forgetNodeCommand(requestID)
	return hubNodeCommandResult{RequestID: requestID, Op: pending.op, Error: "node_timeout"}
}

// logPaneSweepTick writes one line per tick. It carries IDs and codes only.
func (h *HubServer) logPaneSweepTick(tick hubPaneSweepTick) {
	h.logger.Info("ended pane sweep", "nodes", tick.Nodes, "closed", strings.Join(tick.Closed, " "), "failed", strings.Join(tick.Failed, " "))
}
