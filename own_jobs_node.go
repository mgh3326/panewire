package panewire

import (
	"context"
	"errors"
	"time"
)

// job.reap is the node half of `panewire jobs close`: the hub has already
// checked that the caller's lane owns the job, and the node re-proves the same
// facts from the job's own ledger and herdr before it closes anything, because
// the ledger and herdr are the only truths about a pane this process did not
// spawn. The safety rules are reap's (docs/jobs-own.md): never a pane that is
// not the job's, never a job spawned with --keep, and a pane that is still
// working or waiting at a prompt only with force.
const nodeCommandJobReap = "job.reap"

const (
	nodeJobReapClosed        = "closed"
	nodeJobReapPaneGone      = "pane-gone"
	nodeJobReapPaneChanged   = "pane-changed"
	nodeJobReapJobWorking    = "job-working"
	nodeJobReapPaneBlocked   = "pane-blocked"
	nodeJobReapProtected     = "protected"
	nodeJobReapOwnerMismatch = "owner-mismatch"
	nodeJobReapNotAJob       = "not-a-job"
)

type nodeJobReapArgs struct {
	Job   string `json:"job"`
	Lane  string `json:"lane"`
	Force bool   `json:"force"`
}

// nodeJobReapResult names the pane the node acted on, so the hub retires the
// lane rows that pointed at the pane it actually closed.
type nodeJobReapResult struct {
	Outcome string `json:"outcome"`
	Pane    string `json:"pane,omitempty"`
}

func validNodeJobReapArgs(args nodeJobReapArgs) bool {
	return validNodeJobName(args.Job) && LaneNamePattern.MatchString(args.Lane)
}

func validNodeJobReapResult(result nodeJobReapResult) bool {
	switch result.Outcome {
	case nodeJobReapClosed:
		return nodeCommandPaneIDPattern.MatchString(result.Pane)
	case nodeJobReapPaneGone, nodeJobReapPaneChanged, nodeJobReapJobWorking, nodeJobReapPaneBlocked, nodeJobReapProtected, nodeJobReapOwnerMismatch, nodeJobReapNotAJob:
		return result.Pane == "" || nodeCommandPaneIDPattern.MatchString(result.Pane)
	}
	return false
}

// nodeJobReapTarget is what the job's ledger says about the attempt to close:
// the newest claim's owner lane, the spawn receipt's pane and label, and the
// receipt's keep flag. A claim after a reclaim wins the owner; the receipt is
// the only place the pane is named.
type nodeJobReapTarget struct {
	owner, pane, label string
	keep               bool
}

func nodeJobReapTargetFrom(events []nodeJobLedgerEvent) nodeJobReapTarget {
	var target nodeJobReapTarget
	for _, event := range events {
		switch {
		case nodeJobClaimKind(event.kind):
			target.owner = event.owner
		case event.kind == "job.spawned":
			target.pane, target.label = event.pane, event.label
			if target.owner == "" {
				target.owner = event.owner
			}
		}
		target.keep = target.keep || event.keep
	}
	return target
}

// nodeJobReap closes the pane of a job this node ran, or names why it did not.
func (client *HubClient) nodeJobReap(ctx context.Context, args nodeJobReapArgs) (any, string) {
	job, _, found, err := client.openNodeJobDir(args.Job)
	switch {
	case errors.Is(err, errNodeJobsRootUnset):
		return nil, "jobs_root_unset"
	case err != nil:
		return nil, "reap_failed"
	case !found:
		return nodeJobReapResult{Outcome: nodeJobReapNotAJob}, ""
	}
	defer job.Close()
	events, err := readNodeJobLedgerEvents(job)
	if err != nil {
		return nil, "reap_failed"
	}
	target := nodeJobReapTargetFrom(events)
	if target.owner != args.Lane {
		return nodeJobReapResult{Outcome: nodeJobReapOwnerMismatch, Pane: target.pane}, ""
	}
	if target.keep {
		return nodeJobReapResult{Outcome: nodeJobReapProtected, Pane: target.pane}, ""
	}
	if target.pane == "" {
		return nodeJobReapResult{Outcome: nodeJobReapPaneGone}, ""
	}
	row, found, err := client.nodeJobReapRow(ctx, args.Job, target.pane)
	if err != nil {
		return nil, "herdr_failed"
	}
	if !found {
		return nodeJobReapResult{Outcome: nodeJobReapPaneGone, Pane: target.pane}, ""
	}
	// A pane id is host-local and reused, so the label the job's own receipt
	// names is what proves the pane still runs this job.
	if target.label == "" || row.Name != target.label {
		return nodeJobReapResult{Outcome: nodeJobReapPaneChanged, Pane: target.pane}, ""
	}
	if !args.Force {
		switch {
		case row.AgentStatus == "working":
			return nodeJobReapResult{Outcome: nodeJobReapJobWorking, Pane: target.pane}, ""
		case row.AgentStatus == "blocked":
			return nodeJobReapResult{Outcome: nodeJobReapPaneBlocked, Pane: target.pane}, ""
		case !nodePaneIdleStatus[row.AgentStatus]:
			return nodeJobReapResult{Outcome: nodeJobReapPaneChanged, Pane: target.pane}, ""
		}
		settled, err := client.nodeJobReapSettled(ctx, args.Job, row)
		if err != nil {
			return nil, "herdr_failed"
		}
		if !settled {
			return nodeJobReapResult{Outcome: nodeJobReapPaneChanged, Pane: target.pane}, ""
		}
	}
	if _, err := client.nodeHerdrRunnerFor(args.Job, target.pane)(ctx, "pane", "close", target.pane); err != nil {
		return nil, "close_failed"
	}
	return nodeJobReapResult{Outcome: nodeJobReapClosed, Pane: target.pane}, ""
}

// nodeJobReapRow finds the pane's agent row. A pane that is not an agent — a
// plain shell reusing the id — is not the job's pane, and reads as gone.
func (client *HubClient) nodeJobReapRow(ctx context.Context, job, pane string) (nodeHerdrRow, bool, error) {
	agents, err := client.readNodeAgentsInSession(ctx, client.herdrSessionFor(job, pane))
	if err != nil {
		return nodeHerdrRow{}, false, err
	}
	for _, agent := range agents {
		if agent.PaneID == pane {
			return agent, true, nil
		}
	}
	return nodeHerdrRow{}, false, nil
}

// nodeJobReapSettled is pane.close's two-read settle for a job close: the
// finished status and an empty composer must still hold one settle interval
// later, with an unchanged state_change_seq where herdr reports one. A failed
// screen read refuses the close, because an unknown composer is never empty.
func (client *HubClient) nodeJobReapSettled(ctx context.Context, job string, first nodeHerdrRow) (bool, error) {
	composerEmpty := func() bool {
		screen, err := client.nodeReadScreen(ctx, job, first.PaneID)
		return err == nil && nodeComposerState(first.Agent, screen) == nodeComposerEmpty
	}
	if !composerEmpty() {
		return false, nil
	}
	settle := client.nodeCloseSettle
	if settle == 0 {
		settle = nodePaneCloseSettle
	}
	timer := time.NewTimer(settle)
	select {
	case <-timer.C:
	case <-ctx.Done():
		timer.Stop()
		return false, nil
	}
	second, found, err := client.nodeJobReapRow(ctx, job, first.PaneID)
	if err != nil {
		return false, err
	}
	if !found || second.Name != first.Name || second.AgentStatus != first.AgentStatus {
		return false, nil
	}
	if first.StateChangeSeq != nil && second.StateChangeSeq != nil && *first.StateChangeSeq != *second.StateChangeSeq {
		return false, nil
	}
	return composerEmpty(), nil
}
