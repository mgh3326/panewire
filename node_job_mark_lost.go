package panewire

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"strings"

	"github.com/mgh3326/panewire/internal/fleet"
)

// job.mark_lost lets the hub close a holder's hk record through the holder's
// own node, because the hub's hk client is not granted a remote host. The node
// re-checks herdr and its local ledger, then writes the record itself; the
// hub does so directly only when the holder ran on the hub's own host.
const nodeCommandJobMarkLost = "job.mark_lost"

const (
	nodeJobMarkLostLost         = "lost"
	nodeJobMarkLostPaneListed   = "pane-listed"
	nodeJobMarkLostLabelListed  = "label-listed"
	nodeJobMarkLostPanesUnknown = "panes-unknown"
	nodeJobMarkLostCompleted    = "completed"
	nodeJobMarkLostNewerClaim   = "newer-claim"
)

type nodeJobMarkLostArgs struct {
	Job    string `json:"job"`
	PaneID string `json:"pane_id"`
	Label  string `json:"label"`
}

type nodeJobMarkLostResult struct {
	Outcome string `json:"outcome"`
}

func validNodeJobMarkLostArgs(args nodeJobMarkLostArgs) bool {
	if !validNodeJobName(args.Job) || (args.PaneID == "" && args.Label == "") {
		return false
	}
	if args.PaneID != "" && !nodeCommandPaneIDPattern.MatchString(args.PaneID) {
		return false
	}
	return args.Label == "" || hubAgentLabelPattern.MatchString(args.Label)
}

func validNodeJobMarkLostResult(result nodeJobMarkLostResult) bool {
	switch result.Outcome {
	case nodeJobMarkLostLost, nodeJobMarkLostPaneListed, nodeJobMarkLostLabelListed,
		nodeJobMarkLostPanesUnknown, nodeJobMarkLostCompleted, nodeJobMarkLostNewerClaim:
		return true
	}
	return false
}

// nodeJobLedger is what the local event ledger proves about a job: a
// completion, or a claim made after the job was released.
type nodeJobLedger struct {
	completed  bool
	newerClaim bool
}

// readNodeJobLedger reads <job>/events under the job's os.Root. A missing
// events directory proves nothing happened after the claim. An unreadable event
// is an error: the caller keeps the record rather than guessing.
func readNodeJobLedger(job *os.Root) (nodeJobLedger, error) {
	var ledger nodeJobLedger
	events, err := job.OpenRoot("events")
	if errors.Is(err, os.ErrNotExist) {
		return ledger, nil
	}
	if err != nil {
		return ledger, err
	}
	defer events.Close()
	dir, err := events.Open(".")
	if err != nil {
		return ledger, err
	}
	entries, err := dir.ReadDir(-1)
	dir.Close()
	if err != nil {
		return ledger, err
	}
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		if !entry.Type().IsRegular() || !strings.HasSuffix(entry.Name(), ".json") {
			return ledger, errors.New("unreadable job event")
		}
		file, err := openNodeResultFile(events, entry.Name())
		if err != nil {
			return ledger, err
		}
		data, err := io.ReadAll(io.LimitReader(file, 64<<10))
		file.Close()
		if err != nil {
			return ledger, err
		}
		var event struct {
			Kind string `json:"kind"`
		}
		if json.Unmarshal(data, &event) != nil || event.Kind == "" {
			return ledger, errors.New("unreadable job event")
		}
		switch event.Kind {
		case "job.completed":
			ledger.completed = true
		case "job.reclaim":
			ledger.newerClaim = true
		}
	}
	return ledger, nil
}

// nodeJobMarkLost writes a holder's hk record lost with this node's own
// credential, after proving from herdr and the local ledger that the job is
// still the one the hub means and never completed.
func (client *HubClient) nodeJobMarkLost(ctx context.Context, args nodeJobMarkLostArgs) (any, string) {
	ledger, err := client.openNodeJobLedger(args.Job)
	if errors.Is(err, errNodeJobsRootUnset) {
		return nil, "jobs_root_unset"
	}
	if err != nil {
		return nil, "mark_lost_failed"
	}
	switch {
	case ledger.completed:
		return nodeJobMarkLostResult{Outcome: nodeJobMarkLostCompleted}, ""
	case ledger.newerClaim:
		return nodeJobMarkLostResult{Outcome: nodeJobMarkLostNewerClaim}, ""
	}
	list, code := client.nodeAgentsList(ctx)
	if code != "" {
		return nil, code
	}
	agents := list.(nodeAgentsListResult)
	for _, row := range agents.Agents {
		if args.PaneID != "" && row.PaneID == args.PaneID {
			return nodeJobMarkLostResult{Outcome: nodeJobMarkLostPaneListed}, ""
		}
		if args.Label != "" && row.Name == args.Label {
			return nodeJobMarkLostResult{Outcome: nodeJobMarkLostLabelListed}, ""
		}
	}
	for _, row := range agents.Panes {
		if args.PaneID != "" && row.PaneID == args.PaneID {
			return nodeJobMarkLostResult{Outcome: nodeJobMarkLostPaneListed}, ""
		}
	}
	if args.PaneID != "" && !agents.PanesOK {
		return nodeJobMarkLostResult{Outcome: nodeJobMarkLostPanesUnknown}, ""
	}
	store, err := client.nodeJobStoreForCommand()
	if err != nil {
		return nil, "mark_lost_failed"
	}
	if err := store.Put(ctx, args.Job, map[string]any{"state": "lost"}); err != nil {
		return nil, "mark_lost_failed"
	}
	return nodeJobMarkLostResult{Outcome: nodeJobMarkLostLost}, ""
}

// openNodeJobLedger reads the job's local ledger; a node without a jobs root
// cannot prove anything.
func (client *HubClient) openNodeJobLedger(job string) (nodeJobLedger, error) {
	root, _, found, err := client.openNodeJobDir(job)
	if err != nil {
		return nodeJobLedger{}, err
	}
	if !found {
		return nodeJobLedger{}, nil
	}
	defer root.Close()
	return readNodeJobLedger(root)
}

// nodeJobStoreForCommand is the node's own hk client: spawn.json
// handoffkeep_env, never the hub's. Fixtures set nodeJobs.
func (client *HubClient) nodeJobStoreForCommand() (fleet.JobStore, error) {
	if client.nodeJobs != nil {
		return client.nodeJobs, nil
	}
	config, _, err := client.resolveHubSpawnConfig()
	if err != nil || config.HandoffkeepEnv == "" {
		return nil, errors.New("handoffkeep env unset")
	}
	env, err := loadHubHandoffkeepEnv(config.HandoffkeepEnv)
	if err != nil {
		return nil, err
	}
	return fleet.NewJobStore(env.URL, env.Token, env.CFID, env.CFSecret)
}
