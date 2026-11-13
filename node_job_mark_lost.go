package panewire

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

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
	// nodeJobMarkLostLedgerMissing: the job dir or its events/ is gone, so the
	// local ledger cannot prove the attempt is not completed (hk 3685).
	nodeJobMarkLostLedgerMissing = "ledger_missing"
)

type nodeJobMarkLostArgs struct {
	Job    string `json:"job"`
	PaneID string `json:"pane_id"`
	Label  string `json:"label"`
	// EndReason is the hk end_reason the lost record takes, for a job the hub
	// replaced after a quota stop (hk 3334). Empty writes none.
	EndReason string `json:"end_reason,omitempty"`
}

// validNodeEndReason bounds an end_reason the hub asks a node to write: one
// printable ASCII line, as hk's 128-character field takes it.
func validNodeEndReason(reason string) bool {
	if len(reason) > 128 {
		return false
	}
	for _, r := range reason {
		if r < ' ' || r > '~' {
			return false
		}
	}
	return true
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
	if !validNodeEndReason(args.EndReason) {
		return false
	}
	return args.Label == "" || hubAgentLabelPattern.MatchString(args.Label)
}

func validNodeJobMarkLostResult(result nodeJobMarkLostResult) bool {
	switch result.Outcome {
	case nodeJobMarkLostLost, nodeJobMarkLostPaneListed, nodeJobMarkLostLabelListed,
		nodeJobMarkLostPanesUnknown, nodeJobMarkLostCompleted, nodeJobMarkLostNewerClaim, nodeJobMarkLostLedgerMissing:
		return true
	}
	return false
}

// nodeJobLedger is what the local event ledger proves about a job's current
// attempt: a completion after it, or a claim newer than it. A job id can be
// released and reclaimed, so an earlier cycle's completion or claim is not
// evidence about this attempt.
type nodeJobLedger struct {
	completed  bool
	newerClaim bool
}

// nodeJobLedgerEvent is one parsed ledger event. owner and keep are read for
// job.reap, which must prove the job's own identity before it closes a pane;
// the mark-lost path ignores them.
type nodeJobLedgerEvent struct {
	kind, pane, label, role, task, session string
	owner                                  string
	keep                                   bool
}

func nodeJobClaimKind(kind string) bool {
	return kind == "job.claim" || kind == "job.claimed" || kind == "job.reclaim"
}

// readNodeJobLedger reads <job>/events under the job's os.Root and reports what
// happened after the attempt the caller names by pane or label. A missing
// attempt receipt falls back to the newest claim carrying the label; with no
// identity at all the whole ledger is read. An unreadable event is an error:
// the caller keeps the record rather than guessing.
func readNodeJobLedger(job *os.Root, pane, label string) (nodeJobLedger, error) {
	var ledger nodeJobLedger
	events, err := readNodeJobLedgerEvents(job)
	if err != nil {
		return ledger, err
	}
	attempt := -1
	for index, event := range events {
		if event.kind == "job.spawned" && (pane != "" && event.pane == pane || pane == "" && event.label == label) {
			attempt = index
		}
	}
	if attempt < 0 {
		for index, event := range events {
			if nodeJobClaimKind(event.kind) && (label == "" || event.label == label) {
				attempt = index
			}
		}
	}
	if attempt < 0 {
		// No receipt or claim names this attempt: treat the newest claim as
		// it, so the original claim never reads as a newer one.
		for index, event := range events {
			if nodeJobClaimKind(event.kind) {
				attempt = index
			}
		}
	}
	for index, event := range events {
		if index <= attempt {
			continue
		}
		switch {
		case event.kind == "job.completed":
			ledger.completed = true
		case nodeJobClaimKind(event.kind):
			ledger.newerClaim = true
		}
	}
	return ledger, nil
}

// readNodeJobLedgerEvents parses the job's events in file order, which is
// sequence order.
func readNodeJobLedgerEvents(job *os.Root) ([]nodeJobLedgerEvent, error) {
	// A missing events/ is os.ErrNotExist, not an empty ledger: callers that
	// must prove something from the ledger refuse on it.
	root, err := job.OpenRoot("events")
	if err != nil {
		return nil, err
	}
	defer root.Close()
	dir, err := root.Open(".")
	if err != nil {
		return nil, err
	}
	entries, err := dir.ReadDir(-1)
	dir.Close()
	if err != nil {
		return nil, err
	}
	// File.ReadDir is directory order; the ledger's sequence is the file-name
	// order.
	sort.Slice(entries, func(i, j int) bool { return entries[i].Name() < entries[j].Name() })
	events := make([]nodeJobLedgerEvent, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		if !entry.Type().IsRegular() || !strings.HasSuffix(entry.Name(), ".json") {
			return nil, errors.New("unreadable job event")
		}
		file, err := openNodeResultFile(root, entry.Name())
		if err != nil {
			return nil, err
		}
		data, err := io.ReadAll(io.LimitReader(file, 64<<10))
		file.Close()
		if err != nil {
			return nil, err
		}
		var event struct {
			Kind    string `json:"kind"`
			Payload struct {
				PaneID     string `json:"pane_id"`
				Label      string `json:"label"`
				AgentLabel string `json:"agent_label"`
				Role       string `json:"role"`
				OwnerLane  string `json:"owner_lane"`
				Keep       bool   `json:"keep"`
				// TaskID is a string or a number in the ledger.
				TaskID       any    `json:"task_id"`
				HerdrSession string `json:"herdr_session"`
			} `json:"payload"`
		}
		if json.Unmarshal(data, &event) != nil || event.Kind == "" {
			return nil, errors.New("unreadable job event")
		}
		label := event.Payload.Label
		if label == "" {
			label = event.Payload.AgentLabel
		}
		task := ""
		switch id := event.Payload.TaskID.(type) {
		case string:
			task = id
		case float64:
			task = strconv.FormatFloat(id, 'f', -1, 64)
		}
		events = append(events, nodeJobLedgerEvent{kind: event.Kind, pane: event.Payload.PaneID, label: label, role: event.Payload.Role, task: task, owner: event.Payload.OwnerLane, keep: event.Payload.Keep, session: event.Payload.HerdrSession})
	}
	return events, nil
}

// nodeJobMarkLost writes a holder's hk record lost with this node's own
// credential, after proving from herdr and the local ledger that the job is
// still the one the hub means and never completed.
func (client *HubClient) nodeJobMarkLost(ctx context.Context, args nodeJobMarkLostArgs) (any, string) {
	ledger, err := client.openNodeJobLedger(args.Job, args.PaneID, args.Label)
	if errors.Is(err, errNodeJobsRootUnset) {
		return nil, "jobs_root_unset"
	}
	if errors.Is(err, os.ErrNotExist) {
		// Ledger unknown, not "not completed": refuse, write nothing (hk 3685).
		return nodeJobMarkLostResult{Outcome: nodeJobMarkLostLedgerMissing}, ""
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
	session := client.herdrSessionFor(args.Job, args.PaneID)
	agents, code := client.nodeAgentsListInSession(ctx, session)
	if code != "" {
		return nil, code
	}
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
	record := map[string]any{"state": "lost"}
	if args.EndReason != "" {
		record["end_reason"] = args.EndReason
	}
	if err := store.Put(ctx, args.Job, record); err != nil {
		return nil, "mark_lost_failed"
	}
	return nodeJobMarkLostResult{Outcome: nodeJobMarkLostLost}, ""
}

// openNodeJobLedger reads the job's local ledger; a node without a jobs root
// cannot prove anything, and a missing job dir (os.ErrNotExist) leaves the
// ledger unknown rather than empty.
func (client *HubClient) openNodeJobLedger(job, pane, label string) (nodeJobLedger, error) {
	root, _, found, err := client.openNodeJobDir(job)
	if err != nil {
		return nodeJobLedger{}, err
	}
	if !found {
		return nodeJobLedger{}, os.ErrNotExist
	}
	defer root.Close()
	return readNodeJobLedger(root, pane, label)
}

// nodeJobStoreForCommand is the node's own hk client: spawn.json
// handoffkeep_env, never the hub's. Fixtures set nodeJobs.
func (client *HubClient) nodeJobStoreForCommand() (fleet.JobStore, error) {
	if client.nodeJobs != nil {
		return client.nodeJobs, nil
	}
	env, err := client.nodeHandoffkeepEnv()
	if err != nil {
		return nil, err
	}
	return fleet.NewJobStore(env.URL, env.Token, env.CFID, env.CFSecret)
}

// nodeHandoffkeepEnv loads the node's own hk endpoint and credential.
func (client *HubClient) nodeHandoffkeepEnv() (hubHandoffkeepEnv, error) {
	config, _, err := client.resolveHubSpawnConfig()
	if err != nil {
		return hubHandoffkeepEnv{}, errors.New("handoffkeep env unset")
	}
	return config.handoffkeepEnv()
}

// nodeJobRole is the role the job's own ledger names: the newest claim or
// spawned event that carries one. A job with no directory or no role event has
// no role, so a caller that needs one keeps the pane.
func (client *HubClient) nodeJobRole(name string) (string, error) {
	root, _, found, err := client.openNodeJobDir(name)
	if err != nil || !found {
		return "", err
	}
	defer root.Close()
	events, err := readNodeJobLedgerEvents(root)
	if errors.Is(err, os.ErrNotExist) {
		// No events/ names no role; that is not a read failure here.
		err = nil
	}
	role := ""
	for _, event := range events {
		if event.role != "" && (nodeJobClaimKind(event.kind) || event.kind == "job.spawned") {
			role = event.role
		}
	}
	return role, err
}

// nodeJobHerdrSession is the herdr session recorded in the job's ledger: the
// newest spawned or claim event that carries one.
func (client *HubClient) nodeJobHerdrSession(name string) (string, error) {
	if name == "" || client == nil || client.jobsInboxRoot == "" {
		return "", errors.New("empty job or client")
	}
	client.sessionCacheMu.RLock()
	if client.jobSessions != nil {
		if entry, ok := client.jobSessions[name]; ok && time.Since(entry.cachedAt) < 10*time.Second {
			client.sessionCacheMu.RUnlock()
			return entry.session, nil
		}
	}
	client.sessionCacheMu.RUnlock()

	root, _, found, err := client.openNodeJobDir(name)
	if err != nil {
		return "", err
	}
	if !found {
		return "", errors.New("job ledger is unavailable")
	}
	defer root.Close()
	events, err := readNodeJobLedgerEvents(root)
	if errors.Is(err, os.ErrNotExist) {
		err = nil
	}
	session := ""
	for _, event := range events {
		if event.session != "" && (nodeJobClaimKind(event.kind) || event.kind == "job.spawned") {
			session = event.session
		}
	}
	if err == nil {
		client.sessionCacheMu.Lock()
		if client.jobSessions == nil {
			client.jobSessions = make(map[string]sessionCacheEntry)
		}
		client.jobSessions[name] = sessionCacheEntry{session: session, cachedAt: time.Now()}
		client.sessionCacheMu.Unlock()
	}
	return session, err
}

// nodeJobTask is the task the job's ledger names, read as the fleet's job
// fold reads it: the newest claim's task_id, else the spawn receipt's. An
// unreadable ledger names none; the caller then links nothing.
func nodeJobTask(job *os.Root) string {
	events, err := readNodeJobLedgerEvents(job)
	if err != nil {
		return ""
	}
	task := ""
	for _, event := range events {
		switch {
		case nodeJobClaimKind(event.kind):
			task = event.task
		case event.kind == "job.spawned" && task == "":
			task = event.task
		}
	}
	return task
}
