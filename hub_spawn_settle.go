package panewire

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/mgh3326/panewire/internal/fleet"
)

// Settling a spawn the node restarted in the middle of.
//
// A journal row stays `executing` while the node runs the spawn. If the
// process dies, nothing will ever complete the row, and the old answer to a
// replay was `unknown` until the request deadline. The panes live in herdr, a
// separate process, so the new process asks herdr instead:
//
//   - the job's recorded pane (from its job.spawned event) is an agent of the
//     spawn's name: the spawn landed. The row completes with a success result
//     built from that record, so the replay is answered at once, and the
//     detached watcher (re-armed at start) keeps recording the job.
//   - that pane is in neither herdr list, or its id now names another agent:
//     the row completes as lost, with a reason.
//   - anything herdr cannot prove stays executing: a pane only in the pane
//     list (the agent list alone never proves a pane gone), and a spawn with no
//     job.spawned record while a label match exists (labels are not
//     job-unique, so a match is neither this job's pane nor proof of absence).
//
// herdr unreachable leaves the row executing too; the next pass retries. A pass
// runs at start, on a replay of the request, and every settleRetry until no
// row is left. See docs/hub-spawn-operations.md ("Node restart").

const (
	spawnSettleRetry   = 30 * time.Second
	spawnSettleTimeout = 10 * time.Second
	// Reasons a restart-settled spawn is lost. They are the text after
	// "lost: " in the result's stdout tail.
	spawnLostPaneGone       = "pane_gone"
	spawnLostNoPane         = "no_pane_launched"
	spawnLostNoJobRecorded  = "journal_row_without_job"
	spawnSettledLandedStamp = "settled after node restart: pane landed"
)

// settleExecutingSpawns settles every executing row this process is not
// running. It returns how many rows it completed and how many it left.
func (client *HubClient) settleExecutingSpawns(ctx context.Context, journal *nodeSpawnJournal) (settled, left int) {
	client.spawnActiveMu.Lock()
	rows, err := journal.executing(ctx)
	rows = notActive(rows, client.spawnActive)
	client.spawnActiveMu.Unlock()
	if err != nil {
		return 0, 1
	}
	for _, row := range rows {
		if _, ok := client.settleExecutingSpawn(ctx, journal, row); ok {
			settled++
		} else {
			left++
		}
	}
	return settled, left
}

func notActive(rows []executingSpawn, active map[string]bool) []executingSpawn {
	kept := rows[:0]
	for _, row := range rows {
		if !active[row.RequestID] {
			kept = append(kept, row)
		}
	}
	return kept
}

// settleRequest settles one request's row for a replay, and returns the
// stored completion when it settled.
func (client *HubClient) settleRequest(ctx context.Context, journal *nodeSpawnJournal, requestID string) (*hubSpawnResult, bool) {
	client.spawnActiveMu.Lock()
	rows, err := journal.executing(ctx)
	rows = notActive(rows, client.spawnActive)
	client.spawnActiveMu.Unlock()
	if err != nil {
		return nil, false
	}
	for _, row := range rows {
		if row.RequestID == requestID {
			return client.settleExecutingSpawn(ctx, journal, row)
		}
	}
	return nil, false
}

// settleExecutingSpawn decides one row from herdr and the job's local spawn
// record, completes it, and returns what the journal holds afterwards (a
// concurrent settle may have won; complete only writes an executing row).
func (client *HubClient) settleExecutingSpawn(ctx context.Context, journal *nodeSpawnJournal, row executingSpawn) (*hubSpawnResult, bool) {
	ctx, cancel := context.WithTimeout(ctx, spawnSettleTimeout)
	defer cancel()
	result := hubSpawnResult{RequestID: row.RequestID, JobID: row.Job}
	if row.Job == "" {
		result = lostSpawnResult(result, spawnLostNoJobRecorded, false)
	} else {
		spawned := client.readLocalSpawnRecord(row.Job)
		session := firstNonempty(spawned.HerdrSession, client.nodeSpawnSession())
		evidence, ok := client.readSpawnEvidence(ctx, session)
		if !ok {
			return nil, false // herdr unreachable: the row stays executing.
		}
		decided, proven := decideSpawn(evidence, spawned, row.Label)
		if !proven {
			// The agent list alone never proves a pane gone, and a label is not
			// job-unique: an unprovable case leaves the row executing, and the
			// replay answers unknown for the hub's own pane read to decide.
			return nil, false
		}
		switch decided.state {
		case spawnLanded:
			result = landedSpawnResult(result, row, spawned, decided.pane)
		case spawnPaneGone:
			result = lostSpawnResult(result, spawnLostPaneGone, false)
			result.Pane = spawned.PaneID
		default:
			result = lostSpawnResult(result, spawnLostNoPane, false)
		}
	}
	now := client.nowUTC()
	result.CompletedAt = &now
	if err := journal.complete(ctx, result); err != nil {
		return nil, false
	}
	stored, err := journal.resultOf(ctx, row.RequestID)
	if err != nil || stored == nil {
		return nil, false
	}
	client.warnMessage(fmt.Sprintf("spawn %s settled after restart: rc=%d", row.RequestID, stored.RC))
	if stored.RC == 0 {
		client.rearmWatchers()
	}
	return stored, true
}

// spawnEvidence is everything herdr shows about a session: the agent list and
// the pane list, whose rows carry the label and whose pane exists before its
// agent registers or after it exits.
// spawnPaneRow is one `herdr pane list` row; its label arrives under `label`
// or `name` (as fleet.Pane and jobDirPanes read it).
type spawnPaneRow struct{ PaneID, Label, Name string }

type spawnEvidence struct {
	agents []nodeAgentRow
	panes  []spawnPaneRow
}

func (client *HubClient) readSpawnEvidence(ctx context.Context, session string) (spawnEvidence, bool) {
	var evidence spawnEvidence
	agents, err := client.readNodeAgentsInSession(ctx, session)
	if err != nil {
		return evidence, false
	}
	for _, agent := range agents {
		evidence.agents = append(evidence.agents, agent.nodeAgentRow)
	}
	output, err := client.nodeHerdrRunnerForSession(session)(ctx, "pane", "list")
	if err != nil {
		return evidence, false
	}
	// A reply without a panes array is unreadable, not an empty list.
	if _, err := nodeHerdrRows(output, "panes"); err != nil {
		return evidence, false
	}
	var envelope struct {
		Result struct {
			Panes []struct {
				PaneID string `json:"pane_id"`
				Label  string `json:"label"`
				Name   string `json:"name"`
			} `json:"panes"`
		} `json:"result"`
	}
	if json.Unmarshal(output, &envelope) != nil {
		return evidence, false
	}
	for _, pane := range envelope.Result.Panes {
		evidence.panes = append(evidence.panes, spawnPaneRow{pane.PaneID, pane.Label, pane.Name})
	}
	return evidence, true
}

type spawnState int

const (
	spawnLanded spawnState = iota
	spawnPaneGone
	spawnNeverLaunched
)

type spawnDecision struct {
	state spawnState
	pane  nodeAgentRow
}

// decideSpawn judges a restarted spawn. proven is false whenever herdr's
// evidence cannot settle it:
//   - With the job's recorded pane: an agent on that pane with the spawn's
//     name landed it; a pane that is only in the pane list is not proven gone;
//     a pane in neither list, or held by another-named agent, is gone.
//   - Without a record (the crash fell between pane create and the record), a
//     label match is never taken as this job's pane, since labels are not
//     job-unique, and is never proof of absence either: it stays unproven. No
//     match in either list is a spawn that never launched.
func decideSpawn(evidence spawnEvidence, spawned localSpawnRecord, label string) (decision spawnDecision, proven bool) {
	if spawned.PaneID != "" {
		for _, agent := range evidence.agents {
			if agent.PaneID != spawned.PaneID {
				continue
			}
			if label == "" || agent.Name == "" || agent.Name == label {
				return spawnDecision{state: spawnLanded, pane: agent}, true
			}
			return spawnDecision{state: spawnPaneGone}, true // the id now names another agent
		}
		for _, pane := range evidence.panes {
			if pane.PaneID == spawned.PaneID {
				return spawnDecision{}, false
			}
		}
		return spawnDecision{state: spawnPaneGone}, true
	}
	if label != "" {
		for _, agent := range evidence.agents {
			if agent.Name == label {
				return spawnDecision{}, false
			}
		}
		for _, pane := range evidence.panes {
			if pane.Label == label || pane.Name == label {
				return spawnDecision{}, false
			}
		}
	}
	return spawnDecision{state: spawnNeverLaunched}, true
}

func landedSpawnResult(result hubSpawnResult, row executingSpawn, spawned localSpawnRecord, pane nodeAgentRow) hubSpawnResult {
	result.RC, result.Pane = 0, pane.PaneID
	result.StdoutTail = spawnSettledLandedStamp
	result.Receipt = &fleet.ExecutionReceipt{
		Job: row.Job, Host: spawned.Host, Pane: pane.PaneID, Profile: spawned.Profile, Model: spawned.Model,
		Role: spawned.Role, CWD: firstNonempty(spawned.CWD, pane.Cwd), Label: firstNonempty(row.Label, pane.Name),
		Kind: firstNonempty(fleet.ProfileKind(spawned.Profile), pane.Agent), LaneUnregistered: spawned.LaneUnregistered,
	}
	return result
}

func lostSpawnResult(result hubSpawnResult, reason string, retryable bool) hubSpawnResult {
	result.RC, result.Error, result.Retryable = 1, "lost", retryable
	result.StdoutTail = "lost: " + reason + " (the node restarted before this spawn finished)"
	return result
}

// localSpawnRecord is the part of a job's newest job.spawned event the settle
// reads: where the pane is and what the hub's pane check compares.
type localSpawnRecord struct {
	PaneID, HerdrSession, Host, Profile, Model, Role, CWD string
	LaneUnregistered                                      bool
}

func (client *HubClient) readLocalSpawnRecord(job string) localSpawnRecord {
	if client.jobsInboxRoot == "" || !hubJobIDPattern.MatchString(job) {
		return localSpawnRecord{}
	}
	dir := filepath.Join(client.jobsInboxRoot, "jobs", job, "events")
	entries, err := os.ReadDir(dir)
	if err != nil {
		return localSpawnRecord{}
	}
	names := make([]string, 0, len(entries))
	for _, entry := range entries {
		if !entry.IsDir() && strings.HasSuffix(entry.Name(), ".json") {
			names = append(names, entry.Name())
		}
	}
	sort.Strings(names)
	var found localSpawnRecord
	for _, name := range names {
		data, err := os.ReadFile(filepath.Join(dir, name))
		if err != nil || len(data) > 16<<10 {
			continue
		}
		var event struct {
			Type, Kind, Event string
			Payload           struct {
				PaneID           string `json:"pane_id"`
				HerdrSession     string `json:"herdr_session"`
				Host             string `json:"host"`
				Profile          string `json:"profile"`
				Model            string `json:"model_id"`
				Role             string `json:"role"`
				CWD              string `json:"cwd"`
				LaneUnregistered bool   `json:"lane_unregistered"`
			} `json:"payload"`
		}
		if json.Unmarshal(data, &event) != nil || firstNonempty(event.Type, event.Kind, event.Event) != "job.spawned" {
			continue
		}
		p := event.Payload
		found = localSpawnRecord{PaneID: p.PaneID, HerdrSession: p.HerdrSession, Host: p.Host, Profile: p.Profile, Model: p.Model, Role: p.Role, CWD: p.CWD, LaneUnregistered: p.LaneUnregistered}
	}
	return found
}

// runSpawnSettle settles at start and retries while a pass leaves rows
// (herdr not yet answering), so no row stays executing forever.
func (client *HubClient) runSpawnSettle(ctx context.Context) {
	if ctx.Err() != nil {
		return
	}
	journal, err := client.acquireSpawnJournal()
	if err != nil {
		return
	}
	defer client.releaseSpawnJournal()
	for {
		if _, left := client.settleExecutingSpawns(ctx, journal); left == 0 {
			return
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(spawnSettleRetry):
		}
	}
}
