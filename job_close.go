package panewire

import (
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"syscall"
	"unicode/utf8"
)

// `panewire job close` is the owner's end-of-job declaration (#502 E-1). A
// worker that finished without `wrk done` leaves no terminal event, so no
// reaper ever picks its pane up; the lane that owns the job closes it here.
//
// The declaration is one flat record and nothing else: no emit, no hub call,
// no handoffkeep upload, no herdr call. Closing the pane stays reap's job, and
// reap's grace runs from this record's created_at — declare, wait out the
// grace, then reap.
//
// The kind is job.revoked, already in every terminal set (wrk reap's TERMINAL,
// fleetCensusTerminalKinds, stallTerminalKinds, the node's active-job scan).
// Since #507 the relay scanner also forwards it: the record always carries a
// reason and an owner lane, so the declaration reaches the owner lane's parent
// pane and a durable handoffkeep row — the expressiveness gap #507 closed.
// job.completed is still deliberately not used: its payload carries no reason
// and its relay text reads as a report, not a revocation. The hub's own
// revocation writes {"type":"job.revoked",job_id,epoch} — no reason, no owner
// lane — which is exactly what keeps that hub→node marker out of the node→hub
// relay direction; a declaration carries "kind" (the key reap reads) plus
// source, closed_by and outcome, so the two stay distinguishable.
//
// None of this touches done/escalate/joined: their writers and wire stay
// byte-identical to wrk (testdata/job_golden).

const (
	jobCloseKind   = "job.revoked"
	jobCloseSource = "panewire job close"
	// jobCloseRefused is the ownership / state refusal: the arguments were
	// well-formed but the job's own events do not allow this caller to close.
	jobCloseRefused = ExitConditionInvalid
	// jobCloseReasonMax bounds the free-text reason kept in the record.
	jobCloseReasonMax = 2000
	// jobClosePublishAttempts bounds the no-replace publish retries. The lock
	// already serializes cooperating writers; a collision means a writer that
	// does not take the lock took the name, and the scan is simply redone.
	jobClosePublishAttempts = 16
)

// jobCloseOutcomes is the closed vocabulary that keeps "finished" apart from
// "given up" and "replaced" — a queue-health count must never read an
// abandoned job as a completed one.
var jobCloseOutcomes = map[string]bool{"completed": true, "abandoned": true, "superseded": true}

// jobCloseTerminalKinds and jobCloseReviveKinds are wrk reap's TERMINAL and
// REVIVE sets: a job whose latest claim is followed by a terminal event is
// already closed; a claim/spawn after a terminal revives it.
var (
	jobCloseTerminalKinds = map[string]bool{"job.completed": true, "job.joined": true, "job.revoked": true}
	jobCloseReviveKinds   = map[string]bool{"job.claim": true, "job.reclaim": true, "job.spawned": true, "job.reprompted": true}
)

const jobCloseHelp = `Usage: panewire job close JOB --lane LANE --outcome completed|abandoned|superseded --reason TEXT [--operator-override]

Declare JOB ended on behalf of its owner lane. Writes one job.revoked record
into the job's events directory; the pane is not touched (wrk reap closes it
after its grace).

Options:
  --lane LANE           the calling lane; must equal the latest claim's owner_lane exactly
  --outcome OUTCOME     completed, abandoned or superseded
  --reason TEXT         required, kept in the record
  --operator-override   close a job another lane owns; recorded as override=operator
  -h, --help            show this usage
`

// jobCloseRecord is the declaration's on-disk shape. Field order is the
// struct order. There is deliberately no pane_id or tab_id: reap takes a pane
// from any record that carries one, and the spawn receipt must stay the only
// source of the pane to close.
type jobCloseRecord struct {
	Kind      string `json:"kind"`
	JobID     string `json:"job_id"`
	OwnerLane string `json:"owner_lane"`
	ClosedBy  string `json:"closed_by"`
	Outcome   string `json:"outcome"`
	Reason    string `json:"reason"`
	Override  string `json:"override,omitempty"`
	Source    string `json:"source"`
	Host      string `json:"host"`
	CreatedAt string `json:"created_at"`
	Epoch     int    `json:"epoch"`
}

type jobCloseOptions struct {
	job, lane, outcome, reason string
	override                   bool
}

func (c *jobCLI) close(args []string) int {
	if len(args) > 0 && (args[0] == "-h" || args[0] == "--help") {
		if len(args) != 1 {
			return c.exit(jobDie("unexpected argument '%s'", args[1]))
		}
		fmt.Fprint(c.stdout, jobCloseHelp)
		return ExitOK
	}
	options, err := parseJobCloseArgs(args)
	if err != nil {
		return c.exit(err)
	}
	jobDir := jobJobsRoot() + "/" + options.job
	eventsDir := jobDir + "/events"
	if info, err := os.Stat(eventsDir); err != nil || !info.IsDir() {
		return c.exit(jobDie("close: no events directory for job %s", options.job))
	}
	record, name, err := c.publishJobClose(jobDir, eventsDir, options)
	if err != nil {
		return c.exit(err)
	}
	line := fmt.Sprintf("OK job=%s kind=%s outcome=%s closed_by=%s owner_lane=%s", record.JobID, record.Kind, record.Outcome, record.ClosedBy, record.OwnerLane)
	if record.Override != "" {
		line += " override=" + record.Override
	}
	fmt.Fprintf(c.stdout, "%s record=%s\n", line, name)
	return ExitOK
}

func parseJobCloseArgs(args []string) (jobCloseOptions, error) {
	var options jobCloseOptions
	if len(args) > 0 {
		options.job, args = args[0], args[1:]
	}
	if options.job == "" || strings.HasPrefix(options.job, "-") {
		return options, jobDie("close requires a job id")
	}
	// The id names a directory under the jobs root: one path component, the
	// hub's job id shape, so "../x" or "a/b" can never place a record elsewhere.
	if !hubJobIDPattern.MatchString(options.job) {
		return options, jobDie("close: invalid job id '%s'", options.job)
	}
	seen := map[string]bool{}
	for len(args) > 0 {
		flag := args[0]
		if seen[flag] {
			return options, jobDie("close: duplicate option '%s'", flag)
		}
		seen[flag] = true
		switch flag {
		case "--operator-override":
			options.override, args = true, args[1:]
			continue
		case "--lane", "--outcome", "--reason":
		default:
			return options, jobDie("unknown close option '%s'", flag)
		}
		if len(args) < 2 {
			return options, jobDie("%s requires a value", flag)
		}
		value := args[1]
		args = args[2:]
		switch flag {
		case "--lane":
			options.lane = value
		case "--outcome":
			options.outcome = value
		case "--reason":
			options.reason = value
		}
	}
	if options.lane == "" {
		return options, jobDie("close requires --lane")
	}
	if !jobCloseOutcomes[options.outcome] {
		return options, jobDie("close requires --outcome completed|abandoned|superseded")
	}
	if strings.TrimSpace(options.reason) == "" {
		return options, jobDie("close requires a non-empty --reason")
	}
	if !utf8.ValidString(options.reason) || len(options.reason) > jobCloseReasonMax {
		return options, jobDie("close: --reason must be valid UTF-8 of at most %d bytes", jobCloseReasonMax)
	}
	return options, nil
}

// jobCloseState is what the events directory says about closing: the owner
// named by the newest claim, and whether a terminal event already stands
// after the last revival.
type jobCloseState struct {
	owner       string
	claimed     bool
	terminal    string // the standing terminal record's file name, "" if none
	highest     *big.Int
	unreadClaim string // a claim/reclaim file that could not be read or parsed
}

// scanJobCloseState walks the events in name order — the order wrk reap and
// completion metadata use — and keeps the newest claim's owner_lane. A claim
// file that cannot be read is fatal to the ownership check: skipping it would
// let an older claim decide.
func scanJobCloseState(eventsDir string) (jobCloseState, error) {
	state := jobCloseState{highest: big.NewInt(0)}
	entries, err := os.ReadDir(eventsDir)
	if err != nil {
		return state, err
	}
	names := make([]string, 0, len(entries))
	for _, entry := range entries {
		names = append(names, entry.Name())
		prefix, _, _ := strings.Cut(entry.Name(), "-")
		if value, ok := pythonInt(prefix); ok && value.Cmp(state.highest) > 0 {
			state.highest = value
		}
	}
	sort.Strings(names)
	for _, name := range names {
		if !strings.HasSuffix(name, ".json") {
			continue
		}
		claimName := strings.HasSuffix(name, "-job.claim.json") || strings.HasSuffix(name, "-job.reclaim.json")
		contents, err := os.ReadFile(filepath.Join(eventsDir, name))
		var document map[string]any
		if err == nil && utf8.Valid(contents) {
			if value, ok := pythonJSONLoad(contents); ok {
				document, _ = value.(map[string]any)
			}
		}
		if document == nil {
			if claimName && state.unreadClaim == "" {
				state.unreadClaim = name
			}
			continue
		}
		payload := document
		if raw, present := document["payload"]; present {
			payload, _ = raw.(map[string]any)
		}
		kind, _ := document["kind"].(string)
		if kind == "job.claim" || kind == "job.reclaim" {
			state.claimed = true
			state.owner = ""
			if payload != nil {
				state.owner, _ = payload["owner_lane"].(string)
			}
		}
		if jobCloseReviveKinds[kind] {
			state.terminal = ""
		}
		if jobCloseTerminalKinds[kind] {
			state.terminal = name
		}
	}
	return state, nil
}

// publishJobClose checks ownership and publishes the record under the job's
// .wrk-events.lock — the lock job.completed writers (wrk and completionEvent)
// hold across their scan and write — so the check, the seq choice and the
// publish see one state. The record lands by link, which never replaces an
// existing name; a collision rescans and retries.
func (c *jobCLI) publishJobClose(jobDir, eventsDir string, options jobCloseOptions) (jobCloseRecord, string, error) {
	lock, err := os.OpenFile(filepath.Join(jobDir, ".wrk-events.lock"), os.O_WRONLY|os.O_CREATE, 0o666)
	if err != nil {
		return jobCloseRecord{}, "", &jobExit{code: 1, message: "close: " + err.Error()}
	}
	defer lock.Close()
	if err := syscall.Flock(int(lock.Fd()), syscall.LOCK_EX); err != nil {
		return jobCloseRecord{}, "", &jobExit{code: 1, message: "close: " + err.Error()}
	}
	defer syscall.Flock(int(lock.Fd()), syscall.LOCK_UN)
	host := jobHost()
	for attempt := 0; attempt < jobClosePublishAttempts; attempt++ {
		state, err := scanJobCloseState(eventsDir)
		if err != nil {
			return jobCloseRecord{}, "", &jobExit{code: 1, message: "close: " + err.Error()}
		}
		if err := checkJobCloseOwnership(state, options); err != nil {
			return jobCloseRecord{}, "", err
		}
		record := jobCloseRecord{
			Kind: jobCloseKind, JobID: options.job, OwnerLane: state.owner, ClosedBy: options.lane,
			Outcome: options.outcome, Reason: options.reason, Source: jobCloseSource, Host: host,
			CreatedAt: c.now().UTC().Format("2006-01-02T15:04:05Z"), Epoch: 1,
		}
		if options.override {
			record.Override = "operator"
		}
		name := fmt.Sprintf("%05s-%s.json", new(big.Int).Add(state.highest, big.NewInt(1)).String(), jobCloseKind)
		published, err := publishJobCloseRecord(eventsDir, name, record)
		if err != nil {
			return jobCloseRecord{}, "", &jobExit{code: 1, message: "close: " + err.Error()}
		}
		if published {
			return record, name, nil
		}
	}
	return jobCloseRecord{}, "", &jobExit{code: 1, message: "close: event sequence kept colliding; nothing written"}
}

// checkJobCloseOwnership is the ownership guard. The comparison is byte
// equality on purpose: no trimming, no case folding, no prefix or substring
// match — a lane that is merely similar to the owner is another lane.
func checkJobCloseOwnership(state jobCloseState, options jobCloseOptions) error {
	if state.unreadClaim != "" {
		return &jobExit{code: jobCloseRefused, message: fmt.Sprintf("close refused: claim record %s is unreadable; ownership cannot be established", state.unreadClaim)}
	}
	if state.terminal != "" {
		return &jobExit{code: jobCloseRefused, message: fmt.Sprintf("close refused: job %s already has a terminal record (%s) after its latest claim", options.job, state.terminal)}
	}
	if options.override {
		return nil
	}
	if !state.claimed || state.owner == "" {
		return &jobExit{code: jobCloseRefused, message: fmt.Sprintf("close refused: job %s has no claim naming an owner lane; only --operator-override can close it", options.job)}
	}
	if options.lane != state.owner {
		return &jobExit{code: jobCloseRefused, message: fmt.Sprintf("close refused: job %s is owned by lane %q, not %q (latest claim); use --operator-override to close another lane's job", options.job, state.owner, options.lane)}
	}
	return nil
}

// publishJobCloseRecord writes the record to a temporary file and links it to
// name. It reports false, with nothing written, when name already exists.
func publishJobCloseRecord(eventsDir, name string, record jobCloseRecord) (bool, error) {
	body, err := json.Marshal(record)
	if err != nil {
		return false, err
	}
	body = append(body, '\n')
	temporary, err := os.CreateTemp(eventsDir, ".close-")
	if err != nil {
		return false, err
	}
	temporaryName := temporary.Name()
	defer os.Remove(temporaryName)
	if _, err := temporary.Write(body); err != nil {
		_ = temporary.Close()
		return false, err
	}
	if err := temporary.Close(); err != nil {
		return false, err
	}
	if err := os.Chmod(temporaryName, 0o600); err != nil {
		return false, err
	}
	if err := os.Link(temporaryName, filepath.Join(eventsDir, name)); err != nil {
		if errors.Is(err, os.ErrExist) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}
