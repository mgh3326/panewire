// Package wrkgate binds core.Gate to the small durable contract exposed by
// agent-skills' wrk and arbiter CLIs.  It never shells out from package init;
// callers opt in by constructing a Gate for a receiver.
package wrkgate

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os/exec"

	"github.com/mgh3326/panewire/stage2/core"
)

const (
	// These are the stable b010293 wrk contract codes, not generic shell
	// failures.  74 means a live duplicate and 75 means wrk could not prove
	// that duplicate's arbiter state safely.
	ExitActiveJobDuplicate = 74
	ExitJobStateUnreadable = 75
	ExitArbiterNotFound    = 7
)

// Result deliberately exposes only stdout because Gate errors must not relay
// arbitrary stderr from an external CLI into Panewire logs or metadata.
type Result struct {
	Stdout   []byte
	ExitCode int
}

// Runner is a fixture seam.  Production uses ExecRunner; tests use executable
// stub binaries so argument and exit-code behavior stays faithful.
type Runner interface {
	Run(context.Context, string, ...string) (Result, error)
}

type ExecRunner struct{}

func (ExecRunner) Run(ctx context.Context, name string, args ...string) (Result, error) {
	output, err := exec.CommandContext(ctx, name, args...).Output()
	if err == nil {
		return Result{Stdout: output}, nil
	}
	var exitErr *exec.ExitError
	if errors.As(err, &exitErr) {
		return Result{Stdout: output, ExitCode: exitErr.ExitCode()}, nil
	}
	return Result{}, err
}

type Config struct {
	WrkPath     string
	ArbiterPath string
	Runner      Runner
}

type Gate struct {
	wrk     string
	arbiter string
	runner  Runner
}

func New(cfg Config) *Gate {
	wrk := cfg.WrkPath
	if wrk == "" {
		wrk = "wrk"
	}
	arbiter := cfg.ArbiterPath
	if arbiter == "" {
		arbiter = "arbiter"
	}
	runner := cfg.Runner
	if runner == nil {
		runner = ExecRunner{}
	}
	return &Gate{wrk: wrk, arbiter: arbiter, runner: runner}
}

// Spawn accepts only the stable delivery ID as a job key.  A successful wrk
// process exit is not durable evidence by itself: arbiter job-get's receipt is
// re-read before core may ack a delivery.
func (g *Gate) Spawn(ctx context.Context, deliveryID string) (core.GateReceipt, error) {
	if deliveryID == "" {
		return core.GateReceipt{}, fmt.Errorf("wrk gate requires a delivery ID")
	}
	result, err := g.runner.Run(ctx, g.wrk, "spawn", "--job", deliveryID)
	if err != nil {
		return core.GateReceipt{Detail: "wrk invocation failed"}, fmt.Errorf("wrk invocation failed")
	}
	switch result.ExitCode {
	case 0:
		return g.Lookup(ctx, deliveryID)
	case ExitActiveJobDuplicate:
		// 74 is not a durable denial.  Another process may have already made
		// the delivery durable, so core must enter SPAWN_UNKNOWN and recover
		// through the one authoritative arbiter lookup surface.
		receipt, lookupErr := g.Lookup(ctx, deliveryID)
		if lookupErr != nil {
			return core.GateReceipt{Detail: "active duplicate lookup failed"}, lookupErr
		}
		if !receipt.Durable {
			receipt.Detail = "active duplicate requires durable recovery"
		}
		return receipt, nil
	case ExitJobStateUnreadable:
		// wrk already detected an ambiguous duplicate lookup.  Do not retry a
		// spawn and do not turn it into a durable denial.
		return core.GateReceipt{Detail: "wrk duplicate state unreadable"}, fmt.Errorf("wrk duplicate state unreadable")
	default:
		return core.GateReceipt{Detail: "wrk spawn failed"}, fmt.Errorf("wrk spawn failed")
	}
}

// Lookup maps `arbiter job-get --job <id> --json` into core's tri-state model.
// A not-found response is durable evidence of absence, not an accepted or
// denied spawn.  A job whose receipt is the explicit string "absent" remains
// SPAWN_UNKNOWN until a later recovery can prove its terminal state.
func (g *Gate) Lookup(ctx context.Context, deliveryID string) (core.GateReceipt, error) {
	if deliveryID == "" {
		return core.GateReceipt{}, fmt.Errorf("arbiter lookup requires a delivery ID")
	}
	result, err := g.runner.Run(ctx, g.arbiter, "job-get", "--job", deliveryID, "--json")
	if err != nil {
		return core.GateReceipt{Detail: "arbiter invocation failed"}, fmt.Errorf("arbiter invocation failed")
	}
	if result.ExitCode == ExitArbiterNotFound {
		return core.GateReceipt{Found: false, Durable: true, Detail: "arbiter job absent"}, nil
	}
	if result.ExitCode != 0 {
		return core.GateReceipt{Detail: "arbiter lookup failed"}, fmt.Errorf("arbiter lookup failed")
	}

	var payload struct {
		JobID   string          `json:"job_id"`
		Receipt json.RawMessage `json:"receipt"`
	}
	decoder := json.NewDecoder(bytes.NewReader(result.Stdout))
	if err := decoder.Decode(&payload); err != nil || payload.JobID != deliveryID || len(payload.Receipt) == 0 {
		return core.GateReceipt{Detail: "arbiter receipt is malformed"}, fmt.Errorf("arbiter receipt is malformed")
	}
	var absent string
	if err := json.Unmarshal(payload.Receipt, &absent); err == nil {
		if absent != "absent" {
			return core.GateReceipt{Detail: "arbiter receipt is malformed"}, fmt.Errorf("arbiter receipt is malformed")
		}
		return core.GateReceipt{Found: true, Durable: false, Detail: "arbiter spawn receipt absent"}, nil
	}
	var receipt struct {
		PaneID    string `json:"pane_id"`
		SpawnedAt string `json:"spawned_at"`
	}
	if err := json.Unmarshal(payload.Receipt, &receipt); err != nil || receipt.PaneID == "" || receipt.SpawnedAt == "" {
		return core.GateReceipt{Detail: "arbiter receipt is malformed"}, fmt.Errorf("arbiter receipt is malformed")
	}
	return core.GateReceipt{Accepted: true, Durable: true, Found: true, Detail: "arbiter spawn receipt"}, nil
}

var _ core.Gate = (*Gate)(nil)
