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
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

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
	WrkPath         string
	ArbiterPath     string
	SpawnPolicyPath string
	Runner          Runner
}

type Gate struct {
	wrk       string
	arbiter   string
	runner    Runner
	policy    spawnPolicy
	policyErr error
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
	policy, policyErr := loadSpawnPolicy(cfg.SpawnPolicyPath)
	return &Gate{wrk: wrk, arbiter: arbiter, runner: runner, policy: policy, policyErr: policyErr}
}

const maxSpawnPolicyBytes = 1 << 20

// spawnPolicy is intentionally local to the receiver. The sender may choose a
// label, but cannot select any executable, workspace, tier, or working
// directory through the transport envelope.
type spawnPolicy struct {
	Rules []spawnPolicyRule `json:"rules"`
}

type spawnPolicyRule struct {
	LabelPrefix string `json:"label_prefix"`
	Model       string `json:"model"`
	Workspace   string `json:"workspace"`
	Tier        string `json:"t"`
	CWD         string `json:"cwd"`
}

func loadSpawnPolicy(path string) (spawnPolicy, error) {
	if path == "" {
		return spawnPolicy{}, fmt.Errorf("spawn policy path is required")
	}
	info, err := os.Lstat(path)
	if err != nil || info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() {
		return spawnPolicy{}, fmt.Errorf("spawn policy must be a regular file")
	}
	file, err := os.Open(path)
	if err != nil {
		return spawnPolicy{}, fmt.Errorf("open spawn policy")
	}
	defer file.Close()
	data, err := io.ReadAll(io.LimitReader(file, maxSpawnPolicyBytes+1))
	if err != nil || len(data) > maxSpawnPolicyBytes {
		return spawnPolicy{}, fmt.Errorf("read spawn policy")
	}
	var policy spawnPolicy
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&policy); err != nil {
		return spawnPolicy{}, fmt.Errorf("decode spawn policy")
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		return spawnPolicy{}, fmt.Errorf("spawn policy has trailing data")
	}
	if len(policy.Rules) == 0 {
		return spawnPolicy{}, fmt.Errorf("spawn policy has no rules")
	}
	seen := make(map[string]struct{}, len(policy.Rules))
	for _, rule := range policy.Rules {
		if !validSpawnLabel(rule.LabelPrefix) || !safePolicyAtom(rule.Model) || !safePolicyAtom(rule.Workspace) || !validTier(rule.Tier) || !validPolicyCWD(rule.CWD) {
			return spawnPolicy{}, fmt.Errorf("spawn policy rule is invalid")
		}
		if _, exists := seen[rule.LabelPrefix]; exists {
			return spawnPolicy{}, fmt.Errorf("spawn policy has duplicate label prefixes")
		}
		seen[rule.LabelPrefix] = struct{}{}
	}
	return policy, nil
}

func (p spawnPolicy) match(label string) (spawnPolicyRule, bool) {
	if !validSpawnLabel(label) {
		return spawnPolicyRule{}, false
	}
	var selected spawnPolicyRule
	found := false
	for _, rule := range p.Rules {
		if strings.HasPrefix(label, rule.LabelPrefix) && (!found || len(rule.LabelPrefix) > len(selected.LabelPrefix)) {
			selected, found = rule, true
		}
	}
	return selected, found
}

func validSpawnLabel(value string) bool {
	if len(value) == 0 || len(value) > 32 || value[0] < 'a' || value[0] > 'z' {
		return false
	}
	for _, char := range value[1:] {
		if (char >= 'a' && char <= 'z') || (char >= '0' && char <= '9') || char == '_' || char == '-' {
			continue
		}
		return false
	}
	return true
}

func safePolicyAtom(value string) bool {
	return value != "" && !strings.ContainsAny(value, " \t\r\n\x00")
}

func validTier(value string) bool {
	return value == "T0" || value == "T1" || value == "T2" || value == "T3"
}

func validPolicyCWD(value string) bool {
	return value != "" && filepath.IsAbs(value) && filepath.Clean(value) == value && !strings.ContainsRune(value, '\x00')
}

// Spawn accepts the stable delivery ID plus the just-materialized prompt path
// and a sender label. The receiving policy, not the envelope, supplies model,
// workspace, tier, and CWD. A successful wrk process exit is not durable
// evidence by itself: arbiter job-get's receipt is re-read before core may ack.
func (g *Gate) Spawn(ctx context.Context, request core.GateSpawnRequest) (core.GateReceipt, error) {
	if request.DeliveryID == "" {
		return core.GateReceipt{}, fmt.Errorf("wrk gate requires a delivery ID")
	}
	if !filepath.IsAbs(request.PromptPath) {
		return core.GateReceipt{Durable: true, Detail: "materialized prompt path is invalid", RejectionCode: core.CodeGateDenied}, nil
	}
	if g.policyErr != nil {
		return core.GateReceipt{Durable: true, Detail: "wrk spawn policy is unavailable", RejectionCode: core.CodeGateNotInstalled}, nil
	}
	rule, found := g.policy.match(request.Label)
	if !found {
		return core.GateReceipt{Durable: true, Detail: "wrk spawn policy does not match label", RejectionCode: core.CodeGateDenied}, nil
	}
	result, err := g.runner.Run(ctx, g.wrk,
		"spawn", "-c", rule.CWD, "-m", rule.Model, "-p", request.PromptPath,
		"-w", rule.Workspace, "-l", request.Label, "--t", rule.Tier, "--job", request.DeliveryID,
	)
	if err != nil {
		if errors.Is(err, exec.ErrNotFound) || errors.Is(err, os.ErrNotExist) {
			return core.GateReceipt{Durable: true, Detail: "wrk is not installed", RejectionCode: core.CodeGateNotInstalled}, nil
		}
		return core.GateReceipt{Detail: "wrk invocation failed"}, fmt.Errorf("wrk invocation failed")
	}
	switch result.ExitCode {
	case 0:
		return g.Lookup(ctx, request.DeliveryID)
	case ExitActiveJobDuplicate:
		// 74 is not a durable denial.  Another process may have already made
		// the delivery durable, so core must enter SPAWN_UNKNOWN and recover
		// through the one authoritative arbiter lookup surface.
		receipt, lookupErr := g.Lookup(ctx, request.DeliveryID)
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
