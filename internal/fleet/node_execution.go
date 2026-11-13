package fleet

import (
	"context"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"time"
)

type Execution struct {
	Receipt      ExecutionReceipt `json:"receipt"`
	RC           int              `json:"rc"`
	Stage        string           `json:"stage"`
	StageMS      map[string]int64 `json:"stage_ms"`
	Retryable    bool             `json:"retryable"`
	FailureClass string           `json:"failure_class,omitempty"`
}

func ErrorCode(err error) int {
	if err == nil {
		return 0
	}
	var exit *ExitError
	if errors.As(err, &exit) {
		return exit.Code
	}
	return exitFailure
}

// ExecuteNode is the node daemon's in-process launcher boundary. It shares the
// CLI engine and guards, while execution paths and session come from node config.
func ExecuteNode(ctx context.Context, cwd, brief, workspace, session string, args []string, out, errout io.Writer) Execution {
	result := Execution{Stage: "placement", StageMS: map[string]int64{}, Retryable: true}
	a, err := New(out, errout)
	if err != nil {
		result.RC = ErrorCode(err)
		fmt.Fprintln(errout, err)
		return result
	}
	a.HerdrSession = session
	if err = a.trust(); err != nil {
		result.RC = ErrorCode(err)
		fmt.Fprintln(errout, err)
		return result
	}
	s, err := parseSpawn(args)
	if err != nil {
		result.RC = ErrorCode(err)
		fmt.Fprintln(errout, err)
		return result
	}
	s = bindNodeSpawn(s, cwd, brief, workspace)
	a.ObserveStage = func(stage string, duration time.Duration) {
		if stage == "claim" {
			result.Retryable = false
		}
		result.Stage = stage
		result.StageMS[stage] += duration.Milliseconds()
	}
	a.ObserveReceipt = func(receipt ExecutionReceipt) { result.Receipt = receipt }
	a.ObserveRetryable = func(safe bool) { result.Retryable = safe }
	err = a.Spawn(ctx, s)
	result.RC = ErrorCode(err)
	var proof interface{ LaunchFailureClass() string }
	if errors.As(err, &proof) {
		result.FailureClass = proof.LaunchFailureClass()
	}
	if err != nil {
		fmt.Fprintln(errout, err)
		fmt.Fprintln(errout, "Fix:", spawnRefusalFix(err))
	}
	return result
}

// ExecuteLifecycle gives node commands the same completion/escalation entry
// point as the CLI, without launching another panewire process.
func ExecuteLifecycle(ctx context.Context, jobsRoot string, args []string, out, errout io.Writer) error {
	a, err := New(out, errout)
	if err != nil {
		return err
	}
	return a.executeLifecycleAt(ctx, jobsRoot, args)
}

func (a *App) executeLifecycleAt(ctx context.Context, jobsRoot string, args []string) error {
	if !filepath.IsAbs(jobsRoot) {
		return fail(exitUsage, "node lifecycle requires an absolute jobs root")
	}
	a.Jobs = filepath.Clean(jobsRoot)
	return a.Run(ctx, args)
}

type ExecutionReceipt struct {
	Job     string `json:"job"`
	Host    string `json:"host"`
	Pane    string `json:"pane"`
	Profile string `json:"profile"`
	Model   string `json:"model"`
	Role    string `json:"role"`
	CWD     string `json:"cwd"`
	Label   string `json:"label"`
	Kind    string `json:"kind"`
	// LaneUnregistered is true when the pane exists but hub lane registration
	// failed after its retry (receipt field lane_unregistered).
	LaneUnregistered bool `json:"lane_unregistered,omitempty"`
}

// ExistingJob exposes only the durable job read needed before hub acceptance.
func (a *App) ExistingJob(ctx context.Context, id string) (map[string]any, bool, error) {
	store, err := a.jobStore()
	if err != nil {
		return nil, false, err
	}
	record, err := store.Get(ctx, id)
	if errors.Is(err, errJobNotFound) {
		return nil, false, nil
	}
	return record, err == nil, err
}

func (a *App) CheckTrust() error {
	if a.EnforceTrust {
		return a.trust()
	}
	return nil
}

func bindNodeSpawn(s Spawn, cwd, brief, workspace string) Spawn {
	s.CWD, s.Prompt, s.Host, s.Delegated = cwd, brief, "local", true
	// A node-delivered spawn's marker is the --e6-arm the hub sent; the node
	// process environment never fills an empty one (hk 3518).
	s.e6ArmLocked = true
	if s.Workspace == "" {
		s.Workspace = workspace
	}
	return s
}
