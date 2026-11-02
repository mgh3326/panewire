package fleet

import (
	"context"
	"errors"
	"fmt"
	"io"
	"time"
)

type Execution struct {
	RC        int              `json:"rc"`
	Stage     string           `json:"stage"`
	StageMS   map[string]int64 `json:"stage_ms"`
	Retryable bool             `json:"retryable"`
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
	result := Execution{Stage: "placement", StageMS: map[string]int64{}}
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
	s.CWD, s.Prompt, s.Workspace, s.Host, s.Delegated = cwd, brief, workspace, "local", true
	a.ObserveStage = func(stage string, duration time.Duration) {
		result.Stage = stage
		result.StageMS[stage] += duration.Milliseconds()
	}
	a.ObserveRetryable = func(safe bool) { result.Retryable = safe }
	err = a.Spawn(ctx, s)
	result.RC = ErrorCode(err)
	if err != nil {
		fmt.Fprintln(errout, err)
		fmt.Fprintln(errout, "Fix:", spawnRefusalFix(err))
	}
	return result
}

// ExecuteLifecycle gives node commands the same completion/escalation entry
// point as the CLI, without launching another panewire process.
func ExecuteLifecycle(ctx context.Context, args []string, out, errout io.Writer) error {
	a, err := New(out, errout)
	if err != nil {
		return err
	}
	return a.Run(ctx, args)
}
