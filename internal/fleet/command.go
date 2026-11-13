package fleet

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"
	"time"
)

// Runner is the process boundary. Arguments never pass through a local shell.
type Runner interface {
	Run(context.Context, Command) Result
}
type Command struct {
	Name  string
	Args  []string
	Input string
	Env   []string
	Dir   string
}
type Result struct {
	Out, Err string
	Code     int
}
type ExecRunner struct{ Timeout time.Duration }

func (r ExecRunner) Run(ctx context.Context, c Command) Result {
	timeout := r.Timeout
	if timeout == 0 {
		timeout = 2 * time.Minute
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	cmd := exec.CommandContext(ctx, c.Name, c.Args...)
	cmd.WaitDelay = 100 * time.Millisecond
	cmd.Stdin = strings.NewReader(c.Input)
	cmd.Dir = c.Dir
	cmd.Env = append(os.Environ(), c.Env...)
	var out, err bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &err
	code := 0
	if e := cmd.Run(); e != nil && !(errors.Is(e, exec.ErrWaitDelay) && cmd.ProcessState != nil && cmd.ProcessState.Success()) {
		code = 1
		if x, ok := e.(*exec.ExitError); ok && x.ExitCode() >= 0 {
			code = x.ExitCode()
		} else {
			fmt.Fprintln(&err, e)
		}
		if ctx.Err() != nil {
			code = exitTimeout
		}
	}
	return Result{out.String(), err.String(), code}
}

// Exit statuses are part of the CLI contract; docs/config.md lists them and
// TestExitCodesDocumented keeps the two in step. Always pass one of these to
// fail rather than a literal number.
const (
	exitFailure             = 1   // generic failure (find selection, name-sync apply, restore receipt)
	exitUsage               = 2   // usage, task or policy target refusal
	exitUploadTimeout       = 3   // artifact upload confirmation timed out
	exitEvictionUnconfirmed = 4   // artifact Drive eviction unconfirmed
	exitConfig              = 70  // configuration, trust or unverified admission evidence
	exitDuplicate           = 74  // active job duplicate
	exitStateConflict       = 75  // unreadable/changed durable state
	exitSubmitUnconfirmed   = 76  // initial brief submission unconfirmed (ExitSubmitUnconfirmed outside this package)
	exitSeatRefused         = 77  // role/seat/grade/family/E6 refusal
	exitPreflight           = 78  // preflight: a remote host failed a check
	exitMockRefused         = 79  // mock isolation refusal
	exitLocalDisabled       = 80  // local spawning disabled without a remote fallback
	exitPlacement           = 81  // wake/capacity placement failure
	exitLaneUnregistered    = 82  // builder pane committed but lane unregistered
	exitWaitEscalated       = 83  // spawn --wait: the job escalated
	exitWaitLost            = 84  // spawn --wait: the job was lost or revoked
	exitWaitTimeout         = 85  // spawn --wait: the wait deadline expired
	exitRoleWait            = 86  // role routing: every role profile exhausted, wait for a reset
	exitRoleEscalate        = 87  // role routing: every role profile exhausted, ask the parent lane
	exitAuthUnavailable     = 88  // no usable login/account binding
	exitQuotaUnknown        = 89  // quota still unknown after one bounded refresh
	exitTrustPrompt         = 90  // harness is waiting for workspace trust
	exitTimeout             = 124 // a time-bounded CLI call hit its deadline
	exitUnreachable         = 255 // SSH could not reach the host (ssh's own status)
)

// Exit statuses of dependency CLIs that panewire interprets.
const (
	quotaGateBlocked    = 3 // legacy gate adapter: quota blocked; JSON needs typed evidence
	quotaGateRoleDenied = 5 // quota gate: the role may not use the seat
)

type ExitError struct {
	Code    int
	Message string
}

func (e *ExitError) Error() string { return e.Message }

type launchProofError struct{ *ExitError }

func (*launchProofError) LaunchFailureClass() string { return "harness_version" }
func (e *launchProofError) Unwrap() error            { return e.ExitError }
func fail(code int, format string, args ...any) error {
	return &ExitError{code, fmt.Sprintf(format, args...)}
}
func check(r Result, what string) error {
	if r.Code != 0 {
		return fail(r.Code, "%s: %s", what, strings.TrimSpace(r.Err+" "+r.Out))
	}
	return nil
}
func writeJSON(w io.Writer, v any) error {
	b, e := encode(v)
	if e == nil {
		_, e = fmt.Fprintln(w, string(b))
	}
	return e
}

// ExitSubmitUnconfirmed is the rc of a spawn whose initial brief submission was
// not confirmed. The node reads it to tell that kind of failure apart.
const ExitSubmitUnconfirmed = exitSubmitUnconfirmed

// ExitAuthUnavailable is the rc of a spawn refused because the executing host
// cannot confirm the selected account's login.
const ExitAuthUnavailable = exitAuthUnavailable
