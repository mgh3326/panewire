package panewire

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"time"
)

// The codex receiver (hk 4261, docs/session-message-receivers.md). A codex
// session's SessionStart hook runs `panewire codex-thread register`, which
// records the session's thread id for its lane:
//
//	<inbox-root>/codex-threads/<lane>.json
//
// For the lanes in PANEWIRE_CODEX_SESSION_MESSAGE_LANES the node then delivers
// the lane's wake line with `codex queue --thread <id> --message <line>` instead
// of typing it. `codex queue` writes the line to the thread's queue in the
// session's CODEX_HOME; a loaded, idle thread starts a turn from it at once and
// shows it as a plain user message (checked on codex-cli 0.167.0).

const (
	codexSessionMessageLanesEnv = "PANEWIRE_CODEX_SESSION_MESSAGE_LANES"
	codexThreadRegistryDir      = "codex-threads"
	// codexQueueTimeout bounds one `codex queue` run. The command only writes the
	// queue; it does not wait for a turn.
	codexQueueTimeout = 15 * time.Second
)

// errCodexQueueRejected: `codex queue` failed or could not run (no executable,
// no such active thread, a non-zero exit).
var errCodexQueueRejected = errors.New("codex queue did not accept the message")

// codexThreadID is the form codex gives a thread: a UUID. Anything else is
// refused, so a registration can never put a flag or a name in the argv.
var codexThreadID = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$`)

const codexThreadUsage = `usage: panewire codex-thread register [--lane L] [--pane P] [--thread ID] [--inbox-root DIR]
Records this Codex session's thread id for its lane, so the node daemon can
deliver the lane's inbox wake line with 'codex queue'. Run it from the session's
SessionStart hook: the thread id is the hook input's session_id (JSON on stdin),
unless --thread gives it. CODEX_THREAD_ID is not used: a nested session inherits
its parent's.
Defaults: --lane from PANEWIRE_LANE; --pane from HERDR_PANE_ID; --inbox-root as
for panewire emit. The pane is required: the node delivers only to the lane's
routed pane. A CODEX_HOME in the environment is recorded with the thread.
Nothing is printed on success.`

// postCodexWake queues the wake line on the lane's registered codex thread.
func (client *HubClient) postCodexWake(ctx context.Context, lane string, message hubOutboundMessage) error {
	registration, err := readSessionRegistration(emitInboxRoot(client.jobsInboxRoot), codexThreadRegistryDir, lane, message.Pane)
	if err != nil {
		return err
	}
	if !codexThreadID.MatchString(registration.Thread) || !validCodexHome(registration.CodexHome) {
		return errSessionRegistrationInvalid
	}
	queue := client.codexQueue
	if queue == nil {
		queue = runCodexQueue
	}
	ctx, cancel := context.WithTimeout(ctx, codexQueueTimeout)
	defer cancel()
	if err := queue(ctx, registration.CodexHome, registration.Thread, "panewire-hub: "+message.Text); err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return fmt.Errorf("%w: %v", errCodexQueueRejected, err)
	}
	return nil
}

// validCodexHome accepts no CODEX_HOME (the daemon's own) or an absolute, clean
// path.
func validCodexHome(home string) bool {
	return home == "" || (filepath.IsAbs(home) && filepath.Clean(home) == home)
}

// runCodexQueue is the real `codex queue`. Its output (a warning line about
// helper binaries, or the queued message id) is not logged.
func runCodexQueue(ctx context.Context, codexHome, thread, text string) error {
	cmd := exec.CommandContext(ctx, "codex", "queue", "--thread", thread, "--message="+text)
	if codexHome != "" {
		cmd.Env = append(os.Environ(), "CODEX_HOME="+codexHome)
	}
	if _, err := cmd.CombinedOutput(); err != nil {
		return errors.New(codexQueueFailureKind(err))
	}
	return nil
}

func codexQueueFailureKind(err error) string {
	var exit *exec.ExitError
	switch {
	case errors.Is(err, exec.ErrNotFound):
		return "codex not found"
	case errors.As(err, &exit):
		return fmt.Sprintf("exit status %d", exit.ExitCode())
	}
	return "could not run"
}

// codexHookInput is the part of a Codex hook's stdin JSON the registration
// reads.
type codexHookInput struct {
	SessionID string `json:"session_id"`
}

// runCodexThreadCLI is `panewire codex-thread`. stdin is the hook input, or nil
// when there is none; now is a seam for tests.
func runCodexThreadCLI(args []string, stdin io.Reader, stdout, stderr io.Writer, now func() time.Time) int {
	if len(args) == 0 {
		fmt.Fprintln(stderr, codexThreadUsage)
		return ExitUsage
	}
	if args[0] == "--help" || args[0] == "-h" {
		fmt.Fprintln(stdout, codexThreadUsage)
		return ExitOK
	}
	if args[0] != "register" {
		fmt.Fprintln(stderr, codexThreadUsage)
		return ExitUsage
	}
	lane, pane, root, thread := os.Getenv("PANEWIRE_LANE"), os.Getenv("HERDR_PANE_ID"), "", ""
	rest := args[1:]
	for len(rest) > 0 {
		if len(rest) < 2 || (rest[0] != "--lane" && rest[0] != "--pane" && rest[0] != "--inbox-root" && rest[0] != "--thread") {
			fmt.Fprintln(stderr, codexThreadUsage)
			return ExitUsage
		}
		switch rest[0] {
		case "--lane":
			lane = rest[1]
		case "--pane":
			pane = rest[1]
		case "--thread":
			thread = rest[1]
		default:
			root = rest[1]
		}
		rest = rest[2:]
	}
	if !validLaneReference(lane) {
		fmt.Fprintln(stderr, "panewire codex-thread: a valid --lane (or PANEWIRE_LANE) is required")
		return ExitUsage
	}
	if pane == "" {
		fmt.Fprintln(stderr, "panewire codex-thread: a --pane (or HERDR_PANE_ID) is required: the registration is bound to the session's pane")
		return ExitUsage
	}
	if thread == "" && stdin != nil {
		var input codexHookInput
		// A hook's input is small; the cap keeps a wrong pipe from filling memory.
		if json.NewDecoder(io.LimitReader(stdin, 1<<20)).Decode(&input) == nil {
			thread = input.SessionID
		}
	}
	if !codexThreadID.MatchString(thread) {
		fmt.Fprintln(stderr, "panewire codex-thread: no thread id: give --thread, or run it from a Codex SessionStart hook (session_id on stdin)")
		return ExitUsage
	}
	home := os.Getenv("CODEX_HOME")
	if !validCodexHome(home) {
		home = ""
	}
	if !validRegistrationKey(emitInboxRoot(root), lane, pane) {
		fmt.Fprintln(stderr, "panewire codex-thread: registration refused: invalid pane")
		return ExitInternal
	}
	if err := writeSessionRegistration(emitInboxRoot(root), codexThreadRegistryDir, sessionRegistration{Lane: lane, Pane: pane, Thread: thread, CodexHome: home}, now()); err != nil {
		// The thread id and the lane are the caller's; the error names neither.
		fmt.Fprintln(stderr, "panewire codex-thread: registration refused or failed: the registry could not be written")
		return ExitInternal
	}
	return ExitOK
}
