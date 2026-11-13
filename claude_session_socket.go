package panewire

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"
)

// A Claude Code session binds an inbox socket (docs/claude-session-message.md).
// The session's SessionStart hook runs `panewire claude-socket register`, which
// records that socket's PATH for the session's lane under the inbox root:
//
//	<inbox-root>/claude-sockets/<lane>.json
//
// The record also names the herdr pane the session runs in (HERDR_PANE_ID, as
// for panewire emit): the node posts only when that pane is the one the hub
// routed the wake line to, so a lane moved to another pane never has its line
// swallowed by the old session.
//
// Only the path is stored. CLAUDE_CODE_MESSAGING_TOKEN is never read: on macOS
// and Linux the socket accepts a connection without the auth line, and it is
// reachable only by the same operating-system user, which the node daemon is.

const (
	claudeSocketRegistryDir = "claude-sockets"
	claudeSocketEnv         = "CLAUDE_CODE_MESSAGING_SOCKET"
	// claudeSocketPathLimit is the longest Unix socket path Claude Code binds
	// (about 104 bytes on macOS); a longer value cannot be a session's socket.
	claudeSocketPathLimit = 108
)

const claudeSocketUsage = `usage: panewire claude-socket register [--lane L] [--pane P] [--inbox-root DIR]
Records this Claude Code session's inbox socket path for its lane, so the node
daemon can deliver the lane's inbox wake line as a session message. Run it from
the session's SessionStart hook: the path comes from CLAUDE_CODE_MESSAGING_SOCKET.
Defaults: --lane from PANEWIRE_LANE; --pane from HERDR_PANE_ID; --inbox-root as
for panewire emit. The pane is required: the node posts only to the lane's
routed pane.
Nothing is printed on success. The messaging token is never read or stored.`

// claudeSocketRegistration is the stored record for one lane.
type claudeSocketRegistration struct {
	Lane         string `json:"lane"`
	Pane         string `json:"pane"`
	Socket       string `json:"socket"`
	RegisteredAt string `json:"registered_at"`
}

var (
	errClaudeSocketMissing = errors.New("no session socket registered for the lane")
	errClaudeSocketInvalid = errors.New("registered session socket is not usable")
	// errClaudeSocketOtherPane: the session registered from a pane other than
	// the one the wake line was routed to.
	errClaudeSocketOtherPane = errors.New("registered session runs in another pane than the routed one")
)

func claudeSocketRegistryPath(inboxRoot, lane string) string {
	return filepath.Join(inboxRoot, claudeSocketRegistryDir, lane+".json")
}

// validClaudeSocketPath accepts an absolute, clean path to a socket that is
// not reached through a symlink. It does not check who owns it: Claude Code
// binds it under a directory only its user can enter.
func validClaudeSocketPath(path string) error {
	if path == "" || !filepath.IsAbs(path) || filepath.Clean(path) != path || len(path) > claudeSocketPathLimit {
		return errClaudeSocketInvalid
	}
	info, err := os.Lstat(path)
	if err != nil {
		return err
	}
	if info.Mode()&os.ModeSocket == 0 {
		return errClaudeSocketInvalid
	}
	return nil
}

func writeClaudeSocketRegistration(inboxRoot, lane, pane, socket string, now time.Time) error {
	if inboxRoot == "" || !LaneNamePattern.MatchString(lane) || pane == "" || len(pane) > 128 {
		return errClaudeSocketInvalid
	}
	if err := validClaudeSocketPath(socket); err != nil {
		return err
	}
	body, err := json.Marshal(claudeSocketRegistration{Lane: lane, Pane: pane, Socket: socket, RegisteredAt: now.UTC().Format(time.RFC3339)})
	if err != nil {
		return err
	}
	dir := filepath.Join(inboxRoot, claudeSocketRegistryDir)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return err
	}
	return replaceFileAtomically(claudeSocketRegistryPath(inboxRoot, lane), ".claude-socket-*", append(body, '\n'), 0o600)
}

// readClaudeSocketRegistration returns the lane's registered socket path when
// the registration was made from pane.
func readClaudeSocketRegistration(inboxRoot, lane, pane string) (string, error) {
	if inboxRoot == "" || !LaneNamePattern.MatchString(lane) {
		return "", errClaudeSocketMissing
	}
	raw, err := os.ReadFile(claudeSocketRegistryPath(inboxRoot, lane))
	if errors.Is(err, os.ErrNotExist) {
		return "", errClaudeSocketMissing
	}
	if err != nil {
		return "", err
	}
	var registration claudeSocketRegistration
	if json.Unmarshal(raw, &registration) != nil || registration.Lane != lane {
		return "", errClaudeSocketInvalid
	}
	if registration.Pane == "" || registration.Pane != pane {
		return "", errClaudeSocketOtherPane
	}
	if err := validClaudeSocketPath(registration.Socket); err != nil {
		return "", err
	}
	return registration.Socket, nil
}

// runClaudeSocketCLI is `panewire claude-socket`. now is a seam for tests.
func runClaudeSocketCLI(args []string, stdout, stderr io.Writer, now func() time.Time) int {
	if len(args) == 0 {
		fmt.Fprintln(stderr, claudeSocketUsage)
		return ExitUsage
	}
	if args[0] == "--help" || args[0] == "-h" {
		fmt.Fprintln(stdout, claudeSocketUsage)
		return ExitOK
	}
	if args[0] != "register" {
		fmt.Fprintln(stderr, claudeSocketUsage)
		return ExitUsage
	}
	lane, pane, root := os.Getenv("PANEWIRE_LANE"), os.Getenv("HERDR_PANE_ID"), ""
	rest := args[1:]
	for len(rest) > 0 {
		if len(rest) < 2 || (rest[0] != "--lane" && rest[0] != "--pane" && rest[0] != "--inbox-root") {
			fmt.Fprintln(stderr, claudeSocketUsage)
			return ExitUsage
		}
		switch rest[0] {
		case "--lane":
			lane = rest[1]
		case "--pane":
			pane = rest[1]
		default:
			root = rest[1]
		}
		rest = rest[2:]
	}
	if !LaneNamePattern.MatchString(lane) {
		fmt.Fprintln(stderr, "panewire claude-socket: a valid --lane (or PANEWIRE_LANE) is required")
		return ExitUsage
	}
	if pane == "" {
		fmt.Fprintln(stderr, "panewire claude-socket: a --pane (or HERDR_PANE_ID) is required: the registration is bound to the session's pane")
		return ExitUsage
	}
	socket := os.Getenv(claudeSocketEnv)
	if socket == "" {
		fmt.Fprintln(stderr, "panewire claude-socket: "+claudeSocketEnv+" is not set; this is not a Claude Code session with messaging")
		return ExitUsage
	}
	if err := writeClaudeSocketRegistration(emitInboxRoot(root), lane, pane, socket, now()); err != nil {
		// The path and the lane are the caller's; the error names neither.
		fmt.Fprintln(stderr, "panewire claude-socket: registration refused or failed:", claudeSocketFailureKind(err))
		return ExitInternal
	}
	return ExitOK
}

func claudeSocketFailureKind(err error) string {
	switch {
	case errors.Is(err, errClaudeSocketInvalid):
		return "invalid pane, or not an absolute, clean path to a socket"
	case errors.Is(err, os.ErrNotExist):
		return "the socket does not exist"
	}
	return "the registry could not be written"
}
