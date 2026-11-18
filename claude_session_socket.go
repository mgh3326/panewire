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
	// sessionSocketPathLimit is the longest Unix socket path Claude Code binds
	// (about 104 bytes on macOS); a longer value cannot be a session's socket.
	sessionSocketPathLimit = 108
)

const claudeSocketUsage = `usage: panewire claude-socket register [--lane L] [--pane P] [--inbox-root DIR]
Records this Claude Code session's inbox socket path for its lane, so the node
daemon can deliver the lane's inbox wake line as a session message. Run it from
the session's SessionStart hook: the path comes from CLAUDE_CODE_MESSAGING_SOCKET.
Defaults: --lane from PANEWIRE_LANE; --pane from HERDR_PANE_ID; --inbox-root as
for panewire emit. The pane is required: the node posts only to the lane's
routed pane.
Nothing is printed on success. The messaging token is never read or stored.`

// sessionRegistration is the stored record for one lane's session, whatever
// the harness: Claude and omp record a socket, codex records a thread id (and
// the CODEX_HOME it lives in). Unused fields are omitted, so a Claude record
// keeps the form it always had.
type sessionRegistration struct {
	Lane         string `json:"lane"`
	Pane         string `json:"pane"`
	Socket       string `json:"socket,omitempty"`
	Thread       string `json:"thread,omitempty"`
	CodexHome    string `json:"codex_home,omitempty"`
	RegisteredAt string `json:"registered_at"`
}

var (
	errSessionNotRegistered       = errors.New("no session registered for the lane")
	errSessionRegistrationInvalid = errors.New("registered session is not usable")
	// errSessionOtherPane: the session registered from a pane other than the
	// one the wake line was routed to.
	errSessionOtherPane = errors.New("registered session runs in another pane than the routed one")
)

// sessionRegistryPath is <inbox-root>/<subdir>/<lane>.json.
func sessionRegistryPath(inboxRoot, subdir, lane string) string {
	return filepath.Join(inboxRoot, subdir, lane+".json")
}

func claudeSocketRegistryPath(inboxRoot, lane string) string {
	return sessionRegistryPath(inboxRoot, claudeSocketRegistryDir, lane)
}

// validSessionSocketPath accepts an absolute, clean path to a socket that is
// not reached through a symlink. It does not check who owns it: the session
// binds it under a directory only its user can enter.
func validSessionSocketPath(path string) error {
	if path == "" || !filepath.IsAbs(path) || filepath.Clean(path) != path || len(path) > sessionSocketPathLimit {
		return errSessionRegistrationInvalid
	}
	info, err := os.Lstat(path)
	if err != nil {
		return err
	}
	if info.Mode()&os.ModeSocket == 0 {
		return errSessionRegistrationInvalid
	}
	return nil
}

// validRegistrationKey checks what names a registration: the root, the lane and
// the pane. Callers check it before their harness-specific fields, so a bad key
// is reported first.
func validRegistrationKey(inboxRoot, lane, pane string) bool {
	return inboxRoot != "" && validLaneReference(lane) && pane != "" && len(pane) <= 128
}

// writeSessionRegistration stores reg under subdir (mode 0600, replaced
// atomically). The caller has validated the harness-specific fields.
func writeSessionRegistration(inboxRoot, subdir string, reg sessionRegistration, now time.Time) error {
	if !validRegistrationKey(inboxRoot, reg.Lane, reg.Pane) {
		return errSessionRegistrationInvalid
	}
	reg.RegisteredAt = now.UTC().Format(time.RFC3339)
	body, err := json.Marshal(reg)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Join(inboxRoot, subdir), 0o700); err != nil {
		return err
	}
	return replaceFileAtomically(sessionRegistryPath(inboxRoot, subdir, reg.Lane), ".session-registration-*", append(body, '\n'), 0o600)
}

// readSessionRegistration returns the lane's registration when it was made
// from pane.
func readSessionRegistration(inboxRoot, subdir, lane, pane string) (sessionRegistration, error) {
	if inboxRoot == "" || !validLaneReference(lane) {
		return sessionRegistration{}, errSessionNotRegistered
	}
	raw, err := os.ReadFile(sessionRegistryPath(inboxRoot, subdir, lane))
	if errors.Is(err, os.ErrNotExist) {
		return sessionRegistration{}, errSessionNotRegistered
	}
	if err != nil {
		return sessionRegistration{}, err
	}
	var registration sessionRegistration
	if json.Unmarshal(raw, &registration) != nil || registration.Lane != lane {
		return sessionRegistration{}, errSessionRegistrationInvalid
	}
	if registration.Pane == "" || registration.Pane != pane {
		return sessionRegistration{}, errSessionOtherPane
	}
	return registration, nil
}

func writeClaudeSocketRegistration(inboxRoot, lane, pane, socket string, now time.Time) error {
	if !validRegistrationKey(inboxRoot, lane, pane) {
		return errSessionRegistrationInvalid
	}
	if err := validSessionSocketPath(socket); err != nil {
		return err
	}
	return writeSessionRegistration(inboxRoot, claudeSocketRegistryDir, sessionRegistration{Lane: lane, Pane: pane, Socket: socket}, now)
}

// readClaudeSocketRegistration returns the lane's registered socket path when
// the registration was made from pane.
func readClaudeSocketRegistration(inboxRoot, lane, pane string) (string, error) {
	registration, err := readSessionRegistration(inboxRoot, claudeSocketRegistryDir, lane, pane)
	if err != nil {
		return "", err
	}
	if err := validSessionSocketPath(registration.Socket); err != nil {
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
	if !validLaneReference(lane) {
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
	case errors.Is(err, errSessionRegistrationInvalid):
		return "invalid pane, or not an absolute, clean path to a socket"
	case errors.Is(err, os.ErrNotExist):
		return "the socket does not exist"
	}
	return "the registry could not be written"
}
