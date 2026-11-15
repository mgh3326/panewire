// Package daemonop is the wire contract for the local panewire daemon socket.
// The root package serves it; the fleet spawn engine speaks it, so the field
// names and the socket path live here instead of in two copies.
package daemonop

import (
	"path/filepath"
	"time"
)

// SocketPathEnv overrides the daemon's default socket path.
const SocketPathEnv = "PANEWIRE_SOCKET"

// DefaultSocketPath is the local socket under home when the environment names
// none. The daemon listens here and the fleet spawn engine dials the same path
// to ask the node's daemon to act with the hub credential it already holds.
func DefaultSocketPath(home string) string {
	return filepath.Join(home, "Library", "Application Support", "panewire", "panewire.sock")
}

// LanesPutOp registers one lane through the daemon's node hub client.
const LanesPutOp = "lanes.put"

// LanesPut is the payload of the lanes.put op. The caller wraps it in the local
// request envelope (op, timeout_ms) and the daemon answers in the local
// response envelope. Every field is omitempty so the payload leaves other ops'
// socket lines byte-identical.
type LanesPut struct {
	Lane    string `json:"lane,omitempty"`
	Machine string `json:"machine,omitempty"`
	Pane    string `json:"pane,omitempty"`
	Job     string `json:"job,omitempty"`
	Parent  string `json:"parent,omitempty"`
	// Takeover names the predecessor job whose lane row this write moves to
	// the requested route; see `panewire lanes add --takeover`.
	Takeover string `json:"takeover,omitempty"`
}

// InboxOp runs one inbox command with the node hub client the daemon holds, so
// a caller without the hub token reads the lanes its node serves.
const InboxOp = "inbox"

// Inbox is the payload of the inbox op: one `panewire inbox` command, answered
// with the text, streams and exit status that command would have printed
// itself. Both the CLI's own local route and the fleet spawn engine's wait
// send it, so the field names live here.
type Inbox struct {
	Command string        `json:"command"`
	Lane    string        `json:"lane,omitempty"`
	ID      int64         `json:"id,omitempty"`
	Unread  bool          `json:"unread,omitempty"`
	All     bool          `json:"all,omitempty"`
	JSON    bool          `json:"json,omitempty"`
	Timeout time.Duration `json:"timeout"`
	// EventIDs are the ids a status request asks about; no other command
	// carries them.
	EventIDs []string `json:"event_ids,omitempty"`
}

// ExitDaemonUnavailable is the response code for an op the daemon cannot serve
// (no listener, or no hub client behind the op). It is the root package's
// exit status of the same name, so a caller that has another route can fall
// back instead of reporting a failure.
const ExitDaemonUnavailable = 4
