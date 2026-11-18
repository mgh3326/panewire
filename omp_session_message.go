package panewire

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"time"
)

// The omp receiver (hk 4261, docs/session-message-receivers.md). omp has no
// inbox socket of its own, so panewire ships an omp extension
// (docs/examples/omp-session-message.ts). In a session whose lane is in
// PANEWIRE_OMP_SESSION_MESSAGE_LANES the extension binds a socket, records its
// path with `panewire omp-socket register`, and turns each line the node posts
// into pi.sendMessage(..., {triggerTurn: true, deliverAs: "aside"}): an idle
// session starts a turn with it, a working one reads it at the next step
// boundary without interrupting the tool batch in flight. Command Code runs
// through omp and needs no receiver of its own.
//
// The wire is one JSON line each way, and the extension answers only after it
// has called pi.sendMessage, so the node can report a stage it has evidence for
// (acked), unlike a post to a Claude Code socket.

const (
	ompSessionMessageLanesEnv = "PANEWIRE_OMP_SESSION_MESSAGE_LANES"
	ompSocketRegistryDir      = "omp-sockets"
	// ompSessionPostTimeout bounds connect, write and the wait for the ack.
	ompSessionPostTimeout = 5 * time.Second
	// ompAckLimit caps the ack line the node reads.
	ompAckLimit = 4096
)

// errSessionNoAck: the extension did not confirm the line (closed without an
// ok, answered not-ok, or stayed silent).
var errSessionNoAck = errors.New("omp extension did not acknowledge the message")

const ompSocketUsage = `usage: panewire omp-socket register --socket PATH [--lane L] [--pane P] [--inbox-root DIR]
Records this omp session's panewire extension socket for its lane, so the node
daemon can deliver the lane's inbox wake line through pi.sendMessage. The
extension (docs/examples/omp-session-message.ts) runs it at session start.
Defaults: --lane from PANEWIRE_LANE; --pane from HERDR_PANE_ID; --inbox-root as
for panewire emit. The pane is required: the node delivers only to the lane's
routed pane. Nothing is printed on success.`

// ompLine is the node's one line to the extension.
type ompLine struct {
	Type string `json:"type"`
	Text string `json:"text"`
}

// ompAck is the extension's one line back.
type ompAck struct {
	OK bool `json:"ok"`
}

// postOmpWake posts the wake line to the lane's registered omp extension.
func (client *HubClient) postOmpWake(ctx context.Context, lane string, message hubOutboundMessage) error {
	registration, err := readSessionRegistration(emitInboxRoot(client.jobsInboxRoot), ompSocketRegistryDir, lane, message.Pane)
	if err != nil {
		return err
	}
	return postOmpSessionMessage(ctx, registration.Socket, "panewire-hub: "+message.Text)
}

// postOmpSessionMessage writes one line to the extension's socket and waits for
// its ok. Success means the extension handed the line to pi.sendMessage.
func postOmpSessionMessage(ctx context.Context, socket, text string) error {
	if err := validSessionSocketPath(socket); err != nil {
		return err
	}
	body, err := json.Marshal(ompLine{Type: "panewire.message", Text: text})
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(ctx, ompSessionPostTimeout)
	defer cancel()
	var dialer net.Dialer
	conn, err := dialer.DialContext(ctx, "unix", socket)
	if err != nil {
		return err
	}
	defer conn.Close()
	if deadline, ok := ctx.Deadline(); ok {
		_ = conn.SetDeadline(deadline)
	}
	if _, err := conn.Write(append(body, '\n')); err != nil {
		return err
	}
	line, err := bufio.NewReader(io.LimitReader(conn, ompAckLimit)).ReadBytes('\n')
	if err != nil {
		if errors.Is(err, os.ErrDeadlineExceeded) {
			return context.DeadlineExceeded
		}
		return fmt.Errorf("%w: %v", errSessionNoAck, err)
	}
	var ack ompAck
	if json.Unmarshal(line, &ack) != nil || !ack.OK {
		return errSessionNoAck
	}
	return nil
}

// runOmpSocketCLI is `panewire omp-socket`. now is a seam for tests.
func runOmpSocketCLI(args []string, stdout, stderr io.Writer, now func() time.Time) int {
	if len(args) == 0 {
		fmt.Fprintln(stderr, ompSocketUsage)
		return ExitUsage
	}
	if args[0] == "--help" || args[0] == "-h" {
		fmt.Fprintln(stdout, ompSocketUsage)
		return ExitOK
	}
	if args[0] != "register" {
		fmt.Fprintln(stderr, ompSocketUsage)
		return ExitUsage
	}
	lane, pane, root, socket := os.Getenv("PANEWIRE_LANE"), os.Getenv("HERDR_PANE_ID"), "", ""
	rest := args[1:]
	for len(rest) > 0 {
		if len(rest) < 2 || (rest[0] != "--lane" && rest[0] != "--pane" && rest[0] != "--inbox-root" && rest[0] != "--socket") {
			fmt.Fprintln(stderr, ompSocketUsage)
			return ExitUsage
		}
		switch rest[0] {
		case "--lane":
			lane = rest[1]
		case "--pane":
			pane = rest[1]
		case "--socket":
			socket = rest[1]
		default:
			root = rest[1]
		}
		rest = rest[2:]
	}
	if !validLaneReference(lane) {
		fmt.Fprintln(stderr, "panewire omp-socket: a valid --lane (or PANEWIRE_LANE) is required")
		return ExitUsage
	}
	if pane == "" {
		fmt.Fprintln(stderr, "panewire omp-socket: a --pane (or HERDR_PANE_ID) is required: the registration is bound to the session's pane")
		return ExitUsage
	}
	if socket == "" {
		fmt.Fprintln(stderr, "panewire omp-socket: --socket is required")
		return ExitUsage
	}
	if !validRegistrationKey(emitInboxRoot(root), lane, pane) {
		fmt.Fprintln(stderr, "panewire omp-socket: registration refused: invalid pane")
		return ExitInternal
	}
	if err := validSessionSocketPath(socket); err != nil {
		fmt.Fprintln(stderr, "panewire omp-socket: registration refused or failed:", claudeSocketFailureKind(err))
		return ExitInternal
	}
	if err := writeSessionRegistration(emitInboxRoot(root), ompSocketRegistryDir, sessionRegistration{Lane: lane, Pane: pane, Socket: socket}, now()); err != nil {
		fmt.Fprintln(stderr, "panewire omp-socket: registration refused or failed: the registry could not be written")
		return ExitInternal
	}
	return ExitOK
}
