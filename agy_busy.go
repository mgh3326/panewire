package panewire

import (
	"context"
	"strconv"

	"github.com/mgh3326/panewire/internal/agybusy"
)

// agyBusyRow applies the agy busy check (hk 5241) to one herdr row: an agy pane
// herdr reports idle or done is read again, and reads working while its screen
// shows a turn in progress or cannot be read. The rule is internal/agybusy; the
// contract is docs/relay-harness-delivery.md ("agy busy check"). It rewrites
// AgentStatus in place, so every caller of the node's agent listing and of
// readNodeAgentsInSession judges a finished-looking agy pane as working while
// its screen is busy.
func agyBusyRow(ctx context.Context, run relayCommandRunner, row *nodeAgentRow) {
	pane := row.PaneID
	row.AgentStatus = agybusy.Status(row.Agent, row.AgentStatus, func() (string, error) {
		out, err := run(ctx, "agent", "read", pane, "--source", "visible", "--lines", strconv.Itoa(agybusy.VisibleLines))
		return string(out), err
	})
}
