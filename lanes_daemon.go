package panewire

import (
	"context"
	"errors"

	"github.com/mgh3326/panewire/internal/daemonop"
)

// handleLanesPut registers one lane through the daemon's node hub client: the
// same PUT the `lanes add` CLI makes, with the credential, hub URL list and
// Cloudflare Access pair the daemon already holds. A node therefore registers
// a spawned lane without a fleet-config token file, and any refusal keeps the
// CLI's text and exit status so the spawn receipt records it unchanged.
func (d *Daemon) handleLanesPut(ctx context.Context, req daemonop.LanesPut) (any, error) {
	if detail, code := validateLaneAdd(req.Lane, req.Machine, req.Pane, req.Parent, false); code != ExitOK {
		return nil, &codedError{code, errors.New(detail)}
	}
	if d.cfg.Hub.Client == nil {
		return nil, &codedError{ExitDaemonUnavailable, errors.New("lanes: daemon has no hub client")}
	}
	result, detail, code := laneAddThroughHub(ctx, nodeHubClient{ctx: ctx, client: d.cfg.Hub.Client}, lanesCLIOptions{Lane: req.Lane, Machine: req.Machine, Pane: req.Pane, Parent: req.Parent})
	if code != ExitOK {
		return nil, &codedError{code, errors.New(detail)}
	}
	return result, nil
}
