package panewire

import (
	"errors"
	"math/rand/v2"
	"time"
)

// How a node comes back after its hub session ends. Contract: docs/hub-drain.md.
//
// The ordinary reconnect back-off doubles up to a minute, which is right for a
// hub that is down for a reason nobody is fixing. A hub that closed the socket
// because it is restarting is back in seconds, so for a while after a session
// ends the node retries on a short, jittered schedule instead. The jitter keeps
// a fleet of nodes from dialling in the same instant.
const (
	// hubRedialFirstMax bounds the pause before the first retry after a
	// session ends.
	hubRedialFirstMax = 250 * time.Millisecond
	// hubRedialFastDelay caps the retry pause inside the fast window.
	hubRedialFastDelay = time.Second
	// hubRedialFastWindow is how long after a session ended the fast schedule
	// applies. Past it the node falls back to the ordinary back-off.
	hubRedialFastWindow = 2 * time.Minute
)

// errHubRedial ends a hub session on the hub's own request.
var errHubRedial = errors.New("hub asked for a redial")

// hubRedialDelay is the pause before the next dial inside the fast window:
// the back-off capped at hubRedialFastDelay and jittered to between half and one
// and a half times that, and for the first retry a random pause up to
// hubRedialFirstMax so a node that lost its socket redials at once.
func hubRedialDelay(backoff time.Duration, first bool) time.Duration {
	capped := min(backoff, hubRedialFastDelay)
	if first {
		return rand.N(max(1, min(capped, hubRedialFirstMax)))
	}
	return capped/2 + rand.N(max(1, capped))
}
