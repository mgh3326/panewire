package panewire

import (
	"context"
	"net/http"
	"sync"
	"time"

	"github.com/coder/websocket"
)

// The hub's answer to SIGTERM. Contract and unit settings: docs/hub-drain.md.
//
// A restart that interrupts nothing needs the old hub to stop taking work it
// cannot finish, to finish what it has, and to tell every peer to come back
// at once instead of leaving them to a back-off that grew while it was down.

const (
	// hubDrainTimeout bounds how long in-flight requests and spawn dispatches
	// get after SIGTERM before they are cut. A spawn dispatch cut here is
	// resumed by the next hub from its durable row (docs/hub-spawn-operations.md).
	hubDrainTimeout = 20 * time.Second
	// hubDrainPollInterval is how often the drain looks at the in-flight count.
	hubDrainPollInterval = 5 * time.Millisecond
	// hubDrainPeerCloseBudget bounds how long the drain waits for node sockets to
	// answer their close frame.
	hubDrainPeerCloseBudget = time.Second
	// hubDrainShutdownBudget bounds the listener shutdown after the drain.
	hubDrainShutdownBudget = 2 * time.Second
	// hubDrainRetryAfterSeconds is the Retry-After a refused caller is given.
	hubDrainRetryAfterSeconds = "1"
)

// hubDrain is the hub's drain state. The zero value is a hub that is not
// draining.
type hubDrain struct {
	mu       sync.Mutex
	draining bool
	// active counts the requests and spawn dispatches that began before the
	// drain and have not finished.
	active  int
	started chan struct{}
}

func (d *hubDrain) startedLocked() chan struct{} {
	if d.started == nil {
		d.started = make(chan struct{})
	}
	return d.started
}

// Started is closed when the drain begins.
func (d *hubDrain) Started() <-chan struct{} {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.startedLocked()
}

func (d *hubDrain) begin() bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.draining {
		return false
	}
	d.draining = true
	close(d.startedLocked())
	return true
}

// enter counts one unit of work and reports false, counting nothing, once the
// drain has begun. The check and the count share one lock so that no unit of
// work can slip in after the drain looked at the count.
func (d *hubDrain) enter() bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.draining {
		return false
	}
	d.active++
	return true
}

func (d *hubDrain) leave() {
	d.mu.Lock()
	d.active--
	d.mu.Unlock()
}

// admit reports whether new work may start, without counting it. A long-lived
// socket asks this: the drain does not wait for it, it ends it.
func (d *hubDrain) admit() bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	return !d.draining
}

func (d *hubDrain) idle() bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.active == 0
}

// Draining reports whether SIGTERM has been handled and the hub refuses new work.
func (h *HubServer) Draining() bool { return !h.drain.admit() }

// BeginDrain makes the hub refuse new work with a retryable answer and ends
// the long polls in progress. It is idempotent.
func (h *HubServer) BeginDrain() { h.drain.begin() }

// AwaitDrained waits until every request and spawn dispatch that began before
// the drain has finished, or ctx ends. It reports which.
func (h *HubServer) AwaitDrained(ctx context.Context) bool {
	ticker := time.NewTicker(hubDrainPollInterval)
	defer ticker.Stop()
	for !h.drain.idle() {
		select {
		case <-ctx.Done():
			return false
		case <-ticker.C:
		}
	}
	return true
}

// Drain is the whole SIGTERM answer short of closing the listeners: refuse new
// work, let what is in flight finish for at most timeout, then send every
// connected node "redial" and close its socket. It reports whether the
// in-flight work finished inside the budget.
func (h *HubServer) Drain(timeout time.Duration) bool {
	h.BeginDrain()
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	finished := h.AwaitDrained(ctx)
	if !finished {
		h.logger.Warn("hub drain budget spent; cutting work still in flight", "budget", timeout.String())
	}
	h.redialNodes()
	return finished
}

// redialNodes tells each connected node to dial again at once, then closes its
// socket. It runs after the in-flight work, so a spawn result a node still
// owed reached this hub first; a node that reconnects to this process while it
// exits is refused with the drain answer and retries on its fast schedule.
func (h *HubServer) redialNodes() {
	h.mu.Lock()
	agents := make([]*hubAgent, 0, len(h.nodes))
	for _, record := range h.nodes {
		if record.agent != nil {
			agents = append(agents, record.agent)
		}
	}
	h.mu.Unlock()
	var closing sync.WaitGroup
	for _, agent := range agents {
		closing.Add(1)
		go func() {
			defer closing.Done()
			_ = agent.writeJSON(hubOutbound{Type: "redial"})
			_ = agent.conn.Close(websocket.StatusGoingAway, "hub restarting")
		}()
	}
	done := make(chan struct{})
	go func() { closing.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(hubDrainPeerCloseBudget):
		// A peer that does not answer the close frame is left to the process
		// exit; CloseNow would wait behind the handshake it is meant to cut.
	}
}

// writeHubDraining is the retryable answer to work the hub will not start.
func writeHubDraining(writer http.ResponseWriter) {
	writer.Header().Set("Retry-After", hubDrainRetryAfterSeconds)
	writeJSON(writer, http.StatusServiceUnavailable, map[string]any{
		"error": "hub_draining",
		"retry": true,
		"fix":   "retry in a few seconds; the hub is restarting",
	})
}

// drainGuard counts every request and refuses the new ones once the drain has
// begun. /healthz is pinned byte for byte and stays answerable. The node and
// event sockets are not counted: they outlive any request, and the drain ends
// them rather than waiting for them.
func (h *HubServer) drainGuard(next http.Handler) http.Handler {
	return http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		switch request.URL.Path {
		case "/healthz":
		case "/v1/agent", "/v1/events":
			if !h.drain.admit() {
				writeHubDraining(writer)
				return
			}
		default:
			if !h.drain.enter() {
				writeHubDraining(writer)
				return
			}
			defer h.drain.leave()
		}
		next.ServeHTTP(writer, request)
	})
}
