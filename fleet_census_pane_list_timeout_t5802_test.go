package panewire

import (
	"context"
	"errors"
	"testing"
	"time"
)

// A pane.list reply cut at 8192 bytes whose connection stays open is a read
// timeout, not EOF. The first attempt gets half the census budget, so the
// retry still has time and the census reads the whole inventory. Fails on
// base: the first attempt spends the whole budget and the retry starts on a
// spent deadline.
func TestFleetCensusPaneListHeldOpenCutRetriedWithinBudget(t *testing.T) {
	server := newSocketCutHerdrWith(t, socketCutOpts{cutCount: 1, cutAt: 8192, holdOpen: true})
	client, err := NewHerdrClient(server.path)
	if err != nil {
		t.Fatalf("dial stub herdr: %v", err)
	}
	defer func() { _ = client.Close() }()
	// The budget is the wait: the held-open first attempt ends at its own deadline.
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	view, err := readFleetCensusLocal(ctx, client)
	if err != nil {
		t.Fatalf("readFleetCensusLocal: %v", err)
	}
	if got := server.paneListCalls(); got != 2 {
		t.Fatalf("pane.list calls = %d, want the held-open cut reply and one retry", got)
	}
	if !view.paneListOK {
		t.Fatal("held-open pane.list cut read as unreadable; want the retry to read the full inventory")
	}
	if len(view.panes) != 1 || view.panes[0].PaneID != "w1:p1" {
		t.Fatalf("panes %+v, want the one pane from the retried reply", view.panes)
	}
}

// A valid pane.list reply that starts at about 60% of the census budget is read
// in full with no retry. Fails on 087fed8, where the first attempt had half the
// budget and gave up before the reply began. The server's delay is simulated
// latency; the reply is larger than one cut boundary, so it passes through the
// boundary wait too.
func TestFleetCensusPaneListSlowValidReplySucceedsWithoutRetry(t *testing.T) {
	const budget = time.Second
	server := newSocketCutHerdrWith(t, socketCutOpts{cutCount: 0, cutAt: 8192, delay: budget * 6 / 10})
	client, err := NewHerdrClient(server.path)
	if err != nil {
		t.Fatalf("dial stub herdr: %v", err)
	}
	defer func() { _ = client.Close() }()
	ctx, cancel := context.WithTimeout(t.Context(), budget)
	defer cancel()
	view, err := readFleetCensusLocal(ctx, client)
	if err != nil {
		t.Fatalf("readFleetCensusLocal: %v", err)
	}
	if got := server.paneListCalls(); got != 1 {
		t.Fatalf("pane.list calls = %d, want the one valid reply and no retry", got)
	}
	if !view.paneListOK || len(view.panes) != 1 || view.panes[0].PaneID != "w1:p1" {
		t.Fatalf("paneListOK=%v panes=%+v, want the full inventory from a slow valid reply", view.paneListOK, view.panes)
	}
}

// A pane.list reply held open at a byte count that is not a cut boundary is an
// ordinary timeout: it is not retried, and the census leaves the pane set unknown.
func TestFleetCensusPaneListHeldOpenNonBoundaryTimeoutNotRetried(t *testing.T) {
	server := newSocketCutHerdrWith(t, socketCutOpts{cutCount: 1, cutAt: 100, holdOpen: true})
	client, err := NewHerdrClient(server.path)
	if err != nil {
		t.Fatalf("dial stub herdr: %v", err)
	}
	defer func() { _ = client.Close() }()
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	view, err := readFleetCensusLocal(ctx, client)
	if err != nil {
		t.Fatalf("readFleetCensusLocal: %v", err)
	}
	if got := server.paneListCalls(); got != 1 {
		t.Fatalf("pane.list calls = %d, want no retry after a non-boundary timeout", got)
	}
	if view.paneListOK || len(view.panes) != 0 {
		t.Fatalf("paneListOK=%v panes=%+v, want an unknown pane set", view.paneListOK, view.panes)
	}
}

// A valid pane.list reply that pauses 300ms after its first 4096 bytes, longer
// than the stall bound, completes within the census budget. The retry pauses the
// same way, so only the primary read, kept open to the deadline, can finish it.
// Fails on 4263a1f: the bound abandons the reply, and both attempts stall.
func TestFleetCensusPaneListPauseAfterBoundaryCompletes(t *testing.T) {
	server := newSocketCutHerdrWith(t, socketCutOpts{cutCount: 0, cutAt: 8192, pauseAt: 4096, pause: 300 * time.Millisecond})
	client, err := NewHerdrClient(server.path)
	if err != nil {
		t.Fatalf("dial stub herdr: %v", err)
	}
	defer func() { _ = client.Close() }()
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	view, err := readFleetCensusLocal(ctx, client)
	if err != nil {
		t.Fatalf("readFleetCensusLocal: %v", err)
	}
	if !view.paneListOK || len(view.panes) != 1 || view.panes[0].PaneID != "w1:p1" {
		t.Fatalf("paneListOK=%v panes=%+v, want the full inventory from the paused reply", view.paneListOK, view.panes)
	}
}

// A call whose deadline has already passed installs no deadline on the socket
// and reports the context's error. Fails on base: the expired deadline is set on
// the connection and the write fails as an I/O timeout instead, so a retry on a
// spent context reaches the socket.
func TestHerdrCallSpentDeadlineInstallsNoDeadline(t *testing.T) {
	server := newSocketCutHerdr(t, 0)
	client, err := NewHerdrClient(server.path)
	if err != nil {
		t.Fatalf("dial stub herdr: %v", err)
	}
	defer func() { _ = client.Close() }()
	ctx, cancel := context.WithDeadline(t.Context(), time.Now().Add(-time.Second))
	defer cancel()
	_, err = client.Call(ctx, "pane.list", map[string]any{})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Call on a spent context = %v, want context.DeadlineExceeded before any I/O", err)
	}
}
