package panewire

import (
	"context"
	"testing"
	"time"
)

// The primary read of a stalled reply is the census's result, and the retry that
// the stall starts never replaces it. Connection 1 pauses 300ms after its first
// 4096 bytes, longer than the 250ms stall bound, then completes. Connection 2 (the
// retry) is a held-open cut that does not complete. The census takes connection
// 1's reply. The 300ms pause is simulated stall; the held-open cut is the
// double's silence. Mutant: cancelling the primary when the retry starts makes
// the held-open cut the only answer, so the census reads the pane set as unknown
// and this fails.
func TestFleetCensusPaneListPrimaryOnlyStallWinsOverCutRetry(t *testing.T) {
	server := newSocketCutHerdrWith(t, socketCutOpts{
		cutAt: 8192, pauseAt: 4096, pause: 300 * time.Millisecond, pauseOnly: 1,
		cutFrom: 2, holdOpen: true,
	})
	client, err := NewHerdrClient(server.path)
	if err != nil {
		t.Fatalf("dial stub herdr: %v", err)
	}
	defer func() { _ = client.Close() }()
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	view, err := readFleetCensusLocal(ctx, client)
	if err != nil {
		t.Fatalf("readFleetCensusLocal: %v", err)
	}
	if got := server.paneListCalls(); got != 2 {
		t.Fatalf("pane.list connections = %d, want the paused primary and the one retry it started", got)
	}
	if !view.paneListOK || len(view.panes) != 1 || view.panes[0].PaneID != "w1:p1" {
		t.Fatalf("paneListOK=%v panes=%+v, want the primary's full inventory, not the held-open retry", view.paneListOK, view.panes)
	}
}

// A census opens at most three pane.list connections. Every reply is cut at 8192
// bytes and stays silent for 400ms before it closes, which is a stalled cut that
// ends. Attempt 1 hedges: the primary stalls, so one retry starts (2 connections).
// Attempt 2 is a plain read (1 connection). Both attempts are cut, so the pane
// set stays unknown. Fails at 4 connections, which is what a second hedge in
// attempt 2 opened (before this change, 240538b7 and earlier).
func TestFleetCensusPaneListSecondAttemptDoesNotHedge(t *testing.T) {
	server := newSocketCutHerdrWith(t, socketCutOpts{cutCount: 4, cutAt: 8192, holdFor: 400 * time.Millisecond})
	client, err := NewHerdrClient(server.path)
	if err != nil {
		t.Fatalf("dial stub herdr: %v", err)
	}
	defer func() { _ = client.Close() }()
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	view, err := readFleetCensusLocal(ctx, client)
	if err != nil {
		t.Fatalf("readFleetCensusLocal: %v", err)
	}
	if got := server.paneListCalls(); got != 3 {
		t.Fatalf("pane.list connections = %d, want 3: a hedged first attempt (primary and one retry) and one plain second attempt", got)
	}
	if view.paneListOK || len(view.panes) != 0 {
		t.Fatalf("paneListOK=%v panes=%+v, want an unknown pane set after both attempts cut", view.paneListOK, view.panes)
	}
}
