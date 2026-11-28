package panewire

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"github.com/getsentry/sentry-go"
)

// spawnTraceChain are the steps a node reports that run one after another, in
// the order they run: the hub's placement, then the node's checkout, the spawn
// engine's claim, then launch, submit and landing. The window's queue is what is
// left of the window once their sum is taken away (see layoutSpawnWindow). The
// placement key also carries the hub's placement time, so it cannot be split.
// The checkout sub-stages (checkoutStageKeys) are parts of the checkout step,
// not steps of their own, so they stay out of the chain and out of that sum.
var spawnTraceChain = []string{"placement", "checkout", "claim", "launch", "submit", "landing"}

// spawnTraceAttempt is one attempt that ended unstarted and was renewed: the
// hub retried the request, so the next attempt's window opens at EndMS. It is
// kept on the operation (spawn_operations.trace_history) so the landed trace can
// show the time the earlier attempts and their queue waits took, from
// acceptance. Reason is the bounded trace reason, never refusal text.
type spawnTraceAttempt struct {
	Attempt int              `json:"attempt"`
	EndMS   int64            `json:"end_ms"`
	Reason  string           `json:"reason,omitempty"`
	StageMS map[string]int64 `json:"stage_ms,omitempty"`
}

// appendSpawnTraceAttempt records the attempt that a renewal just ended. It
// runs in the renewal's own transaction, so a renewal that does not commit
// records nothing. The window of the next attempt opens at end.
func (s *laneInboxStore) appendSpawnTraceAttempt(ctx context.Context, tx *sql.Tx, requestID string, attempt int, outcome hubSpawnOutcome, end time.Time) error {
	history, err := s.spawnTraceHistoryIn(ctx, tx, requestID)
	if err != nil {
		return err
	}
	history = append(history, spawnTraceAttempt{Attempt: attempt, EndMS: end.UnixMilli(), Reason: spawnTraceReason(outcome.Reason), StageMS: outcome.StageMS})
	raw, err := json.Marshal(history)
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, s.q(`UPDATE spawn_operations SET trace_history=? WHERE request_id=?`), string(raw), requestID)
	return err
}

// spawnTraceHistory reads the renewed attempts of an operation, oldest first.
func (s *laneInboxStore) spawnTraceHistory(ctx context.Context, requestID string) ([]spawnTraceAttempt, error) {
	return s.spawnTraceHistoryIn(ctx, s.db, requestID)
}

type spawnTraceQuerier interface {
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

func (s *laneInboxStore) spawnTraceHistoryIn(ctx context.Context, q spawnTraceQuerier, requestID string) ([]spawnTraceAttempt, error) {
	var raw string
	if err := q.QueryRowContext(ctx, s.q(`SELECT trace_history FROM spawn_operations WHERE request_id=?`), requestID).Scan(&raw); err != nil {
		return nil, err
	}
	var history []spawnTraceAttempt
	if raw == "" {
		return nil, nil
	}
	if err := json.Unmarshal([]byte(raw), &history); err != nil {
		return nil, fmt.Errorf("spawn trace history: %w", err)
	}
	return history, nil
}

// layoutSpawnWindow lays one attempt's window [open, end] under parent (hk
// 5585, hk 7112). The chain steps run one after another and are laid back to
// back so they end at the window's end; the time before the first of them is
// a "queue" span: the wait before any step ran, for example parked for a node
// slot. The queue is window minus the sum of the chain, so the queue and the
// steps together cover the window and consecutive windows cover acceptance to
// landed. The checkout sub-stages are drawn from the start of the checkout
// step, back to back, and are not counted in the sum, since checkout already
// contains them. A node sends them only with the checkout total (hk 6075), so
// without a checkout step they are not drawn. It returns the earliest start
// among the steps and the queue, so the caller can widen the transaction if a
// step reaches before the window.
func layoutSpawnWindow(parent *sentry.Span, open, end time.Time, stages map[string]int64) time.Time {
	var sum int64
	for _, stage := range spawnTraceChain {
		if ms, ok := spawnTraceStageMS(stages, stage); ok {
			sum += ms
		}
	}
	cursor := end.Add(-time.Duration(sum) * time.Millisecond)
	earliest := open
	if cursor.Before(earliest) {
		earliest = cursor
	}
	if cursor.After(open) {
		queue := parent.StartChild("queue")
		queue.StartTime = open
		queue.EndTime = cursor
		queue.Finish()
	}
	for _, stage := range spawnTraceChain {
		ms, ok := spawnTraceStageMS(stages, stage)
		if !ok {
			continue
		}
		child := parent.StartChild(stage)
		child.StartTime = cursor
		cursor = cursor.Add(time.Duration(ms) * time.Millisecond)
		child.EndTime = cursor
		if stage == "checkout" {
			layoutCheckoutSubStages(parent, child.StartTime, stages)
		}
		child.Finish()
	}
	return earliest
}

// layoutCheckoutSubStages draws the checkout sub-stages back to back from the
// start of the checkout step. They are siblings of the steps under parent, so
// each keeps its own duration.
func layoutCheckoutSubStages(parent *sentry.Span, checkoutStart time.Time, stages map[string]int64) {
	cursor := checkoutStart
	for _, key := range checkoutStageKeys {
		ms, ok := spawnTraceStageMS(stages, key)
		if !ok {
			continue
		}
		child := parent.StartChild(key)
		child.StartTime = cursor
		cursor = cursor.Add(time.Duration(ms) * time.Millisecond)
		child.EndTime = cursor
		child.Finish()
	}
}

// spawnTraceStageMS returns a reported step's duration when it is a value the
// trace may draw; an absent or out-of-range step is not drawn.
func spawnTraceStageMS(stages map[string]int64, key string) (int64, bool) {
	ms, ok := stages[key]
	if !ok || ms < 0 || ms > 3600000 {
		return 0, false
	}
	return ms, true
}

// layoutRenewedAttempts adds each renewed attempt as an "attempt" span whose
// window runs from the previous window's end (the first from acceptance) to its
// own end. It returns the window the landed attempt opens at, and the earliest
// time any span reaches.
func layoutRenewedAttempts(trace *sentry.Span, created time.Time, history []spawnTraceAttempt) (time.Time, time.Time) {
	open, earliest := created, created
	for _, renewed := range history {
		end := time.UnixMilli(renewed.EndMS)
		wrap := trace.StartChild("attempt")
		wrap.StartTime = open
		wrap.EndTime = end
		wrap.SetTag("attempt", strconv.Itoa(renewed.Attempt))
		if renewed.Reason != "" {
			wrap.SetTag("reason", renewed.Reason)
		}
		if reach := layoutSpawnWindow(wrap, open, end, renewed.StageMS); reach.Before(earliest) {
			earliest = reach
		}
		wrap.Finish()
		open = end
	}
	return open, earliest
}
