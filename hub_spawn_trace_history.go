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

// spawnTraceStages are the steps a node reports, in the order the trace names
// them. Each is end-aligned to its window's end (see layoutSpawnWindow).
var spawnTraceStages = []string{"claim", "placement", "checkout", "launch", "submit", "landing", checkoutStageLockWait, checkoutStageFetch, checkoutStageWorktreeAdd}

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

// layoutSpawnWindow lays one attempt's window [open, end] under parent. Each
// reported step ends at the window's end, as the trace has always placed them
// (hk 5585). The part of the window before the longest step starts is a
// "queue" span: time the attempt waited before any step of it ran, for example
// parked for a node slot. The queue and the steps together cover the window,
// so consecutive windows cover acceptance to landed. It returns the earliest
// start among the steps and the queue, so the caller can widen the transaction
// if a step reaches before the window.
func layoutSpawnWindow(parent *sentry.Span, open, end time.Time, stages map[string]int64) time.Time {
	earliest := open
	var longest int64
	for _, stage := range spawnTraceStages {
		ms, ok := stages[stage]
		if !ok || ms < 0 || ms > 3600000 {
			continue
		}
		longest = max(longest, ms)
		child := parent.StartChild(stage)
		child.EndTime = end
		child.StartTime = end.Add(-time.Duration(ms) * time.Millisecond)
		if child.StartTime.Before(earliest) {
			earliest = child.StartTime
		}
		child.Finish()
	}
	if ready := end.Add(-time.Duration(longest) * time.Millisecond); ready.After(open) {
		queue := parent.StartChild("queue")
		queue.StartTime = open
		queue.EndTime = ready
		queue.Finish()
	}
	return earliest
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
