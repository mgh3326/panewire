package fleet

import (
	"context"
	"time"
)

// SweptPane describes a pane an ended-job sweep closed.
type SweptPane struct {
	Job    string `json:"job"`
	Pane   string `json:"pane"`
	Role   string `json:"role"`
	Reason string `json:"reason"`
}

// SweepEnded runs the hub-ordered ended-job pass (hk 4693): the panes of jobs
// whose end lies at least age ago are judged — and, proven the job's own by
// pane id and harness session (decision 265), closed — by the one unified
// pass the local sweep runs (docs/pane-sweep.md).
func (s *TaskPaneSweeper) SweepEnded(ctx context.Context, a *App, age time.Duration) ([]SweptPane, error) {
	return s.pass(ctx, a, age)
}
