package panewire

import (
	"context"
	"time"
)

// stallSafetyProbe captures the two observations once per inactive episode.
// The caller publishes the persisted evidence separately.
type stallSafetyProbe struct {
	state   *stallSafetyState
	read    func(context.Context, string) (string, error)
	explain func(context.Context, string) (string, error)
}

func (p *stallSafetyProbe) observe(ctx context.Context, job, pane string, working, busy bool, now time.Time, limit time.Duration) error {
	due, err := p.state.observe(ctx, job, pane, working, busy, now, limit)
	if err != nil || !due {
		return err
	}
	screen, err := p.read(ctx, pane)
	if err != nil {
		screen = "screen unavailable"
	}
	explanation, err := p.explain(ctx, pane)
	if err != nil {
		explanation = "herdr explanation unavailable"
	}
	_, err = p.state.recordExamination(ctx, job, pane, screen, explanation)
	return err
}
