package panewire

import (
	"context"
	"time"
)

// Stage2Config is deliberately opt-in. A zero Config leaves the established
// stage1 daemon and launchd behavior untouched.
type Stage2Config struct {
	Enabled      bool
	Publisher    interface{ PublishPending(context.Context) error }
	Receiver     interface{ PollOnce(context.Context) error }
	PollInterval time.Duration
	// Close releases the separately-owned stage2 metadata connection after the
	// loop has stopped. It is nil for the historical default-off configuration.
	Close func() error
}

func (d *Daemon) stage2Loop(ctx context.Context) {
	interval := d.cfg.Stage2.PollInterval
	if interval <= 0 {
		interval = 30 * time.Second
	}
	run := func() {
		if d.cfg.Stage2.Publisher != nil {
			if err := d.cfg.Stage2.Publisher.PublishPending(ctx); err != nil && ctx.Err() == nil {
				d.cfg.Logger.Warn("stage2 publisher poll failed", "error", err)
			}
		}
		if d.cfg.Stage2.Receiver != nil {
			if err := d.cfg.Stage2.Receiver.PollOnce(ctx); err != nil && ctx.Err() == nil {
				d.cfg.Logger.Warn("stage2 receiver poll failed", "error", err)
			}
		}
	}
	run()
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			run()
		}
	}
}
