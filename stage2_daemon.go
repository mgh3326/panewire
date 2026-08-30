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

// SentinelConfig is separate from Stage2Config on purpose.  A node may publish
// its L2 heartbeat without enabling stage-2 message publishing/receiving, and
// it may enable heartbeat publication without peer evaluation.
type SentinelConfig struct {
	Enabled bool
	Watch   bool
	Runner  interface {
		EmitHeartbeat(context.Context) error
		Evaluate(context.Context) error
	}
	HeartbeatInterval time.Duration
	WatchInterval     time.Duration
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

func (d *Daemon) sentinelLoop(ctx context.Context) {
	if d.cfg.Sentinel.Runner == nil {
		return
	}
	heartbeatInterval := d.cfg.Sentinel.HeartbeatInterval
	if heartbeatInterval <= 0 {
		heartbeatInterval = time.Minute
	}
	watchInterval := d.cfg.Sentinel.WatchInterval
	if watchInterval <= 0 {
		watchInterval = 2 * time.Minute
	}
	runHeartbeat := func() {
		if err := d.cfg.Sentinel.Runner.EmitHeartbeat(ctx); err != nil && ctx.Err() == nil {
			// The runner deliberately returns a stable error only; do not log an
			// arbitrary transport response that could reflect a credential.
			d.cfg.Logger.Warn("sentinel heartbeat poll failed")
		}
	}
	runWatch := func() {
		if err := d.cfg.Sentinel.Runner.Evaluate(ctx); err != nil && ctx.Err() == nil {
			d.cfg.Logger.Warn("sentinel peer evaluation failed")
		}
	}
	runHeartbeat()
	if d.cfg.Sentinel.Watch {
		runWatch()
	}
	heartbeats := time.NewTicker(heartbeatInterval)
	defer heartbeats.Stop()
	var watches *time.Ticker
	var watchTicks <-chan time.Time
	if d.cfg.Sentinel.Watch {
		watches = time.NewTicker(watchInterval)
		defer watches.Stop()
		watchTicks = watches.C
	}
	for {
		select {
		case <-ctx.Done():
			return
		case <-heartbeats.C:
			runHeartbeat()
		case <-watchTicks:
			runWatch()
		}
	}
}
