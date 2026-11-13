package panewire

import (
	"context"
	"io"
	"log/slog"
	"sync"
)

// hubAlertLog observes delivered alerts through the hub log, the only sink an
// alert has. It forwards every record to next, so a test that also reads the
// plain log keeps working, and it rebuilds a HubAlert from each "hub alert"
// record's attributes.
type hubAlertLog struct {
	state *hubAlertLogState
	next  slog.Handler
}

type hubAlertLogState struct {
	mu     sync.Mutex
	alerts []HubAlert
}

// newHubAlertLog returns an observer whose plain log goes to logs; nil discards
// it.
func newHubAlertLog(logs io.Writer) *hubAlertLog {
	if logs == nil {
		logs = io.Discard
	}
	return &hubAlertLog{state: &hubAlertLogState{}, next: slog.NewTextHandler(logs, nil)}
}

// Logger is the logger a hub under test is built with.
func (log *hubAlertLog) Logger() *slog.Logger { return slog.New(log) }

// Accepted counts the alerts the hub has logged so far.
func (log *hubAlertLog) Accepted() int { return len(log.Alerts()) }

// Alerts returns the logged alerts in the order the hub logged them.
func (log *hubAlertLog) Alerts() []HubAlert {
	log.state.mu.Lock()
	defer log.state.mu.Unlock()
	return append([]HubAlert(nil), log.state.alerts...)
}

func (log *hubAlertLog) Enabled(ctx context.Context, level slog.Level) bool {
	return log.next.Enabled(ctx, level)
}

func (log *hubAlertLog) Handle(ctx context.Context, record slog.Record) error {
	if record.Message == "hub alert" {
		var alert HubAlert
		record.Attrs(func(attr slog.Attr) bool {
			switch attr.Key {
			case "machine":
				alert.MachineID = attr.Value.String()
			case "reason":
				alert.Reason = attr.Value.String()
			case "check":
				alert.Check = attr.Value.String()
			case "recovery":
				alert.Recovery = attr.Value.Bool()
			}
			return true
		})
		log.state.mu.Lock()
		log.state.alerts = append(log.state.alerts, alert)
		log.state.mu.Unlock()
	}
	return log.next.Handle(ctx, record)
}

func (log *hubAlertLog) WithAttrs(attrs []slog.Attr) slog.Handler {
	return &hubAlertLog{state: log.state, next: log.next.WithAttrs(attrs)}
}

func (log *hubAlertLog) WithGroup(name string) slog.Handler {
	return &hubAlertLog{state: log.state, next: log.next.WithGroup(name)}
}

// Texts returns each logged alert as the hub renders it for a reader.
func (log *hubAlertLog) Texts() []string {
	alerts := log.Alerts()
	texts := make([]string, 0, len(alerts))
	for _, alert := range alerts {
		texts = append(texts, formatHubAlert(alert))
	}
	return texts
}
