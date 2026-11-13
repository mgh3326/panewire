package panewire

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"strings"
	"sync"
	"testing"
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

// toggleWriter fails every write while down is set, as a full disk would.
type toggleWriter struct {
	mu   sync.Mutex
	down bool
	out  []byte
}

func (writer *toggleWriter) Write(p []byte) (int, error) {
	writer.mu.Lock()
	defer writer.mu.Unlock()
	if writer.down {
		return 0, errors.New("toggle writer is down")
	}
	writer.out = append(writer.out, p...)
	return len(p), nil
}

func (writer *toggleWriter) setDown(down bool) {
	writer.mu.Lock()
	defer writer.mu.Unlock()
	writer.down = down
}

// TestHubAlertStaysPendingUntilTheLogAcceptsIt pins the delivery rule: an alert
// counts as delivered only when the log handler accepted the record. A failed
// write leaves the incident pending, and the next pass delivers it once.
func TestHubAlertStaysPendingUntilTheLogAcceptsIt(t *testing.T) {
	writer := &toggleWriter{down: true}
	hub, err := NewHubServer(HubServerConfig{
		Tokens: map[string]string{"operator": r6OperatorToken},
		Logger: slog.New(slog.NewTextHandler(writer, nil)),
	})
	if err != nil {
		t.Fatal(err)
	}
	hub.mu.Lock()
	hub.alerts["node:node-a"] = &hubAlertState{active: true, incidentPending: true, incidentGeneration: 1, reason: hubAlertReasonDisconnected}
	hub.mu.Unlock()
	incident := hubNotification{key: "node:node-a", incident: true, generation: 1, alert: HubAlert{MachineID: "node-a", Reason: hubAlertReasonDisconnected, Check: hubAlertNoCheck}}

	hub.dispatchHubNotifications([]hubNotification{incident})
	hub.mu.Lock()
	state := *hub.alerts["node:node-a"]
	hub.mu.Unlock()
	if state.incidentDelivered || !state.incidentPending {
		t.Fatalf("failed write was recorded as delivered: %+v", state)
	}

	writer.setDown(false)
	hub.dispatchHubNotifications([]hubNotification{incident})
	hub.mu.Lock()
	state = *hub.alerts["node:node-a"]
	hub.mu.Unlock()
	if !state.incidentDelivered || state.incidentPending {
		t.Fatalf("accepted write was not recorded as delivered: %+v", state)
	}
	if got := strings.Count(string(writer.out), "hub alert"); got != 1 {
		t.Fatalf("incident logged %d times, want 1: %s", got, writer.out)
	}
}
