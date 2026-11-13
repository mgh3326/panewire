package panewire

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/coder/websocket"
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

func (writer *toggleWriter) text() string {
	writer.mu.Lock()
	defer writer.mu.Unlock()
	return string(writer.out)
}

// TestHubAlertRetriedBySweepAfterTheLogFails pins the delivery rule through the
// real path: an alert counts as delivered only when the log handler accepted
// the record, and a failed write is retried by the next Sweep, not dropped.
func TestHubAlertRetriedBySweepAfterTheLogFails(t *testing.T) {
	clock := time.Date(2026, 8, 31, 4, 0, 0, 0, time.UTC)
	writer := &toggleWriter{down: true}
	hub, err := NewHubServer(HubServerConfig{
		Tokens: map[string]string{"operator": r6OperatorToken, "node-a": r6NodeAToken},
		Now:    func() time.Time { return clock }, StaleAfter: 30 * time.Second, KeepaliveInterval: 10 * time.Second,
		GracePeriod: defaultHubGracePeriod, Logger: slog.New(slog.NewTextHandler(writer, nil)),
	})
	if err != nil {
		t.Fatal(err)
	}
	hubHTTP := httptest.NewServer(hub.Handler())
	defer hubHTTP.Close()

	node := r6DialAgent(t, hubHTTP.URL, "node-a", r6NodeAToken)
	r6Write(t, node, map[string]any{"type": "hello", "machine_id": "node-a", "version": "alert-retry"})
	eventually(t, time.Second, "node connected", func() bool { return r6NodeState(r6Nodes(t, hubHTTP), "node-a") == "connected" })
	if err := node.Close(websocket.StatusNormalClosure, "fixture disconnect"); err != nil {
		t.Fatal(err)
	}
	eventually(t, time.Second, "node disconnected", func() bool { return r6NodeState(r6Nodes(t, hubHTTP), "node-a") == "disconnected" })
	delivered := func() bool {
		hub.mu.Lock()
		defer hub.mu.Unlock()
		state := hub.alerts["node:node-a"]
		return state != nil && state.incidentDelivered
	}

	// (a) The incident fires while the writer is down: nothing is logged and
	// nothing is recorded as delivered.
	clock = clock.Add(defaultHubGracePeriod)
	hub.Sweep()
	clock = clock.Add(time.Second)
	hub.Sweep()
	clock = clock.Add(time.Second)
	hub.Sweep()
	if got := strings.Count(writer.text(), "hub alert"); got != 0 {
		t.Fatalf("alert logged while the writer was down: %s", writer.text())
	}
	if delivered() {
		t.Fatal("failed write was recorded as delivered")
	}

	// (b) The writer comes back: the next Sweep writes the incident once.
	writer.setDown(false)
	clock = clock.Add(time.Second)
	hub.Sweep()
	eventually(t, time.Second, "incident logged and recorded", func() bool {
		return strings.Count(writer.text(), "hub alert") == 1 && delivered()
	})
	if !strings.Contains(writer.text(), "reason=disconnected") {
		t.Fatalf("logged alert is not the incident: %s", writer.text())
	}
	clock = clock.Add(time.Second)
	hub.Sweep()
	if got := strings.Count(writer.text(), "hub alert"); got != 1 {
		t.Fatalf("incident logged %d times, want 1: %s", got, writer.text())
	}

	// (c) The node reconnects: the recovery is logged.
	node = r6DialAgent(t, hubHTTP.URL, "node-a", r6NodeAToken)
	defer node.Close(websocket.StatusNormalClosure, "")
	r6Write(t, node, map[string]any{"type": "hello", "machine_id": "node-a", "version": "alert-retry"})
	eventually(t, time.Second, "node reconnected", func() bool { return r6NodeState(r6Nodes(t, hubHTTP), "node-a") == "connected" })
	clock = clock.Add(time.Second)
	hub.Sweep()
	clock = clock.Add(time.Second)
	hub.Sweep()
	eventually(t, time.Second, "recovery logged", func() bool {
		return strings.Count(writer.text(), "hub alert") == 2 && strings.Contains(writer.text(), "recovery=true")
	})
}

// TestHubAlertDeliveredWhenOnlyTheFileSinkFails pins the --log-file tee rule: a
// failing file sink does not hold back an alert the primary sink wrote, and the
// alert is written once, not again on every later pass.
func TestHubAlertDeliveredWhenOnlyTheFileSinkFails(t *testing.T) {
	var primary bytes.Buffer
	file := &toggleWriter{down: true}
	hub, err := NewHubServer(HubServerConfig{
		Tokens: map[string]string{"operator": r6OperatorToken},
		Logger: slog.New(hubLogTeeHandler{primary: slog.NewTextHandler(&primary, nil), file: slog.NewTextHandler(file, nil)}),
	})
	if err != nil {
		t.Fatal(err)
	}
	alert := HubAlert{MachineID: "node-a", Reason: hubAlertReasonDisconnected, Check: hubAlertNoCheck}
	if err := hub.logHubAlert(alert); err != nil {
		t.Fatalf("primary accepted the alert but the handler reported %v", err)
	}
	if got := strings.Count(primary.String(), "hub alert"); got != 1 {
		t.Fatalf("primary sink logged %d copies, want 1: %s", got, primary.String())
	}
}
