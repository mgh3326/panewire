package panewire

import (
	"context"
	"encoding/json"
	"errors"
	"regexp"
	"runtime"
	"strconv"
	"time"

	"github.com/mgh3326/panewire/internal/fleet"
)

// Host activity is the opt-in user-input report on the heartbeat, plus the
// node's swap-out rate (docs/placement.md, Telemetry). It carries one number: whole
// seconds since the last keyboard or pointer input. It reads no app names, window
// titles, input content or process lists. It is off unless the node's [local]
// report_user_idle is true. The node sends it, and host_memory.swap_out_per_s,
// only to a hub that advertised hubFeatureHostActivity: an older hub rejects an
// unknown heartbeat field, and with it the node's liveness and job state.

const (
	hubFeatureHostActivity = "host_activity"

	hostActivityTimeout = 5 * time.Second
	// userIdleWarnInterval bounds the probe-failure log to one line an hour.
	userIdleWarnInterval = time.Hour
)

var errUserIdleUnsupported = errors.New("user idle is reported on macOS only")

// HubHostActivity is the heartbeat's host_activity object.
type HubHostActivity struct {
	UserIdleS *int64 `json:"user_idle_s,omitempty"`
}

// decodeHubHostActivity reads the object leniently, as decodeHubHostSignals does:
// an unknown name is a newer node's signal, and an invalid value loses only
// itself, never the heartbeat.
func decodeHubHostActivity(raw json.RawMessage) *HubHostActivity {
	var fields map[string]json.RawMessage
	if json.Unmarshal(raw, &fields) != nil {
		return nil
	}
	var idle *int64
	if json.Unmarshal(fields["user_idle_s"], &idle) != nil || idle == nil || *idle < 0 {
		return nil
	}
	return &HubHostActivity{UserIdleS: idle}
}

// hidIdleTimeRE matches the HIDIdleTime property in `ioreg -c IOHIDSystem`
// output; the value is in nanoseconds.
var hidIdleTimeRE = regexp.MustCompile(`"HIDIdleTime" = ([0-9]+)`)

// parseHIDIdleSeconds converts the HIDIdleTime nanoseconds to whole seconds.
func parseHIDIdleSeconds(output string) (int64, bool) {
	match := hidIdleTimeRE.FindStringSubmatch(output)
	if len(match) != 2 {
		return 0, false
	}
	nanoseconds, err := strconv.ParseInt(match[1], 10, 64)
	return nanoseconds / int64(time.Second), err == nil && nanoseconds >= 0
}

// collectUserIdle reads whole seconds since the last user input. Only macOS has
// the probe; any other platform answers errUserIdleUnsupported, which is not a
// failure. Errors name the condition, never the command output.
func collectUserIdle(ctx context.Context, goos string, run func(context.Context, ...string) ([]byte, error)) (int64, error) {
	if goos != "darwin" {
		return 0, errUserIdleUnsupported
	}
	ctx, cancel := context.WithTimeout(ctx, hostActivityTimeout)
	defer cancel()
	// launchd's PATH may not hold ioreg, so the absolute path goes first.
	output, err := run(ctx, "/usr/sbin/ioreg", "-c", "IOHIDSystem")
	if err != nil {
		if output, err = run(ctx, "ioreg", "-c", "IOHIDSystem"); err != nil {
			return 0, errors.New("ioreg unavailable")
		}
	}
	seconds, ok := parseHIDIdleSeconds(string(output))
	if !ok {
		return 0, errors.New("HIDIdleTime not found")
	}
	return seconds, nil
}

// reportUserIdle reads the node's [local] report_user_idle switch. Tests replace
// it through userIdleSwitch.
func (client *HubClient) reportUserIdle() bool {
	if client.userIdleSwitch != nil {
		return client.userIdleSwitch()
	}
	return fleet.ReportUserIdle()
}

// activityPeerAccepted is whether this connection's hub advertised
// hubFeatureHostActivity.
func (client *HubClient) activityPeerAccepted() bool {
	peer := client.signalsPeer.Load()
	return peer != nil && peer.acceptsHostActivity.Load()
}

// hostActivityForHeartbeat collects the user-input report when the hub accepts it
// and the switch is on. A failed probe omits the object and logs at most once an
// hour, with no command output in the line.
func (client *HubClient) hostActivityForHeartbeat(ctx context.Context) *HubHostActivity {
	if !client.activityPeerAccepted() || !client.reportUserIdle() {
		return nil
	}
	probe := client.userIdleProbe
	if probe == nil {
		probe = func(ctx context.Context) (int64, error) {
			return collectUserIdle(ctx, runtime.GOOS, runHubMeasurement)
		}
	}
	idle, err := probe(ctx)
	if err != nil {
		if !errors.Is(err, errUserIdleUnsupported) {
			client.warnUserIdleFailure(err)
		}
		return nil
	}
	return &HubHostActivity{UserIdleS: &idle}
}

func (client *HubClient) warnUserIdleFailure(err error) {
	client.userIdleWarnMu.Lock()
	defer client.userIdleWarnMu.Unlock()
	now := client.nowUTC()
	if !client.userIdleWarnedAt.IsZero() && now.Sub(client.userIdleWarnedAt) < userIdleWarnInterval {
		return
	}
	client.userIdleWarnedAt = now
	client.warnMessage("user idle unavailable: " + err.Error())
}

// recordNodeLiveSignals copies the node's live signals onto a candidate's
// signals: the swap and pressure readings, user idle when reported, the 1-minute
// CPU busy, the power source, and the age of the heartbeat they came from. They
// are recorded for the hk placement record and for `place --explain` only.
// Admission, ranking and the score read other fields, and
// placement_signals_t3649_test.go pins that.
func recordNodeLiveSignals(signals *PlacementSignals, record *hubNodeRecord, memory *HubHostMemory, now time.Time) {
	signals.CPUBusyShort = cloneMemoryFloat(signals.CPUBusy1m)
	if record == nil {
		return
	}
	if !record.lastHeartbeat.IsZero() {
		signals.SignalAgeS = memoryFloat(max(now.Sub(record.lastHeartbeat).Seconds(), 0))
	}
	signals.PowerSource = record.powerSource
	if memory != nil {
		signals.SwapInPerS = cloneMemoryFloat(memory.SwapInPerS)
		signals.SwapOutPerS = cloneMemoryFloat(memory.SwapOutPerS)
		signals.Pressure = cloneMemoryFloat(memory.Pressure)
	}
	if record.hostActivity != nil && record.hostActivity.UserIdleS != nil {
		idle := *record.hostActivity.UserIdleS
		signals.UserIdleS = &idle
	}
}
