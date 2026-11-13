package panewire

import (
	"context"
	"encoding/json"
	"runtime"
	"slices"
	"strings"
	"time"
)

// Host signals are the placement inputs a node reports on its heartbeat
// (docs/placement.md). A node sends them only to a hub that advertised
// hubFeatureHostSignals: an older hub rejects a heartbeat with an unknown field,
// and with it the node's liveness and job state.

const (
	hubFeatureHostSignals = "host_signals"
	hubFeatureLaunchProof = "launch_proof_reason"

	powerSourceAC      = "ac"
	powerSourceBattery = "battery"

	// placementReasonOnBattery and placementReasonPowerUnknown are wrapped in
	// not_accepting(...), the text placement consumers refuse a host on.
	placementReasonOnBattery    = "on_battery"
	placementReasonPowerUnknown = "power_unknown"

	// placementPowerReportMaxAge is how long a listed laptop may go without a
	// power report before it is refused.
	placementPowerReportMaxAge = 3 * time.Minute

	hostSignalsTimeout = 5 * time.Second
)

// HubHostSignals is the heartbeat's host_signals object.
type HubHostSignals struct {
	PowerSource string `json:"power_source,omitempty"`
}

// hubFeaturesEvent tells a node what its hub understands. An older node drops
// the unknown type.
type hubFeaturesEvent struct {
	Type     string   `json:"type"`
	Features []string `json:"features"`
}

// decodeHubHostSignals reads the object leniently, unlike the rest of the
// heartbeat: an unknown name is a newer node's signal and an invalid value is one
// lost signal, and neither may cost the hub the node's liveness and job state.
func decodeHubHostSignals(raw json.RawMessage) *HubHostSignals {
	var fields map[string]json.RawMessage
	if json.Unmarshal(raw, &fields) != nil {
		return nil
	}
	var power string
	if json.Unmarshal(fields["power_source"], &power) != nil || (power != powerSourceAC && power != powerSourceBattery) {
		return nil
	}
	return &HubHostSignals{PowerSource: power}
}

// collectHubHostSignals answers nil when there is nothing to report. Only macOS
// reports power: the laptops the policy can list are Macs.
func collectHubHostSignals(ctx context.Context) *HubHostSignals {
	if runtime.GOOS != "darwin" {
		return nil
	}
	return collectDarwinHostSignals(ctx, runHubMeasurement)
}

func collectDarwinHostSignals(ctx context.Context, run func(context.Context, ...string) ([]byte, error)) *HubHostSignals {
	ctx, cancel := context.WithTimeout(ctx, hostSignalsTimeout)
	defer cancel()
	// launchd's PATH may not hold pmset, so the absolute path goes first.
	output, err := run(ctx, "/usr/bin/pmset", "-g", "batt")
	if err != nil {
		if output, err = run(ctx, "pmset", "-g", "batt"); err != nil {
			return nil
		}
	}
	power := parsePmsetPowerSource(string(output))
	if power == "" {
		return nil
	}
	return &HubHostSignals{PowerSource: power}
}

// parsePmsetPowerSource reads the first line of `pmset -g batt`. Only mains
// counts as ac: a UPS is a battery. Anything unrecognized reports nothing, which a
// listed laptop turns into power_unknown rather than a guess.
func parsePmsetPowerSource(output string) string {
	first, _, _ := strings.Cut(output, "\n")
	switch {
	case strings.Contains(first, "'AC Power'"):
		return powerSourceAC
	case strings.Contains(first, "'Battery Power'"), strings.Contains(first, "'UPS Power'"):
		return powerSourceBattery
	}
	return ""
}

// placementPowerRefusal names why a connected listed laptop may not take new
// work, or answers "". A host not in policy.Laptops is never refused for power,
// whatever it reports. The grace from connect covers the first report, which
// takes a heartbeat to ask for and another to carry.
func placementPowerRefusal(policy PlacementPolicy, machine string, record *hubNodeRecord, now time.Time) string {
	if record == nil || !slices.Contains(policy.Laptops, machine) {
		return ""
	}
	last := record.powerAt
	if record.connectedSince.After(last) {
		last = record.connectedSince
	}
	switch {
	case now.Sub(last) > placementPowerReportMaxAge:
		return placementReasonPowerUnknown
	case record.powerSource == powerSourceBattery:
		return placementReasonOnBattery
	}
	return ""
}

// observeHostSignalsLocked records a heartbeat's signals. A changed power source,
// or a report that ends a stale period, can change the placement answer, so it
// drops the cached one. A change is logged: that line is how an operator confirms
// a laptop reports before listing it.
func (h *HubServer) observeHostSignalsLocked(record *hubNodeRecord, signals *HubHostSignals, received time.Time) {
	if signals == nil || signals.PowerSource == "" {
		return
	}
	if received.Sub(record.powerAt) > placementPowerReportMaxAge {
		h.placementCache = placementCache{}
	}
	if record.powerSource != signals.PowerSource {
		h.placementCache = placementCache{}
		h.logger.Info("host power source", "machine", record.machineID, "power_source", signals.PowerSource)
	}
	record.powerSource, record.powerAt = signals.PowerSource, received
}

// queueFeatures tells a node once per connection what this hub understands. A
// dropped event stays unsent, so the next heartbeat retries it.
func (agent *hubAgent) queueFeatures() {
	if agent == nil || agent.features == nil || agent.featuresSent.Load() {
		return
	}
	select {
	case agent.features <- hubFeaturesEvent{Type: "hub.features", Features: []string{hubFeatureHostSignals, hubFeatureReadiness, hubFeatureLaneEventPane, hubFeaturePaneCaptureDoc, hubFeatureHostActivity, hubFeatureLaunchProof}}:
		agent.featuresSent.Store(true)
	default:
	}
}

// hostSignalsForHeartbeat collects the signals when this connection's hub
// accepts them.
func (client *HubClient) hostSignalsForHeartbeat(ctx context.Context) *HubHostSignals {
	if peer := client.signalsPeer.Load(); peer == nil || !peer.acceptsSignals.Load() {
		return nil
	}
	collect := client.hostSignalsCollector
	if collect == nil {
		collect = collectHubHostSignals
	}
	return collect(ctx)
}
