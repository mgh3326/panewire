package panewire

import (
	"context"
	"encoding/json"
	"regexp"
	"slices"
	"strings"
	"time"
)

// Node side of the fleet state (docs/fleet-state.md): for the pools it
// measures, the node publishes the shared quota reading pacelever keeps fresh.

const (
	defaultFleetQuotaInterval = time.Minute
	// fleetQuotaRepublish re-sends an unchanged reading so a hub that
	// restarted (or a reader that reset) regains it within this bound.
	fleetQuotaRepublish = 5 * time.Minute
	fleetQuotaTimeout   = 60 * time.Second
)

var fleetQuotaPoolPattern = regexp.MustCompile(`^[a-z][a-z0-9-]{0,31}$`)

// parseFleetQuotaPools reads --state-quota-pools: a comma-separated pool list.
func parseFleetQuotaPools(raw string) ([]string, bool) {
	var pools []string
	for _, item := range strings.Split(raw, ",") {
		pool := strings.TrimSpace(item)
		if pool == "" {
			continue
		}
		if !fleetQuotaPoolPattern.MatchString(pool) {
			return nil, false
		}
		if !slices.Contains(pools, pool) {
			pools = append(pools, pool)
		}
	}
	return pools, true
}

// resyncFleetState makes the next quota round re-announce everything.
// serveConnection calls it for every new hub connection, so a
// restarted hub regains this node's state at once.
func (client *HubClient) resyncFleetState() {
	client.fleetStateMu.Lock()
	client.fleetQuotaResync = true
	client.fleetStateMu.Unlock()
}

// paceleverReading is the part of `pacelever refresh <pool>` output the node
// republishes. Provider error text is read only to refuse the reading.
type paceleverReading struct {
	ID      string `json:"id"`
	Source  string `json:"source"`
	Stale   bool   `json:"stale"`
	Error   string `json:"error"`
	Buckets []struct {
		Window   string   `json:"window"`
		UsedPct  *float64 `json:"used_pct"`
		ResetsAt string   `json:"resets_at"`
		Scope    struct {
			Kind string `json:"kind"`
			Name string `json:"name"`
		} `json:"scope"`
	} `json:"buckets"`
	FetchedAt     float64 `json:"fetched_at"`
	AccountFP     string  `json:"account_fp"`
	AccountFPKind string  `json:"account_fp_kind"`
	PublishFP     string  `json:"publish_fp"`
	PublishFPKind string  `json:"publish_fp_kind"`
}

// fleetQuotaFromPacelever turns one refresh output into a publishable value.
// It refuses a stale, failed or unattributed reading: the node publishes only
// what the shared quota cache would accept as current.
func fleetQuotaFromPacelever(pool string, output []byte) (FleetStatePublish, bool) {
	var reading paceleverReading
	// fetched_at is epoch seconds; the upper bound (year 2286) keeps the
	// millisecond conversion below inside int64.
	if json.Unmarshal(output, &reading) != nil || reading.ID != pool || reading.Stale || reading.Error != "" || !(reading.FetchedAt > 0 && reading.FetchedAt < 1e10) {
		return FleetStatePublish{}, false
	}
	fp := ""
	switch {
	case reading.PublishFPKind == "account" && reading.PublishFP != "":
		fp = reading.PublishFP
	case reading.AccountFPKind == "account" && reading.AccountFP != "":
		fp = reading.AccountFP
	}
	key := pool + ":" + fp
	if !fleetStateAccountKeyPattern.MatchString(key) {
		return FleetStatePublish{}, false
	}
	quota := &FleetStateQuota{AccountKey: key, Pool: pool, Buckets: []FleetStateBucket{}}
	if fleetStateTokenPattern.MatchString(reading.Source) {
		quota.Source = reading.Source
	}
	for _, bucket := range reading.Buckets {
		if bucket.UsedPct == nil {
			// An unmeasured bucket is left out, as the shared cache does.
			continue
		}
		value := *bucket.UsedPct
		out := FleetStateBucket{Window: bucket.Window, UsedPct: &value, Scope: bucket.Scope.Kind, Model: bucket.Scope.Name}
		if bucket.ResetsAt != "" {
			reset, err := time.Parse(time.RFC3339Nano, bucket.ResetsAt)
			if err != nil {
				return FleetStatePublish{}, false
			}
			out.ResetsAt = reset.UTC().Format(time.RFC3339)
		}
		quota.Buckets = append(quota.Buckets, out)
	}
	if len(quota.Buckets) == 0 {
		// Every bucket unmeasured is no reading at all: publishing it would
		// stand in for a measurement and stop consumers measuring directly.
		return FleetStatePublish{}, false
	}
	measured := time.UnixMilli(int64(reading.FetchedAt * 1000)).UTC()
	publish := FleetStatePublish{Kind: fleetStateKindQuota, Key: key, MeasuredAt: measured, Quota: quota}
	encoded, err := json.Marshal(publish)
	if err != nil {
		return FleetStatePublish{}, false
	}
	if _, valid := decodeFleetStatePublish(encoded); !valid {
		return FleetStatePublish{}, false
	}
	return publish, true
}

type fleetQuotaSent struct {
	body    string
	at      time.Time
	failing bool
}

// runFleetQuota refreshes every configured pool on a fixed interval and
// publishes a reading when it changed or the republish bound passed.
// pacelever reads the shared cache first and probes only a stale reading, so
// the interval bounds how long the account's shared reading stays stale.
func (client *HubClient) runFleetQuota(ctx context.Context) {
	if !client.statePublish || len(client.stateQuotaPools) == 0 {
		return
	}
	interval := client.stateQuotaInterval
	if interval <= 0 {
		interval = defaultFleetQuotaInterval
	}
	sent := make(map[string]fleetQuotaSent)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		client.fleetStateMu.Lock()
		if client.fleetQuotaResync {
			client.fleetQuotaResync = false
			clear(sent)
		}
		client.fleetStateMu.Unlock()
		for _, pool := range client.stateQuotaPools {
			if ctx.Err() != nil {
				return
			}
			client.refreshFleetQuota(ctx, pool, sent)
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func (client *HubClient) refreshFleetQuota(ctx context.Context, pool string, sent map[string]fleetQuotaSent) {
	run, cancel := context.WithTimeout(ctx, fleetQuotaTimeout)
	defer cancel()
	output, err := client.nodePaceleverRunner()(run, "refresh", pool)
	publish, ok := FleetStatePublish{}, false
	if err == nil {
		publish, ok = fleetQuotaFromPacelever(pool, output)
	}
	previous := sent[pool]
	if !ok {
		// One warning per failure streak, not one per interval. Consumers see
		// the reading age past its TTL and measure directly.
		if !previous.failing {
			client.warnMessage("fleet state: quota reading for a configured pool is unavailable; not published")
		}
		previous.failing = true
		sent[pool] = previous
		return
	}
	now := client.nowUTC()
	body, _ := json.Marshal(publish)
	if previous.body == string(body) && now.Sub(previous.at) < fleetQuotaRepublish {
		return
	}
	if client.Publish("state", publish) {
		sent[pool] = fleetQuotaSent{body: string(body), at: now}
	}
}
