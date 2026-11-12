package panewire

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"slices"
	"strings"
	"time"

	"github.com/mgh3326/panewire/internal/quota"
	"github.com/mgh3326/panewire/internal/quota/shared"
)

// Node side of the fleet state (docs/fleet-state.md): for the pools it
// measures, the node publishes the shared quota reading it keeps fresh. The
// claude and codex pools are measured in this process (quota_native.go); other
// pools run pacelever.

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

type fleetQuotaSent struct {
	body    string
	at      time.Time
	failing bool
}

// runFleetQuota refreshes every configured pool on a fixed interval and
// publishes a reading when it changed or the republish bound passed.
// pacelever reads the shared cache first and probes only a stale reading, so
// the interval bounds how long the account's shared reading stays stale. Each
// round also announces presence for the credit pools this host holds
// (quota_credit_presence.go); that is the only publisher they have.
func (client *HubClient) runFleetQuota(ctx context.Context) {
	if !client.statePublish {
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
		client.creditPresencePublisher().Publish(ctx)
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

// fleetQuotaLoginMissingKinds are pacelever's error kinds (kinds.go) that mean
// the pool has no usable login. Every other failure leaves the login unknown.
var fleetQuotaLoginMissingKinds = map[string]bool{"auth": true, "auth_failed": true, "credentials": true, "token_expired": true}

var fleetQuotaErrorKindPattern = regexp.MustCompile(`^[a-z][a-z0-9_]{0,31}$`)

// fleetQuotaErrorKindUnreadable names a failure whose kind pacelever's status
// did not give: a timeout, an unreadable status, or a pool it does not list.
const fleetQuotaErrorKindUnreadable = "unreadable"

func (client *HubClient) refreshFleetQuota(ctx context.Context, pool string, sent map[string]fleetQuotaSent) {
	// The login probe follows this pool's own measurement in the same
	// goroutine, so it never runs beside it (quota_login_probe.go).
	defer client.probeFleetLogin(ctx, pool)
	run, cancel := context.WithTimeout(ctx, fleetQuotaTimeout)
	defer cancel()
	publish, ok, measuredHere, kind := client.measureFleetQuotaObserved(run, ctx, pool)
	previous := sent[pool]
	if !ok {
		if measuredHere {
			client.noteOwnQuotaMeasurement(pool)
		}
		// One warning per failure streak, not one per interval. Consumers see
		// the reading age past its TTL and measure directly.
		if !previous.failing {
			client.warnMessage(fmt.Sprintf("fleet state: quota reading for pool %s is unavailable (error_kind %s); not published", pool, kind))
		}
		previous.failing = true
		sent[pool] = previous
		// An agy host with no measurable account is a configuration gap, not a
		// login problem (hk 3783): its failure reads unknown, as the login
		// probe leaves that host alone.
		if fleetQuotaLoginMissingKinds[kind] && !client.agyUnmeasurableHere(pool) {
			client.noteQuotaAccount(pool, "")
		} else {
			client.noteQuotaUnknown(pool)
		}
		return
	}
	client.noteQuotaAccount(pool, publish.Key)
	if measuredHere {
		client.noteOwnQuotaMeasurement(pool)
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

// agyUnmeasurableHere says pool is agy on a host that holds no measurable agy
// account: the same rule measurableHere applies to the login probe.
func (client *HubClient) agyUnmeasurableHere(pool string) bool {
	return pool == "agy" && !client.nativeQuotaSource(pool).agyMeasurable()
}

// measureFleetQuota takes pool's reading for the fleet-state publish. The
// pools this host measures natively (claude, codex, and commandcode once its
// switch is flipped) are measured in this process; every other pool runs
// `pacelever refresh`, and a pool in shadow also logs the node's own
// measurement beside it (quota_shadow.go). A failed measurement answers the error kind that says
// why: the login rule reads it. run is the measurement's own deadline and
// parent the loop's context, which the failure-kind status lookup outlives it
// on.
func (client *HubClient) measureFleetQuota(run, parent context.Context, pool string) (FleetStatePublish, bool, string) {
	publish, ok, _, kind := client.measureFleetQuotaObserved(run, parent, pool)
	return publish, ok, kind
}

func (client *HubClient) measureFleetQuotaObserved(run, parent context.Context, pool string) (FleetStatePublish, bool, bool, string) {
	if isNativeQuotaPool(pool) {
		reading, measuredHere, err := client.nativeQuotaSource(pool).RefreshWithMeasurement(run)
		if err != nil {
			// A timed-out refresh says nothing about the login.
			if run.Err() != nil {
				return FleetStatePublish{}, false, measuredHere, fleetQuotaErrorKindUnreadable
			}
			return FleetStatePublish{}, false, measuredHere, quotaErrorKind(err)
		}
		publish, ok := fleetQuotaFromReading(pool, reading, client.nowUTC())
		if !ok {
			return FleetStatePublish{}, false, measuredHere, quota.ErrKindParse
		}
		return publish, true, measuredHere, ""
	}
	return FleetStatePublish{}, false, false, "unmeasurable"
}

// measureFleetQuotaWithPacelever is the fleet-state measurement of a pool the
// node does not measure itself, or measures only in shadow. It also returns
// pacelever's raw output, which the shadow comparison reads on its own terms.
func fleetQuotaFromReading(pool string, reading shared.Reading, now time.Time) (FleetStatePublish, bool) {
	if !fleetStateAccountKeyPattern.MatchString(reading.Key) || !strings.HasPrefix(reading.Key, pool+":") {
		return FleetStatePublish{}, false
	}
	quotaValue := &FleetStateQuota{AccountKey: reading.Key, Pool: pool, Buckets: []FleetStateBucket{}}
	if fleetStateTokenPattern.MatchString(reading.Source) {
		quotaValue.Source = reading.Source
	}
	for _, bucket := range reading.Buckets {
		value := bucket.UsedPct
		out := FleetStateBucket{Window: bucket.Window, UsedPct: &value, Scope: bucket.Scope, Model: bucket.Model}
		if bucket.ResetsAt != nil {
			out.ResetsAt = bucket.ResetsAt.UTC().Format(time.RFC3339)
		}
		quotaValue.Buckets = append(quotaValue.Buckets, out)
	}
	if reading.Free != nil {
		free := &FleetStateFree{State: reading.Free.State}
		if reading.Free.ResetsAt != nil {
			at := reading.Free.ResetsAt.UTC()
			free.ResetsAt = &at
		}
		quotaValue.Free = free
	}
	if len(quotaValue.Buckets) == 0 && quotaValue.Free == nil {
		return FleetStatePublish{}, false
	}
	publish := FleetStatePublish{Kind: fleetStateKindQuota, Key: reading.Key, MeasuredAt: reading.MeasuredAt.UTC().Truncate(time.Millisecond), Quota: quotaValue}
	if confirmed := reading.ConfirmedAt; confirmed != nil && confirmed.After(reading.MeasuredAt) && !confirmed.After(now.Add(time.Minute)) {
		at := confirmed.UTC()
		publish.ConfirmedAt = &at
	}
	encoded, err := json.Marshal(publish)
	if err != nil {
		return FleetStatePublish{}, false
	}
	if _, valid := decodeFleetStatePublish(encoded); !valid {
		return FleetStatePublish{}, false
	}
	return publish, true
}

// fleetQuotaErrorKind reads pool's error_kind from `pacelever --json` with its
// own fleetQuotaTimeout. A status that exits non-zero but still lists the pool
// is read: pacelever reports failing providers that way.
// fleetQuotaObservationTransport is the confirmed_at read transport; nil uses the default.
var fleetQuotaObservationTransport http.RoundTripper

func (client *HubClient) fleetQuotaConfirmedAt(ctx context.Context, key string, measured time.Time) time.Time {
	if !fleetStateAccountKeyPattern.MatchString(key) {
		return time.Time{}
	}
	config, err := loadHubSpawnConfig(client.spawnConfigPath)
	if err != nil || config.HandoffkeepEnv == "" {
		return time.Time{}
	}
	env, err := loadHubHandoffkeepEnv(config.HandoffkeepEnv)
	if err != nil {
		return time.Time{}
	}
	base, err := url.Parse(strings.TrimSuffix(env.URL, "/"))
	if err != nil {
		return time.Time{}
	}
	base.Path, base.RawPath = "/v1/quota/accounts/"+key+"/latest", ""
	base.RawQuery, base.ForceQuery, base.Fragment, base.RawFragment = "", false, "", ""
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, base.String(), nil)
	if err != nil {
		return time.Time{}
	}
	env.setAuth(request)
	request.Header.Set("Accept", "application/json")
	response, err := (&http.Client{Transport: fleetQuotaObservationTransport, Timeout: handoffkeepTimeout, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}).Do(request)
	if err != nil {
		return time.Time{}
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return time.Time{}
	}
	var body struct {
		Observation *struct {
			MeasuredAt  time.Time  `json:"measured_at"`
			ConfirmedAt *time.Time `json:"confirmed_at"`
		} `json:"observation"`
	}
	if json.NewDecoder(io.LimitReader(response.Body, 64<<10)).Decode(&body) != nil || body.Observation == nil || body.Observation.ConfirmedAt == nil {
		return time.Time{}
	}
	o := body.Observation
	if d := o.MeasuredAt.Sub(measured); d < -time.Second || d > time.Second {
		return time.Time{}
	}
	if !o.ConfirmedAt.After(o.MeasuredAt) || o.ConfirmedAt.After(client.nowUTC().Add(time.Minute)) {
		return time.Time{}
	}
	return o.ConfirmedAt.UTC()
}
