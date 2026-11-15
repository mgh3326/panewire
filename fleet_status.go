package panewire

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"sort"
	"time"

	"github.com/mgh3326/panewire/internal/fleet"
)

// The fleet status snapshot: the hub writes one hk document every few minutes
// with the host presence, the quota readings by host and account, and the plan
// a T1 implementer spawn would follow for each grade. The contract is
// docs/fleet-status.md; hk 4149's console page reads it.
//
// It carries no credential and no account email: an account appears only as
// the shared quota cache's fingerprint key (`<pool>:<16-hex>`), the same short
// id `panewire quota status` prints.

const (
	fleetStatusDocKey   = "fleet/status/latest"
	fleetStatusDocKind  = "note"
	fleetStatusVersion  = 1
	fleetStatusMaxBytes = 64 << 10

	// Per-list caps keep the body bounded before the byte cap is checked. The
	// byte cap is authoritative: the body drops trailing entries until it fits.
	fleetStatusMaxHosts   = 64
	fleetStatusMaxQuota   = 64
	fleetStatusMaxWindows = 8

	// defaultFleetStatusInterval is how often `panewire hub run` writes the
	// snapshot unless --fleet-status-interval overrides it.
	defaultFleetStatusInterval = 3 * time.Minute
	// fleetStatusPlanTimeout bounds one plan pass, so a slow quota, hk or hub
	// read cannot hold the next write.
	fleetStatusPlanTimeout  = 30 * time.Second
	fleetStatusWriteTimeout = 30 * time.Second

	// fleetStatusDefaultFresh is the shared quota freshness window for a pool
	// without a special default (fleet.sharedFreshTTL's 300 s); claude keeps
	// the 900 s window a spawn's gate applies.
	fleetStatusDefaultFresh = 300 * time.Second
	fleetStatusClaudeFresh  = 900 * time.Second

	fleetStatusBandUnknown = "unknown"
)

var errFleetStatusTooLarge = errors.New("fleet status snapshot exceeds its byte cap")

type fleetStatusDoc struct {
	V     int                `json:"v"`
	At    time.Time          `json:"at"`
	Hosts []fleetStatusHost  `json:"hosts"`
	Quota []fleetStatusQuota `json:"quota"`
	Plan  []fleetStatusPlan  `json:"plan"`
}

type fleetStatusHost struct {
	Host      string `json:"host"`
	Connected bool   `json:"connected"`
	Version   string `json:"version,omitempty"`
	Accepting bool   `json:"accepting"`
	LanesUsed int    `json:"lanes_used"`
	LanesCap  int    `json:"lanes_cap"`
}

type fleetStatusWindow struct {
	Window   string   `json:"window"`
	UsedPct  *float64 `json:"used_pct"`
	ResetsAt string   `json:"resets_at,omitempty"`
}

type fleetStatusQuota struct {
	Host       string              `json:"host"`
	Pool       string              `json:"pool"`
	Account    string              `json:"account"`
	Windows    []fleetStatusWindow `json:"windows"`
	MeasuredAt time.Time           `json:"measured_at"`
	AgeS       int64               `json:"age_s"`
	FreshTTLS  int64               `json:"fresh_ttl_s"`
	Fresh      bool                `json:"fresh"`
	Band       string              `json:"band"`
}

type fleetStatusPlan struct {
	Grade   string `json:"grade"`
	T       string `json:"t"`
	Seat    string `json:"seat,omitempty"`
	Host    string `json:"host,omitempty"`
	Pool    string `json:"pool,omitempty"`
	Account string `json:"account,omitempty"`
	Tier    string `json:"tier,omitempty"`
	Outcome string `json:"outcome"`
}

// runFleetStatus writes the snapshot once at start and then every interval
// until the maintenance context ends. A failed write is logged once and the
// next tick tries again; there is no tighter retry loop.
func (h *HubServer) runFleetStatus(ctx context.Context) {
	if h.fleetStatusInterval <= 0 {
		return
	}
	h.publishFleetStatus(ctx)
	ticker := time.NewTicker(h.fleetStatusInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			h.publishFleetStatus(ctx)
		}
	}
}

// publishFleetStatus builds one snapshot and writes it. A build or write
// failure is logged once; the next cycle retries.
func (h *HubServer) publishFleetStatus(ctx context.Context) {
	now := h.now().UTC()
	body, err := h.fleetStatusDocument(ctx, now).body()
	if err == nil {
		writeCtx, cancel := context.WithTimeout(ctx, fleetStatusWriteTimeout)
		err = h.fleetStatusWrite(writeCtx, body)
		cancel()
	}
	if err != nil {
		h.logger.Warn("fleet status snapshot not written; retrying next cycle", "key", fleetStatusDocKey, "error", err)
	}
}

// fleetStatusWrite writes the body with the hub's hk credential, unless a test
// injected a writer.
func (h *HubServer) fleetStatusWrite(ctx context.Context, body string) error {
	if h.fleetStatusPublish != nil {
		return h.fleetStatusPublish(ctx, body)
	}
	return fleet.PutHKDocument(ctx, fleetStatusDocKey, fleetStatusDocKind, "", body)
}

// fleetStatusDocument assembles the snapshot at now. Nothing here writes state
// or measures an account: quota and hosts come from the hub's own latest
// values, and the plan is a read-only dry run.
func (h *HubServer) fleetStatusDocument(ctx context.Context, now time.Time) fleetStatusDoc {
	routes := loadReportRelayRoutes(h.reportRelayPath)
	doc := fleetStatusDoc{
		V:     fleetStatusVersion,
		At:    now,
		Hosts: h.fleetStatusHosts(routes),
		Quota: h.fleetStatusQuota(now),
		Plan:  h.fleetStatusPlansNow(ctx),
	}
	return doc
}

// fleetStatusHosts lists every host the hub knows: a node record (a retained
// disconnected record included) and any machine that owns a lane row.
func (h *HubServer) fleetStatusHosts(routes map[string]reportRelayRoute) []fleetStatusHost {
	hosts := make(map[string]fleetStatusHost)
	for _, node := range h.Nodes() {
		hosts[node.MachineID] = fleetStatusHost{
			Host:      node.MachineID,
			Connected: node.State == "connected",
			Version:   node.RemoteMeta["version"],
			Accepting: node.Accepting,
		}
	}
	for _, route := range routes {
		if route.Sink || route.Machine == "" {
			continue
		}
		if _, known := hosts[route.Machine]; !known {
			hosts[route.Machine] = fleetStatusHost{Host: route.Machine}
		}
	}
	out := make([]fleetStatusHost, 0, len(hosts))
	for _, host := range hosts {
		host.LanesUsed = countMachineLanes(routes, host.Host)
		host.LanesCap = h.lanesNodeRowCap
		out = append(out, host)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Host < out[j].Host })
	if len(out) > fleetStatusMaxHosts {
		out = out[:fleetStatusMaxHosts]
	}
	return out
}

// fleetStatusQuota lists one row per (host, account) reading the hub holds,
// oldest reading of the same account last replaced in the state store. Rows
// sort by host then account so the document is stable across cycles.
func (h *HubServer) fleetStatusQuota(now time.Time) []fleetStatusQuota {
	policy, policyErr := fleet.LoadPoolPolicy("")
	out := make([]fleetStatusQuota, 0)
	for _, message := range h.stateStore().snapshot(now).States {
		if message.Kind != fleetStateKindQuota || message.Quota == nil {
			continue
		}
		quota := message.Quota
		entry := fleetStatusQuota{
			Host:       message.Host,
			Pool:       quota.Pool,
			Account:    quota.AccountKey,
			MeasuredAt: message.MeasuredAt.UTC(),
			Windows:    make([]fleetStatusWindow, 0, len(quota.Buckets)),
		}
		windows := make([]fleet.QuotaWindow, 0, len(quota.Buckets))
		for i, bucket := range quota.Buckets {
			windows = append(windows, fleet.QuotaWindow{Window: bucket.Window, UsedPct: bucket.UsedPct, ResetsAt: bucket.ResetsAt})
			if i >= fleetStatusMaxWindows {
				continue
			}
			entry.Windows = append(entry.Windows, fleetStatusWindow{Window: bucket.Window, UsedPct: bucket.UsedPct, ResetsAt: bucket.ResetsAt})
		}
		observed := fleetStateObservedAt(message)
		ttl := fleetStatusFreshTTL(quota.Pool)
		age := now.Sub(observed)
		entry.AgeS = int64(age / time.Second)
		entry.FreshTTLS = int64(ttl / time.Second)
		entry.Fresh = age >= -time.Minute && age <= ttl
		entry.Band = fleetStatusBandUnknown
		if policyErr == nil {
			entry.Band = policy.Band(quota.Pool, fleet.QuotaView(windows, message.MeasuredAt))
		}
		out = append(out, entry)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Host != out[j].Host {
			return out[i].Host < out[j].Host
		}
		return out[i].Account < out[j].Account
	})
	if len(out) > fleetStatusMaxQuota {
		out = out[:fleetStatusMaxQuota]
	}
	return out
}

// fleetStatusFreshTTL is the shared-quota freshness window for pool, matching
// fleet.sharedFreshTTL's defaults: claude 900 s, every other pool 300 s. The
// document carries the value it applied so the page can say how stale is stale.
func fleetStatusFreshTTL(pool string) time.Duration {
	if pool == "claude" {
		return fleetStatusClaudeFresh
	}
	return fleetStatusDefaultFresh
}

// fleetStatusPlansNow answers the per-grade plan. The hub's own fleet App
// places through the same state a spawn does, with a transport that refuses
// both dispatch and measurement.
func (h *HubServer) fleetStatusPlansNow(ctx context.Context) []fleetStatusPlan {
	if h.fleetStatusPlanFn != nil {
		return fleetStatusPlanRows(h.fleetStatusPlanFn(ctx))
	}
	app, err := h.newSpawnApp(io.Discard, io.Discard)
	if err != nil || app == nil {
		return fleetStatusUnavailablePlans()
	}
	h.bindPlacement(app, planOnlyTransport{})
	planCtx, cancel := context.WithTimeout(ctx, fleetStatusPlanTimeout)
	defer cancel()
	return fleetStatusPlanRows(app.PlanStatusGrades(planCtx, fleet.StatusPlanGrades))
}

func fleetStatusPlanRows(plans []fleet.StatusPlan) []fleetStatusPlan {
	out := make([]fleetStatusPlan, 0, len(plans))
	for _, plan := range plans {
		out = append(out, fleetStatusPlan{
			Grade: plan.Grade, T: plan.T, Seat: plan.Seat, Host: plan.Host,
			Pool: plan.Pool, Account: plan.AccountKey, Tier: plan.Tier, Outcome: plan.Outcome,
		})
	}
	return out
}

// fleetStatusUnavailablePlans keeps the document's shape when the hub cannot
// build its fleet App: one row per grade, with the outcome a reader already
// knows from the plan code.
func fleetStatusUnavailablePlans() []fleetStatusPlan {
	out := make([]fleetStatusPlan, 0, len(fleet.StatusPlanGrades))
	for _, grade := range fleet.StatusPlanGrades {
		out = append(out, fleetStatusPlan{Grade: grade, T: fleet.StatusPlanT, Outcome: "POLICY_UNAVAILABLE"})
	}
	return out
}

// body marshals the document under fleetStatusMaxBytes. It drops trailing
// quota rows first, then trailing hosts, then trailing plan rows, until the
// body fits; the caps above make that a last resort.
func (doc fleetStatusDoc) body() (string, error) {
	for {
		encoded, err := json.Marshal(doc)
		if err != nil {
			return "", err
		}
		if len(encoded) <= fleetStatusMaxBytes {
			return string(encoded), nil
		}
		switch {
		case len(doc.Quota) > 0:
			doc.Quota = doc.Quota[:len(doc.Quota)-1]
		case len(doc.Hosts) > 0:
			doc.Hosts = doc.Hosts[:len(doc.Hosts)-1]
		case len(doc.Plan) > 0:
			doc.Plan = doc.Plan[:len(doc.Plan)-1]
		default:
			return "", errFleetStatusTooLarge
		}
	}
}
