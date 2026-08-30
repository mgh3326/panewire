package supabase

import (
	"context"
	"errors"
	"net/http"
	"time"

	"github.com/mgh3326/panewire/sentinel"
)

type sentinelHeartbeatWire struct {
	MachineID string                          `json:"machine_id"`
	SeenAt    time.Time                       `json:"seen_at"`
	Checks    map[string]sentinel.CheckStatus `json:"checks_json"`
	Version   string                          `json:"version"`
}

// UpsertHeartbeat uses the authenticated table policy: a caller can write
// only the machine selected by its own JWT.  Check argv/output is not part of
// this wire shape.
func (a *Adapter) UpsertHeartbeat(ctx context.Context, heartbeat sentinel.Heartbeat) error {
	if err := sentinel.ValidateHeartbeat(heartbeat); err != nil {
		return errors.New("invalid sentinel heartbeat")
	}
	input := []sentinelHeartbeatWire{{
		MachineID: heartbeat.MachineID,
		SeenAt:    heartbeat.SeenAt.UTC(),
		Checks:    cloneSentinelChecks(heartbeat.Checks),
		Version:   heartbeat.Version,
	}}
	headers := make(http.Header)
	headers.Set("Prefer", "resolution=merge-duplicates,return=minimal")
	if err := a.callWithHeaders(ctx, http.MethodPost, "/rest/v1/sentinel_heartbeats?on_conflict=machine_id", input, nil, headers); err != nil {
		return errors.New("Supabase sentinel heartbeat upsert failed")
	}
	return nil
}

// ListHeartbeats is intentionally available to every authenticated machine:
// mutual monitoring needs peer visibility, unlike destination-scoped queue
// reads.  Invalid rows are rejected before callers can format an alert.
func (a *Adapter) ListHeartbeats(ctx context.Context) ([]sentinel.Heartbeat, error) {
	var rows []sentinelHeartbeatWire
	if err := a.call(ctx, http.MethodGet, "/rest/v1/sentinel_heartbeats?select=machine_id,seen_at,checks_json,version&order=machine_id.asc", nil, &rows); err != nil {
		return nil, errors.New("Supabase sentinel heartbeat list failed")
	}
	heartbeats := make([]sentinel.Heartbeat, 0, len(rows))
	for _, row := range rows {
		heartbeat := sentinel.Heartbeat{MachineID: row.MachineID, SeenAt: row.SeenAt.UTC(), Checks: cloneSentinelChecks(row.Checks), Version: row.Version}
		if err := sentinel.ValidateHeartbeat(heartbeat); err != nil {
			return nil, errors.New("Supabase sentinel heartbeat response is invalid")
		}
		heartbeats = append(heartbeats, heartbeat)
	}
	return heartbeats, nil
}

type sentinelClaimRequest struct {
	IncidentKey     string    `json:"p_incident_key"`
	AlertWindow     time.Time `json:"p_alert_window"`
	ClaimTTLSeconds int64     `json:"p_claim_ttl_seconds"`
}

type sentinelClaimResponse struct {
	Claimed bool `json:"claimed"`
}

// ClaimAlert calls the database-side TTL-aware unique insert.  The function
// takes the claimant identity from auth.uid(), never from this request.
func (a *Adapter) ClaimAlert(ctx context.Context, claim sentinel.AlertClaim) (bool, error) {
	seconds, ok := sentinelClaimSeconds(claim)
	if !ok {
		return false, errors.New("sentinel alert claim is invalid")
	}
	var rows []sentinelClaimResponse
	if err := a.call(ctx, http.MethodPost, "/rest/v1/rpc/panewire_sentinel_claim_alert", sentinelClaimRequest{
		IncidentKey: claim.IncidentKey, AlertWindow: claim.AlertWindow.UTC(), ClaimTTLSeconds: seconds,
	}, &rows); err != nil {
		return false, errors.New("Supabase sentinel alert claim failed")
	}
	if len(rows) != 1 {
		return false, errors.New("Supabase sentinel alert claim response is invalid")
	}
	return rows[0].Claimed, nil
}

type sentinelDeliveryRequest struct {
	IncidentKey string    `json:"p_incident_key"`
	AlertWindow time.Time `json:"p_alert_window"`
}

type sentinelDeliveryResponse struct {
	Delivered bool `json:"delivered"`
}

func (a *Adapter) MarkAlertDelivered(ctx context.Context, claim sentinel.AlertClaim) error {
	if _, ok := sentinelClaimSeconds(claim); !ok {
		return errors.New("sentinel alert delivery mark is invalid")
	}
	var rows []sentinelDeliveryResponse
	if err := a.call(ctx, http.MethodPost, "/rest/v1/rpc/panewire_sentinel_mark_alert_delivered", sentinelDeliveryRequest{
		IncidentKey: claim.IncidentKey, AlertWindow: claim.AlertWindow.UTC(),
	}, &rows); err != nil {
		return errors.New("Supabase sentinel alert delivery mark failed")
	}
	if len(rows) != 1 || !rows[0].Delivered {
		return errors.New("Supabase sentinel alert delivery mark was not accepted")
	}
	return nil
}

func sentinelClaimSeconds(claim sentinel.AlertClaim) (int64, bool) {
	if claim.IncidentKey == "" || claim.AlertWindow.IsZero() || claim.TTL < 5*time.Second || claim.TTL > 10*time.Minute {
		return 0, false
	}
	seconds := int64(claim.TTL / time.Second)
	if claim.TTL%time.Second != 0 {
		seconds++
	}
	return seconds, true
}

func cloneSentinelChecks(checks map[string]sentinel.CheckStatus) map[string]sentinel.CheckStatus {
	if len(checks) == 0 {
		return map[string]sentinel.CheckStatus{}
	}
	copy := make(map[string]sentinel.CheckStatus, len(checks))
	for name, status := range checks {
		copy[name] = status
	}
	return copy
}

var _ sentinel.Remote = (*Adapter)(nil)
