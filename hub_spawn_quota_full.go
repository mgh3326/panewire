package panewire

import (
	"context"
	"encoding/json"
	"slices"
	"strings"
	"time"

	"github.com/mgh3326/panewire/internal/fleet"
)

// A full quota at the node's gate moves an automatic request on (hk 5358).
//
// The hub's selection and the node's gate read the account's quota from
// different copies: the hub from the shared store at selection, the node from
// the store plus its own free-limit record at the gate. When they disagree the
// node refuses the chosen seat, and until hk 5358 that refusal ended the
// request. nextHostAfterRefusal now treats it like a host-local refusal for an
// automatic request: it unbinds the attempt, records an exclusion for the seat's
// quota band, and wakes placement, which selects again inside the same request
// (same request id, same job). The refusal's reset is used at once: the node
// records and publishes it (fleet.recordRefusedFreeLimit), and the hub cools the
// band in memory so the very next selection skips it. The requester sees one
// success, or the placement's own refusal naming every candidate when none is
// left. docs/hub-spawn-operations.md, "A full quota moves an automatic request on".

// quotaFullReason is the reason a node's full-quota refusal carries.
const quotaFullReason = "spawn_failed:quota_full"

// spawnReasonQuotaFull reports a stored refusal reason of a full quota.
func spawnReasonQuotaFull(reason string) bool {
	code, _, _ := strings.Cut(reason, ": ")
	return code == quotaFullReason
}

// spawnQuotaFull reads a node result that refused on a full quota at the gate.
// The gate refuses with rc 3 (quotaGateBlocked); an unmeasured reading, a
// policy refusal and every other rc keep their own handling.
func spawnQuotaFull(result hubSpawnResult) (fleet.QuotaFull, bool) {
	if result.RC != 3 || result.Error != "spawn_failed" {
		return fleet.QuotaFull{}, false
	}
	return fleet.QuotaFullRefusal(spawnResultRefusal(result.StdoutTail))
}

// spawnProfileAuto reports a request that lets selection choose the profile.
// A pinned -m keeps the refusal: the requester asked for that seat.
func spawnProfileAuto(args []string) bool {
	profile := hubSpawnArg(args, "-m")
	return profile == "" || profile == "auto"
}

// quotaBand names the quota band a refused seat draws on, for --exclude-pool:
// devin-free for a SWE-2 free rung, else the pool the placement pass recorded
// for the chosen seat. Empty when neither is known, in which case only the
// seat (host and profile) is excluded.
func (t *directSpawnTransport) quotaBand(ctx context.Context, node hubSpawnRequest, profile string) string {
	if fleet.IsFreeRung(profile) {
		return "devin-free"
	}
	trace, ok := t.savedPlacementChoice(ctx, node.Machine, profile)
	if !ok || trace.Pool == "" {
		return ""
	}
	return fleet.BandKey(trace.Pool)
}

// savedPlacementChoice is the pass's recorded choice, when it chose host and
// profile.
func (t *directSpawnTransport) savedPlacementChoice(ctx context.Context, host, profile string) (fleet.PlacementTrace, bool) {
	var trace fleet.PlacementTrace
	if t.h.inbox == nil {
		return trace, false
	}
	payload, _, _, err := t.h.inbox.store.spawnPlacementSnapshot(ctx, t.task.Request.RequestID, t.task.Attempt)
	if err != nil || len(payload) == 0 || json.Unmarshal(payload, &trace) != nil {
		return trace, false
	}
	return trace, trace.Chosen == host && trace.Profile == profile
}

// learnQuotaFull applies what a full-quota refusal teaches to the exclusion
// about to be stored. A free limit has an end time: the exclusion lapses then,
// and the hub cools the band until then so the next selection of any request
// skips it (the node publishes the same reset into the shared reading, which is
// the durable copy across a hub restart). A refusal with no end time excludes
// the band for the request's life.
func (h *HubServer) learnQuotaFull(exclusion hubSpawnExclusion, full fleet.QuotaFull, band string) hubSpawnExclusion {
	if full.Reset.IsZero() || !full.Reset.After(h.now()) {
		return exclusion
	}
	exclusion.ExpiresMS = full.Reset.UnixMilli()
	if full.Free && band != "" {
		h.observePoolCooling("quota_stop pool="+band+" reset_at="+full.Reset.UTC().Format(time.RFC3339), h.now())
	}
	return exclusion
}

// withQuotaExclusions adds the bands the request's live full-quota refusals
// ruled out to its --exclude-pool list, so the next selection leaves them out.
// It returns args unchanged when there are none, and never edits args in place.
func withQuotaExclusions(args []string, live []hubSpawnExclusion) []string {
	var bands []string
	for _, excluded := range live {
		if excluded.Pool != "" && spawnReasonQuotaFull(excluded.Reason) && !slices.Contains(bands, excluded.Pool) {
			bands = append(bands, excluded.Pool)
		}
	}
	if len(bands) == 0 {
		return args
	}
	next := slices.Clone(args)
	for i := 0; i+1 < len(next); i++ {
		if next[i] != "--exclude-pool" {
			continue
		}
		for _, existing := range strings.Split(next[i+1], ",") {
			if existing != "" && !slices.Contains(bands, existing) {
				bands = append(bands, existing)
			}
		}
		slices.Sort(bands)
		next[i+1] = strings.Join(bands, ",")
		return next
	}
	slices.Sort(bands)
	return append(next, "--exclude-pool", strings.Join(bands, ","))
}

// spawnQuotaStartMargin is the time a request needs after a limit ends to
// still place and start: a wait is chosen only when the limit's end leaves this
// much of the request's time limit.
const spawnQuotaStartMargin = time.Minute

// quotaWaitUntil decides between waiting and moving on (hk 5358, operator
// decision 435). A free limit whose end time fits inside the request's time
// limit (--expires, the requester's time for the job) is waited out: the
// request is held until the end and starts on the same pool. A limit that ends
// later, a refusal that names no end, or a request with no time limit moves on
// to the next candidate at once. The wait is measured on the hub clock, like
// every recorded time on the server.
func (h *HubServer) quotaWaitUntil(task hubSpawnTask, full fleet.QuotaFull) (time.Time, bool) {
	now := h.now()
	if !full.Free || full.Reset.IsZero() || task.Expires.IsZero() || !full.Reset.After(now) {
		return time.Time{}, false
	}
	if full.Reset.Add(spawnQuotaStartMargin).After(task.Expires) {
		return time.Time{}, false
	}
	return full.Reset.Add(time.Second), true
}
