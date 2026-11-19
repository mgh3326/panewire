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

// quotaBand names the pool a full-quota refusal rules out for the rest of the
// request, for --exclude-pool: only devin-free, the SWE-2 free rungs of the one
// Devin account that shares the free bucket, which the quota-stop replacement
// and the exhausted-pool sweep also hold as a band. Every other pool holds
// several accounts, and a refusal describes only the selected one: excluding the
// pool would discard the funded accounts too, so those refusals exclude the seat
// (host and profile) and, for CommandCode credit, the account (refusedAccount).
func (t *directSpawnTransport) quotaBand(profile string) string {
	if fleet.IsFreeRung(profile) {
		return "devin-free"
	}
	return ""
}

// refusedAccount is the account the refused seat ran on, from the placement
// pass's recorded choice. A CommandCode credit refusal also marks it with the
// hub's durable account mark (hk 5138), so selection skips that account alone
// until a reading shows credits again, for this request and every other.
func (t *directSpawnTransport) refusedAccount(ctx context.Context, node hubSpawnRequest, profile string, full fleet.QuotaFull) string {
	trace, ok := t.savedPlacementChoice(ctx, node.Machine, profile)
	if !ok || trace.AccountKey == "" {
		return ""
	}
	if trace.Pool == commandcodePoolName && strings.HasPrefix(full.Reason, "credit_exhausted") {
		if err := t.h.markAccountCreditExhausted(trace.Pool, trace.AccountKey, t.h.now()); err != nil {
			t.h.logger.Warn("account credit mark could not be stored", "pool", trace.Pool, "account", trace.AccountKey, "error", err)
		}
	}
	return trace.AccountKey
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

// quotaFullExcluded returns the request's live full-quota exclusions when the
// selection or the placement ended on "no candidate" (role_no_candidate, no
// placement candidate accepted). The request's own refusals caused that, so
// waiting for a host to appear cannot change it: the request ends with the
// refusals named, not parked until its limit (hk 5358).
func quotaFullExcluded(task hubSpawnTask, now time.Time, err error) []hubSpawnExclusion {
	if err == nil || strings.Contains(err.Error(), "concurrency cap") {
		return nil
	}
	text := err.Error()
	if code, _, _ := strings.Cut(text, ":"); code != "role_no_candidate" && !strings.Contains(text, "no placement candidate accepted") && !strings.Contains(text, "no remote candidate completed") {
		return nil
	}
	// The request's own refusals are the cause only when nothing else is
	// holding a candidate: a host that is disconnected, full or backing off, or
	// another seat dropped for its own quota, can clear by itself, and the
	// request keeps parking for it.
	if spawnTransientBlocker(text) {
		return nil
	}
	var out []hubSpawnExclusion
	for _, excluded := range liveExclusions(task.Exclusions, now) {
		if spawnReasonQuotaFull(excluded.Reason) {
			out = append(out, excluded)
		}
	}
	return out
}

// spawnTransientBlockerTokens are the drop reasons of a candidate that can
// become eligible without any change to the request (spawnParkReason reads the
// same words to park it).
var spawnTransientBlockerTokens = []string{"task_slots_full", "active_jobs>=max", "lanes_full", "disconnected", "hub_node_unavailable", "not connected", "did not connect", "launch_backoff", "quota", "QUOTA_"}

// spawnTransientBlocker reports a placement refusal that names a transient
// blocker besides this request's own full-quota refusals (hub_quota_full).
func spawnTransientBlocker(text string) bool {
	text = strings.ReplaceAll(text, "hub_quota_full", "")
	for _, token := range spawnTransientBlockerTokens {
		if strings.Contains(text, token) {
			return true
		}
	}
	return false
}

// quotaFullRefusedNotes names each seat a full quota ruled out, appended to the
// placement's own refusal, so the requester reads every candidate and why it
// was dropped.
func quotaFullRefusedNotes(refused []hubSpawnExclusion) string {
	if len(refused) == 0 {
		return ""
	}
	notes := make([]string, 0, len(refused))
	for _, excluded := range refused {
		note := excluded.Machine + "/" + excluded.Profile + " quota_full"
		if excluded.Pool != "" {
			note += " pool=" + excluded.Pool
		}
		if excluded.Account != "" {
			note += " account=" + excluded.Account
		}
		if excluded.ExpiresMS > 0 {
			note += " until " + time.UnixMilli(excluded.ExpiresMS).UTC().Format(time.RFC3339)
		}
		notes = append(notes, note)
	}
	return "; refused by the node's quota gate: " + strings.Join(notes, ", ")
}

// learnQuotaFull applies what a full-quota refusal teaches to the exclusion
// about to be stored. A free limit has an end time: the exclusion lapses then,
// and the hub cools the band until then so the next selection of any request
// skips it (the node publishes the same reset into the shared reading, which is
// the durable copy across a hub restart). A refusal with no end time excludes
// the band for the request's life.
func (h *HubServer) learnQuotaFull(exclusion hubSpawnExclusion, full fleet.QuotaFull, band string) hubSpawnExclusion {
	if full.Reset.IsZero() {
		return exclusion
	}
	// The reset is the exclusion's expiry even when it has already passed (a
	// refusal that reaches the hub a second after its limit ended): zero would
	// rule the band out for the request's life.
	exclusion.ExpiresMS = full.Reset.UnixMilli()
	if !full.Reset.After(h.now()) {
		return exclusion
	}
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
