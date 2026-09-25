package panewire

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
)

// relayDedupeMode is the #725 rollout switch. Shadow is the default: every
// dedupe decision is counted, journaled and logged, but delivery behavior is
// exactly what it was without the feature. Apply suppresses a proven repeat
// and re-acknowledges it so the hub retires the durable row instead of
// replaying it forever. Off disables the machinery outright.
type relayDedupeMode int

const (
	relayDedupeShadow relayDedupeMode = iota
	relayDedupeApply
	relayDedupeOff
)

// relayDedupeModeFromEnv reads PANEWIRE_RELAY_DEDUPE. Any value that is not
// an explicit opt-in falls back to shadow: a misspelled mode must never
// silently start suppressing deliveries.
func relayDedupeModeFromEnv() relayDedupeMode {
	switch os.Getenv("PANEWIRE_RELAY_DEDUPE") {
	case "apply":
		return relayDedupeApply
	case "off":
		return relayDedupeOff
	default:
		return relayDedupeShadow
	}
}

func (mode relayDedupeMode) applies() bool { return mode == relayDedupeApply }

// relayDedupeCounters is the node-side operator surface for the shadow week:
// how many repeats the gate would have dropped, how many it dropped, how many
// same-key arrivals carried a different payload or destination, and how many
// injects carried no verifiable identity at all.
type relayDedupeCounters struct {
	WouldSuppress uint64 `json:"would_suppress"`
	Suppressed    uint64 `json:"suppressed"`
	Mismatch      uint64 `json:"mismatch"`
	Unknown       uint64 `json:"unknown"`
}

// relayDedupeClaimKey scopes one in-flight dedupe claim to (lane, event_id):
// the durable identity the hub's idempotency index already makes unique.
func relayDedupeClaimKey(lane string, eventID int64) string {
	return lane + "\x00" + strconv.FormatInt(eventID, 10)
}

// relayPayloadFingerprint is the content side of the delivered record. The
// row id proves an event's identity; the fingerprint proves what the pane
// actually saw under it.
func relayPayloadFingerprint(text string) string {
	sum := sha256.Sum256([]byte(text))
	return hex.EncodeToString(sum[:])
}

// journalDedupe leaves each decision in the node's event journal so shadow
// counts survive a restart and stay greppable next to every other relay
// record. A journal failure is visibility loss, never a delivery decision.
func (manager *relayBusyManager) journalDedupe(ctx context.Context, action string, message hubOutboundMessage) {
	store := manager.client.relayStore()
	if store == nil {
		return
	}
	payload, _ := json.Marshal(struct {
		Action  string `json:"action"`
		Lane    string `json:"lane"`
		EventID int64  `json:"event_id"`
		Pane    string `json:"pane"`
	}{Action: action, Lane: message.Lane, EventID: message.EventID, Pane: message.Pane})
	_ = store.RecordEvent(ctx, Event{Source: "panewire", Kind: "relay.dedupe", PaneID: message.Pane, Payload: payload})
}

// relayDeliveredIdentity gates one inject against the durable delivered
// record. It returns true only when the caller must stop: an apply-mode
// suppression of a proven duplicate. A mismatch — the same identity carrying
// a different payload or naming a different destination pane — is counted
// and delivered, never swallowed.
func (manager *relayBusyManager) relayDeliveredIdentity(parent context.Context, message hubOutboundMessage, mode relayDedupeMode) bool {
	store := manager.client.relayStore()
	if store == nil {
		return false
	}
	previous, found, err := store.RelayDeliveredByKey(parent, message.Lane, message.EventID)
	if err != nil || !found {
		return false
	}
	if previous.Pane != message.Pane || previous.PayloadSHA != relayPayloadFingerprint(message.Text) {
		manager.dedupeMismatch.Add(1)
		manager.journalDedupe(parent, "mismatch", message)
		manager.client.warnMessage(fmt.Sprintf("relay dedupe mismatch: lane=%s event_id=%d pane=%s arrived with changed payload or destination; delivering", message.Lane, message.EventID, message.Pane))
		return false
	}
	if !mode.applies() {
		manager.dedupeWouldSuppress.Add(1)
		manager.journalDedupe(parent, "would_suppress", message)
		manager.client.warnMessage(fmt.Sprintf("relay dedupe shadow: lane=%s event_id=%d pane=%s is a repeat of a delivered event; delivering", message.Lane, message.EventID, message.Pane))
		return false
	}
	manager.dedupeSuppressed.Add(1)
	manager.journalDedupe(parent, "suppressed", message)
	manager.client.warnMessage(fmt.Sprintf("relay dedupe suppressed: lane=%s event_id=%d pane=%s was already delivered; re-acknowledging", message.Lane, message.EventID, message.Pane))
	// The pane already has this note; the durable row is still undelivered,
	// which is why the hub keeps re-injecting it. This acknowledgement is
	// the truthful close: it marks delivered_at through the ordinary
	// relay.delivered path and ends the replay loop.
	manager.emit("relay.delivered", relayAckPayload{JobID: message.JobID, Pane: message.Pane, Reason: "dedupe_suppressed", OriginalEventID: message.EventID})
	// A delivered row must not stay queued: a crash between the delivery
	// record and the held-row delete is the one state where they coexist.
	_, _ = store.DeleteRelayHeld(parent, message.EventID)
	return true
}

// claimDedupeInFlight serializes concurrent arrivals of the same identity.
// offer() runs on a goroutine per inject, so two copies can race the durable
// check before either has recorded anything. A copy that loses the race is
// not itself a verdict: it waits on the returned channel until the winner's
// outcome is decided, then re-evaluates the delivered record — whether this
// copy is a duplicate depends on whether the first one actually landed.
func (manager *relayBusyManager) claimDedupeInFlight(lane string, eventID int64) (<-chan struct{}, bool) {
	key := relayDedupeClaimKey(lane, eventID)
	manager.mu.Lock()
	defer manager.mu.Unlock()
	if done, taken := manager.dedupeInFlight[key]; taken {
		return done, false
	}
	manager.dedupeInFlight[key] = make(chan struct{})
	return nil, true
}

func (manager *relayBusyManager) releaseDedupeInFlight(lane string, eventID int64) {
	key := relayDedupeClaimKey(lane, eventID)
	manager.mu.Lock()
	if done, taken := manager.dedupeInFlight[key]; taken {
		delete(manager.dedupeInFlight, key)
		close(done)
	}
	manager.mu.Unlock()
}

// noteDedupeUnknown covers the contract's legacy clause: an inject whose
// durable identity cannot be verified (no lane or no row id — pre-durable
// senders and fire-and-forget notices) is always delivered and counted, and
// is never suppressed under a manufactured key.
func (manager *relayBusyManager) noteDedupeUnknown(parent context.Context, message hubOutboundMessage) {
	manager.dedupeUnknown.Add(1)
	manager.journalDedupe(parent, "unknown", message)
	manager.client.warnMessage(fmt.Sprintf("relay dedupe unknown identity: job=%s pane=%s event_id=%d has no verifiable lane/event identity; delivering", message.JobID, message.Pane, message.EventID))
}

func (manager *relayBusyManager) RelayDedupeCounts() relayDedupeCounters {
	return relayDedupeCounters{
		WouldSuppress: manager.dedupeWouldSuppress.Load(),
		Suppressed:    manager.dedupeSuppressed.Load(),
		Mismatch:      manager.dedupeMismatch.Load(),
		Unknown:       manager.dedupeUnknown.Load(),
	}
}
