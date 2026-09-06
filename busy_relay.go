package panewire

import (
	"context"
	"encoding/json"
	"errors"
	"os/exec"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	defaultRelayMaxWait = 30 * time.Minute
	relayBatchTextLimit = 8 << 10
	// A single item is intentionally allowed to exceed a batch. It stays one
	// prompt; this upper transport bound leaves room inside the hub websocket.
	relaySingleItemMax = 24 << 10
	// relayCancelledPane is a handoffkeep delivery sentinel, never a pane id.
	relayCancelledPane = "cancelled"
)

type relayCommandRunner func(context.Context, ...string) ([]byte, error)

type relayDeliveryPolicy struct {
	Name    string
	MaxWait time.Duration
}

func parseRelayDeliveryPolicy(value string) (relayDeliveryPolicy, bool) {
	value = strings.TrimSpace(value)
	if value == "" || value == "idle" {
		return relayDeliveryPolicy{Name: "idle", MaxWait: defaultRelayMaxWait}, true
	}
	if value == "now" {
		return relayDeliveryPolicy{Name: "now"}, true
	}
	seconds, found := strings.CutPrefix(value, "max_wait=")
	if !found {
		return relayDeliveryPolicy{}, false
	}
	n, err := strconv.ParseInt(seconds, 10, 64)
	if err != nil || n < 1 || n > int64((24*time.Hour)/time.Second) {
		return relayDeliveryPolicy{}, false
	}
	return relayDeliveryPolicy{Name: value, MaxWait: time.Duration(n) * time.Second}, true
}

func defaultRelayCommand(ctx context.Context, args ...string) ([]byte, error) {
	return exec.CommandContext(ctx, "herdr", args...).CombinedOutput()
}

// parseRelayAgentStatus parses the captured herdr JSON shape instead of
// reproducing its vocabulary in a test double.
func parseRelayAgentStatus(raw []byte) (string, bool) {
	var response struct {
		Result struct {
			Agent struct {
				Status string `json:"agent_status"`
			} `json:"agent"`
		} `json:"result"`
	}
	if json.Unmarshal(raw, &response) != nil {
		return "", false
	}
	switch response.Result.Agent.Status {
	case "idle", "working", "blocked", "done", "unknown":
		return response.Result.Agent.Status, true
	default:
		return "", false
	}
}

func relayWaitTimedOut(raw []byte) bool {
	var response struct {
		Error struct {
			Code string `json:"code"`
		} `json:"error"`
	}
	return json.Unmarshal(raw, &response) == nil && response.Error.Code == "timeout"
}

type relayHeldPayload struct {
	EventID       int64  `json:"event_id"`
	JobID         string `json:"job_id"`
	Pane          string `json:"pane"`
	Lane          string `json:"lane"`
	Reason        string `json:"reason"`
	Preview       string `json:"preview"`
	HeldSince     string `json:"held_since"`
	DeliverPolicy string `json:"deliver_policy"`
}

type relayReleasedPayload struct {
	JobID           string `json:"job_id"`
	Pane            string `json:"pane"`
	Lane            string `json:"lane"`
	FinalText       string `json:"final_text"`
	Edited          bool   `json:"edited"`
	OriginalEventID int64  `json:"original_event_id"`
}

type relayCancelledPayload struct {
	OriginalEventID int64 `json:"original_event_id"`
}

type relayBatchedPayload struct {
	Pane     string  `json:"pane"`
	Lane     string  `json:"lane"`
	EventIDs []int64 `json:"event_ids"`
}

// relayBusyManager serializes each pane, so a returned wait can batch every
// item that arrived while it was busy.  It never polls agent get.
type relayBusyManager struct {
	client *HubClient
	mu     sync.Mutex
	held   map[string][]relayHeld
	waits  map[string]context.CancelFunc
}

func (client *HubClient) relayBusyManager() *relayBusyManager {
	client.busyRelayMu.Lock()
	defer client.busyRelayMu.Unlock()
	if client.busyRelay == nil {
		client.busyRelay = &relayBusyManager{client: client, held: make(map[string][]relayHeld), waits: make(map[string]context.CancelFunc)}
	}
	return client.busyRelay
}

func (client *HubClient) relayRunner() relayCommandRunner {
	if client.relayCommand != nil {
		return client.relayCommand
	}
	// relayInject is package-private fixture plumbing. Never make an older
	// fixture with that seam accidentally contact a real herdr installation.
	if client.relayInject != nil {
		return nil
	}
	return defaultRelayCommand
}

func (client *HubClient) relayNow() time.Time {
	if client.now != nil {
		return client.now().UTC()
	}
	return time.Now().UTC()
}

func (client *HubClient) relayEmit(event hubClientEvent) {
	client.busyRelayMu.Lock()
	emit := client.relayEmitter
	client.busyRelayMu.Unlock()
	if emit != nil {
		emit(event)
	}
}

func (client *HubClient) setRelayEmitter(emit func(hubClientEvent)) {
	client.busyRelayMu.Lock()
	client.relayEmitter = emit
	client.busyRelayMu.Unlock()
}

func (manager *relayBusyManager) emit(kind string, payload any) {
	encoded, err := json.Marshal(payload)
	if err == nil {
		manager.client.relayEmit(hubClientEvent{Kind: kind, Payload: encoded})
	}
}

func (manager *relayBusyManager) offer(parent context.Context, message hubOutboundMessage) {
	if message.Pane == relayCancelledPane {
		return
	}
	if message.EventID > 0 && message.Lane != "" {
		if existing, found, err := manager.client.relayHeldByKey(parent, message.Lane, message.EventID); err == nil && found {
			manager.recoverExisting(parent, existing)
			return
		}
	}
	policy, valid := parseRelayDeliveryPolicy(message.DeliverPolicy)
	if !valid {
		policy = relayDeliveryPolicy{Name: "idle", MaxWait: defaultRelayMaxWait}
	}
	if policy.Name == "now" || message.EventID == 0 || message.Lane == "" {
		manager.deliver(parent, []relayHeld{{Pane: message.Pane, Lane: message.Lane, EventID: message.EventID, JobID: message.JobID, Text: message.Text, HeldSince: manager.client.relayNow(), DeliverPolicy: policy.Name, RecvSeq: message.RecvSeq}}, false)
		return
	}
	runner := manager.client.relayRunner()
	if runner == nil {
		// Fixture-only clients without the explicit command seam are fail-open.
		manager.deliver(parent, []relayHeld{{Pane: message.Pane, Lane: message.Lane, EventID: message.EventID, JobID: message.JobID, Text: message.Text, HeldSince: manager.client.relayNow(), DeliverPolicy: policy.Name, RecvSeq: message.RecvSeq}}, false)
		return
	}
	getContext, cancel := context.WithTimeout(parent, manager.client.relayInjectTimeout())
	output, err := runner(getContext, "agent", "get", message.Pane)
	cancel()
	status, parsed := parseRelayAgentStatus(output)
	if err != nil || !parsed || status == "unknown" || status == "idle" || status == "done" {
		manager.deliver(parent, []relayHeld{{Pane: message.Pane, Lane: message.Lane, EventID: message.EventID, JobID: message.JobID, Text: message.Text, HeldSince: manager.client.relayNow(), DeliverPolicy: policy.Name, RecvSeq: message.RecvSeq}}, false)
		return
	}
	held := relayHeld{Pane: message.Pane, Lane: message.Lane, EventID: message.EventID, JobID: message.JobID, Text: message.Text, HeldSince: manager.client.relayNow(), DeliverPolicy: policy.Name, MaxWait: policy.MaxWait, RecvSeq: message.RecvSeq, fresh: true}
	manager.hold(parent, held, status)
}

func (manager *relayBusyManager) hold(parent context.Context, item relayHeld, reason string) {
	if item.Pane == relayCancelledPane {
		return
	}
	store := manager.client.relayStore()
	if store == nil {
		manager.deliver(parent, []relayHeld{item}, false)
		return
	}
	inserted, err := store.InsertRelayHeld(parent, item)
	if err != nil {
		manager.deliver(parent, []relayHeld{item}, false)
		return
	}
	if !inserted {
		// A hub replay after a node restart is the same local hold, not another
		// prompt. Re-report it so a restarted hub rebuilds its projection.
		rows, err := store.RelayHeldForPane(parent, item.Pane)
		if err == nil {
			manager.mu.Lock()
			manager.held[item.Pane] = rows
			manager.mu.Unlock()
			manager.arm(parent, item.Pane)
		}
		manager.reportHeld(item, reason)
		return
	}
	rows, err := store.RelayHeldForPane(parent, item.Pane)
	if err != nil {
		return
	}
	for index := range rows {
		if rows[index].EventID == item.EventID {
			// SQLite stores milliseconds for restart recovery. Keep the precise
			// receipt instant in this live process so max_wait=N reaches herdr as
			// exactly N*1000 milliseconds on its first arm.
			rows[index].HeldSince = item.HeldSince
			rows[index].fresh = true
		}
	}
	manager.mu.Lock()
	manager.held[item.Pane] = rows
	manager.mu.Unlock()
	manager.reportHeld(item, reason)
	manager.arm(parent, item.Pane)
}

func (manager *relayBusyManager) recoverExisting(parent context.Context, item relayHeld) {
	if item.Pane == relayCancelledPane {
		return
	}
	store := manager.client.relayStore()
	if store == nil {
		return
	}
	rows, err := store.RelayHeldForPane(parent, item.Pane)
	if err != nil {
		return
	}
	manager.mu.Lock()
	manager.held[item.Pane] = rows
	manager.mu.Unlock()
	manager.reportHeld(item, "restored")
	manager.arm(parent, item.Pane)
}

func (manager *relayBusyManager) reportHeld(item relayHeld, reason string) {
	manager.emit("relay.held", relayHeldPayload{EventID: item.EventID, JobID: item.JobID, Pane: item.Pane, Lane: item.Lane, Reason: reason, Preview: truncateRelayText(item.Text, 240), HeldSince: item.HeldSince.UTC().Format(time.RFC3339Nano), DeliverPolicy: item.DeliverPolicy})
}

func (manager *relayBusyManager) arm(parent context.Context, pane string) {
	manager.mu.Lock()
	if cancel := manager.waits[pane]; cancel != nil {
		cancel()
	}
	items := append([]relayHeld(nil), manager.held[pane]...)
	if len(items) == 0 {
		delete(manager.waits, pane)
		manager.mu.Unlock()
		return
	}
	waitFor := items[0].MaxWait
	if !items[0].fresh || len(items) > 1 {
		deadline := items[0].HeldSince.Add(items[0].MaxWait)
		for _, item := range items[1:] {
			if candidate := item.HeldSince.Add(item.MaxWait); candidate.Before(deadline) {
				deadline = candidate
			}
		}
		waitFor = time.Until(deadline)
	}
	if waitFor < 0 {
		waitFor = 0
	} else {
		// herdr accepts whole milliseconds. Round up so conversion never makes
		// a max_wait=N policy expire before N seconds.
		waitFor = ((waitFor + time.Millisecond - 1) / time.Millisecond) * time.Millisecond
	}
	// Any subsequent re-arm must use the original deadline, not grant a fresh
	// full wait merely because another relay/control event arrived.
	for index := range manager.held[pane] {
		manager.held[pane][index].fresh = false
	}
	ctx, cancel := context.WithCancel(parent)
	manager.waits[pane] = cancel
	manager.mu.Unlock()
	go manager.waitForPane(ctx, pane, waitFor)
}

func (manager *relayBusyManager) waitForPane(ctx context.Context, pane string, waitFor time.Duration) {
	runner := manager.client.relayRunner()
	if runner == nil {
		return
	}
	output, err := runner(ctx, "agent", "wait", pane, "--until", "idle", "--until", "done", "--timeout", strconv.FormatInt(waitFor.Milliseconds(), 10))
	if errors.Is(err, context.Canceled) || ctx.Err() != nil {
		return
	}
	_, parsed := parseRelayAgentStatus(output)
	// Only the documented wait timeout is an expiry. Other command failures
	// fail open, but are not mislabeled as a policy deadline.
	timedOut := err != nil && relayWaitTimedOut(output)
	// A malformed wait reply cannot safely leave an item held forever. Its
	// status lookup was already conservative; delivery here is fail-open.
	if err == nil && !parsed {
		timedOut = false
	}
	manager.release(ctx, pane, timedOut)
}

func (manager *relayBusyManager) release(parent context.Context, pane string, expired bool) {
	store := manager.client.relayStore()
	if store == nil {
		return
	}
	manager.mu.Lock()
	items, err := store.RelayHeldForPane(parent, pane)
	if err != nil || len(items) == 0 {
		manager.mu.Unlock()
		return
	}
	delete(manager.waits, pane)
	delete(manager.held, pane)
	manager.mu.Unlock()
	// The state transition is serialized above, but prompt itself must not hold
	// the manager-wide map lock: another pane's read-loop work stays independent.
	manager.deliver(parent, items, expired)
}

func relayBatchText(items []relayHeld, expired bool, now time.Time) string {
	if len(items) == 1 {
		text := items[0].Text
		if expired {
			minutes := int(now.Sub(items[0].HeldSince).Minutes())
			text = "[대기 만료 " + strconv.Itoa(minutes) + "분] " + text
		}
		return text
	}
	var builder strings.Builder
	builder.WriteString("[batch ")
	builder.WriteString(strconv.Itoa(len(items)))
	builder.WriteString("건]")
	for index, item := range items {
		builder.WriteString(" ")
		builder.WriteString(strconv.Itoa(index + 1))
		builder.WriteString(") ")
		if expired {
			builder.WriteString("[대기 만료 ")
			builder.WriteString(strconv.Itoa(int(now.Sub(item.HeldSince).Minutes())))
			builder.WriteString("분] ")
		}
		builder.WriteString(item.Text)
	}
	return builder.String()
}

func relayBatchGroups(items []relayHeld, expired bool, now time.Time) [][]relayHeld {
	var groups [][]relayHeld
	for _, item := range items {
		if len(groups) == 0 {
			groups = append(groups, []relayHeld{item})
			continue
		}
		last := groups[len(groups)-1]
		candidate := append(append([]relayHeld(nil), last...), item)
		if len(item.Text) > relayBatchTextLimit || len(relayBatchText(candidate, expired, now)) > relayBatchTextLimit {
			groups = append(groups, []relayHeld{item})
			continue
		}
		groups[len(groups)-1] = candidate
	}
	return groups
}

func (manager *relayBusyManager) deliver(parent context.Context, items []relayHeld, expired bool) {
	if len(items) == 0 || items[0].Pane == relayCancelledPane {
		return
	}
	sort.SliceStable(items, func(i, j int) bool { return items[i].RecvSeq < items[j].RecvSeq })
	now := manager.client.relayNow()
	for _, group := range relayBatchGroups(items, expired, now) {
		text := relayBatchText(group, expired, now)
		inject := manager.client.relayInject
		if inject == nil {
			inject = defaultHubRelayInject
		}
		ctx, cancel := context.WithTimeout(parent, manager.client.relayInjectTimeout())
		success := inject(ctx, group[0].Pane, text)
		cancel()
		if !success {
			for _, item := range group {
				manager.emit("relay.unconfirmed", relayAckPayload{JobID: item.JobID, Pane: item.Pane, OriginalEventID: item.EventID})
			}
			continue
		}
		if len(group) > 1 {
			ids := make([]int64, 0, len(group))
			for _, item := range group {
				ids = append(ids, item.EventID)
			}
			manager.emit("relay.batched", relayBatchedPayload{Pane: group[0].Pane, Lane: group[0].Lane, EventIDs: ids})
		}
		for _, item := range group {
			if store := manager.client.relayStore(); store != nil && item.EventID != 0 {
				_, _ = store.DeleteRelayHeld(parent, item.EventID)
			}
			released := relayReleasedPayload{JobID: item.JobID, Pane: item.Pane, Lane: item.Lane, FinalText: text, Edited: item.Edited, OriginalEventID: item.EventID}
			manager.emit("relay.released", released)
			manager.emit("relay.delivered", relayAckPayload{JobID: item.JobID, Pane: item.Pane, FinalText: text, Edited: item.Edited, OriginalEventID: item.EventID})
		}
	}
}

func (manager *relayBusyManager) edit(ctx context.Context, eventID int64, text string) bool {
	if text == "" || eventID < 1 {
		return false
	}
	store := manager.client.relayStore()
	if store == nil {
		return false
	}
	manager.mu.Lock()
	defer manager.mu.Unlock()
	changed, err := store.UpdateRelayHeldText(ctx, eventID, text)
	if !changed || err != nil {
		return false
	}
	for pane, items := range manager.held {
		for i := range items {
			if items[i].EventID == eventID {
				items[i].Text, items[i].Edited = text, true
				manager.held[pane] = items
			}
		}
	}
	return true
}

func (manager *relayBusyManager) cancel(ctx context.Context, eventID int64) bool {
	if eventID < 1 {
		return false
	}
	store := manager.client.relayStore()
	if store == nil {
		return false
	}
	manager.mu.Lock()
	deleted, err := store.DeleteRelayHeld(ctx, eventID)
	if !deleted || err != nil {
		manager.mu.Unlock()
		return false
	}
	var panes []string
	for pane, items := range manager.held {
		removed := false
		filtered := items[:0]
		for _, item := range items {
			if item.EventID != eventID {
				filtered = append(filtered, item)
			} else {
				removed = true
			}
		}
		manager.held[pane] = filtered
		if removed {
			panes = append(panes, pane)
		}
	}
	manager.mu.Unlock()
	for _, pane := range panes {
		manager.arm(ctx, pane)
	}
	manager.emit("relay.cancelled", relayCancelledPayload{OriginalEventID: eventID})
	return true
}

func (manager *relayBusyManager) restore(ctx context.Context) {
	store := manager.client.relayStore()
	if store == nil {
		return
	}
	items, err := store.RelayHeldAll(ctx)
	if err != nil {
		return
	}
	byPane := make(map[string][]relayHeld)
	var maxRecvSeq int64
	for _, item := range items {
		if item.Pane == relayCancelledPane {
			_, _ = store.DeleteRelayHeld(ctx, item.EventID)
			continue
		}
		if item.RecvSeq > maxRecvSeq {
			maxRecvSeq = item.RecvSeq
		}
		byPane[item.Pane] = append(byPane[item.Pane], item)
	}
	manager.client.seedRelayRecvSeq(maxRecvSeq)
	manager.mu.Lock()
	manager.held = byPane
	manager.mu.Unlock()
}

func (manager *relayBusyManager) resume(ctx context.Context) {
	manager.mu.Lock()
	panes := make([]string, 0, len(manager.held))
	for pane, items := range manager.held {
		panes = append(panes, pane)
		for _, item := range items {
			manager.reportHeld(item, "restored")
		}
	}
	manager.mu.Unlock()
	for _, pane := range panes {
		manager.arm(ctx, pane)
	}
}
