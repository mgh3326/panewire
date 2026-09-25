package panewire

import (
	"context"
	"encoding/json"
	"errors"
	"os/exec"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode/utf8"
)

const (
	defaultRelayMaxWait = 30 * time.Minute
	relayBatchTextLimit = 8 << 10
	// A single item is intentionally allowed to exceed a batch. It stays one
	// prompt; this upper transport bound leaves room inside the hub websocket.
	relaySingleItemMax = 24 << 10
	// relayCancelledPane is a handoffkeep delivery sentinel, never a pane id.
	relayCancelledPane = "cancelled"
	// relayMaxInjectAttempts bounds #264 D1's rearm-on-failure retry so a
	// permanently broken pane (deleted, unreachable) cannot spin the busy
	// manager forever; once hit, the held row is dropped explicitly instead
	// of rotting past its policy's max_wait.
	relayMaxInjectAttempts = 3
	// relayDevinMaxInjectAttempts is devin's cap (#547): the first inject
	// plus at most one re-inject. claude/codex keep relayMaxInjectAttempts.
	relayDevinMaxInjectAttempts = 2
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

// relayAgentNotFound is the definitive-absence answer: herdr itself says no
// agent occupies the pane. Other command failures (dead socket, timeout,
// malformed output) prove nothing and are handled as unverifiable instead.
func relayAgentNotFound(raw []byte) bool {
	var response struct {
		Error struct {
			Code string `json:"code"`
		} `json:"error"`
	}
	return json.Unmarshal(raw, &response) == nil && response.Error.Code == "agent_not_found"
}

// relayOccupant is the agent-instance identity a held lease binds to. Only
// fields stable for the life of one occupant belong here: revision,
// state_change_seq, cwd, and terminal_title all move while the same agent
// keeps running, so they can never be part of an equality check.
type relayOccupant struct {
	Pane      string
	Agent     string
	Session   string
	Name      string
	Terminal  string
	Workspace string
}

// key serializes the occupant into the stored lease string. An occupant with
// no discriminating field at all (older herdr without agent_session, no
// registered name, no terminal id) cannot prove sameness later, so it keys
// to ” and is handled like a pre-lease row rather than as a match.
func (occupant relayOccupant) key() string {
	if occupant.Session == "" && occupant.Name == "" && occupant.Terminal == "" {
		return ""
	}
	return occupant.Agent + "\x00" + occupant.Session + "\x00" + occupant.Name + "\x00" + occupant.Terminal + "\x00" + occupant.Workspace
}

func parseRelayAgentOccupant(raw []byte) (relayOccupant, bool) {
	var response struct {
		Result struct {
			Type  string `json:"type"`
			Agent *struct {
				PaneID       string `json:"pane_id"`
				Agent        string `json:"agent"`
				Name         string `json:"name"`
				TerminalID   string `json:"terminal_id"`
				WorkspaceID  string `json:"workspace_id"`
				AgentSession *struct {
					Value string `json:"value"`
				} `json:"agent_session"`
			} `json:"agent"`
		} `json:"result"`
	}
	if json.Unmarshal(raw, &response) != nil || response.Result.Agent == nil {
		return relayOccupant{}, false
	}
	// Any valid JSON used to parse "ok" with an all-zero occupant, which the
	// caller then read as a real occupant and expired rows as
	// pane_occupant_changed. A reply is only an agent_info when it says so —
	// or, for older herdr builds without the type field, when it at least
	// names a pane. Everything else is unreadable, not evidence.
	agent := response.Result.Agent
	if response.Result.Type != "agent_info" && agent.PaneID == "" {
		return relayOccupant{}, false
	}
	occupant := relayOccupant{Pane: agent.PaneID, Agent: agent.Agent, Name: agent.Name, Terminal: agent.TerminalID, Workspace: agent.WorkspaceID}
	if agent.AgentSession != nil {
		occupant.Session = agent.AgentSession.Value
	}
	return occupant, true
}

// relayHeldLeaseStale is the fail-closed membership judgement for one held
// row. The occupant probe outcome is three-valued: a pane herdr reports as
// agent_not_found is definitively stale, and only a present occupant can
// satisfy the lease. A pre-lease row (”) can still prove membership from
// the fleet's agent-name convention — the occupant's registered name equals
// the lane — and is expired when even that is absent. Nothing here counts
// failures; every verdict names the membership evidence it rested on.
func relayHeldLeaseStale(item relayHeld, occupant relayOccupant, occupantKnown bool) (bool, string) {
	if !occupantKnown {
		return true, "pane_occupant_gone"
	}
	if item.Lease == "" {
		if occupant.Name != "" && occupant.Name == item.Lane {
			return false, ""
		}
		return true, "lease_unverifiable"
	}
	if occupant.key() != item.Lease {
		return true, "pane_occupant_changed"
	}
	return false, ""
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

type relayDroppedPayload struct {
	JobID           string `json:"job_id"`
	Pane            string `json:"pane"`
	Lane            string `json:"lane"`
	OriginalEventID int64  `json:"original_event_id"`
	Reason          string `json:"reason"`
}

// relayBusyManager serializes each pane, so a returned wait can batch every
// item that arrived while it was busy.  It never polls agent get.
type relayBusyManager struct {
	client *HubClient
	mu     sync.Mutex
	held   map[string][]relayHeld
	waits  map[string]context.CancelFunc
	// lanePane is the newest route observation each inject directive carries:
	// the hub re-resolves lanes.json for every relay, so the (lane, pane) an
	// inject names is authoritative for its arrival instant. A held row whose
	// lane is later observed on a different pane is stale membership evidence
	// even while the old pane's occupant is unchanged.
	lanePane map[string]string
}

func (client *HubClient) relayBusyManager() *relayBusyManager {
	client.busyRelayMu.Lock()
	defer client.busyRelayMu.Unlock()
	if client.busyRelay == nil {
		client.busyRelay = &relayBusyManager{client: client, held: make(map[string][]relayHeld), waits: make(map[string]context.CancelFunc), lanePane: make(map[string]string)}
	}
	return client.busyRelay
}

func (manager *relayBusyManager) noteLaneRoute(lane, pane string) {
	manager.mu.Lock()
	manager.lanePane[lane] = pane
	manager.mu.Unlock()
}

func (manager *relayBusyManager) observedLanePane(lane string) (string, bool) {
	manager.mu.Lock()
	defer manager.mu.Unlock()
	pane, known := manager.lanePane[lane]
	return pane, known
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
	// Every lane-carrying inject is a fresh route observation — including
	// replays that resolve to a different pane after a re-route. Recording it
	// before the dedupe path returns early is what lets a later release judge
	// the old row stale instead of re-confirming it.
	if message.Lane != "" {
		manager.noteLaneRoute(message.Lane, message.Pane)
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
	// The lease rides the same agent get that proved the pane busy — no extra
	// probe is needed to bind the row to the occupant instance it waited on.
	if occupant, ok := parseRelayAgentOccupant(output); ok && occupant.Pane == message.Pane {
		held.Lease = occupant.key()
	}
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

// retryOrDrop is #264 D1's fix: deliver() used to leave a failed inject's
// row in relay_held forever (no rearm, no delete), which is why stale rows
// were observed there hours past their max_wait. A held-eligible item
// (EventID/Lane set) is re-armed with a fresh hold so it retries the next
// time its pane goes idle; once relayMaxInjectAttempts is exhausted it is
// deleted and reported via relay.dropped instead of rotting silently.
// Fire-and-forget items (EventID==0 or Lane=="") were never held in the
// first place, so there is nothing to rearm or drop.
//
// This deliberately uses context.Background() rather than deliver()'s
// incoming context: deliver() is reached from waitForPane's own per-pane
// wait context, and re-arming calls arm(), which cancels that same context
// as "the previous wait" -- reusing it here would cancel the retry before
// it starts.
func (manager *relayBusyManager) retryOrDrop(item relayHeld) {
	manager.retryOrDropWithin(item, relayMaxInjectAttempts)
}

func (manager *relayBusyManager) retryOrDropWithin(item relayHeld, maxAttempts int) {
	if item.EventID == 0 || item.Lane == "" {
		return
	}
	ctx := context.Background()
	if store := manager.client.relayStore(); store != nil {
		_, _ = store.DeleteRelayHeld(ctx, item.EventID)
	}
	item.Attempts++
	if item.Attempts >= maxAttempts {
		manager.emit("relay.dropped", relayDroppedPayload{JobID: item.JobID, Pane: item.Pane, Lane: item.Lane, OriginalEventID: item.EventID, Reason: "inject_failed_max_attempts"})
		return
	}
	item.HeldSince = manager.client.relayNow()
	manager.hold(ctx, item, "retry")
}

// inject runs one relay inject and returns its three-way result. The legacy
// bool seam maps to delivered/retryable only.
func (manager *relayBusyManager) inject(ctx context.Context, group []relayHeld, text string) relayInjectResult {
	pane := group[0].Pane
	if inject := manager.client.relayInject; inject != nil {
		if inject(ctx, pane, text) {
			return relayInjectResult{Outcome: relayInjectDelivered}
		}
		return relayInjectResult{Outcome: relayInjectRetryable}
	}
	verdict := manager.client.relayInjectVerdict
	if verdict == nil {
		verdict = defaultHubRelayInjectVerdict
	}
	var members []string
	if len(group) > 1 {
		for _, item := range group {
			members = append(members, item.Text)
		}
	}
	return verdict(ctx, pane, text, members)
}

// stopWithoutRetry ends a held item whose message may already be in the pane
// (#547): re-injecting it could duplicate it, so the row is removed without a
// rearm. relay.dropped clears the hub's held projection; the hub's durable
// row stays undelivered, and a later hub replay meets devinRelayInject's
// presend check before anything is typed.
func (manager *relayBusyManager) stopWithoutRetry(item relayHeld) {
	if item.EventID == 0 || item.Lane == "" {
		return
	}
	if store := manager.client.relayStore(); store != nil {
		_, _ = store.DeleteRelayHeld(context.Background(), item.EventID)
	}
	manager.emit("relay.dropped", relayDroppedPayload{JobID: item.JobID, Pane: item.Pane, Lane: item.Lane, OriginalEventID: item.EventID, Reason: "maybe_in_pane"})
}

// relayAckReason keeps a reason inside the hub's relay ack bounds: one line,
// at most 240 bytes.
func relayAckReason(reason string) string {
	reason = strings.Join(strings.Fields(reason), " ")
	if len(reason) <= 240 {
		return reason
	}
	cut := 240
	for cut > 0 && !utf8.RuneStart(reason[cut]) {
		cut--
	}
	return reason[:cut]
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
	manager.deliver(parent, manager.filterDeliverableHeld(parent, items), expired)
}

// filterDeliverableHeld is the fail-closed lease gate between a finished wait
// and the inject. A held row is a lease on (lane, pane, occupant-at-hold) —
// all three membership signals are re-checked here because each can break
// while the row sits:
//   - the lane was since observed on a different pane (lane_rerouted);
//   - herdr reports no agent on the pane at all (pane_occupant_gone);
//   - the occupant is not the instance the lease names (pane_occupant_changed),
//     or a pre-lease row cannot even prove membership from the agent's
//     registered name (lease_unverifiable).
//
// Expiry is the explicit default disposition — never silent reinjection, never
// rebinding to the new pane: the row is deleted and reported on the existing
// relay.dropped channel, while the durable handoffkeep record stays
// undelivered so hub replay can still reach the lane's current route. A pane
// whose occupant cannot be read is not stale evidence, so an unverifiable
// probe (dead socket, malformed output, missing runner) goes through
// retryOrDrop's bounded rearm instead of expiring the row on a transient
// failure.
func (manager *relayBusyManager) filterDeliverableHeld(parent context.Context, items []relayHeld) []relayHeld {
	var keep []relayHeld
	var survivors []relayHeld
	for _, item := range items {
		if manager.laneRerouted(item) {
			manager.expireHeldLease(parent, item, "lane_rerouted")
			continue
		}
		survivors = append(survivors, item)
	}
	if len(survivors) == 0 {
		return keep
	}
	pane := survivors[0].Pane
	var occupant relayOccupant
	occupantKnown, unverifiable := false, false
	runner := manager.client.relayRunner()
	if runner == nil {
		unverifiable = true
	} else {
		getContext, cancel := context.WithTimeout(parent, manager.client.relayInjectTimeout())
		output, err := runner(getContext, "agent", "get", pane)
		cancel()
		switch {
		case err == nil:
			parsed := false
			occupant, parsed = parseRelayAgentOccupant(output)
			// A get that answers for a different pane than asked is not
			// evidence about this one — count it as unreadable, not absent.
			if parsed && (occupant.Pane == "" || occupant.Pane == pane) {
				occupantKnown = true
			} else {
				unverifiable = true
			}
		case relayAgentNotFound(output):
			// Definitive absence: the pane no longer hosts any agent.
		default:
			unverifiable = true
		}
	}
	for _, item := range survivors {
		if unverifiable {
			manager.retryOrDrop(item)
			continue
		}
		if stale, reason := relayHeldLeaseStale(item, occupant, occupantKnown); stale {
			manager.expireHeldLease(parent, item, reason)
			continue
		}
		keep = append(keep, item)
	}
	return keep
}

// laneRerouted reports whether the lane a held row is bound to has since been
// observed on a different pane. It is the shared membership check both the
// release filter and the deliver-time recheck use, so a reroute observed
// between the two cannot slip an item to its stale pane.
func (manager *relayBusyManager) laneRerouted(item relayHeld) bool {
	observed, known := manager.observedLanePane(item.Lane)
	return known && observed != item.Pane
}

// expireHeldLease ends a stale lease: the local row is deleted and the drop is
// reported so the hub projection and operator feed show the row leaving held
// for a named reason. The durable event is deliberately untouched — expiring
// the lease is not delivering it, and replay still owes it to the lane.
func (manager *relayBusyManager) expireHeldLease(parent context.Context, item relayHeld, reason string) {
	if store := manager.client.relayStore(); store != nil {
		_, _ = store.DeleteRelayHeld(parent, item.EventID)
	}
	manager.emit("relay.dropped", relayDroppedPayload{JobID: item.JobID, Pane: item.Pane, Lane: item.Lane, OriginalEventID: item.EventID, Reason: reason})
}

// relayBatchText composes the injectable text for one group. #687: every
// member's row-derived nonce rides at the head of its text, so pane-side
// proof never depends on matching a rendered body -- the #683 R4-1 class
// of failure (a hard wrap inside a word like changed_at) cannot touch the
// bracketed token.
func relayBatchText(items []relayHeld, expired bool, now time.Time) string {
	if len(items) == 1 {
		text := relayNonce(items[0]) + " " + items[0].Text
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
		builder.WriteString(relayNonce(item))
		builder.WriteString(" ")
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
		// The route observation can move between the release filter and this
		// inject — a "now" delivery skips the filter entirely. Re-check each
		// item's lane immediately before prompting so a reroute observed in
		// the gap still fails closed instead of reaching the stale pane.
		var live []relayHeld
		for _, item := range group {
			if manager.laneRerouted(item) {
				manager.expireHeldLease(parent, item, "lane_rerouted")
				continue
			}
			live = append(live, item)
		}
		if len(live) == 0 {
			continue
		}
		group = live
		text := relayBatchText(group, expired, now)
		ctx, cancel := context.WithTimeout(parent, manager.client.relayInjectTimeout())
		result := manager.inject(ctx, group, text)
		cancel()
		switch result.Outcome {
		case relayInjectRetryable:
			maxAttempts := relayMaxInjectAttempts
			if strings.EqualFold(result.Harness, "devin") {
				maxAttempts = relayDevinMaxInjectAttempts
			}
			for _, item := range group {
				manager.emit("relay.unconfirmed", relayAckPayload{JobID: item.JobID, Pane: item.Pane, Reason: relayAckReason(result.Evidence), OriginalEventID: item.EventID})
				manager.retryOrDropWithin(item, maxAttempts)
			}
			continue
		case relayInjectMaybeInPane:
			for _, item := range group {
				manager.emit("relay.unconfirmed", relayAckPayload{JobID: item.JobID, Pane: item.Pane, Reason: relayAckReason("maybe_in_pane " + result.Evidence), OriginalEventID: item.EventID})
				manager.stopWithoutRetry(item)
			}
			continue
		}
		// delivered, or queued: devin accepted the message into its queue
		// and submits it when its turn ends (verified live, #547). Neither is
		// retried; the reason says which.
		reason := result.Evidence
		if result.Outcome == relayInjectQueued {
			reason = "queued " + reason
		}
		// #687: a batch is delivered per member, on each member's own nonce.
		// A verdict that names no proven nonces (a fixture stub, or the
		// no-submission-evidence carve-out) applies to the whole group; a
		// member whose own nonce was not seen is unconfirmed, not delivered.
		proven := func(item relayHeld) bool {
			return result.Proven == nil || slices.Contains(result.Proven, relayNonce(item))
		}
		var landed []relayHeld
		for _, item := range group {
			if !proven(item) {
				manager.emit("relay.unconfirmed", relayAckPayload{JobID: item.JobID, Pane: item.Pane, Reason: relayAckReason("maybe_in_pane nonce_missing " + result.Evidence), OriginalEventID: item.EventID})
				manager.stopWithoutRetry(item)
				continue
			}
			landed = append(landed, item)
		}
		if len(landed) > 1 {
			ids := make([]int64, 0, len(landed))
			for _, item := range landed {
				ids = append(ids, item.EventID)
			}
			manager.emit("relay.batched", relayBatchedPayload{Pane: landed[0].Pane, Lane: landed[0].Lane, EventIDs: ids})
		}
		for _, item := range landed {
			if store := manager.client.relayStore(); store != nil && item.EventID != 0 {
				_, _ = store.DeleteRelayHeld(parent, item.EventID)
			}
			// final_text is the row's own text, not the nonce-prefixed
			// batch blob that was typed: #687 delivers per member, and the
			// row text is what the 24KiB wire bound was validated against.
			released := relayReleasedPayload{JobID: item.JobID, Pane: item.Pane, Lane: item.Lane, FinalText: item.Text, Edited: item.Edited, OriginalEventID: item.EventID}
			manager.emit("relay.released", released)
			manager.emit("relay.delivered", relayAckPayload{JobID: item.JobID, Pane: item.Pane, Reason: relayAckReason(reason), FinalText: item.Text, Edited: item.Edited, OriginalEventID: item.EventID})
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
