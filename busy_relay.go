package panewire

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
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

// relayDeliverDirective is the delivery policy the hub gives a worker
// directive. It is "now" except for a blocked pane, whose approval prompt the
// text could answer (R27).
const relayDeliverDirective = "directive"

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
	if value == relayDeliverDirective {
		// A worker directive (hk 3276): typed at once while the pane is idle,
		// done or working, and held while it is blocked on a prompt. A harness
		// that cannot prove a directive typed into a working pane also holds
		// it while working (hk 3370; relayHarnessAdapter.workingUnprovable).
		return relayDeliveryPolicy{Name: relayDeliverDirective, MaxWait: defaultRelayMaxWait}, true
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
	session := relaySessionFromContext(ctx, "")
	cmd := exec.CommandContext(ctx, herdrExecutable(), args...)
	cmd.Env = hubSpawnEnvironment(os.Environ(), session)
	return cmd.CombinedOutput()
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
	// Harness is the pane's harness at the drop, when the node could read it.
	// The hub's Sentry fingerprint names it; an older hub ignores the field.
	Harness string `json:"harness,omitempty"`
}

// relayBusyManager serializes each pane, so a returned wait can batch every
// item that arrived while it was busy.  It never polls agent get.
type relayBusyManager struct {
	client *HubClient
	mu     sync.Mutex
	held   map[string][]relayHeld
	waits  map[string]context.CancelFunc
	// inflight is the event ids a release is delivering right now. A row stays
	// in the store until its inject and proof are done, so another wake's
	// snapshot would otherwise find it again and type it a second time.
	inflight map[int64]struct{}
	// lanePane is the newest route observation each inject directive carries:
	// the hub re-resolves lanes.json for every relay, so the (lane, pane) an
	// inject names is authoritative for its arrival instant. A held row whose
	// lane is later observed on a different pane is stale membership evidence
	// even while the old pane's occupant is unchanged.
	lanePane map[string]string
	// dedupeInFlight holds one (lane, event_id) claim per offer() currently
	// between arrival and its resting state — the window the relay_held row
	// or the relay_delivered record has not covered yet. The channel closes
	// on release, so a concurrent arrival waits for the winner's verdict
	// instead of guessing whether it is a duplicate: suppressing a copy
	// whose original then dies mid-flight (reroute, failed inject) is a
	// lost delivery. #725's counters are the shadow/apply operator surface.
	dedupeInFlight map[string]chan struct{}
	// cmdHoldUntil is the deadline a working cmd row's hold was moved to (hk
	// 3411, keepWorkingCmdHold), by event id. arm waits at least until it, so
	// a store that rejects the extension write cannot make the next wait end
	// at once. Entries are dropped once they pass.
	cmdHoldUntil map[int64]time.Time
	// cmdScreens is the screen each working cmd pane showed at the re-check that
	// first saw it (hk 3517, cmdStaleWorkingDigest), by pane. It is in memory
	// only: a node restart starts the frozen window again.
	cmdScreens map[string]relayScreenSample
	// dedupeRestorePending gates the held-vs-delivered reconcile to the one
	// resume() that follows restore(): emits made in restore() land before
	// the relay emitter exists, so the reconcile belongs to the post-hello
	// resume instead.
	dedupeRestorePending bool
	dedupeSuppressed     atomic.Uint64
	dedupeWouldSuppress  atomic.Uint64
	dedupeMismatch       atomic.Uint64
	dedupeUnknown        atomic.Uint64
}

func (client *HubClient) relayBusyManager() *relayBusyManager {
	client.busyRelayMu.Lock()
	defer client.busyRelayMu.Unlock()
	if client.busyRelay == nil {
		client.busyRelay = &relayBusyManager{client: client, held: make(map[string][]relayHeld), waits: make(map[string]context.CancelFunc), inflight: make(map[int64]struct{}), lanePane: make(map[string]string), dedupeInFlight: make(map[string]chan struct{}), cmdHoldUntil: make(map[int64]time.Time), cmdScreens: make(map[string]relayScreenSample)}
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

func (client *HubClient) relayRunnerFor(jobID, pane string) relayCommandRunner {
	if client == nil {
		return defaultRelayCommand
	}
	if client.relayCommand != nil {
		return client.relayCommand
	}
	// relayInject is package-private fixture plumbing. Never make an older
	// fixture with that seam accidentally contact a real herdr installation.
	if client.relayInject != nil {
		return nil
	}
	session := client.herdrSessionFor(jobID, pane)
	return func(ctx context.Context, args ...string) ([]byte, error) {
		cmd := exec.CommandContext(ctx, herdrExecutable(), args...)
		cmd.Env = hubSpawnEnvironment(os.Environ(), session)
		return cmd.CombinedOutput()
	}
}

func (client *HubClient) relayRunner() relayCommandRunner {
	return client.relayRunnerFor("", "")
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
	// hk 4258: a trial lane's wake line goes to its Claude session instead of
	// the pane; a miss falls through to the typed line below.
	if manager.client.deliverWakeAsSessionMessage(parent, message) {
		return
	}
	// Every lane-carrying inject is a fresh route observation — including
	// replays that resolve to a different pane after a re-route. Recording it
	// before the dedupe path returns early is what lets a later release judge
	// the old row stale instead of re-confirming it.
	if message.Lane != "" {
		manager.noteLaneRoute(message.Lane, message.Pane)
	}
	// #725: the durable dedupe gate runs before the queue check. A row that
	// is both delivered and still held is the crash-window leftover the
	// delivered record must win over. An inject with no verifiable identity
	// is delivered and counted, never suppressed under a manufactured key.
	identified := message.EventID > 0 && message.Lane != ""
	mode := relayDedupeModeFromEnv()
	if mode != relayDedupeOff {
		if !identified {
			manager.noteDedupeUnknown(parent, message)
		} else {
			// The claim covers the whole arrival-to-outcome window. A
			// concurrent copy waits for the winner's verdict — by release
			// time the delivered record exists iff the first copy actually
			// landed — so a rerouted or failed original never takes its
			// retry down with it. The record check runs only inside the
			// claim: read before claiming, it could slip between the
			// winner's record write and its release.
			for {
				done, owned := manager.claimDedupeInFlight(message.Lane, message.EventID)
				if owned {
					defer manager.releaseDedupeInFlight(message.Lane, message.EventID)
					break
				}
				select {
				case <-done:
				case <-parent.Done():
					return
				}
			}
			if manager.relayDeliveredIdentity(parent, message, mode) {
				return
			}
		}
	}
	if identified {
		if existing, found, err := manager.client.relayHeldByKey(parent, message.Lane, message.EventID); err == nil && found {
			if mode != relayDedupeOff && message.Pane != "" && existing.Pane != relayCancelledPane && existing.Pane != message.Pane {
				// A resend bound to a different pane means the lane's
				// route moved after this row was queued — the
				// noteLaneRoute above already recorded it. Folding this
				// copy into the stale row would strand the event on a
				// pane the lane no longer uses (the row would expire as
				// lane_rerouted at its release anyway and replay then
				// has only the 30-minute freshness window), so the
				// stale lease is retired now and this copy delivers
				// itself through the normal path: probe, hold with a
				// fresh lease on the current pane, or land immediately.
				manager.dedupeMismatch.Add(1)
				manager.journalDedupe(parent, "mismatch", message)
				manager.client.warnMessage(fmt.Sprintf("relay dedupe mismatch: lane=%s event_id=%d resend targets pane=%s but the queued row sits on %s; retiring the stale lease and delivering this copy", message.Lane, message.EventID, message.Pane, existing.Pane))
				manager.expireHeldLease(parent, existing, "lane_rerouted", "")
				manager.mu.Lock()
				if queued := manager.held[existing.Pane]; queued != nil {
					kept := queued[:0]
					for _, other := range queued {
						if other.EventID != existing.EventID {
							kept = append(kept, other)
						}
					}
					manager.held[existing.Pane] = kept
				}
				manager.mu.Unlock()
				manager.arm(parent, existing.Pane)
			} else {
				// The queued row is the same local delivery, not
				// another prompt — but a resend disagreeing with
				// the row's payload is a recorded mismatch, the
				// same fail-open rule the delivered record
				// applies. The queued row's text wins over a replay
				// carrying a different payload.
				if mode != relayDedupeOff && existing.Text != message.Text {
					manager.dedupeMismatch.Add(1)
					manager.journalDedupe(parent, "mismatch", message)
					manager.client.warnMessage(fmt.Sprintf("relay dedupe mismatch: lane=%s event_id=%d pane=%s resend payload disagrees with the queued row; keeping the queued row", message.Lane, message.EventID, message.Pane))
				}
				manager.recoverExisting(parent, existing)
				return
			}
		}
	}
	policy, valid := parseRelayDeliveryPolicy(message.DeliverPolicy)
	if !valid {
		policy = relayDeliveryPolicy{Name: "idle", MaxWait: defaultRelayMaxWait}
	}
	if policy.Name == "now" || message.EventID == 0 || message.Lane == "" {
		manager.deliver(parent, []relayHeld{{Pane: message.Pane, Lane: message.Lane, EventID: message.EventID, JobID: message.JobID, Text: message.Text, HeldSince: manager.client.nowUTC(), DeliverPolicy: policy.Name, RecvSeq: message.RecvSeq}}, false)
		return
	}
	runner := manager.client.relayRunnerFor(message.JobID, message.Pane)
	if runner == nil {
		// Fixture-only clients without the explicit command seam are fail-open.
		manager.deliver(parent, []relayHeld{{Pane: message.Pane, Lane: message.Lane, EventID: message.EventID, JobID: message.JobID, Text: message.Text, HeldSince: manager.client.nowUTC(), DeliverPolicy: policy.Name, RecvSeq: message.RecvSeq}}, false)
		return
	}
	getContext, cancel := context.WithTimeout(parent, manager.client.relayInjectTimeout())
	output, err := runner(getContext, "agent", "get", message.Pane)
	cancel()
	status, parsed := parseRelayAgentStatus(output)
	// A working pane is typed into only for a directive, and never for a harness
	// that cannot prove text typed into a working pane (hk 3370, hk 3384): every
	// other policy holds it.
	if err != nil || !parsed || status == "unknown" || status == "idle" || status == "done" || (policy.Name == relayDeliverDirective && status != "blocked" && !relayPaneWorkingUnprovable(status, parseRelayAgentHarness(output))) {
		manager.deliver(parent, []relayHeld{{Pane: message.Pane, Lane: message.Lane, EventID: message.EventID, JobID: message.JobID, Text: message.Text, HeldSince: manager.client.nowUTC(), DeliverPolicy: policy.Name, RecvSeq: message.RecvSeq}}, false)
		return
	}
	held := relayHeld{Pane: message.Pane, Lane: message.Lane, EventID: message.EventID, JobID: message.JobID, Text: message.Text, HeldSince: manager.client.nowUTC(), DeliverPolicy: policy.Name, MaxWait: policy.MaxWait, RecvSeq: message.RecvSeq, fresh: true}
	if ceiling := manager.client.relayCmdHoldMax(); relayPaneWorkingUnprovable(status, parseRelayAgentHarness(output)) && ceiling < held.MaxWait {
		// hk 3411: a working cmd row is held at most relay.cmd_hold_max, so
		// its first wait ends no later than that ceiling.
		held.MaxWait = ceiling
	}
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
	// The insert, the snapshot and the in-memory update are one step under the
	// manager lock, the same lock a release takes around its own snapshot: a
	// release can never interleave between this row's snapshot and its
	// registration, and so cannot have the row it delivered restored here.
	manager.mu.Lock()
	inserted, err := store.InsertRelayHeld(parent, item)
	if err != nil {
		manager.mu.Unlock()
		manager.deliver(parent, []relayHeld{item}, false)
		return
	}
	rows, rowsErr := store.RelayHeldForPane(parent, item.Pane)
	if rowsErr == nil {
		if inserted {
			for index := range rows {
				if rows[index].EventID == item.EventID {
					// SQLite stores milliseconds for restart recovery. Keep the precise
					// receipt instant in this live process so max_wait=N reaches herdr as
					// exactly N*1000 milliseconds on its first arm.
					rows[index].HeldSince = item.HeldSince
					rows[index].fresh = true
				}
			}
		}
		manager.held[item.Pane] = rows
	}
	manager.mu.Unlock()
	if !inserted {
		// A hub replay after a node restart is the same local hold, not another
		// prompt. Re-report it so a restarted hub rebuilds its projection.
		if rowsErr == nil {
			manager.arm(parent, item.Pane)
		}
		manager.reportHeld(item, reason)
		return
	}
	if rowsErr != nil {
		return
	}
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
func (manager *relayBusyManager) retryOrDrop(item relayHeld, harness string) {
	manager.retryOrDropWithin(item, relayMaxInjectAttempts, harness)
}

func (manager *relayBusyManager) retryOrDropWithin(item relayHeld, maxAttempts int, harness string) {
	if item.EventID == 0 || item.Lane == "" {
		return
	}
	ctx := context.Background()
	if store := manager.client.relayStore(); store != nil {
		_, _ = store.DeleteRelayHeld(ctx, item.EventID)
	}
	item.Attempts++
	if item.Attempts >= maxAttempts {
		manager.emit("relay.dropped", relayDroppedPayload{JobID: item.JobID, Pane: item.Pane, Lane: item.Lane, OriginalEventID: item.EventID, Reason: "inject_failed_max_attempts", Harness: harness})
		return
	}
	item.HeldSince = manager.client.nowUTC()
	manager.hold(ctx, item, "retry")
}

// inject runs one relay inject and returns its three-way result. The legacy
// bool seam maps to delivered/retryable only.
func (manager *relayBusyManager) inject(ctx context.Context, group []relayHeld, text string) relayInjectResult {
	if len(group) == 0 {
		return relayInjectResult{Outcome: relayInjectRetryable}
	}
	pane := group[0].Pane
	if pane == relayCancelledPane {
		for _, item := range group {
			manager.expireHeldLease(ctx, item, "cancelled_pane", "")
		}
		return relayInjectResult{Outcome: relayInjectRetryable}
	}
	if _, ok := relaySessionFrom(ctx); !ok && manager.client != nil {
		session := manager.client.herdrSessionFor(group[0].JobID, pane)
		ctx = withRelaySession(ctx, session)
	}
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
// row stays undelivered. The drop reports an actionable reason.
func (manager *relayBusyManager) stopWithoutRetry(item relayHeld, harness, evidence string) {
	if item.EventID == 0 || item.Lane == "" {
		return
	}
	if store := manager.client.relayStore(); store != nil {
		_, _ = store.DeleteRelayHeld(context.Background(), item.EventID)
	}
	reason := relayDroppedCheckPane
	if strings.HasPrefix(evidence, "presend:unproven:read_failed") {
		reason = relayDroppedPaneReadFailed
	}
	manager.emit("relay.dropped", relayDroppedPayload{JobID: item.JobID, Pane: item.Pane, Lane: item.Lane, OriginalEventID: item.EventID, Reason: reason, Harness: harness})
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
		deadline := manager.heldDeadlineLocked(items[0])
		for _, item := range items[1:] {
			if candidate := manager.heldDeadlineLocked(item); candidate.Before(deadline) {
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
	directiveHeld := false
	for _, item := range items {
		directiveHeld = directiveHeld || item.DeliverPolicy == relayDeliverDirective
	}
	ctx, cancel := context.WithCancel(parent)
	manager.waits[pane] = cancel
	manager.mu.Unlock()
	go manager.waitForPane(ctx, pane, waitFor, directiveHeld)
}

// heldDeadlineLocked is when item's wait runs out: its policy deadline, or
// the later deadline its cmd hold was moved to. The caller holds manager.mu.
func (manager *relayBusyManager) heldDeadlineLocked(item relayHeld) time.Time {
	deadline := item.HeldSince.Add(item.MaxWait)
	if until, ok := manager.cmdHoldUntil[item.EventID]; ok && until.After(deadline) {
		deadline = until
	}
	return deadline
}

func (manager *relayBusyManager) waitForPane(ctx context.Context, pane string, waitFor time.Duration, directiveHeld bool) {
	runner := manager.client.relayRunnerFor("", pane)
	if runner == nil {
		return
	}
	args := []string{"agent", "wait", pane, "--until", "idle", "--until", "done"}
	if directiveHeld && !manager.paneWorkingUnprovable(ctx, runner, pane) {
		// hk 3276: a directive is held only while the pane is blocked. Once
		// the prompt is answered the pane turns working, which is no reason
		// to keep it waiting for the end of the turn. A harness that cannot
		// prove a directive typed into a working pane (hk 3370) keeps waiting
		// for idle or done: a wait that ended on working would end at once.
		args = append(args, "--until", "working")
	}
	output, err := runner(ctx, append(args, "--timeout", strconv.FormatInt(waitFor.Milliseconds(), 10))...)
	if errors.Is(err, context.Canceled) || ctx.Err() != nil {
		return
	}
	status, parsed := parseRelayAgentStatus(output)
	// Only the documented wait timeout is an expiry. Other command failures
	// fail open, but are not mislabeled as a policy deadline.
	timedOut := err != nil && relayWaitTimedOut(output)
	// A malformed wait reply cannot safely leave an item held forever. Its
	// status lookup was already conservative; delivery here is fail-open.
	if err == nil && !parsed {
		timedOut = false
	}
	if status == "working" {
		manager.releaseWorking(ctx, pane, timedOut)
		return
	}
	manager.release(ctx, pane, timedOut)
}

// paneWorkingUnprovable reports whether the harness occupying pane is one whose
// held rows are not typed while it works (hk 3370, hk 3384). An unknown harness
// is not: it keeps the wait a held directive has always had. An unreadable pane
// is treated as one (hk 3384): a wait that also ended on working would wake a
// release that, for a cmd pane, must hold everything again, and the next
// unreadable probe would wake it again at once.
func (manager *relayBusyManager) paneWorkingUnprovable(ctx context.Context, runner relayCommandRunner, pane string) bool {
	getContext, cancel := context.WithTimeout(ctx, manager.client.relayInjectTimeout())
	defer cancel()
	output, err := runner(getContext, "agent", "get", pane)
	return err != nil || relayHarnessWorkingUnprovable(parseRelayAgentHarness(output))
}

// releaseWorking is the wake-up when a blocked pane turned working: only the
// directives held on it are delivered. A row on the idle policy keeps waiting
// for the pane to finish its turn. It runs on the wait's own context, which arm
// cancels as "the previous wait", so every re-arm here starts from a stable
// context (as retryOrDrop does): a wait derived from the canceled one would
// exit at once and leave the idle rows held with no waiter.
func (manager *relayBusyManager) releaseWorking(parent context.Context, pane string, expired bool) {
	store := manager.client.relayStore()
	if store == nil {
		return
	}
	// The store snapshot and the removal of the directives from the in-memory
	// set happen under one lock, and only the delivered rows are removed from
	// the current set: a row an offer holds meanwhile stays held and is armed.
	manager.mu.Lock()
	items, err := manager.heldSnapshot(parent, store, pane)
	if err != nil {
		manager.mu.Unlock()
		// A transient store error must not strand the held rows with no
		// waiter: this wait is over, so wait again shortly.
		time.AfterFunc(relayWorkingRetryDelay, func() { manager.arm(context.Background(), pane) })
		return
	}
	var directives []relayHeld
	delivering := map[int64]bool{}
	others := false
	for _, item := range items {
		if item.DeliverPolicy == relayDeliverDirective {
			directives = append(directives, item)
			delivering[item.EventID] = true
		} else {
			others = true
		}
	}
	if len(items) == 0 {
		manager.mu.Unlock()
		return
	}
	if !others || expired {
		// Everything held is released, and exactly the snapshot read above: a
		// second snapshot after the unlock could pick up an idle row an offer
		// holds in the gap and deliver it to a working pane.
		delete(manager.waits, pane)
		delete(manager.held, pane)
		manager.markInflight(items)
		manager.mu.Unlock()
		if relayWorkingReleaseGap != nil {
			relayWorkingReleaseGap()
		}
		manager.deliverReleased(parent, pane, items, expired)
		manager.finishInflight(items)
		manager.reconcileHeld(parent, pane)
		manager.arm(context.Background(), pane)
		return
	}
	if len(directives) == 0 {
		// An unexpected working wake with no directive: keep everything held.
		manager.mu.Unlock()
		manager.arm(context.Background(), pane)
		return
	}
	var remaining []relayHeld
	for _, item := range manager.held[pane] {
		if !delivering[item.EventID] {
			remaining = append(remaining, item)
		}
	}
	manager.held[pane] = remaining
	// Detach this wait, as release does: a row an offer holds from here on arms
	// a wait of its own, and arm cancels the wait registered for the pane. If
	// that were the wait this delivery runs on, its inject and its one retry
	// would start from a canceled context and be reported unconfirmed.
	delete(manager.waits, pane)
	manager.markInflight(directives)
	manager.mu.Unlock()
	if relayWorkingReleaseGap != nil {
		relayWorkingReleaseGap()
	}
	manager.deliverReleased(parent, pane, directives, false)
	manager.finishInflight(directives)
	manager.reconcileHeld(parent, pane)
	manager.arm(context.Background(), pane)
}

// relayWorkingReleaseGap, when set, runs between the snapshot a working wake
// decided on and the delivery of it. Tests use it to hold a row in that gap.
var relayWorkingReleaseGap func()

// relayWorkingRetryDelay spaces the re-wait after a held snapshot could not be
// read during a working wake.
var relayWorkingRetryDelay = time.Second

// reconcileHeld makes the in-memory held set match the store for a pane, so a
// row that was delivered (and deleted) cannot linger in memory and keep the next
// wait looking for a state that is already true. In-memory timing (the precise
// receipt instant and the fresh flag) is kept for rows that are still held. An
// unreadable store leaves the set as it is.
func (manager *relayBusyManager) reconcileHeld(parent context.Context, pane string) {
	store := manager.client.relayStore()
	if store == nil {
		return
	}
	manager.mu.Lock()
	defer manager.mu.Unlock()
	rows, err := store.RelayHeldForPane(parent, pane)
	if err != nil {
		return
	}
	current := map[int64]relayHeld{}
	for _, item := range manager.held[pane] {
		current[item.EventID] = item
	}
	for index := range rows {
		if live, ok := current[rows[index].EventID]; ok {
			rows[index].HeldSince, rows[index].fresh = live.HeldSince, live.fresh
		}
	}
	if len(rows) == 0 {
		delete(manager.held, pane)
		return
	}
	manager.held[pane] = rows
}

// heldSnapshot reads the rows held for a pane from the store, leaving out the
// ones a release is delivering. The caller holds manager.mu.
func (manager *relayBusyManager) heldSnapshot(parent context.Context, store *Store, pane string) ([]relayHeld, error) {
	items, err := store.RelayHeldForPane(parent, pane)
	if err != nil || len(manager.inflight) == 0 {
		return items, err
	}
	kept := items[:0:0]
	for _, item := range items {
		if _, busy := manager.inflight[item.EventID]; !busy {
			kept = append(kept, item)
		}
	}
	return kept, nil
}

// markInflight and finishInflight bracket a delivery. markInflight needs
// manager.mu; finishInflight takes it.
func (manager *relayBusyManager) markInflight(items []relayHeld) {
	for _, item := range items {
		manager.inflight[item.EventID] = struct{}{}
	}
}

func (manager *relayBusyManager) finishInflight(items []relayHeld) {
	manager.mu.Lock()
	for _, item := range items {
		delete(manager.inflight, item.EventID)
	}
	manager.mu.Unlock()
}

func (manager *relayBusyManager) release(parent context.Context, pane string, expired bool) {
	store := manager.client.relayStore()
	if store == nil {
		return
	}
	manager.mu.Lock()
	items, err := manager.heldSnapshot(parent, store, pane)
	if err != nil || len(items) == 0 {
		manager.mu.Unlock()
		return
	}
	delete(manager.waits, pane)
	delete(manager.held, pane)
	manager.markInflight(items)
	manager.mu.Unlock()
	// The state transition is serialized above, but prompt itself must not hold
	// the manager-wide map lock: another pane's read-loop work stays independent.
	manager.deliverReleased(parent, pane, items, expired)
	manager.finishInflight(items)
	// A retry inside deliver re-holds a row, and a cmd release can leave rows
	// held in the store (hk 3384); a wake that came while they were in flight
	// skipped them, so wait again for whatever is held now.
	manager.reconcileHeld(parent, pane)
	manager.mu.Lock()
	rearm := len(manager.held[pane]) > 0
	manager.mu.Unlock()
	if rearm {
		manager.arm(context.Background(), pane)
	}
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
//
// The same read names the harness, which the caller needs to decide how the
// kept rows are typed (deliverReleased). A harness that cannot prove text typed
// into a working pane (cmd) gets nothing typed while that pane is working, on
// any policy (hk 3370, hk 3384). Before the wait runs out the row simply stays
// held; past it the row keeps waiting while its job is live (hk 3411,
// keepWorkingCmdHold) and is dropped and told as working_wait_expired only
// past the relay.cmd_hold_max ceiling. The exception is a pane proven stuck at
// working from herdr's own reads (hk 3517, cmdStaleWorkingDigest): its rows are
// kept for the idle path, marked with the stale screen their proof starts from.
func (manager *relayBusyManager) filterDeliverableHeld(parent context.Context, items []relayHeld, expired bool) (keep []relayHeld, harness string) {
	var survivors []relayHeld
	for _, item := range items {
		if manager.laneRerouted(item) {
			manager.expireHeldLease(parent, item, "lane_rerouted", "")
			continue
		}
		survivors = append(survivors, item)
	}
	if len(survivors) == 0 {
		return keep, ""
	}
	pane := survivors[0].Pane
	var occupant relayOccupant
	occupantKnown, unverifiable := false, false
	paneWorkingUnprovable := false
	runner := manager.client.relayRunnerFor(survivors[0].JobID, pane)
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
				status, _ := parseRelayAgentStatus(output)
				harness = parseRelayAgentHarness(output)
				paneWorkingUnprovable = relayPaneWorkingUnprovable(status, harness)
			} else {
				unverifiable = true
			}
		case relayAgentNotFound(output):
			// Definitive absence: the pane no longer hosts any agent.
		default:
			unverifiable = true
		}
	}
	if !unverifiable && !paneWorkingUnprovable {
		// No longer working (or gone): there is no frozen window to measure.
		manager.forgetCmdScreen(pane)
	}
	staleDigest, staleChecked := "", false
	for _, item := range survivors {
		if unverifiable {
			manager.retryOrDrop(item, harness)
			continue
		}
		if stale, reason := relayHeldLeaseStale(item, occupant, occupantKnown); stale {
			manager.expireHeldLease(parent, item, reason, harness)
			continue
		}
		if relayHarnessWorkingUnprovable(harness) && relayLaneJobEnded(manager.client.jobsInboxRoot, item.Lane) {
			// hk 3411: a cmd row is held for its job; once the job has ended it
			// is dropped, whether the pane is still working or turned idle.
			manager.expireHeldLease(parent, item, relayDroppedJobEnded, harness)
			continue
		}
		if paneWorkingUnprovable {
			if !expired {
				// The pane turned working again before this release read it.
				// The row is still in the store; the release re-arms its wait.
				continue
			}
			if !staleChecked {
				// hk 3517: one screen read per re-check, shared by every row held
				// on the pane.
				staleDigest, staleChecked = manager.cmdStaleWorkingDigest(parent, item, occupant.key()), true
			}
			if staleDigest != "" {
				// Proven stuck at working: the pane takes the idle path, as if
				// herdr had reported idle.
				item.staleScreen = staleDigest
				keep = append(keep, item)
				continue
			}
			// hk 3370, hk 3384: the wait ran out with the pane still working,
			// and this harness cannot prove text typed into a working pane —
			// whatever the row's policy. Typed anyway it sits unsent in the
			// composer (or, for a directive, is typed twice), so nothing is
			// typed. hk 3411: while its job is live the row has no working-wait
			// expiry and keeps waiting for idle or done.
			if manager.keepWorkingCmdHold(parent, item) {
				continue
			}
			// Past relay.cmd_hold_max the hub tells the design lane once, and
			// the durable row stays undelivered for a replay.
			manager.emit("relay.unconfirmed", relayAckPayload{JobID: item.JobID, Pane: item.Pane, Reason: relayDroppedWorkingExpired, OriginalEventID: item.EventID})
			manager.expireHeldLease(parent, item, relayDroppedWorkingExpired, harness)
			continue
		}
		keep = append(keep, item)
	}
	return keep, harness
}

// deliverReleased types the rows a release kept. A pane-level harness (cmd)
// gets one row per inject (hk 3384): its return gate reads the composer after
// the text lands and cannot match a batch the composer wraps over several rows,
// so a batch is typed and left unsent. Each row then has its own nonce, presend
// check, gated return and proof. A submitted row turns the pane working, so
// before each later row the pane is read again, and unless it is idle or done
// the rest stay held in the store for the release to re-arm. Every other
// harness keeps the batch.
func (manager *relayBusyManager) deliverReleased(parent context.Context, pane string, items []relayHeld, expired bool) {
	keep, harness := manager.filterDeliverableHeld(parent, items, expired)
	if !harnessPaneTransport(harness) || len(keep) < 2 {
		manager.deliver(parent, keep, expired)
		return
	}
	sort.SliceStable(keep, func(i, j int) bool { return keep[i].RecvSeq < keep[j].RecvSeq })
	for index, item := range keep {
		if index > 0 {
			if !manager.paneIdleOrDone(parent, pane) {
				return
			}
			// The pane left working after the first row, so this row has the
			// ordinary idle proof and nothing of the stale screen applies.
			item.staleScreen = ""
		}
		manager.deliver(parent, []relayHeld{item}, expired)
	}
}

// paneIdleOrDone reads pane's status between two one-row injects. Anything but
// idle or done, an unreadable pane included, stops the run.
func (manager *relayBusyManager) paneIdleOrDone(parent context.Context, pane string) bool {
	runner := manager.client.relayRunnerFor("", pane)
	if runner == nil {
		return false
	}
	getContext, cancel := context.WithTimeout(parent, manager.client.relayInjectTimeout())
	defer cancel()
	output, err := runner(getContext, "agent", "get", pane)
	status, parsed := parseRelayAgentStatus(output)
	return err == nil && parsed && (status == "idle" || status == "done")
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
func (manager *relayBusyManager) expireHeldLease(parent context.Context, item relayHeld, reason, harness string) {
	if store := manager.client.relayStore(); store != nil {
		_, _ = store.DeleteRelayHeld(parent, item.EventID)
	}
	manager.emit("relay.dropped", relayDroppedPayload{JobID: item.JobID, Pane: item.Pane, Lane: item.Lane, OriginalEventID: item.EventID, Reason: reason, Harness: harness})
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
		if relayGroupImmediate(last) != relayGroupImmediate([]relayHeld{item}) {
			// A directive has its own retry and escalation disposition, so it
			// never shares a send with an ordinary held row.
			groups = append(groups, []relayHeld{item})
			continue
		}
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
	// A stop waits for the injection that is typing into the pane, so the
	// delivered record and the hub's acknowledgement are not left half written.
	endWork, _ := manager.client.beginWork(false)
	defer endWork()
	sort.SliceStable(items, func(i, j int) bool { return items[i].RecvSeq < items[j].RecvSeq })
	now := manager.client.nowUTC()
	for _, group := range relayBatchGroups(items, expired, now) {
		// The route observation can move between the release filter and this
		// inject — a "now" delivery skips the filter entirely. Re-check each
		// item's lane immediately before prompting so a reroute observed in
		// the gap still fails closed instead of reaching the stale pane.
		var live []relayHeld
		for _, item := range group {
			if manager.laneRerouted(item) {
				manager.expireHeldLease(parent, item, "lane_rerouted", "")
				continue
			}
			live = append(live, item)
		}
		if len(live) == 0 {
			continue
		}
		group = live
		text := relayBatchText(group, expired, now)
		session := ""
		if manager.client != nil {
			session = manager.client.herdrSessionFor(group[0].JobID, group[0].Pane)
		}
		ctx, cancel := context.WithTimeout(parent, manager.client.relayInjectTimeout())
		if session != "" {
			ctx = withRelaySession(ctx, session)
		}
		// hk 3517: a row released into a pane proven stale-working is typed
		// once. Its proof starts from the stale screen, and an unproven send is
		// never typed again: the pane may already hold the row.
		staleScreen := group[0].staleScreen
		if staleScreen != "" {
			ctx = withRelayStaleScreen(ctx, staleScreen)
		}
		result := manager.inject(ctx, group, text)
		cancel()
		immediate := relayGroupImmediate(group)
		previousUncertain := false
		if immediate && staleScreen == "" && relayRetriesOnce(result) {
			previousUncertain = (result.Evidence == "prompt_failed" && strings.EqualFold(result.Harness, "devin")) || (result.Outcome == relayInjectMaybeInPane && !strings.HasPrefix(result.Evidence, "presend:"))
			// hk 3276: a worker directive is not held for a quieter pane, so a
			// send that was rejected or never showed up is typed once more
			// right away. The row's nonce is unchanged, and the text already
			// tells the reader a repeat is not a reason to re-run it.
			retryContext, retryCancel := context.WithTimeout(parent, manager.client.relayInjectTimeout())
			if session != "" {
				retryContext = withRelaySession(retryContext, session)
			}
			result = manager.inject(retryContext, group, text)
			retryCancel()
		}
		if staleScreen != "" {
			result.Evidence = relayStaleWorkingEvidence + result.Evidence
		}
		switch result.Outcome {
		case relayInjectRetryable:
			if previousUncertain {
				for _, item := range group {
					manager.emit("relay.unconfirmed", relayAckPayload{JobID: item.JobID, Pane: item.Pane, Reason: relayAckReason("maybe_in_pane previous_attempt_unproven " + result.Evidence), OriginalEventID: item.EventID})
					manager.stopWithoutRetry(item, result.Harness, "previous_attempt_unproven:"+result.Evidence)
				}
				continue
			}
			maxAttempts := relayMaxInjectAttempts
			if adapter, known := relayAdapterFor(result.Harness); known {
				maxAttempts = adapter.injectAttempts()
			}
			if immediate {
				// The one retry is spent: drop now (and escalate through the
				// hub) instead of re-holding the row until the pane is idle.
				maxAttempts = 1
			}
			for _, item := range group {
				manager.emit("relay.unconfirmed", relayAckPayload{JobID: item.JobID, Pane: item.Pane, Reason: relayAckReason(result.Evidence), OriginalEventID: item.EventID})
				manager.retryOrDropWithin(item, maxAttempts, result.Harness)
			}
			continue
		case relayInjectMaybeInPane:
			dropEvidence := result.Evidence
			if previousUncertain {
				dropEvidence = "previous_attempt_unproven:" + dropEvidence
			}
			for _, item := range group {
				manager.emit("relay.unconfirmed", relayAckPayload{JobID: item.JobID, Pane: item.Pane, Reason: relayAckReason("maybe_in_pane " + result.Evidence), OriginalEventID: item.EventID})
				manager.stopWithoutRetry(item, result.Harness, dropEvidence)
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
				manager.stopWithoutRetry(item, result.Harness, result.Evidence)
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
				// #725: the delivered record is written while the held row
				// still exists and before the ack leaves — a crash in this
				// gap must produce a replayed duplicate the dedupe gate can
				// answer, never a lost event or an orphaned held row. A
				// record failure is logged but never stalls delivery.
				if item.Lane != "" {
					if err := store.RecordRelayDelivered(parent, item.Lane, item.EventID, item.Pane, relayPayloadFingerprint(item.Text), now); err != nil {
						manager.client.warnMessage("relay delivery was not recorded for dedupe")
					}
				} else {
					// #989: a delivered item that cannot name its row must not
					// pass silently — this skip is what left panes shown but
					// relay_delivered empty.
					manager.client.warnMessage(fmt.Sprintf("relay delivery was not recorded for dedupe: no lane identity (event_id=%d pane=%s)", item.EventID, item.Pane))
				}
				_, _ = store.DeleteRelayHeld(parent, item.EventID)
			} else if item.EventID == 0 {
				manager.client.warnMessage(fmt.Sprintf("relay delivery was not recorded for dedupe: inject carried no durable event id (lane=%q pane=%s)", item.Lane, item.Pane))
			} else {
				manager.client.warnMessage("relay delivery was not recorded for dedupe: no relay store")
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

// relayGroupImmediate reports whether every row in group is a worker directive
// the hub marked deliver=directive: sent at once unless the pane is blocked.
// An explicit deliver=now keeps its established single send.
func relayGroupImmediate(group []relayHeld) bool {
	for _, item := range group {
		if item.DeliverPolicy != relayDeliverDirective || item.EventID == 0 || item.Lane == "" {
			return false
		}
	}
	return len(group) > 0
}

// relayRetriesOnce reports whether an immediate delivery's first attempt is
// worth one more send: herdr rejected it, or herdr took it and nothing showed
// it landed. A verdict that says the text may already be in the pane for any
// other reason (a composer that holds a foreign draft, a changed harness) is
// final.
func relayRetriesOnce(result relayInjectResult) bool {
	switch result.Outcome {
	case relayInjectRetryable:
		return true
	case relayInjectMaybeInPane:
		// Omp has no composer rule to guard a repeat after a send.
		if strings.EqualFold(result.Harness, "omp") && !strings.HasPrefix(result.Evidence, "presend:") {
			return false
		}
		// "unproven:..." (also behind a "presend:" prefix) is the reads
		// showing nothing: no adapter, an unreadable pane, an empty or
		// unlocated composer. The scope contract is one retry for all of them;
		// the second inject's own presend check still refuses a pending
		// composer or a changed harness. "postsend:...:none" is devin's.
		if strings.Contains(result.Evidence, "unproven:") {
			return true
		}
		// devin's own unreadable-pane verdicts: before the send nothing was
		// typed, after it the retry's presend check guards the composer.
		if result.Evidence == "presend:read_failed" || result.Evidence == "postsend:read_failed" {
			return true
		}
		return strings.HasPrefix(result.Evidence, "postsend:") && strings.HasSuffix(result.Evidence, ":none")
	}
	return false
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
	// #725: a row that is both held and delivered means the process died
	// between the delivery record and the held-row delete. The reconcile
	// itself waits for resume(): restore() runs before the first hello, so
	// a re-acknowledgement emitted here would land in a nil emitter and be
	// silently dropped.
	manager.dedupeRestorePending = true
	manager.mu.Unlock()
}

// reconcileDeliveredHeld runs the restore-time dedupe pass once, from the
// resume() that follows the relay emitter's attachment. A restored row
// whose (lane, event_id) already has a delivered record must not re-arm
// into a second prompt: apply retires the stale lease and re-acknowledges
// the durable row, shadow counts the repeat and keeps the row, and a row
// disagreeing with its record is a mismatch — kept deliverable, never
// silently retired.
func (manager *relayBusyManager) reconcileDeliveredHeld(ctx context.Context) {
	store := manager.client.relayStore()
	if store == nil {
		return
	}
	manager.mu.Lock()
	if !manager.dedupeRestorePending {
		manager.mu.Unlock()
		return
	}
	manager.dedupeRestorePending = false
	var items []relayHeld
	for _, paneItems := range manager.held {
		items = append(items, paneItems...)
	}
	manager.mu.Unlock()
	mode := relayDedupeModeFromEnv()
	if mode == relayDedupeOff || len(items) == 0 {
		return
	}
	var doomed map[int64]struct{}
	for _, item := range items {
		if item.Lane == "" || item.EventID == 0 {
			continue
		}
		previous, found, err := store.RelayDeliveredByKey(ctx, item.Lane, item.EventID)
		if err != nil || !found {
			continue
		}
		message := hubOutboundMessage{Kind: "lane.event", JobID: item.JobID, Pane: item.Pane, Lane: item.Lane, EventID: item.EventID}
		if previous.Pane != item.Pane || previous.PayloadSHA != relayPayloadFingerprint(item.Text) {
			manager.dedupeMismatch.Add(1)
			manager.journalDedupe(ctx, "mismatch", message)
			manager.client.warnMessage(fmt.Sprintf("relay dedupe mismatch on restored held row: lane=%s event_id=%d pane=%s differs from the delivered record; keeping it deliverable", item.Lane, item.EventID, item.Pane))
			continue
		}
		if !mode.applies() {
			manager.dedupeWouldSuppress.Add(1)
			manager.journalDedupe(ctx, "would_suppress_restore", message)
			continue
		}
		manager.dedupeSuppressed.Add(1)
		manager.journalDedupe(ctx, "suppressed_restore", message)
		manager.client.warnMessage(fmt.Sprintf("relay dedupe suppressed restored held row: lane=%s event_id=%d was already delivered", item.Lane, item.EventID))
		_, _ = store.DeleteRelayHeld(ctx, item.EventID)
		manager.emit("relay.released", relayReleasedPayload{JobID: item.JobID, Pane: item.Pane, Lane: item.Lane, FinalText: item.Text, Edited: item.Edited, OriginalEventID: item.EventID})
		manager.emit("relay.delivered", relayAckPayload{JobID: item.JobID, Pane: item.Pane, Reason: "dedupe_suppressed", OriginalEventID: item.EventID})
		if doomed == nil {
			doomed = make(map[int64]struct{})
		}
		doomed[item.EventID] = struct{}{}
	}
	if len(doomed) == 0 {
		return
	}
	manager.mu.Lock()
	for pane, paneItems := range manager.held {
		kept := paneItems[:0]
		for _, item := range paneItems {
			if _, drop := doomed[item.EventID]; !drop {
				kept = append(kept, item)
			}
		}
		manager.held[pane] = kept
	}
	manager.mu.Unlock()
}

func (manager *relayBusyManager) resume(ctx context.Context) {
	// #725: the delivered-record reconcile runs before any restored row is
	// reported or armed — resume() is the first point where the emitter is
	// guaranteed to exist (setRelayEmitter precedes it on the hello path).
	manager.reconcileDeliveredHeld(ctx)
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
