package panewire

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"log/slog"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode/utf8"
)

const (
	defaultIdleWakeSettle      = 60 * time.Second
	idleWakeRouteRetry         = 10 * time.Second
	idleWakeObservationPoll    = 5 * time.Second
	idleWakeNamespaceKey       = "sequence_namespace_v1"
	idleWakeDecisionAssigned   = "assigned"
	idleWakeDecisionCancelled  = "cancelled"
	idleWakeDecisionSuppressed = "suppressed"
)

// HerdrAgentState is the bounded subset of agent.list used by idle-wake.
type HerdrAgentState struct {
	PaneID, WorkspaceID, Label, Status string
	Revision                           int64
	// Authoritative is true for an agent.list snapshot. Only a snapshot may
	// declare that herdr's revision namespace moved backwards; an older event
	// racing a newer snapshot is ignored instead.
	Authoritative bool
}

type idleWakeCandidate struct {
	PaneID, WorkspaceID, Label, Status string
	EventID, OwnerLane, Text, JobID    string
	StateChangeSeq                     uint64
	ChangedAt, DueAt, SettledAt        time.Time
}

type idleWakeRouteRequest struct {
	EventID        string `json:"event_id"`
	Pane           string `json:"pane"`
	WorkspaceID    string `json:"workspace_id,omitempty"`
	Label          string `json:"label,omitempty"`
	State          string `json:"state"`
	ChangedAt      string `json:"changed_at"`
	StateChangeSeq uint64 `json:"state_change_seq"`
}

type hubIdleWakeRouteEvent struct {
	Type     string `json:"type"`
	EventID  string `json:"event_id"`
	Pane     string `json:"pane"`
	Eligible bool   `json:"eligible"`
	Lane     string `json:"lane,omitempty"`
	Text     string `json:"text,omitempty"`
	JobID    string `json:"job_id,omitempty"`
	Reason   string `json:"reason,omitempty"`
}

type idleWakeManager struct {
	mu          sync.Mutex
	store       *Store
	inboxRoot   string
	settle      time.Duration
	now         func() time.Time
	request     func(idleWakeRouteRequest) bool
	enqueue     func(hubScannedRelayEvent) bool
	writeRecord func(string, emitRecord) (string, error)
	logger      *slog.Logger
}

func newIdleWakeManager(store *Store, inboxRoot string, settle time.Duration, request func(idleWakeRouteRequest) bool, enqueue func(hubScannedRelayEvent) bool, logger *slog.Logger) (*idleWakeManager, error) {
	if store == nil || inboxRoot == "" || request == nil || enqueue == nil {
		return nil, errors.New("idle-wake configuration is incomplete")
	}
	if settle <= 0 {
		settle = defaultIdleWakeSettle
	}
	if logger == nil {
		logger = slog.Default()
	}
	manager := &idleWakeManager{store: store, inboxRoot: inboxRoot, settle: settle, now: time.Now, request: request, enqueue: enqueue, writeRecord: ensureLaneEmitRecord, logger: logger}
	if _, err := store.idleWakeNamespace(context.Background()); err != nil {
		return nil, err
	}
	return manager, nil
}

func validIdleWakePane(value string) bool {
	if value == "" || len(value) > 128 || !utf8.ValidString(value) {
		return false
	}
	for _, character := range value {
		if character <= 0x1f || (character >= 0x7f && character <= 0x9f) {
			return false
		}
	}
	return true
}

func validIdleWakeMetadata(value string) bool {
	if len(value) > 512 || !utf8.ValidString(value) {
		return false
	}
	for _, character := range value {
		if character <= 0x1f || (character >= 0x7f && character <= 0x9f) {
			return false
		}
	}
	return true
}

// Optional display metadata must never make a valid pane/state observation
// disappear. Invalid or overlong values are dropped before they can cross the
// typed hub protocol; pane identity itself remains strict and fail-closed.
func normalizedIdleWakeMetadata(value string) string {
	if !validIdleWakeMetadata(value) {
		return ""
	}
	return value
}

func eligibleIdleWakeStatus(status string) bool { return status == "idle" || status == "done" }

func validObservedAgentStatus(status string) bool {
	switch status {
	case "idle", "working", "blocked", "done", "unknown":
		return true
	default:
		return false
	}
}

func idleWakeEventID(namespace, pane string, sequence uint64) string {
	return "idle-wake:" + namespace + ":" + base64.RawURLEncoding.EncodeToString([]byte(pane)) + ":" + strconv.FormatUint(sequence, 10)
}

func validIdleWakeEventID(value, pane string, sequence uint64) bool {
	parts := strings.Split(value, ":")
	if len(parts) != 4 || parts[0] != "idle-wake" || len(parts[1]) != 32 || parts[3] != strconv.FormatUint(sequence, 10) {
		return false
	}
	if _, err := hex.DecodeString(parts[1]); err != nil {
		return false
	}
	decoded, err := base64.RawURLEncoding.DecodeString(parts[2])
	return err == nil && string(decoded) == pane && validLaneEventID(value)
}

func canonicalIdleWakeTime(value string) (time.Time, bool) {
	if !strings.HasSuffix(value, "Z") {
		return time.Time{}, false
	}
	parsed, err := time.Parse(time.RFC3339Nano, value)
	if err != nil || parsed.IsZero() || parsed.UTC().Format(time.RFC3339Nano) != value {
		return time.Time{}, false
	}
	return parsed.UTC(), true
}

func idleWakeRouteRequestFor(candidate idleWakeCandidate) idleWakeRouteRequest {
	return idleWakeRouteRequest{
		EventID: candidate.EventID, Pane: candidate.PaneID, WorkspaceID: candidate.WorkspaceID,
		Label: candidate.Label, State: candidate.Status, ChangedAt: candidate.ChangedAt.UTC().Format(time.RFC3339Nano),
		StateChangeSeq: candidate.StateChangeSeq,
	}
}

func validIdleWakeRouteRequest(request idleWakeRouteRequest) bool {
	_, validTime := canonicalIdleWakeTime(request.ChangedAt)
	return validIdleWakePane(request.Pane) && validIdleWakeMetadata(request.WorkspaceID) && validIdleWakeMetadata(request.Label) &&
		eligibleIdleWakeStatus(request.State) && request.StateChangeSeq > 0 && validIdleWakeEventID(request.EventID, request.Pane, request.StateChangeSeq) && validTime
}

func (s *Store) idleWakeNamespace(ctx context.Context) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var namespace string
	err := s.db.QueryRowContext(ctx, `SELECT value FROM idle_wake_meta WHERE key=?`, idleWakeNamespaceKey).Scan(&namespace)
	if err == nil {
		if len(namespace) != 32 {
			return "", errors.New("invalid idle-wake sequence namespace")
		}
		if _, decodeErr := hex.DecodeString(namespace); decodeErr != nil {
			return "", errors.New("invalid idle-wake sequence namespace")
		}
		return namespace, nil
	}
	if err != sql.ErrNoRows {
		return "", err
	}
	bytes := make([]byte, 16)
	if _, err := rand.Read(bytes); err != nil {
		return "", err
	}
	namespace = hex.EncodeToString(bytes)
	if _, err := s.db.ExecContext(ctx, `INSERT INTO idle_wake_meta(key,value) VALUES(?,?)`, idleWakeNamespaceKey, namespace); err != nil {
		return "", err
	}
	return namespace, nil
}

type idleWakePaneState struct {
	WorkspaceID, Label, Status           string
	StateChangeSeq                       uint64
	UpstreamRevision, UpstreamGeneration int64
	ChangedAt                            time.Time
}

func scanIdleWakePaneState(row *sql.Row) (idleWakePaneState, bool, error) {
	var state idleWakePaneState
	var sequence, changedAt int64
	err := row.Scan(&state.WorkspaceID, &state.Label, &state.Status, &sequence, &state.UpstreamRevision, &state.UpstreamGeneration, &changedAt)
	if err == sql.ErrNoRows {
		return idleWakePaneState{}, false, nil
	}
	if err != nil {
		return idleWakePaneState{}, false, err
	}
	state.StateChangeSeq = uint64(sequence)
	state.ChangedAt = time.UnixMilli(changedAt).UTC()
	return state, true, nil
}

// Observe records a state change before starting its settle. state_change_seq
// is node-journal local and monotonic per pane; upstream herdr revision is only
// used to detect a lost upstream namespace and is never an idempotency key.
func (m *idleWakeManager) Observe(ctx context.Context, observation HerdrAgentState, at time.Time) error {
	if !validIdleWakePane(observation.PaneID) || !validObservedAgentStatus(observation.Status) {
		return errors.New("invalid idle-wake observation")
	}
	observation.WorkspaceID = normalizedIdleWakeMetadata(observation.WorkspaceID)
	observation.Label = normalizedIdleWakeMetadata(observation.Label)
	if at.IsZero() {
		at = m.now()
	}
	at = at.UTC()
	namespace, err := m.store.idleWakeNamespace(ctx)
	if err != nil {
		return err
	}
	m.store.mu.Lock()
	defer m.store.mu.Unlock()
	tx, err := m.store.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	state, found, err := scanIdleWakePaneState(tx.QueryRowContext(ctx, `SELECT workspace_id,label,agent_status,state_change_seq,upstream_revision,upstream_generation,changed_at FROM idle_wake_panes WHERE pane_id=?`, observation.PaneID))
	if err != nil {
		return err
	}
	if !found {
		_, err = tx.ExecContext(ctx, `INSERT INTO idle_wake_panes(pane_id,workspace_id,label,agent_status,state_change_seq,upstream_revision,upstream_generation,changed_at) VALUES(?,?,?,?,?,?,?,?)`, observation.PaneID, observation.WorkspaceID, observation.Label, observation.Status, 1, observation.Revision, 1, at.UnixMilli())
		if err != nil {
			return err
		}
		return tx.Commit()
	}
	workspace, label := state.WorkspaceID, state.Label
	if observation.WorkspaceID != "" {
		workspace = observation.WorkspaceID
	}
	if observation.Label != "" {
		label = observation.Label
	}
	generation := state.UpstreamGeneration
	nextRevision := observation.Revision
	upstreamReset := observation.Authoritative && state.UpstreamRevision > 0 && (observation.Revision == 0 || observation.Revision < state.UpstreamRevision)
	if nextRevision == 0 && !upstreamReset {
		// Some event variants omit revision. They may carry state, but must not
		// erase the last comparable revision and manufacture a later reset.
		nextRevision = state.UpstreamRevision
	}
	if observation.Revision > 0 && state.UpstreamRevision > 0 && observation.Revision < state.UpstreamRevision && !observation.Authoritative {
		return tx.Commit()
	}
	if observation.Revision > 0 && state.UpstreamRevision > 0 && observation.Revision == state.UpstreamRevision && observation.Status != state.Status {
		// A single upstream revision cannot prove two different state changes.
		// Treat the conflicting observation as ambiguous rather than inventing a
		// new node-local state_change_seq.
		return tx.Commit()
	}
	if upstreamReset {
		generation++
		// A not-yet-settled candidate cannot prove continuous non-working
		// across a lost herdr revision namespace. Already-settled candidates
		// retain their right to route and remain idempotent.
		if _, err = tx.ExecContext(ctx, `UPDATE idle_wake_candidates SET decision=?,decision_reason=? WHERE pane_id=? AND decision='' AND settled_at IS NULL`, idleWakeDecisionCancelled, "source_namespace_reset", observation.PaneID); err != nil {
			return err
		}
	}
	if observation.Status == state.Status {
		_, err = tx.ExecContext(ctx, `UPDATE idle_wake_panes SET workspace_id=?,label=?,upstream_revision=?,upstream_generation=? WHERE pane_id=?`, workspace, label, nextRevision, generation, observation.PaneID)
		if err != nil {
			return err
		}
		return tx.Commit()
	}
	if !upstreamReset && eligibleIdleWakeStatus(state.Status) {
		if _, err = tx.ExecContext(ctx, `UPDATE idle_wake_candidates SET settled_at=due_at WHERE pane_id=? AND decision='' AND settled_at IS NULL AND due_at<=?`, observation.PaneID, at.UnixMilli()); err != nil {
			return err
		}
	}
	sequence := state.StateChangeSeq + 1
	if !eligibleIdleWakeStatus(observation.Status) {
		if _, err = tx.ExecContext(ctx, `UPDATE idle_wake_candidates SET decision=?,decision_reason=? WHERE pane_id=? AND decision='' AND settled_at IS NULL`, idleWakeDecisionCancelled, "state_flap", observation.PaneID); err != nil {
			return err
		}
	}
	if !upstreamReset && state.Status == "working" && eligibleIdleWakeStatus(observation.Status) {
		due := at.Add(m.settle)
		eventID := idleWakeEventID(namespace, observation.PaneID, sequence)
		if _, err = tx.ExecContext(ctx, `INSERT INTO idle_wake_candidates(pane_id,state_change_seq,workspace_id,label,agent_status,changed_at,due_at,event_id) VALUES(?,?,?,?,?,?,?,?)`, observation.PaneID, int64(sequence), workspace, label, observation.Status, at.UnixMilli(), due.UnixMilli(), eventID); err != nil {
			return err
		}
	}
	_, err = tx.ExecContext(ctx, `UPDATE idle_wake_panes SET workspace_id=?,label=?,agent_status=?,state_change_seq=?,upstream_revision=?,upstream_generation=?,changed_at=? WHERE pane_id=?`, workspace, label, observation.Status, int64(sequence), nextRevision, generation, at.UnixMilli(), observation.PaneID)
	if err != nil {
		return err
	}
	return tx.Commit()
}

// ObserveSnapshot applies one complete agent.list view and then records panes
// missing from that authoritative view as unknown. A disappeared pane cannot
// keep settling from its last remembered idle state, while its local sequence
// remains monotonic if herdr later reuses the pane identity.
func (m *idleWakeManager) ObserveSnapshot(ctx context.Context, observations []HerdrAgentState, at time.Time) error {
	if at.IsZero() {
		at = m.now()
	}
	at = at.UTC()
	seen := make(map[string]struct{}, len(observations))
	for _, observation := range observations {
		observation.Authoritative = true
		if err := m.Observe(ctx, observation, at); err != nil {
			return err
		}
		seen[observation.PaneID] = struct{}{}
	}
	return m.store.markIdleWakePanesMissing(ctx, seen, at)
}

func (s *Store) markIdleWakePanesMissing(ctx context.Context, seen map[string]struct{}, at time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	rows, err := tx.QueryContext(ctx, `SELECT pane_id FROM idle_wake_panes WHERE agent_status!='unknown'`)
	if err != nil {
		return err
	}
	var missing []string
	for rows.Next() {
		var pane string
		if err := rows.Scan(&pane); err != nil {
			_ = rows.Close()
			return err
		}
		if _, exists := seen[pane]; !exists {
			missing = append(missing, pane)
		}
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return err
	}
	if err := rows.Close(); err != nil {
		return err
	}
	for _, pane := range missing {
		if _, err := tx.ExecContext(ctx, `UPDATE idle_wake_candidates SET decision=?,decision_reason=? WHERE pane_id=? AND decision='' AND settled_at IS NULL`, idleWakeDecisionCancelled, "pane_missing", pane); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `UPDATE idle_wake_panes SET agent_status='unknown',state_change_seq=state_change_seq+1,changed_at=? WHERE pane_id=? AND agent_status!='unknown'`, at.UTC().UnixMilli(), pane); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func scanIdleWakeCandidate(rows *sql.Rows) (idleWakeCandidate, error) {
	var candidate idleWakeCandidate
	var sequence, changedAt, dueAt, settledAt int64
	if err := rows.Scan(&candidate.PaneID, &sequence, &candidate.WorkspaceID, &candidate.Label, &candidate.Status, &changedAt, &dueAt, &settledAt, &candidate.EventID, &candidate.OwnerLane, &candidate.Text, &candidate.JobID); err != nil {
		return idleWakeCandidate{}, err
	}
	candidate.StateChangeSeq = uint64(sequence)
	candidate.ChangedAt = time.UnixMilli(changedAt).UTC()
	candidate.DueAt = time.UnixMilli(dueAt).UTC()
	candidate.SettledAt = time.UnixMilli(settledAt).UTC()
	return candidate, nil
}

func (s *Store) idleWakeCandidatesReady(ctx context.Context, now time.Time, advanceSettle bool) ([]idleWakeCandidate, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	if advanceSettle {
		// Only a currently eligible state can cross the settle boundary. A flap
		// is cancelled synchronously by Observe; this join is the restart-safe
		// guard. Recovery-only scans deliberately skip this update until herdr
		// has supplied a current observation in this process.
		if _, err = tx.ExecContext(ctx, `UPDATE idle_wake_candidates SET settled_at=due_at
		 WHERE decision='' AND settled_at IS NULL AND due_at<=? AND EXISTS (
		  SELECT 1 FROM idle_wake_panes p WHERE p.pane_id=idle_wake_candidates.pane_id AND p.agent_status IN ('idle','done')
		 )`, now.UnixMilli()); err != nil {
			return nil, err
		}
	}
	rows, err := tx.QueryContext(ctx, `SELECT pane_id,state_change_seq,workspace_id,label,agent_status,changed_at,due_at,settled_at,event_id,owner_lane,text,job_id
	 FROM idle_wake_candidates WHERE decision='' AND settled_at IS NOT NULL AND (route_requested_at IS NULL OR route_requested_at<=?) ORDER BY settled_at,pane_id,state_change_seq`, now.Add(-idleWakeRouteRetry).UnixMilli())
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var candidates []idleWakeCandidate
	for rows.Next() {
		candidate, scanErr := scanIdleWakeCandidate(rows)
		if scanErr != nil {
			return nil, scanErr
		}
		candidates = append(candidates, candidate)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return candidates, nil
}

func (s *Store) dueIdleWakeCandidates(ctx context.Context, now time.Time) ([]idleWakeCandidate, error) {
	return s.idleWakeCandidatesReady(ctx, now, true)
}

func (s *Store) settledIdleWakeCandidates(ctx context.Context, now time.Time) ([]idleWakeCandidate, error) {
	return s.idleWakeCandidatesReady(ctx, now, false)
}

func (s *Store) markIdleWakeRouteRequested(ctx context.Context, eventID string, at time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, err := s.db.ExecContext(ctx, `UPDATE idle_wake_candidates SET route_requested_at=? WHERE event_id=? AND decision=''`, at.UnixMilli(), eventID)
	return err
}

func (s *Store) decideIdleWakeRoute(ctx context.Context, decision hubIdleWakeRouteEvent) (idleWakeCandidate, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return idleWakeCandidate{}, false, err
	}
	defer tx.Rollback()
	var existing, pane string
	if err := tx.QueryRowContext(ctx, `SELECT decision,pane_id FROM idle_wake_candidates WHERE event_id=? AND settled_at IS NOT NULL`, decision.EventID).Scan(&existing, &pane); err == sql.ErrNoRows {
		return idleWakeCandidate{}, false, nil
	} else if err != nil {
		return idleWakeCandidate{}, false, err
	}
	if pane != decision.Pane || existing != "" {
		return idleWakeCandidate{}, false, nil
	}
	if !decision.Eligible {
		if _, err := tx.ExecContext(ctx, `UPDATE idle_wake_candidates SET decision=?,decision_reason=? WHERE event_id=? AND decision=''`, idleWakeDecisionSuppressed, decision.Reason, decision.EventID); err != nil {
			return idleWakeCandidate{}, false, err
		}
		return idleWakeCandidate{}, false, tx.Commit()
	}
	if _, err := tx.ExecContext(ctx, `UPDATE idle_wake_candidates SET decision=?,owner_lane=?,text=?,job_id=? WHERE event_id=? AND decision=''`, idleWakeDecisionAssigned, decision.Lane, decision.Text, decision.JobID, decision.EventID); err != nil {
		return idleWakeCandidate{}, false, err
	}
	rows, err := tx.QueryContext(ctx, `SELECT pane_id,state_change_seq,workspace_id,label,agent_status,changed_at,due_at,settled_at,event_id,owner_lane,text,job_id FROM idle_wake_candidates WHERE event_id=?`, decision.EventID)
	if err != nil {
		return idleWakeCandidate{}, false, err
	}
	defer rows.Close()
	if !rows.Next() {
		return idleWakeCandidate{}, false, errors.New("idle-wake route decision disappeared")
	}
	candidate, err := scanIdleWakeCandidate(rows)
	if err != nil {
		return idleWakeCandidate{}, false, err
	}
	if err := rows.Close(); err != nil {
		return idleWakeCandidate{}, false, err
	}
	if err := tx.Commit(); err != nil {
		return idleWakeCandidate{}, false, err
	}
	return candidate, true, nil
}

func (s *Store) assignedIdleWakeCandidates(ctx context.Context) ([]idleWakeCandidate, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	rows, err := s.db.QueryContext(ctx, `SELECT pane_id,state_change_seq,workspace_id,label,agent_status,changed_at,due_at,settled_at,event_id,owner_lane,text,job_id FROM idle_wake_candidates WHERE decision=? AND materialized_at IS NULL ORDER BY settled_at,pane_id,state_change_seq`, idleWakeDecisionAssigned)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var candidates []idleWakeCandidate
	for rows.Next() {
		candidate, scanErr := scanIdleWakeCandidate(rows)
		if scanErr != nil {
			return nil, scanErr
		}
		candidates = append(candidates, candidate)
	}
	return candidates, rows.Err()
}

func (s *Store) markIdleWakeMaterialized(ctx context.Context, eventID string, at time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, err := s.db.ExecContext(ctx, `UPDATE idle_wake_candidates SET materialized_at=? WHERE event_id=? AND decision=?`, at.UnixMilli(), eventID, idleWakeDecisionAssigned)
	return err
}

func (m *idleWakeManager) Tick(ctx context.Context, at time.Time) {
	m.tick(ctx, at, true)
}

// RetrySettled recovers only decisions that had already crossed the settle
// boundary. It is safe while herdr is unavailable: an unverified, merely-due
// candidate cannot become settled through this path.
func (m *idleWakeManager) RetrySettled(ctx context.Context, at time.Time) {
	m.tick(ctx, at, false)
}

func (m *idleWakeManager) tick(ctx context.Context, at time.Time, advanceSettle bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if at.IsZero() {
		at = m.now()
	}
	at = at.UTC()
	var candidates []idleWakeCandidate
	var err error
	if advanceSettle {
		candidates, err = m.store.dueIdleWakeCandidates(ctx, at)
	} else {
		candidates, err = m.store.settledIdleWakeCandidates(ctx, at)
	}
	if err != nil {
		m.logger.Warn("idle-wake settle scan unavailable")
		return
	}
	for _, candidate := range candidates {
		if m.request(idleWakeRouteRequestFor(candidate)) {
			if err := m.store.markIdleWakeRouteRequested(ctx, candidate.EventID, at); err != nil {
				m.logger.Warn("idle-wake route request was not recorded")
			}
		}
	}
	m.materializeAssigned(ctx, at)
}

func (m *idleWakeManager) ApplyRoute(ctx context.Context, decision hubIdleWakeRouteEvent) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if !validHubIdleWakeRouteEvent(decision) {
		m.logger.Warn("idle-wake route decision rejected")
		return
	}
	_, _, err := m.store.decideIdleWakeRoute(ctx, decision)
	if err != nil {
		m.logger.Warn("idle-wake route decision was not recorded")
		return
	}
	m.materializeAssigned(ctx, m.now().UTC())
}

func (m *idleWakeManager) materializeAssigned(ctx context.Context, at time.Time) {
	candidates, err := m.store.assignedIdleWakeCandidates(ctx)
	if err != nil {
		m.logger.Warn("idle-wake materialization scan unavailable")
		return
	}
	for _, candidate := range candidates {
		record := emitRecord{Type: "lane.event", Epoch: 1, CreatedAt: candidate.ChangedAt.UTC().Format(time.RFC3339Nano), OwnerLane: candidate.OwnerLane, Label: candidate.Label, PaneID: candidate.PaneID, EventID: candidate.EventID, Text: candidate.Text}
		if _, err := m.writeRecord(m.inboxRoot, record); err != nil {
			m.logger.Warn("idle-wake durable event file unavailable")
			continue
		}
		event := hubScannedRelayEvent{Kind: "lane.event", HubActiveJob: HubActiveJob{JobID: laneEventTransportID(candidate.OwnerLane, candidate.EventID), Epoch: 1, OwnerLane: candidate.OwnerLane, Label: candidate.Label}, PaneID: candidate.PaneID, EventID: candidate.EventID, Text: candidate.Text}
		m.enqueue(event)
		if err := m.store.markIdleWakeMaterialized(ctx, candidate.EventID, at); err != nil {
			m.logger.Warn("idle-wake materialization was not recorded")
		}
	}
}

type idleWakeOwnerResolution struct {
	Lane, JobID, Label, Reason string
}

const (
	idleWakeReasonUnknown    = "unknown_owner"
	idleWakeReasonAmbiguous  = "ambiguous_owner"
	idleWakeReasonParentless = "parentless_lane"
	idleWakeReasonInvalid    = "invalid_route"
)

func validIdleWakeDecisionReason(reason string) bool {
	switch reason {
	case idleWakeReasonUnknown, idleWakeReasonAmbiguous, idleWakeReasonParentless, idleWakeReasonInvalid:
		return true
	default:
		return false
	}
}

func (h *HubServer) resolveIdleWakeOwner(machineID, pane string) idleWakeOwnerResolution {
	// Direct ownership comes from the existing hot-loaded reportRelayRoute
	// Machine/Pane/Parent fields. The fallback is the latest authenticated
	// heartbeat's hubNodeRecord.activeJobs, originally proven by
	// scanHubActiveJobsWithPanes from job.claimed plus job.spawned metadata.
	routes, err := loadReportRelayRoutesResult(h.reportRelayPath)
	if err != nil {
		return idleWakeOwnerResolution{Reason: idleWakeReasonInvalid}
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	var direct []string
	for lane, route := range routes {
		if !route.Sink && route.Machine == machineID && route.Pane == pane {
			direct = append(direct, lane)
		}
	}
	if len(direct) > 1 {
		return idleWakeOwnerResolution{Reason: idleWakeReasonAmbiguous}
	}
	if len(direct) == 1 {
		route := routes[direct[0]]
		if route.Parent == "" {
			return idleWakeOwnerResolution{Reason: idleWakeReasonParentless}
		}
		if _, exists := routes[route.Parent]; !exists || !hubAgentLabelPattern.MatchString(route.Parent) {
			return idleWakeOwnerResolution{Reason: idleWakeReasonInvalid}
		}
		return idleWakeOwnerResolution{Lane: route.Parent, Label: direct[0]}
	}
	record := h.nodes[machineID]
	if record == nil {
		return idleWakeOwnerResolution{Reason: idleWakeReasonUnknown}
	}
	var matches []HubActiveJob
	for _, job := range record.activeJobs {
		if job.Pane == pane && job.OwnerLane != "" {
			matches = append(matches, job)
		}
	}
	if len(matches) == 0 {
		return idleWakeOwnerResolution{Reason: idleWakeReasonUnknown}
	}
	if len(matches) > 1 {
		return idleWakeOwnerResolution{Reason: idleWakeReasonAmbiguous}
	}
	job := matches[0]
	if _, exists := routes[job.OwnerLane]; !exists || !hubAgentLabelPattern.MatchString(job.OwnerLane) {
		return idleWakeOwnerResolution{Reason: idleWakeReasonInvalid}
	}
	return idleWakeOwnerResolution{Lane: job.OwnerLane, JobID: job.JobID, Label: job.AgentLabel}
}

func idleWakeRouteText(request idleWakeRouteRequest, resolution idleWakeOwnerResolution) string {
	label := request.Label
	if label == "" {
		label = resolution.Label
	}
	wake := struct {
		Kind           string `json:"kind"`
		Pane           string `json:"pane"`
		Label          string `json:"label,omitempty"`
		State          string `json:"state"`
		ChangedAt      string `json:"changed_at"`
		StateChangeSeq uint64 `json:"state_change_seq"`
		JobID          string `json:"job_id,omitempty"`
	}{Kind: "idle-wake", Pane: request.Pane, Label: label, State: request.State, ChangedAt: request.ChangedAt, StateChangeSeq: request.StateChangeSeq, JobID: resolution.JobID}
	encoded, _ := json.Marshal(wake)
	if len(encoded) > laneEventTextLimit {
		// Label is optional display context and the only field whose JSON
		// escaping can expand enough to exceed the direct-lane bound. Dropping
		// it keeps the durable text both bounded and valid JSON.
		wake.Label = ""
		encoded, _ = json.Marshal(wake)
	}
	return string(encoded)
}

func validHubIdleWakeRouteEvent(event hubIdleWakeRouteEvent) bool {
	if event.Type != "idle-wake.route" || !validIdleWakePane(event.Pane) || !validLaneEventID(event.EventID) {
		return false
	}
	if event.Eligible {
		return hubAgentLabelPattern.MatchString(event.Lane) && validLaneEventText(event.Text) && len(event.Text) <= laneEventTextLimit && event.Reason == "" && (event.JobID == "" || hubJobIDPattern.MatchString(event.JobID))
	}
	return event.Lane == "" && event.Text == "" && event.JobID == "" && validIdleWakeDecisionReason(event.Reason)
}

func idleWakeRouteDecision(request idleWakeRouteRequest, resolution idleWakeOwnerResolution) hubIdleWakeRouteEvent {
	decision := hubIdleWakeRouteEvent{Type: "idle-wake.route", EventID: request.EventID, Pane: request.Pane}
	if resolution.Lane == "" {
		decision.Reason = resolution.Reason
		return decision
	}
	decision.Eligible, decision.Lane, decision.JobID = true, resolution.Lane, resolution.JobID
	decision.Text = idleWakeRouteText(request, resolution)
	return decision
}

func decodeIdleWakeRouteRequest(payload []byte) (idleWakeRouteRequest, bool) {
	var fields map[string]json.RawMessage
	if json.Unmarshal(payload, &fields) != nil || len(fields) < 5 || len(fields) > 7 {
		return idleWakeRouteRequest{}, false
	}
	for name := range fields {
		if name != "event_id" && name != "pane" && name != "workspace_id" && name != "label" && name != "state" && name != "changed_at" && name != "state_change_seq" {
			return idleWakeRouteRequest{}, false
		}
	}
	var request idleWakeRouteRequest
	if json.Unmarshal(payload, &request) != nil || !validIdleWakeRouteRequest(request) {
		return idleWakeRouteRequest{}, false
	}
	return request, true
}
