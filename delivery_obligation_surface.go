package panewire

// The operator surface of the delivery obligations (hk 6823, hk 4289 T6;
// docs/r20-relay-persistence.md, "Delivery obligations"): the obligation an
// ack-state answer names, the feed counters, and the marker lifecycle.

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"time"
)

// obligationSurface is what a row's obligation may add to the event's
// ack-state answer: the obligation's state, the fault it carries, and the
// reason it settled with.
type obligationSurface struct {
	state, fault, settleReason string
}

// obligationSurfaces answers the obligation of each stored event key, for the
// ack-state lookup. An inbox row's event_key is the obligation's. A key can
// stand for several obligations — a folded job's later round has the row's —
// so an open one, then the newest, wins.
func (s *laneInboxStore) obligationSurfaces(parent context.Context, eventKeys []string) (map[string]obligationSurface, error) {
	if len(eventKeys) == 0 {
		return nil, nil
	}
	marks := strings.TrimSuffix(strings.Repeat("?,", len(eventKeys)), ",")
	args := make([]any, len(eventKeys))
	for i, key := range eventKeys {
		args[i] = key
	}
	ctx, done := s.op(parent)
	defer done()
	rows, err := s.db.QueryContext(ctx, s.q(`SELECT event_key,state,fault,settle_reason FROM delivery_obligations WHERE event_key IN (`+marks+`) ORDER BY id`), args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	rank := func(state string) int {
		if state == obligationConfirmed || state == obligationSettled {
			return 1
		}
		return 2
	}
	out := map[string]obligationSurface{}
	best := map[string]int{}
	for rows.Next() {
		var key string
		var o obligationSurface
		if err := rows.Scan(&key, &o.state, &o.fault, &o.settleReason); err != nil {
			return nil, err
		}
		if r := rank(o.state); r >= best[key] {
			best[key], out[key] = r, o
		}
	}
	return out, rows.Err()
}

// obligationMarkerEvent is the legacy unconfirmed marker still pending for
// the obligation, "" when there is none.
func (s *laneInboxStore) obligationMarkerEvent(parent context.Context, id int64) (string, error) {
	ctx, done := s.op(parent)
	defer done()
	var markerID string
	err := s.db.QueryRowContext(ctx, s.q(`SELECT marker_event_id FROM delivery_obligations WHERE id=?`), id).Scan(&markerID)
	if errors.Is(err, sql.ErrNoRows) {
		return "", nil
	}
	return markerID, err
}

// clearObligationMarker drops the marker link after its retire is durable.
func (s *laneInboxStore) clearObligationMarker(parent context.Context, id int64) error {
	ctx, done := s.op(parent)
	defer done()
	_, err := s.db.ExecContext(ctx, s.q(`UPDATE delivery_obligations SET marker_event_id='' WHERE id=?`), id)
	return err
}

// markedTerminalObligation is a closed obligation whose marker link a failed
// retire left behind.
type markedTerminalObligation struct {
	id       int64
	markerID string
}

// markedTerminalObligations is at most limit closed obligations still naming a
// marker: the retire at close failed, and each pass retries it.
func (s *laneInboxStore) markedTerminalObligations(parent context.Context, limit int) ([]markedTerminalObligation, error) {
	ctx, done := s.op(parent)
	defer done()
	rows, err := s.db.QueryContext(ctx, s.q(`SELECT id,marker_event_id FROM delivery_obligations WHERE marker_event_id<>'' AND NOT `+obligationOpen+` ORDER BY id LIMIT ?`), limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []markedTerminalObligation
	for rows.Next() {
		var o markedTerminalObligation
		if err := rows.Scan(&o.id, &o.markerID); err != nil {
			return nil, err
		}
		out = append(out, o)
	}
	return out, rows.Err()
}

// obligationMarkerRetireLimit bounds the marker retirements one pass retries.
const obligationMarkerRetireLimit = 10

// closeObligation ends an open obligation and retires the legacy marker its
// row kept as a downgrade breadcrumb, in the same pass (v2 L2): once the
// origin is terminal an older binary must not replay it.
func (h *HubServer) closeObligation(ctx context.Context, id int64, state, reason string, now time.Time) error {
	if err := h.inbox.store.closeObligation(ctx, id, state, reason, now); err != nil {
		return err
	}
	h.retireObligationMarker(ctx, id)
	return nil
}

// retireObligationMarker retires the marker the obligation links to, when the
// row is terminal or being closed, and clears the link once the retire is
// durable. A marker that cannot be found or is already delivered clears the
// link too; a failed retire leaves it for a later pass.
func (h *HubServer) retireObligationMarker(ctx context.Context, id int64) {
	store := h.inbox.store
	markerID, err := store.obligationMarkerEvent(ctx, id)
	if err != nil {
		h.logger.Warn("delivery obligation marker was not read", "obligation", id, "error", store.loggable("obligation marker", err))
		return
	}
	if markerID == "" {
		return
	}
	marker, found, err := h.obligationMarkerRecord(ctx, markerID)
	if err != nil {
		h.logger.Warn("delivery obligation marker row could not be read", "obligation", id, "error_class", hubHKReadErrorClass(err))
		return
	}
	retired := !found || marker.DeliveredAt != "" || strings.HasPrefix(marker.DeliveredTo, relayReplayRetiredMarker)
	if !retired {
		retired = h.retireRelayReplay(marker, relayUnconfirmedMarkRetireReason)
	}
	if !retired {
		return
	}
	if err := store.clearObligationMarker(ctx, id); err != nil {
		h.logger.Warn("delivery obligation marker link was not cleared", "obligation", id, "error", store.loggable("clear marker", err))
	}
}

// obligationMarkerRecord finds the marker row an obligation names: in the
// remembered ids the undelivered walks see, else by the transport id the
// marker carries.
func (h *HubServer) obligationMarkerRecord(ctx context.Context, markerID string) (handoffkeepRelayEvent, bool, error) {
	if id, ok := h.obligations.markers.Load(markerID); ok {
		record, found, err := h.handoffkeep.relayEvent(ctx, id.(int64))
		if err != nil || found {
			return record, found, err
		}
	}
	return h.relayEventByTransportID(ctx, laneEventTransportID(relayUnconfirmedMarkLane, markerID))
}

// retireObligationMarkers retries the marker retirements a close could not
// finish: the obligation is terminal and its marker link survives.
func (h *HubServer) retireObligationMarkers(ctx context.Context) {
	marked, err := h.inbox.store.markedTerminalObligations(ctx, obligationMarkerRetireLimit)
	if err != nil {
		h.logger.Warn("marked terminal obligations were not read", "error", h.inbox.store.loggable("marked obligations", err))
		return
	}
	for _, o := range marked {
		if ctx.Err() != nil {
			return
		}
		h.retireObligationMarker(ctx, o.id)
	}
}

// hubObligationCounts is the feed's count of the obligation tables: the
// obligations by state (open holds owed, noticed and exhausted; the parked
// exhausted rows are named on their own too), the copies spent, the incidents
// open and closed by reason, and the members folded into an incident that was
// not opened for them.
type hubObligationCounts struct {
	Open            int64            `json:"open"`
	Confirmed       int64            `json:"confirmed"`
	Settled         int64            `json:"settled"`
	Exhausted       int64            `json:"exhausted"`
	Retries         int64            `json:"retries"`
	Revived         int64            `json:"revived"`
	IncidentsOpen   int64            `json:"incidents_open"`
	IncidentsClosed map[string]int64 `json:"incidents_closed,omitempty"`
	NoticesFolded   int64            `json:"notices_folded"`
}

// deliveryCounts reads the counters off the store, so a hub restart loses
// nothing and a store copy carries them. Each transition moves a count once.
func (s *laneInboxStore) deliveryCounts(parent context.Context) (hubObligationCounts, error) {
	var c hubObligationCounts
	ctx, done := s.op(parent)
	defer done()
	rows, err := s.db.QueryContext(ctx, s.q(`SELECT state,count(*) FROM delivery_obligations GROUP BY state`))
	if err != nil {
		return c, err
	}
	states := map[string]int64{}
	for rows.Next() {
		var state string
		var n int64
		if err := rows.Scan(&state, &n); err != nil {
			rows.Close()
			return c, err
		}
		states[state] = n
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return c, err
	}
	c.Open = states[obligationOwed] + states[obligationNoticed] + states[obligationExhausted]
	c.Confirmed, c.Settled, c.Exhausted = states[obligationConfirmed], states[obligationSettled], states[obligationExhausted]
	err = s.db.QueryRowContext(ctx, s.q(`SELECT COALESCE(SUM(CASE WHEN spent>1 THEN spent-1 ELSE 0 END),0),COALESCE(SUM(revived),0) FROM delivery_obligations`)).Scan(&c.Retries, &c.Revived)
	if err != nil {
		return c, err
	}
	incidents, err := s.db.QueryContext(ctx, s.q(`SELECT state,close_reason,count(*) FROM delivery_incidents GROUP BY state,close_reason`))
	if err != nil {
		return c, err
	}
	c.IncidentsClosed = map[string]int64{}
	for incidents.Next() {
		var state, reason string
		var n int64
		if err := incidents.Scan(&state, &reason, &n); err != nil {
			incidents.Close()
			return c, err
		}
		if state == incidentStateOpen {
			c.IncidentsOpen += n
		} else {
			c.IncidentsClosed[reason] = n
		}
	}
	incidents.Close()
	if err := incidents.Err(); err != nil {
		return c, err
	}
	if len(c.IncidentsClosed) == 0 {
		c.IncidentsClosed = nil
	}
	// Members an incident counted beyond its first are notices that folded
	// into the incident instead of opening one.
	return c, s.db.QueryRowContext(ctx, s.q(`SELECT COALESCE(SUM(members),0)-count(*) FROM delivery_incidents`)).Scan(&c.NoticesFolded)
}

// obligationCounts feeds the operator surface's obligation counters, nil when
// the hub has no store or the read failed.
func (h *HubServer) obligationCounts() *hubObligationCounts {
	if h.inbox == nil {
		return nil
	}
	counts, err := h.inbox.store.deliveryCounts(h.inbox.store.life)
	if err != nil {
		h.logger.Warn("delivery obligation counters were not read", "error", h.inbox.store.loggable("obligation counters", err))
		return nil
	}
	return &counts
}
