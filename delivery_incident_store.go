package panewire

// The hub store's side of the delivery incidents (hk 6821, hk 4289 T4;
// docs/r20-relay-persistence.md, "Incidents and notices"). An obligation row is
// due while its design lane is still to be told of its fault; the rows due for
// one (design lane, machine, fault) are members of one open incident, and the
// notice slot of that key spaces the notices across incidents.

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"
	"unicode/utf8"
)

const (
	incidentStateOpen   = "open"
	incidentStateClosed = "closed"

	// The close reasons of delivery_incidents.close_reason.
	incidentRecovered   = "recovered"
	incidentAllTerminal = "all-terminal"
	incidentMoot        = "moot"
	incidentQuiet       = "quiet"
)

// obligationNeverNoticed is the predicate of the faults no report makes due:
// unrouted names a missing route rather than a failed copy, and no pane takes an
// ended job's row.
const obligationNeverNoticed = `fault IN ('` + obligationFaultUnrouted + `','` + relayDroppedJobEnded + `')`

// obligationUntold is the predicate of a row a notice may still become due for.
const obligationUntold = `kind='lane.event' AND notice_due_at=0 AND told_at=0`

// obligationNodeDropped is the predicate of the safe faults a node's drop
// names. The design lane was told of them from the drop before there were
// incidents, so a row that loses its route under one is told then.
const obligationNodeDropped = `fault IN ('` + relayDroppedPaneReadFailed + `','` + relayDroppedRetriesExhausted + `')`

// obligationFailed is the predicate of a row that had a failed copy to tell of.
// A row renamed unrouted keeps that fault in fault_reason; one renamed by a
// build from before it kept any has nothing to tell.
const obligationFailed = `fault NOT IN ('','` + relayDroppedJobEnded + `') AND (fault<>'` + obligationFaultUnrouted + `' OR fault_reason<>'')`

// obligationDueWhen is the SET fragment that makes a row matching when due a
// notice of its failure: its fault, or what an unrouted rename kept. It binds
// one value, the time. MySQL reads an assigned column's new value, so it goes
// ahead of any assignment to state or fault.
func obligationDueWhen(when string) string {
	return `fault_reason=CASE WHEN ` + when + ` AND fault<>'` + obligationFaultUnrouted + `' THEN fault ELSE fault_reason END,
		notice_due_at=CASE WHEN ` + when + ` THEN ? ELSE notice_due_at END,`
}

// incidentKey is what one incident, and the spacing of its notices, belongs to.
type incidentKey struct{ design, machine, fault string }

// bounded cuts the key to its column widths: MySQL refuses a longer value.
func (k incidentKey) bounded() incidentKey {
	return incidentKey{utf8PrefixWithin(k.design, 255), utf8PrefixWithin(k.machine, 255), utf8PrefixWithin(k.fault, 64)}
}

// openSlot is the open_slot value that keeps one incident open per key. A key
// too long for the column is stored as its digest.
func (k incidentKey) openSlot() string {
	slot := k.design + "|" + k.machine + "|" + k.fault
	if utf8.RuneCountInString(slot) > 512 {
		return fmt.Sprintf("sha256:%x", sha256.Sum256([]byte(slot)))
	}
	return slot
}

// deliveryIncident is one open incident.
type deliveryIncident struct {
	id                     int64
	key                    incidentKey
	announced              int
	openedAt, lastMemberAt int64
	// eventID is the last notice id the incident posted or committed.
	eventID string
}

// noticeID is the producer id of the incident's next notice. It is fixed until
// a notice is committed: only recordNotice advances announced.
func (i deliveryIncident) noticeID() string {
	return fmt.Sprintf("%si%d-n%d", unconfirmedEscalationPrefix, i.id, i.announced)
}

// unresolved reports whether the next notice was posted and not committed, so
// that handoffkeep or an inbox may already hold it.
func (i deliveryIncident) unresolved() bool { return i.eventID == i.noticeID() }

// noticeMember is an obligation as the incident pass reads it.
type noticeMember struct {
	id, relayID          int64
	lane, machine, pane  string
	state, fault         string
	dueAt                int64
	lastFaultAt, updated int64
}

// terminal reports whether the row's obligation is closed.
func (r noticeMember) terminal() bool {
	return r.state == obligationConfirmed || r.state == obligationSettled
}

// fault_reason is the fault a due row's notice reports. It is the fault the row
// had when it became due, or what an orphaned drop said.
const noticeMemberColumns = `id,relay_id,lane,machine,pane,state,fault_reason,notice_due_at,last_fault_at,updated_at`

func (s *laneInboxStore) noticeMembers(parent context.Context, query string, args ...any) ([]noticeMember, error) {
	ctx, done := s.op(parent)
	defer done()
	rows, err := s.db.QueryContext(ctx, s.q(`SELECT `+noticeMemberColumns+` FROM delivery_obligations WHERE `+query), args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []noticeMember
	for rows.Next() {
		var r noticeMember
		if err := rows.Scan(&r.id, &r.relayID, &r.lane, &r.machine, &r.pane, &r.state, &r.fault, &r.dueAt, &r.lastFaultAt, &r.updated); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// obligationReportDue is the SET fragment, with its values, by which a report
// or a drop of fault makes its row due a notice in the same write: nothing for
// a fault not noticed at its report. A row of an ended job is left alone. The
// fragment reads the fault the row had, so it goes ahead of the assignment to
// fault: MySQL reads an assigned column's new value.
func obligationReportDue(fault string, at int64) (set string, values []any) {
	if !noticedAtReport(fault) {
		return "", nil
	}
	const when = obligationUntold + ` AND fault<>'` + relayDroppedJobEnded + `'`
	return `fault_reason=CASE WHEN ` + when + ` THEN ? ELSE fault_reason END,
		notice_due_at=CASE WHEN ` + when + ` THEN ? ELSE notice_due_at END,`, []any{fault, at}
}

// markNoticeDue records that the open lane.event rows matching where have a
// fault their design lane is to be told of. A row already due, a row a notice
// has covered, and a row whose fault is never noticed keep what they have.
func (s *laneInboxStore) markNoticeDue(parent context.Context, fault string, now time.Time, where string, keys ...any) error {
	ctx, done := s.op(parent)
	defer done()
	_, err := s.db.ExecContext(ctx, s.q(`UPDATE delivery_obligations SET fault_reason=?,notice_due_at=?
		WHERE kind='lane.event' AND notice_due_at=0 AND told_at=0 AND NOT (`+obligationNeverNoticed+`) AND `+obligationOpen+` AND `+where),
		append([]any{utf8PrefixWithin(fault, 128), now.UnixMilli()}, keys...)...)
	return err
}

// endUnprovenNotice makes an owed row whose retries have ended by age due a
// notice of its failure (obligationRetriesEnd).
func (s *laneInboxStore) endUnprovenNotice(parent context.Context, id int64, now time.Time) error {
	ctx, done := s.op(parent)
	defer done()
	_, err := s.db.ExecContext(ctx, s.q(`UPDATE delivery_obligations SET `+obligationDueWhen(obligationRetriesEnd)+`updated_at=?
		WHERE id=? AND `+obligationRetriesEnd), now.UnixMilli(), now.UnixMilli(), id)
	return err
}

// forgetNotice ends a row's due notice without telling anyone: the row was
// confirmed, or nobody can act on it.
func (s *laneInboxStore) forgetNotice(parent context.Context, id int64) error {
	ctx, done := s.op(parent)
	defer done()
	_, err := s.db.ExecContext(ctx, s.q(`UPDATE delivery_obligations SET notice_due_at=0 WHERE id=?`), id)
	return err
}

// noticeJoins is at most limit due rows that are in no incident, the longest
// untouched first.
func (s *laneInboxStore) noticeJoins(parent context.Context, limit int) ([]noticeMember, error) {
	return s.noticeMembers(parent, `incident_id=0 AND notice_due_at>0 ORDER BY updated_at,id LIMIT ?`, limit)
}

// deferNoticeJoin sends a row the pass could not join behind the other rows.
func (s *laneInboxStore) deferNoticeJoin(parent context.Context, id int64, now time.Time) error {
	ctx, done := s.op(parent)
	defer done()
	_, err := s.db.ExecContext(ctx, s.q(`UPDATE delivery_obligations SET updated_at=? WHERE id=? AND incident_id=0`), now.UnixMilli(), id)
	return err
}

// incidentMembers is every row of an incident, the longest due first.
func (s *laneInboxStore) incidentMembers(parent context.Context, incident int64) ([]noticeMember, error) {
	return s.noticeMembers(parent, `incident_id=? ORDER BY notice_due_at,relay_id,id`, incident)
}

func (s *laneInboxStore) openIncidents(parent context.Context) ([]deliveryIncident, error) {
	ctx, done := s.op(parent)
	defer done()
	rows, err := s.db.QueryContext(ctx, s.q(`SELECT id,design_lane,machine,fault,announced,opened_at,last_member_at,notice_event_id FROM delivery_incidents WHERE open_slot IS NOT NULL ORDER BY id`))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []deliveryIncident
	for rows.Next() {
		var i deliveryIncident
		if err := rows.Scan(&i.id, &i.key.design, &i.key.machine, &i.key.fault, &i.announced, &i.openedAt, &i.lastMemberAt, &i.eventID); err != nil {
			return nil, err
		}
		out = append(out, i)
	}
	return out, rows.Err()
}

var errNoticeMemberChanged = errors.New("delivery incident row changed")

// joinIncident makes the due row id a member of the open incident of key,
// opening one if the key has none. An exhausted row becomes noticed. A withheld
// notice is recorded at once: the row is told, without taking the key's notice
// slot and without advancing announced, which names the notice a retry may be
// posting; only recordNotice advances it. It returns the incident id.
//
// An incident id starts at the clock, not at 1, so a hub that starts on a new
// store does not reuse a notice id handoffkeep already holds.
func (s *laneInboxStore) joinIncident(parent context.Context, key incidentKey, harness string, id int64, withheld bool, now time.Time) (int64, error) {
	key = key.bounded()
	ctx, done := s.op(parent)
	defer done()
	tx, err := s.beginTx(ctx)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()
	at := now.UnixMilli()
	var incident int64
	err = tx.QueryRowContext(ctx, s.q(`SELECT id FROM delivery_incidents WHERE open_slot=?`), key.openSlot()).Scan(&incident)
	if errors.Is(err, sql.ErrNoRows) {
		var last int64
		if err := tx.QueryRowContext(ctx, `SELECT COALESCE(MAX(id),0) FROM delivery_incidents`).Scan(&last); err != nil {
			return 0, err
		}
		incident = max(last+1, at)
		_, err = tx.ExecContext(ctx, s.q(`INSERT INTO delivery_incidents(id,design_lane,machine,fault,harness,state,open_slot,opened_at,last_member_at) VALUES(?,?,?,?,?,?,?,?,?)`),
			incident, key.design, key.machine, key.fault, utf8PrefixWithin(harness, 64), incidentStateOpen, key.openSlot(), at, at)
	}
	if err != nil {
		return 0, err
	}
	result, err := tx.ExecContext(ctx, s.q(`UPDATE delivery_obligations SET incident_id=?,state=CASE WHEN state='`+obligationExhausted+`' THEN '`+obligationNoticed+`' ELSE state END,updated_at=?
		WHERE id=? AND incident_id=0 AND notice_due_at>0`), incident, at, id)
	if err != nil {
		return 0, err
	}
	if joined, err := s.dialect.rowsMatched(result); err != nil || joined != 1 {
		return 0, errors.Join(errNoticeMemberChanged, err)
	}
	counted := `members=members+1,last_member_at=?`
	values := []any{at}
	if withheld {
		if _, err := tx.ExecContext(ctx, s.q(`UPDATE delivery_obligations SET told_at=?,notice_due_at=0 WHERE id=?`), at, id); err != nil {
			return 0, err
		}
		counted += `,notice_at=?`
		values = append(values, at)
	}
	if _, err := tx.ExecContext(ctx, s.q(`UPDATE delivery_incidents SET `+counted+` WHERE id=?`), append(values, incident)...); err != nil {
		return 0, err
	}
	return incident, tx.Commit()
}

// noticeSlot is when the key's last notice was stored, or 0 for never.
func (s *laneInboxStore) noticeSlot(parent context.Context, key incidentKey) (int64, error) {
	key = key.bounded()
	ctx, done := s.op(parent)
	defer done()
	var at int64
	err := s.db.QueryRowContext(ctx, s.q(`SELECT notice_at FROM delivery_notice_slots WHERE design_lane=? AND machine=? AND fault=?`), key.design, key.machine, key.fault).Scan(&at)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, nil
	}
	return at, err
}

// markNoticePosted records the id of a notice about to be posted, ahead of the
// post: until recordNotice commits it, the incident is unresolved.
func (s *laneInboxStore) markNoticePosted(parent context.Context, incident int64, eventID string) error {
	ctx, done := s.op(parent)
	defer done()
	_, err := s.db.ExecContext(ctx, s.q(`UPDATE delivery_incidents SET notice_event_id=? WHERE id=?`), utf8PrefixWithin(eventID, 255), incident)
	return err
}

// storedNotice is a notice that is durable: relayID is its handoffkeep row, or
// 0 when it was filed in an inbox. told names the rows its text covers.
type storedNotice struct {
	eventID string
	relayID int64
	told    []int64
}

// recordNotice commits a durable notice in one transaction: the incident's
// notice and announced count, the key's notice slot, and the rows it told. A
// notice whose rows are unknown (told is empty) still advances announced, so
// the next one has an id of its own.
func (s *laneInboxStore) recordNotice(parent context.Context, incident deliveryIncident, notice storedNotice, now time.Time) error {
	key := incident.key
	ctx, done := s.op(parent)
	defer done()
	tx, err := s.beginTx(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	at := now.UnixMilli()
	result, err := tx.ExecContext(ctx, s.q(`UPDATE delivery_incidents SET announced=announced+?,notice_at=?,notice_relay_id=?,notice_event_id=? WHERE id=? AND announced=?`),
		max(len(notice.told), 1), at, notice.relayID, utf8PrefixWithin(notice.eventID, 255), incident.id, incident.announced)
	if err != nil {
		return err
	}
	if matched, err := s.dialect.rowsMatched(result); err != nil || matched != 1 {
		return errors.Join(errNoticeMemberChanged, err)
	}
	if _, err := tx.ExecContext(ctx, s.q(s.dialect.insertIgnore(`INSERT INTO delivery_notice_slots(design_lane,machine,fault) VALUES(?,?,?)`, "design_lane,machine,fault")), key.design, key.machine, key.fault); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, s.q(`UPDATE delivery_notice_slots SET notice_at=?,notices=notices+1 WHERE design_lane=? AND machine=? AND fault=?`), at, key.design, key.machine, key.fault); err != nil {
		return err
	}
	for chunk := range slices.Chunk(notice.told, laneInboxClaimChunk) {
		args := []any{at}
		for _, id := range chunk {
			args = append(args, id)
		}
		marks := strings.TrimSuffix(strings.Repeat("?,", len(chunk)), ",")
		if _, err := tx.ExecContext(ctx, s.q(`UPDATE delivery_obligations SET told_at=?,notice_due_at=0 WHERE told_at=0 AND id IN (`+marks+`)`), args...); err != nil {
			return err
		}
	}
	return tx.Commit()
}

// closeIncident closes an open incident for reason and detaches its members: a
// noticed one returns to exhausted. A moot close drops their due notices; the
// others keep them, and join the next incident. A quiet or all-terminal close
// does nothing if a member became due, or one joined, since the caller read the
// incident. It reports whether it closed.
func (s *laneInboxStore) closeIncident(parent context.Context, incident deliveryIncident, reason string, now time.Time) (bool, error) {
	ctx, done := s.op(parent)
	defer done()
	tx, err := s.beginTx(ctx)
	if err != nil {
		return false, err
	}
	defer tx.Rollback()
	at := now.UnixMilli()
	if reason == incidentQuiet || reason == incidentAllTerminal {
		var due int
		if err := tx.QueryRowContext(ctx, s.q(`SELECT count(*) FROM delivery_obligations WHERE incident_id=? AND notice_due_at>0`), incident.id).Scan(&due); err != nil || due > 0 {
			return false, err
		}
	}
	result, err := tx.ExecContext(ctx, s.q(`UPDATE delivery_incidents SET state=?,open_slot=NULL,closed_at=?,close_reason=? WHERE id=? AND open_slot IS NOT NULL AND last_member_at=?`),
		incidentStateClosed, at, reason, incident.id, incident.lastMemberAt)
	if err != nil {
		return false, err
	}
	if matched, err := s.dialect.rowsMatched(result); err != nil || matched != 1 {
		return false, err
	}
	if reason == incidentMoot {
		if _, err := tx.ExecContext(ctx, s.q(`UPDATE delivery_obligations SET notice_due_at=0 WHERE incident_id=?`), incident.id); err != nil {
			return false, err
		}
	}
	if _, err := tx.ExecContext(ctx, s.q(`UPDATE delivery_obligations SET state=CASE WHEN state='`+obligationNoticed+`' THEN '`+obligationExhausted+`' ELSE state END,incident_id=0,updated_at=? WHERE incident_id=?`), at, incident.id); err != nil {
		return false, err
	}
	return true, tx.Commit()
}

// readIncidentMemberNext makes the pass read an incident member's row at once:
// the hub just recorded a delivery of it, which closes the incident as
// recovered.
func (s *laneInboxStore) readIncidentMemberNext(parent context.Context, relayID int64) error {
	ctx, done := s.op(parent)
	defer done()
	_, err := s.db.ExecContext(ctx, s.q(`UPDATE delivery_obligations SET next_at=0 WHERE relay_id=? AND incident_id>0 AND `+obligationOpen), relayID)
	return err
}
