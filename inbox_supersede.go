package panewire

import (
	"context"
	"database/sql"
	"regexp"
	"strings"
	"time"
)

var inboxCausePattern = regexp.MustCompile(`^(?:pr:[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+#[1-9][0-9]*|job:[A-Za-z0-9_.-]+|task:[A-Za-z0-9_.-]+)$`)

func validInboxCause(cause string) bool {
	return cause == "" || (len(cause) <= 256 && inboxCausePattern.MatchString(cause))
}

// laneEventRefLimit bounds the --pr and --head fields of a lane event, the
// same bytes the hub's lane.event decoder accepts for them and for cause.
const laneEventRefLimit = 256

// validLaneEventRef reports whether a lane event's pr or head value would pass
// the hub decode: at most laneEventRefLimit bytes, no CR, LF or NUL.
func validLaneEventRef(value string) bool {
	return len(value) <= laneEventRefLimit && !strings.ContainsAny(value, "\r\n\x00")
}

func nullableCause(cause string) any {
	if cause == "" {
		return nil
	}
	return cause
}

// supersedeRules defines the terminal event and the earlier events it replaces.
var supersedeRules = []struct {
	prefix   string
	matches  func(laneInboxItem) bool
	replaces func(laneInboxItem, laneInboxItem) bool
}{
	{"pr:", func(i laneInboxItem) bool { return i.Event.NotifyType == "merged" || i.Event.NotifyType == "closed" }, func(old, next laneInboxItem) bool { return true }},
	{"pr:", func(i laneInboxItem) bool { return i.Event.Head != "" && supersedePRType(i.Event.NotifyType) }, func(old, next laneInboxItem) bool {
		return supersedePRType(old.Event.NotifyType) && old.Event.Head != next.Event.Head
	}},
	{"job:", func(i laneInboxItem) bool {
		// job.escalate is not an end: an ESC asks a question and the job stays
		// open (hk 3012), so a second ESC must not supersede the first.
		return i.Kind == "job.completed" || i.Kind == "job.lost" || i.Kind == "job.revoked" || (i.Kind == "lane.event" && i.Event.NotifyType == "merged")
	}, func(old, next laneInboxItem) bool {
		return old.Event.NotifyType == "progress" || old.Event.NotifyType == "idle-wake" || old.Event.NotifyType == "decision"
	}},
}

func supersedePRType(t string) bool { return t == "ready" || t == "blocked" || t == "findings" }

func (s *laneInboxStore) supersede(ctx context.Context, tx *sql.Tx, next laneInboxItem, now time.Time) error {
	rows, err := tx.QueryContext(ctx, s.q(`SELECT `+laneInboxColumns+` FROM lane_inbox WHERE lane=? AND cause=? AND id<? AND acked_at=0 AND superseded_at=0`), next.Lane, next.Event.Cause, next.ID)
	if err != nil {
		return err
	}
	var ids []int64
	for rows.Next() {
		old, err := scanLaneInbox(rows)
		if err != nil {
			rows.Close()
			return err
		}
		for _, rule := range supersedeRules {
			if strings.HasPrefix(next.Event.Cause, rule.prefix) && rule.matches(next) && rule.replaces(old, next) {
				ids = append(ids, old.ID)
				break
			}
		}
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return err
	}
	return s.updateIDs(ctx, tx, `UPDATE lane_inbox SET superseded_at=?,superseded_by=?`, []any{now.UnixMilli(), next.ID}, ids)
}
