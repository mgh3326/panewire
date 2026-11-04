package panewire

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"time"
)

// The scheduler owns digest creation. This table freezes the small handoff
// attached to an interrupt, and records surfacing only on receiver confirmation.
const inboxSurfaceSchema = `CREATE TABLE IF NOT EXISTS lane_inbox_surfaces (
 delivery_key TEXT PRIMARY KEY, lane TEXT NOT NULL, through_id BIGINT NOT NULL,
 summary TEXT NOT NULL, delivered_at BIGINT NOT NULL DEFAULT 0)`

func inboxSurfaceKey(key string) string {
	sum := sha256.Sum256([]byte(key))
	return hex.EncodeToString(sum[:])
}

func (s *laneInboxStore) summaryFor(key string) (string, bool, error) {
	ctx, cancel := s.op(s.life)
	defer cancel()
	var line string
	err := s.db.QueryRowContext(ctx, s.q(`SELECT summary FROM lane_inbox_surfaces WHERE delivery_key=?`), inboxSurfaceKey(key)).Scan(&line)
	if errors.Is(err, sql.ErrNoRows) {
		return "", false, nil
	}
	return line, err == nil, err
}

func (s *laneInboxStore) unsurfacedInbox(lane string) ([]laneInboxItem, error) {
	ctx, cancel := s.op(s.life)
	defer cancel()
	rows, err := s.db.QueryContext(ctx, s.q(`SELECT `+laneInboxColumns+` FROM lane_inbox WHERE lane=? AND class='inbox' AND `+laneInboxUnread+` AND id>COALESCE((SELECT max(through_id) FROM lane_inbox_surfaces WHERE lane=? AND delivered_at>0),0) ORDER BY id`), lane, lane)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var items []laneInboxItem
	for rows.Next() {
		i, e := scanLaneInbox(rows)
		if e != nil {
			return nil, e
		}
		items = append(items, i)
	}
	return items, rows.Err()
}

func (s *laneInboxStore) saveInboxSummary(key, lane, line string, through int64) (string, error) {
	ctx, cancel := s.op(s.life)
	defer cancel()
	_, err := s.db.ExecContext(ctx, s.q(`INSERT INTO lane_inbox_surfaces(delivery_key,lane,through_id,summary) VALUES(?,?,?,?) ON CONFLICT(delivery_key) DO NOTHING`), inboxSurfaceKey(key), lane, through, line)
	if err != nil {
		return "", err
	}
	saved, _, err := s.summaryFor(key)
	return saved, err
}

func (s *laneInboxStore) surfaceInboxSummary(key string, now time.Time) error {
	ctx, cancel := s.op(s.life)
	defer cancel()
	_, err := s.db.ExecContext(ctx, s.q(`UPDATE lane_inbox_surfaces SET delivered_at=? WHERE delivery_key=? AND delivered_at=0`), now.UnixMilli(), inboxSurfaceKey(key))
	return err
}

// Inbox titles mirror the scheduler's visible kinds. Typed events add precision
// while the existing text remains readable by older scheduler versions.
func inboxSummaryKind(i laneInboxItem) string {
	text := deliveryEventText(i.Event)
	switch {
	case strings.Contains(text, "[dots] RESULT"):
		return "dots"
	case i.Event.NotifyType != "":
		return i.Event.NotifyType
	case strings.Contains(text, `"kind":"idle-wake"`) || strings.Contains(text, `"kind": "idle-wake"`):
		return "idle-wake"
	case strings.Contains(text, "[mergewake] MERGED"):
		return "merged"
	case strings.Contains(text, "[mergewake] READY"):
		return "ready"
	case strings.Contains(text, "[report]") || i.Kind == "job.completed" || i.Kind == "job.joined":
		return "report"
	default:
		return i.Kind
	}
}

func inboxSummaryLine(items []laneInboxItem, doc string, limit int) string {
	counts := map[string]int{}
	var titles []string
	for _, i := range items {
		kind := inboxSummaryKind(i)
		counts[kind]++
		if kind == "dots" || kind == "report" {
			// A report's first line is its title. Never copy the full report body.
			source := i.Event.Text
			if source == "" {
				source = i.Event.ReportLastLine
			}
			text := strings.Join(strings.Fields(strings.SplitN(source, "\n", 2)[0]), " ")
			for _, mark := range []string{"[dots] RESULT ", "[report] "} {
				text = strings.TrimPrefix(text, mark)
			}
			if text != "" {
				titles = append(titles, string([]rune(text)[:min(len([]rune(text)), 60)]))
			}
		}
	}
	kinds := make([]string, 0, len(counts))
	for kind := range counts {
		kinds = append(kinds, kind)
	}
	sort.Strings(kinds)
	parts := make([]string, 0, len(kinds))
	for _, kind := range kinds {
		parts = append(parts, fmt.Sprintf("%s %d", kind, counts[kind]))
	}
	content := fmt.Sprintf("[inbox] %d new: %s", len(items), strings.Join(parts, ", "))
	if len(titles) > 0 {
		content += " | " + strings.Join(titles, "; ")
	}
	pointer := " | hk:doc " + doc
	budget := limit - len([]rune(pointer))
	if len([]rune(content)) > budget {
		content = string([]rune(content)[:max(0, budget-1)]) + "…"
	}
	return content + pointer
}

// The script timestamps keys in its own timezone. UTC yesterday/today/tomorrow
// cover every timezone, and keep listing bounded as old digest history grows.
func (h *HubServer) latestInboxDigest(prefix string) (string, error) {
	if h.handoffkeep == nil {
		return "", errors.New("digest store unavailable")
	}
	ctx, cancel := context.WithTimeout(h.closeCtx, 3*time.Second)
	defer cancel()
	best := ""
	var updated time.Time
	for _, day := range []int{-1, 0, 1} {
		q := url.Values{"prefix": {prefix + h.now().UTC().AddDate(0, 0, day).Format("20060102")}, "limit": {"1000"}}
		status, raw, err := h.handoffkeep.do(ctx, http.MethodGet, h.handoffkeep.endpoint("/v1/documents")+"?"+q.Encode(), nil)
		if err != nil || status != http.StatusOK {
			return "", errors.New("digest lookup unavailable")
		}
		var docs struct {
			Documents []struct {
				Key     string    `json:"key"`
				Updated time.Time `json:"updated_at"`
			} `json:"documents"`
		}
		if err := json.Unmarshal(raw, &docs); err != nil {
			return "", err
		}
		for _, doc := range docs.Documents {
			if strings.HasPrefix(doc.Key, prefix) && !strings.ContainsAny(doc.Key, "\r\n |") && len(doc.Key) <= 180 && (best == "" || doc.Updated.After(updated)) {
				best, updated = doc.Key, doc.Updated
			}
		}
	}
	if best == "" {
		return "", errors.New("no scheduler digest yet")
	}
	return best, nil
}

// inboxSummaryJoin separates the directive text from its summary line. The node
// refuses relay.inject text holding \r, \n or NUL (validRelayFinalText) and drops
// the whole directive, so the separator must stay printable.
const inboxSummaryJoin = " | "

func (h *HubServer) appendInboxSummary(kind string, e hubJobEventPayload, lane, text string) string {
	if h.inbox == nil {
		return text
	}
	settings := h.inbox.policy.Lanes[lane].InboxSummary
	if settings == nil {
		return text
	}
	key := relayEventDedupeKey(kind, e)
	line, found, err := h.inbox.store.summaryFor(key)
	if err != nil {
		return text
	}
	if found {
		if line == "" {
			return text
		}
		return text + inboxSummaryJoin + line
	}
	items, err := h.inbox.store.unsurfacedInbox(lane)
	if err != nil {
		return text
	}
	through := int64(0)
	if len(items) > 0 {
		doc, err := h.latestInboxDigest(settings.DigestPrefix)
		if err != nil {
			h.logger.Warn("inbox summary awaiting scheduler digest", "lane", lane)
			return text
		}
		line = inboxSummaryLine(items, doc, settings.MaxChars)
		through = items[len(items)-1].ID
	}
	line, err = h.inbox.store.saveInboxSummary(key, lane, line, through)
	if err != nil || line == "" {
		return text
	}
	return text + inboxSummaryJoin + line
}
