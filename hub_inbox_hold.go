package panewire

import (
	"encoding/json"
	"fmt"
	"net/http"
	"regexp"
	"sort"
	"strings"

	"github.com/google/uuid"
)

// Claim the complete lane queue in one transaction. The first arrival owns the
// cap; later arrivals cannot extend it. A claim survives hub restart.
func (h *HubServer) makeHeldBatches() error {
	s := h.inbox.store
	ctx, done := s.op(s.life)
	defer done()
	s.mu.Lock()
	defer s.mu.Unlock()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	rows, err := tx.QueryContext(ctx, `SELECT `+laneInboxColumns+` FROM lane_inbox WHERE hold_until>0 AND digest_batch='' AND acked_at=0 ORDER BY id`)
	if err != nil {
		return err
	}
	groups := map[string][]laneInboxItem{}
	for rows.Next() {
		item, err := scanLaneInbox(rows)
		if err != nil {
			rows.Close()
			return err
		}
		groups[item.Lane] = append(groups[item.Lane], item)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return err
	}
	for lane, items := range groups {
		if h.receiverInboxState(lane).working && h.now().UnixMilli() < items[0].HoldUntil {
			continue
		}
		id := "held-" + h.now().UTC().Format("20060102T150405.000000000Z") + "-" + uuid.NewString()
		text := heldBatchText(items)
		if _, err := tx.ExecContext(ctx, s.q(`INSERT INTO lane_inbox_batches(id,lane,text) VALUES(?,?,?)`), id, lane, text); err != nil {
			return err
		}
		ids := make([]int64, len(items))
		for n, i := range items {
			ids[n] = i.ID
		}
		if err := s.updateIDs(ctx, tx, `UPDATE lane_inbox SET digest_batch=?`, []any{id}, ids); err != nil {
			return err
		}
	}
	return tx.Commit()
}

var heldNoticeKind = regexp.MustCompile(`(?i)\b(?:READY|MERGED|FINDINGS|BLOCKED)\b`)

func heldBatchText(items []laneInboxItem) string {
	counts := map[string]int{}
	texts := make([]string, 0, len(items))
	for _, i := range items {
		kind := strings.ToUpper(i.Event.NotifyType)
		if kind == "" {
			kind = strings.ToUpper(i.Kind)
			if tag := heldNoticeKind.FindString(deliveryEventText(i.Event)); tag != "" {
				kind = strings.ToUpper(tag)
			}
		}
		counts[kind]++
		fields := []string{i.Event.Text, i.Event.ReportLastLine, i.Event.Question, i.Event.Reason, i.Event.ReportPath, i.Event.PR, i.Event.Head}
		var content []string
		for _, field := range fields {
			if field != "" {
				content = append(content, strings.Join(strings.Fields(field), " "))
			}
		}
		texts = append(texts, fmt.Sprintf("#%d %s: %s", i.ID, i.Kind, strings.Join(content, " · ")))
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
	return fmt.Sprintf("[batch %d: %s] | %s", len(items), strings.Join(parts, ", "), strings.Join(texts, " | "))
}

func (h *HubServer) prepareHeldBatch(b laneInboxBatch) (laneInboxBatch, error) {
	if strings.Contains(b.Text, " | hk:doc inbox/") {
		return b, nil
	}
	s := h.inbox.store
	ctx, done := s.op(s.life)
	defer done()
	rows, err := s.db.QueryContext(ctx, s.q(`SELECT `+laneInboxColumns+` FROM lane_inbox WHERE digest_batch=? AND acked_at=0 ORDER BY id`), b.ID)
	if err != nil {
		return b, err
	}
	var items []laneInboxItem
	for rows.Next() {
		i, e := scanLaneInbox(rows)
		if e != nil {
			rows.Close()
			return b, e
		}
		items = append(items, i)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return b, err
	}
	if len(items) == 0 {
		b.Text = ""
		return b, h.inbox.store.markBatchSent(b.ID)
	}
	b.Text = heldBatchText(items)
	settings := h.inbox.policy.holdPolicy(b.Lane)
	bytes, count := 1500, 3
	if settings != nil {
		if settings.HandoffBytes > 0 {
			bytes = settings.HandoffBytes
		}
		if settings.HandoffItems > 0 {
			count = settings.HandoffItems
		}
	}
	if (len(b.Text) > bytes || len(items) > count) && h.handoffkeep != nil {
		// The timestamp and suffix belong to the durable batch, so retries PUT the
		// same document and simultaneous releases never overwrite one another.
		key := "inbox/" + b.Lane + "/" + strings.TrimPrefix(b.ID, "held-")
		body, _ := json.Marshal(map[string]string{"kind": "handoff", "body": b.Text})
		status, _, err := h.handoffkeep.do(h.closeCtx, http.MethodPut, h.handoffkeep.endpoint("/v1/documents/"+key), body)
		if err == nil && status == http.StatusOK {
			text := strings.Split(b.Text, " | ")[0] + " | hk:doc " + key
			titleLimit := max(8, min(100, (laneEventTextLimit-len(text)-120)/len(items)-24))
			for _, i := range items {
				title := strings.Join(strings.Fields(deliveryEventText(i.Event)), " ")
				if title == "" {
					title = i.Kind
				}
				addition := " | #" + fmt.Sprint(i.ID) + " " + truncateRelayText(title, titleLimit)
				if len(text)+len(addition) > laneEventTextLimit-120 {
					text += " | …"
					break
				}
				text += addition
			}
			b.Text = text
		}
	}
	// A failed document write keeps the full body. Oversized legacy directives
	// stay durable and retry the document instead of losing or truncating items.
	if len(b.Text) > relaySingleItemMax-512 {
		return b, fmt.Errorf("held batch needs document handoff before delivery")
	}
	_, err = s.db.ExecContext(ctx, s.q(`UPDATE lane_inbox_batches SET text=? WHERE id=?`), b.Text, b.ID)
	return b, err
}

// The relay row holds a small stable reference, while the inbox retains the
// actual injection. Use it on first delivery and every replay path.
func (h *HubServer) inboxRelayDirective(kind string, e hubJobEventPayload, route reportRelayRoute, id int64) hubRelayInjectEvent {
	d := relayInjectDirective(kind, e, route, id)
	if h.inbox == nil || kind != "lane.event" || !strings.HasPrefix(e.EventID, "held-") {
		return d
	}
	ctx, done := h.inbox.store.op(h.inbox.store.life)
	defer done()
	var text string
	err := h.inbox.store.db.QueryRowContext(ctx, h.inbox.store.q(`SELECT text FROM lane_inbox_batches WHERE id=? AND lane=?`), e.EventID, e.OwnerLane).Scan(&text)
	if err == nil {
		d.Text = text
	}
	return d
}

func (h *HubServer) isHeldBatch(lane, id string) bool {
	if !strings.HasPrefix(id, "held-") {
		return false
	}
	ctx, done := h.inbox.store.op(h.inbox.store.life)
	defer done()
	var n int
	err := h.inbox.store.db.QueryRowContext(ctx, h.inbox.store.q(`SELECT count(*) FROM lane_inbox_batches WHERE id=? AND lane=?`), id, lane).Scan(&n)
	return err == nil && n == 1
}
