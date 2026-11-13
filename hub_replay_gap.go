package panewire

// hk 2777: after a long gap — a hub down for a deploy, a node away — a lane
// can owe dozens of rows. Replaying each buries the pane under old news, so a
// lane over relayReplayGapThreshold gets one summary line that names where to
// read them (the lane inbox) and the rows the inbox holds retire.

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"time"
)

// relayReplayGapThreshold is the largest backlog one lane replays row by row.
const relayReplayGapThreshold = 20

// relayGapRetireReason is the replay-retired reason of a row a summary line
// stands for.
const relayGapRetireReason = "gap"

// replayCandidate is a lane.event row that passed every replay gate and
// waits for the walk to finish counting its lane.
type replayCandidate struct {
	record handoffkeepRelayEvent
	key    string
	event  hubJobEventPayload
	route  reportRelayRoute
	agent  *hubAgent
}

// replayGap collects the candidates of one listing walk, by lane, in listing
// (row id) order.
type replayGap struct {
	byLane map[string][]replayCandidate
}

func newReplayGap() *replayGap { return &replayGap{byLane: make(map[string][]replayCandidate)} }

func (g *replayGap) add(c replayCandidate) {
	g.byLane[c.record.OwnerLane] = append(g.byLane[c.record.OwnerLane], c)
}

// flushReplayGaps ends a walk: a lane within the threshold replays its rows,
// a lane over it gets one summary line.
func (h *HubServer) flushReplayGaps(ctx context.Context, g *replayGap, source string) {
	lanes := make([]string, 0, len(g.byLane))
	for lane := range g.byLane {
		lanes = append(lanes, lane)
	}
	sort.Strings(lanes)
	for _, lane := range lanes {
		candidates := g.byLane[lane]
		if len(candidates) > relayReplayGapThreshold {
			candidates = h.summarizeReplayGap(ctx, lane, candidates)
		}
		for _, c := range candidates {
			h.injectReplayedLaneEvent(c, source)
		}
	}
}

// summarizeReplayGap persists the summary line first, then retires the rows
// it stands for, so a crash between the two leaves rows still open and the
// next walk reuses the same summary (its event id names the last row). The
// summary points at `panewire inbox list`, so it stands only for rows the lane
// inbox holds: without an inbox (the kill switch, no store) or for a row it
// lacks, nothing is retired. It returns the candidates left to replay; all of
// them when the summary could not be filed, so none is lost.
func (h *HubServer) summarizeReplayGap(ctx context.Context, lane string, candidates []replayCandidate) []replayCandidate {
	if h.inbox == nil {
		return candidates
	}
	var covered, rest []replayCandidate
	for _, c := range candidates {
		held, err := h.inbox.store.holds(c.key)
		if err != nil {
			h.logger.Warn("inbox evidence unavailable; replaying the rows", "lane", lane, "event_id", c.record.ID)
			return candidates
		}
		if held {
			covered = append(covered, c)
		} else {
			rest = append(rest, c)
		}
	}
	if len(covered) <= relayReplayGapThreshold {
		return candidates
	}
	first, last := covered[0].record, covered[len(covered)-1].record
	eventID := "replay-gap-" + strconv.FormatInt(last.ID, 10)
	text := fmt.Sprintf("[panewire] gap: %d events for %s were not replayed after the restart (first %s, last %s) | panewire inbox list --lane %s",
		len(covered), lane, replayGapStamp(first), replayGapStamp(last), lane)
	event := hubJobEventPayload{JobID: laneEventTransportID(lane, eventID), Epoch: 1, OwnerLane: lane, EventID: eventID, Text: text, DeliveryClass: "interrupt", DeliverPolicy: "now", noDigest: true}
	result := h.relayLaneEventContext(ctx, event, nil)
	if result.PersistFailed || result.RejectedTooLong {
		h.logger.Warn("replay gap summary was not filed; replaying the rows", "lane", lane, "rows", len(covered))
		return candidates
	}
	for _, c := range covered {
		h.forgetRelayEvent(c.key)
		h.retireRelayReplay(c.record, relayGapRetireReason)
	}
	h.logger.Info("replay gap summarized", "lane", lane, "rows", len(covered), "first_event_id", first.ID, "last_event_id", last.ID)
	return rest
}

func replayGapStamp(record handoffkeepRelayEvent) string {
	when, ok := unconfirmedMarkFirstSent(record)
	if !ok {
		return "unknown-time"
	}
	return when.In(relayUnconfirmedLabelZone).Format(time.DateTime + " MST")
}
