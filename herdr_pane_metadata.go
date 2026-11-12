package panewire

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"sort"
	"strconv"
	"time"
)

// Job panes carry their panewire state as herdr pane tokens (source
// "panewire", keys pw_*), so the pane list shows job, owner, and held count
// without asking the daemon. The publisher is the only writer of pw_ keys.
// Scope is the spawned job panes in stall_jobs; desk and design panes are
// not published.

const paneMetadataSource = "panewire"

const (
	// paneMetadataPublishInterval is the ledger scan cadence. A hold or
	// release republishes at once through the store observer; this is the
	// floor for everything else.
	paneMetadataPublishInterval = 10 * time.Second
	// paneMetadataRefreshAfter re-sends an unchanged pane once this long has
	// passed since its last report: one publish interval before the TTL lapses.
	paneMetadataRefreshAfter = 2 * paneMetadataPublishInterval
	// paneMetadataTTL is how long herdr keeps each reported key without a
	// refresh. If panewire stops, its tokens expire within this window.
	paneMetadataTTL = 3 * paneMetadataPublishInterval
)

// paneMetadataCallTimeout bounds one pane.report_metadata round trip. It is a
// var so a test can shorten it.
var paneMetadataCallTimeout = 5 * time.Second

// The only token keys panewire sends. Anything else on a pane is not ours.
const (
	paneTokenJob   = "pw_job"
	paneTokenOwner = "pw_owner"
	paneTokenHeld  = "pw_held"
)

// paneMetadataReport is the pane.report_metadata request body. A nil token
// value clears that key. TTLms is omitted on a clear.
type paneMetadataReport struct {
	PaneID string         `json:"pane_id"`
	Source string         `json:"source"`
	Tokens map[string]any `json:"tokens"`
	TTLms  int64          `json:"ttl_ms,omitempty"`
	Seq    int64          `json:"seq"`
}

// ReportPaneMetadata sends one pane.report_metadata request. A herdr refusal
// is a *herdrReplyError; any other error is a connection failure. The round
// trip is bounded: Call sets a socket deadline only when ctx has one, and the
// daemon's run context has none, so a herdr that accepts the request and never
// replies would block Daemon.Stop on paneMetaDone.
func (c *HerdrClient) ReportPaneMetadata(ctx context.Context, report paneMetadataReport) error {
	ctx, cancel := context.WithTimeout(ctx, paneMetadataCallTimeout)
	defer cancel()
	_, err := c.Call(ctx, "pane.report_metadata", report)
	return err
}

// paneMetadataPublisher keeps herdr's pw_ tokens in step with the job ledger.
// cycle runs on Run's goroutine alone, so published and lastSeq need no lock.
// Kick is the only method other goroutines call.
type paneMetadataPublisher struct {
	store     *Store
	dial      func() (*HerdrClient, error)
	now       func() time.Time
	logger    *slog.Logger
	kick      chan struct{}
	published map[string]paneMetadataSnapshot
	lastSeq   int64
	stopped   bool
}

// paneMetadataSnapshot is the token set herdr was last told for one pane.
type paneMetadataSnapshot struct {
	tokens     map[string]string
	reportedAt time.Time
}

func newPaneMetadataPublisher(store *Store, socket string, logger *slog.Logger) *paneMetadataPublisher {
	return &paneMetadataPublisher{
		store:     store,
		dial:      func() (*HerdrClient, error) { return NewHerdrClient(socket) },
		now:       time.Now,
		logger:    logger,
		kick:      make(chan struct{}, 1),
		published: map[string]paneMetadataSnapshot{},
	}
}

// Kick asks for a cycle now. It never blocks: a store write must not wait on
// the publisher, and one pending kick already covers any number of writes.
func (p *paneMetadataPublisher) Kick() {
	select {
	case p.kick <- struct{}{}:
	default:
	}
}

// Run publishes until ctx ends. It does not clear tokens on exit: they expire
// within paneMetadataTTL, so shutdown never waits on herdr.
func (p *paneMetadataPublisher) Run(ctx context.Context) {
	ticker := time.NewTicker(paneMetadataPublishInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		case <-p.kick:
		}
		p.cycle(ctx)
	}
}

// cycle makes herdr match the desired set: a pane whose tokens changed, or is
// due a refresh, is reported; a pane that left the set is cleared.
func (p *paneMetadataPublisher) cycle(ctx context.Context) {
	if p.stopped {
		return
	}
	desired, err := p.desiredPanes(ctx)
	if err != nil {
		p.logger.Debug("pane metadata: ledger read failed", "error", err)
		return
	}
	client, err := p.dial()
	if err != nil {
		p.logger.Debug("pane metadata: herdr unreachable", "error", err)
		return
	}
	defer client.Close()
	now := p.now()
	var gone []string
	for pane := range p.published {
		if _, ok := desired[pane]; !ok {
			gone = append(gone, pane)
		}
	}
	sort.Strings(gone)
	for _, pane := range gone {
		if !p.report(ctx, client, pane, nil, now) {
			return
		}
	}
	live := make([]string, 0, len(desired))
	for pane := range desired {
		live = append(live, pane)
	}
	sort.Strings(live)
	for _, pane := range live {
		next := desired[pane]
		prev, had := p.published[pane]
		if had && sameTokens(prev.tokens, next) && now.Sub(prev.reportedAt) < paneMetadataRefreshAfter {
			continue
		}
		if !p.report(ctx, client, pane, next, now) {
			return
		}
	}
}

// desiredPanes maps each job pane in the ledger to its bounded token set. A
// pane with no tokens is left out: herdr rejects an empty report. When two
// non-terminal rows name one pane, the later spawn wins.
func (p *paneMetadataPublisher) desiredPanes(ctx context.Context) (map[string]map[string]string, error) {
	jobs, err := p.store.stallJobs(ctx, false)
	if err != nil {
		return nil, err
	}
	held, err := p.store.RelayHeldAll(ctx)
	if err != nil {
		return nil, err
	}
	heldPerPane := map[string]int{}
	for _, row := range held {
		heldPerPane[row.Pane]++
	}
	spawnedAt := map[string]time.Time{}
	desired := map[string]map[string]string{}
	for _, job := range jobs {
		if job.PaneID == "" {
			continue
		}
		if prev, ok := spawnedAt[job.PaneID]; ok && job.SpawnedAt.Before(prev) {
			continue
		}
		tokens := map[string]string{}
		if job.JobID != "" {
			tokens[paneTokenJob] = truncatePaneMetadataValue(job.JobID)
		}
		if job.OwnerLane != "" {
			tokens[paneTokenOwner] = truncatePaneMetadataValue(job.OwnerLane)
		}
		if n := heldPerPane[job.PaneID]; n > 0 {
			tokens[paneTokenHeld] = strconv.Itoa(n)
		}
		if len(tokens) == 0 {
			continue
		}
		spawnedAt[job.PaneID] = job.SpawnedAt
		desired[job.PaneID] = tokens
	}
	return desired, nil
}

// report sends the difference between what herdr was last told and next; a
// nil next clears every key the pane carried. It returns false when the cycle
// must end: herdr refused publishing, the connection failed, or ctx ended.
func (p *paneMetadataPublisher) report(ctx context.Context, client *HerdrClient, pane string, next map[string]string, now time.Time) bool {
	if ctx.Err() != nil {
		// Shutdown started: send nothing more. Run returns on its next select.
		return false
	}
	prev := p.published[pane].tokens
	tokens := map[string]any{}
	for key := range prev {
		if _, ok := next[key]; !ok {
			tokens[key] = nil
		}
	}
	for key, value := range next {
		tokens[key] = value
	}
	seq := now.UnixMilli()
	if seq <= p.lastSeq {
		seq = p.lastSeq + 1
	}
	p.lastSeq = seq
	report := paneMetadataReport{PaneID: pane, Source: paneMetadataSource, Tokens: tokens, Seq: seq}
	if next != nil {
		report.TTLms = paneMetadataTTL.Milliseconds()
	}
	err := client.ReportPaneMetadata(ctx, report)
	var refused *herdrReplyError
	switch {
	case err == nil:
	case errors.As(err, &refused) && replyErrorCode(refused) == "pane_not_found":
		// The pane closed, so there is nothing left to clear. Forget it and
		// keep publishing for the other panes.
		delete(p.published, pane)
		return true
	case errors.As(err, &refused):
		p.stop(replyErrorCode(refused))
		return false
	default:
		p.logger.Debug("pane metadata: report failed", "error", err)
		return false
	}
	if next == nil {
		delete(p.published, pane)
	} else {
		p.published[pane] = paneMetadataSnapshot{tokens: next, reportedAt: now}
	}
	return true
}

// stop ends publishing for the life of the daemon. Only the error code is
// logged: never a token value, pane id, or message text.
func (p *paneMetadataPublisher) stop(code string) {
	p.stopped = true
	p.logger.Warn("pane metadata publishing stopped: herdr refused pane.report_metadata", "code", code)
}

// replyErrorCode returns the code in herdr's error object, or "" when the
// object has another shape.
func replyErrorCode(refused *herdrReplyError) string {
	var body struct {
		Code string `json:"code"`
	}
	if json.Unmarshal(refused.raw, &body) != nil {
		return ""
	}
	return body.Code
}

func sameTokens(a, b map[string]string) bool {
	if len(a) != len(b) {
		return false
	}
	for key, value := range a {
		if other, ok := b[key]; !ok || other != value {
			return false
		}
	}
	return true
}
