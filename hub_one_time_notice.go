package panewire

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/getsentry/sentry-go"
	"github.com/mgh3326/panewire/internal/fleet"
)

// The hub's one-time notice worker owns the told state and every ACTION NEEDED
// send (hub_one_time_action.go decides what counts as a need). Handlers hand it
// observations and return. Each delivery attempt is answered only by its own
// 2xx: a drop, a rate-limit skip, a non-2xx, a network error, a timeout or
// shutdown leaves the need untold, so the next sighting tries again. A need is
// recorded as told, and stored, only after that attempt's own 2xx.

// oneTimeSentryDeadline bounds one delivery attempt's request.
const oneTimeSentryDeadline = 2 * time.Second

var errOneTimeSentryUnconfigured = errors.New("sentry is not configured")

// oneTimeNotifier sends one ACTION NEEDED event and answers whether Sentry
// accepted it. HubServer.oneTimeNotify replaces the Sentry send in tests.
type oneTimeNotifier func(event *sentry.Event) (accepted bool, err error)

// oneTimeObservation is one thing handed to the worker, in arrival order. A
// sighting carries machine, kind, need and at; a readiness report carries
// machine, report and at; a barrier carries done and no observation. The worker
// closes a barrier once everything queued before it has been applied and any
// delivery those started has finished.
type oneTimeObservation struct {
	machine, kind, need string
	at                  time.Time
	report              *fleet.Readiness
	done                chan struct{}
}

// oneTimeNotices is the queue to the notice worker, guarded by mu. The worker
// starts on the first observation and exits when Close stops it.
type oneTimeNotices struct {
	mu     sync.Mutex
	queue  []oneTimeObservation
	closed bool
	wake   chan struct{} // capacity 1: the queue is non-empty
	done   chan struct{} // closed when the worker has exited; nil before start
	stop   context.CancelFunc
}

// enqueueOneTimeObservation hands obs to the worker and returns at once. An
// observation after Close is dropped.
func (h *HubServer) enqueueOneTimeObservation(obs oneTimeObservation) {
	n := &h.oneTimeQueue
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.closed {
		return
	}
	if n.wake == nil {
		h.startOneTimeWorkerLocked(n)
	}
	n.queue = append(n.queue, obs)
	select {
	case n.wake <- struct{}{}:
	default:
	}
}

// startOneTimeWorkerLocked starts the one notice worker under the hub's shutdown
// context. The caller holds n.mu.
func (h *HubServer) startOneTimeWorkerLocked(n *oneTimeNotices) {
	base := h.closeCtx
	if base == nil {
		base = context.Background()
	}
	ctx, stop := context.WithCancel(base)
	n.wake = make(chan struct{}, 1)
	n.done = make(chan struct{})
	n.stop = stop
	sender := &oneTimeSender{dsn: strings.TrimSpace(h.oneTimeDSN), base: ctx}
	go h.runOneTimeWorker(ctx, n, n.wake, n.done, sender)
}

// closeOneTimeNotices stops accepting observations, cancels the active delivery,
// and joins the worker, which releases its Sentry client. Close calls it before
// the store closes, because the worker's last writes go to the store.
func (h *HubServer) closeOneTimeNotices() {
	n := &h.oneTimeQueue
	n.mu.Lock()
	n.closed = true
	done, stop := n.done, n.stop
	n.mu.Unlock()
	if stop != nil {
		stop()
	}
	if done != nil {
		<-done
	}
}

// runOneTimeWorker applies queued observations in batches until ctx is done.
func (h *HubServer) runOneTimeWorker(ctx context.Context, n *oneTimeNotices, wake <-chan struct{}, done chan struct{}, sender *oneTimeSender) {
	defer close(done)
	defer sender.release()
	for {
		select {
		case <-ctx.Done():
			return
		case <-wake:
		}
		n.mu.Lock()
		batch := n.queue
		n.queue = nil
		n.mu.Unlock()
		if ctx.Err() != nil {
			releaseOneTimeBarriers(batch)
			return
		}
		h.applyOneTimeObservations(batch, sender)
	}
}

func releaseOneTimeBarriers(batch []oneTimeObservation) {
	for _, obs := range batch {
		if obs.done != nil {
			close(obs.done)
		}
	}
}

// applyOneTimeObservations applies one batch in arrival order, then delivers at
// most one notice per need the batch leaves untold. A need therefore has one
// attempt in flight, and repeat sightings in a batch coalesce into that attempt.
func (h *HubServer) applyOneTimeObservations(batch []oneTimeObservation, sender *oneTimeSender) {
	var untold []oneTimeSighting
	queued := map[oneTimeActionKey]bool{}
	touched := map[oneTimeActionKey]bool{}
	var closing []oneTimeActionKey
	seenClosing := map[oneTimeActionKey]bool{}
	var barriers []chan struct{}
	for _, obs := range batch {
		switch {
		case obs.done != nil:
			barriers = append(barriers, obs.done)
		case obs.report != nil:
			for _, key := range h.clearOneTimeActions(obs.machine, obs.report) {
				if !seenClosing[key] {
					seenClosing[key] = true
					closing = append(closing, key)
				}
			}
			for _, kind := range fleet.ReadinessKinds {
				harness, ok := obs.report.Harnesses[kind]
				if !ok {
					continue
				}
				if harness.Dialog != "" {
					h.considerOneTimeSighting(obs.machine, kind, harness.Dialog, obs.at, queued, &untold, touched)
				}
				if kind != "cmd" && harness.Login == fleet.ReadinessLoginMissing {
					h.considerOneTimeSighting(obs.machine, kind, oneTimeLoginMissing, obs.at, queued, &untold, touched)
				}
			}
		default:
			h.considerOneTimeSighting(obs.machine, obs.kind, obs.need, obs.at, queued, &untold, touched)
		}
	}
	for _, s := range untold {
		if sender.base.Err() != nil {
			break
		}
		if err := h.deliverOneTimeAction(sender, s); err != nil {
			h.logUndeliveredOneTimeAction(s, err)
			continue
		}
		// Only this attempt's own 2xx reaches here: the state and its row are
		// written for this need and no other.
		h.oneTimeMu.Lock()
		if h.oneTimeActions == nil {
			h.oneTimeActions = map[oneTimeActionKey]oneTimeActionState{}
		}
		state := oneTimeActionState{ToldAt: s.at}
		// A confirmed-clear row being replaced by a recurrence keeps its wait
		// ids: the handoffkeep wait is still open, so the new episode tracks
		// the same wait and the pending close is cancelled.
		if prior, ok := h.oneTimeActions[s.key]; ok {
			state.WaitTaskID, state.WaitID = prior.WaitTaskID, prior.WaitID
		}
		h.oneTimeActions[s.key] = state
		h.putOneTimeActionLocked(s.key, state)
		h.oneTimeMu.Unlock()
		h.logger.Warn("host needs a one-time action", "host", s.key.machine, "kind", s.key.kind, "need", s.key.need)
	}
	h.syncOneTimeWaits(sender.base, touched, closing)
	for _, done := range barriers {
		close(done)
	}
}

// considerOneTimeSighting applies one sighting. A told need is only restarted
// in its clear countdown when it shows again. An untold need joins this batch's
// delivery once, at the first sighting. touched records every need the batch
// saw so the handoffkeep wait sync can re-assert the standing ones.
func (h *HubServer) considerOneTimeSighting(machine, kind, need string, at time.Time, queued map[oneTimeActionKey]bool, untold *[]oneTimeSighting, touched map[oneTimeActionKey]bool) {
	s, ok := oneTimeSightingFor(machine, kind, need, at)
	if !ok {
		return
	}
	touched[s.key] = true
	h.oneTimeMu.Lock()
	if state, told := h.oneTimeActions[s.key]; told && state.ConfirmedAt.IsZero() {
		if !state.GoneSince.IsZero() {
			state.GoneSince = time.Time{}
			h.oneTimeActions[s.key] = state
			h.putOneTimeActionLocked(s.key, state)
		}
		h.oneTimeMu.Unlock()
		return
	}
	h.oneTimeMu.Unlock()
	if queued[s.key] {
		return
	}
	queued[s.key] = true
	*untold = append(*untold, s)
}

// deliverOneTimeAction sends the ACTION NEEDED event for s. It answers an error
// unless Sentry accepted this event. oneTimeMu is not held during the send.
func (h *HubServer) deliverOneTimeAction(sender *oneTimeSender, s oneTimeSighting) error {
	// The title and fingerprint carry the action without its quotes, so one
	// issue is one host and action and the Discord rule posts it once.
	plain := strings.ReplaceAll(s.action, `"`, "")
	event := &sentry.Event{Level: sentry.LevelWarning, Message: fmt.Sprintf("ACTION NEEDED: %s %s — %s", s.key.machine, s.key.kind, plain), Fingerprint: []string{"action-needed", s.key.machine, s.key.kind, plain}, Tags: map[string]string{"host": s.key.machine, "kind": s.key.kind, "need": s.key.need}}
	var accepted bool
	var err error
	if h.oneTimeNotify != nil {
		accepted, err = h.oneTimeNotify(event)
	} else {
		accepted, err = sender.notify(event)
	}
	if err != nil {
		return err
	}
	if !accepted {
		return errors.New("sentry did not accept the action-needed event")
	}
	return nil
}

func (h *HubServer) logUndeliveredOneTimeAction(s oneTimeSighting, err error) {
	if errors.Is(err, errOneTimeSentryUnconfigured) {
		h.logger.Debug("one-time action not told: no Sentry DSN", "host", s.key.machine, "kind", s.key.kind, "need", s.key.need)
		return
	}
	h.logger.Warn("one-time action not told; the next readiness report tries again", "host", s.key.machine, "kind", s.key.kind, "need", s.key.need, "error", err)
}

// oneTimeSender is the worker's Sentry delivery. It builds one synchronous
// client from the panewire DSN on first use and releases it when the worker
// exits. Without that DSN it builds nothing, so the SDK's ambient SENTRY_DSN
// fallback is never reached. It is used only by the worker goroutine.
type oneTimeSender struct {
	dsn       string
	base      context.Context
	client    *sentry.Client
	transport *oneTimeTransport
}

// notify sends event as one delivery attempt and reports whether that attempt's
// own request was answered 2xx.
func (s *oneTimeSender) notify(event *sentry.Event) (bool, error) {
	if s.dsn == "" {
		return false, errOneTimeSentryUnconfigured
	}
	if s.client == nil {
		if err := s.open(); err != nil {
			return false, err
		}
	}
	attempt := &oneTimeAttempt{}
	ctx, cancel := context.WithTimeout(context.WithValue(s.base, oneTimeAttemptKey{}, attempt), oneTimeSentryDeadline)
	defer cancel()
	s.transport.ctx = ctx
	if s.client.CaptureEvent(event, nil, sentry.NewScope()) == nil {
		return false, errors.New("sentry dropped the action-needed event")
	}
	return attempt.accepted(), nil
}

// open builds the worker's client. Its transport is synchronous and its HTTP
// answers are recorded per attempt; the spawn trace client is a separate client.
func (s *oneTimeSender) open() error {
	inner := sentry.NewHTTPSyncTransport()
	inner.Timeout = oneTimeSentryDeadline
	s.transport = &oneTimeTransport{HTTPSyncTransport: inner}
	options := spawnSentryOptions(s.dsn)
	options.Transport = s.transport
	options.HTTPTransport = oneTimeRecorder{base: http.DefaultTransport}
	client, err := sentry.NewClient(options)
	if err != nil {
		return err
	}
	s.client = client
	return nil
}

func (s *oneTimeSender) release() {
	if s.client != nil {
		s.client.Close()
		s.client = nil
	}
}

// oneTimeTransport sends each event under the attempt context set by the sender,
// so the request carries its attempt and its deadline.
type oneTimeTransport struct {
	*sentry.HTTPSyncTransport
	ctx context.Context
}

func (t *oneTimeTransport) SendEvent(event *sentry.Event) {
	t.SendEventWithContext(t.ctx, event)
}

// oneTimeAttemptKey carries an attempt on the request context.
type oneTimeAttemptKey struct{}

// oneTimeAttempt is one delivery attempt and whether its own request was
// answered 2xx.
type oneTimeAttempt struct {
	mu sync.Mutex
	ok bool
}

func (a *oneTimeAttempt) accepted() bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.ok
}

// oneTimeRecorder records each request's answer on the attempt named by that
// request's context. A late answer lands on the attempt that sent it, which has
// already given up; it can never settle a later attempt.
type oneTimeRecorder struct{ base http.RoundTripper }

func (r oneTimeRecorder) RoundTrip(request *http.Request) (*http.Response, error) {
	ctx := request.Context()
	response, err := r.base.RoundTrip(request)
	attempt, _ := ctx.Value(oneTimeAttemptKey{}).(*oneTimeAttempt)
	if attempt != nil && err == nil && response.StatusCode >= 200 && response.StatusCode <= 299 && ctx.Err() == nil {
		attempt.mu.Lock()
		attempt.ok = true
		attempt.mu.Unlock()
	}
	return response, err
}
