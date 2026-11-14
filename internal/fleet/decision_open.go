package fleet

import (
	"encoding/json"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"time"
)

// DefaultDecisionTimeout is how long a recorded question stays open without an
// answer: the same six hours the watcher waits before it ends the job with
// decision_unanswered (watch.go decisionTimeout).
const DefaultDecisionTimeout = 6 * time.Hour

// ErrJobPaneChanged reports that the job's newest spawned pane is not the pane
// the caller named, so the job's records say nothing about that pane.
var ErrJobPaneChanged = errors.New("job pane changed")

// LaneEventAsksDecision reports whether a lane event asks a question a person
// must answer. The declared notify type is the documented form (`--type
// decision`, and `--type blocked` for a worker stuck on a person); the ESC text
// marker is the form the watcher already recognises. It lives here so the
// watcher and the close guard can share one predicate.
func LaneEventAsksDecision(notifyType, text string) bool {
	return notifyType == "decision" || notifyType == "blocked" || laneDecisionMarker.MatchString(text)
}

// OpenLaneQuestion reports the question pane is still waiting on, decided from
// the node's own records at call time rather than from a copy another process
// wrote earlier.
//
// The job's ledger names the pane and the start of its current attempt, and the
// lane-event files hold what the pane asked. A question is open when the newest
// lane event from that pane since the attempt asks one (LaneEventAsksDecision),
// it is younger than timeout, and no job.decision_resumed follows it. A later
// lane event from the same pane answers it, exactly as the watcher's
// updateDecision decides (watch.go).
//
// An unreadable record is an error, never "no question": a caller that closes
// on this answer must fail closed. A job whose newest spawned pane is not pane
// answers ErrJobPaneChanged, so a reused job id cannot vouch for another pane.
func OpenLaneQuestion(inboxRoot, jobID, pane string, now time.Time, timeout time.Duration) (string, bool, error) {
	if !validID(jobID) || pane == "" {
		return "", false, errors.New("invalid job or pane")
	}
	if timeout <= 0 {
		timeout = DefaultDecisionTimeout
	}
	since, resumedAt, spawned, err := readJobDecisionLedger(filepath.Join(inboxRoot, "jobs", jobID, "events"))
	if err != nil {
		return "", false, err
	}
	// A job whose launch failed before its spawn receipt committed has no
	// job.spawned to compare: the caller's pane is the only identity its records
	// can have, and the hk 3179 no-work path still needs to release it.
	if spawned != "" && spawned != pane {
		return "", false, ErrJobPaneChanged
	}
	latest, err := newestLaneEventFromPane(filepath.Join(inboxRoot, "events-lane"), pane, since)
	if err != nil {
		return "", false, err
	}
	if latest.id == "" || !LaneEventAsksDecision(latest.notifyType, latest.text) {
		return "", false, nil
	}
	if now.Sub(latest.at) >= timeout {
		return "", false, nil
	}
	if !resumedAt.IsZero() && resumedAt.After(latest.at) {
		return "", false, nil
	}
	return latest.id, true, nil
}

// readJobDecisionLedger reads the job's events in ledger order (file-name
// order) and reports the start of the current attempt, the newest
// job.decision_resumed, and the pane the newest job.spawned named.
func readJobDecisionLedger(dir string) (since, resumedAt time.Time, spawned string, err error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		// A job whose launch never wrote an event has no events folder; that
		// is an empty ledger, not an unreadable one, or its pane could never
		// be released (hk 3179).
		if errors.Is(err, fs.ErrNotExist) {
			return time.Time{}, time.Time{}, "", nil
		}
		return time.Time{}, time.Time{}, "", err
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].Name() < entries[j].Name() })
	for _, entry := range entries {
		if !entry.Type().IsRegular() || filepath.Ext(entry.Name()) != ".json" {
			continue
		}
		body, err := os.ReadFile(filepath.Join(dir, entry.Name()))
		if err != nil {
			return time.Time{}, time.Time{}, "", err
		}
		var event struct {
			Kind      string `json:"kind"`
			CreatedAt string `json:"created_at"`
			TS        string `json:"ts"`
			Payload   struct {
				PaneID string `json:"pane_id"`
			} `json:"payload"`
		}
		if err := json.Unmarshal(body, &event); err != nil || event.Kind == "" {
			return time.Time{}, time.Time{}, "", errors.New("unreadable job event")
		}
		at := eventTime(event.CreatedAt, event.TS)
		if at.IsZero() {
			if info, err := entry.Info(); err == nil {
				at = info.ModTime()
			}
		}
		switch event.Kind {
		case "job.spawned":
			since, spawned = at, event.Payload.PaneID
		case "job.reclaim", "job.reprompted":
			since = at
		case "job.decision_resumed":
			resumedAt = at
		}
	}
	return since, resumedAt, spawned, nil
}

// laneQuestion is the newest lane event one pane wrote since an attempt began.
type laneQuestion struct {
	id, notifyType, text string
	at                   time.Time
}

// newestLaneEventFromPane finds the newest lane event pane wrote at or after
// since. The lane-event directory is shared, so every file is read; the same
// size, kind, pane and event-id filters the watcher applies keep a foreign
// file out. A missing directory is no events — a node that never recorded one
// has nothing to answer — while any other unreadable input is an error.
func newestLaneEventFromPane(dir, pane string, since time.Time) (laneQuestion, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return laneQuestion{}, nil
		}
		return laneQuestion{}, err
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].Name() < entries[j].Name() })
	var newest laneQuestion
	for _, entry := range entries {
		info, err := entry.Info()
		if err != nil || !info.Mode().IsRegular() || filepath.Ext(entry.Name()) != ".json" || info.Size() > laneEventMaxBytes {
			continue
		}
		if !since.IsZero() && info.ModTime().Before(since.Truncate(time.Second)) {
			continue
		}
		body, err := os.ReadFile(filepath.Join(dir, entry.Name()))
		if err != nil {
			return laneQuestion{}, err
		}
		var event struct {
			Kind       string `json:"kind"`
			PaneID     string `json:"pane_id"`
			Text       string `json:"text"`
			EventID    string `json:"event_id"`
			NotifyType string `json:"notify_type"`
		}
		if err := json.Unmarshal(body, &event); err != nil {
			return laneQuestion{}, errors.New("unreadable lane event")
		}
		if event.Kind != "lane.event" || event.PaneID != pane || event.EventID == "" {
			continue
		}
		if newest.id != "" && info.ModTime().Before(newest.at) {
			continue
		}
		newest = laneQuestion{id: event.EventID, notifyType: event.NotifyType, text: event.Text, at: info.ModTime()}
	}
	return newest, nil
}

// eventTime is the event's own timestamp, zero when neither field parses.
func eventTime(createdAt, ts string) time.Time {
	for _, value := range []string{createdAt, ts} {
		if t, err := time.Parse(time.RFC3339Nano, value); err == nil {
			return t
		}
	}
	return time.Time{}
}
