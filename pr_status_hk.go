package panewire

import (
	"encoding/json"
	"regexp"
	"strconv"
	"strings"
	"time"
)

// The default pr.status read is handoffkeep's `GET /v1/changes/one`
// (docs/pr-status.md#sources-and-fallback). This file turns that response into
// the snapshot shape `pr status` and `pr findings` already render, and decides
// whether every section the snapshot needs is fresh enough to trust. The wire
// contract is handoffkeep's, not panewire's, so the response is read leniently:
// a field hk adds later is dropped instead of failing the op.

const (
	prStatusSourceHK = "hk"

	// Closed reasons a hk section is not trusted.
	prStatusSectionMissing = "missing"
	prStatusSectionStale   = "older_than_bound"

	prStatusMaxStale = 8
)

// prStatusHKSections are the sections a snapshot is built from, in the order a
// note names them. findings is not one: open findings are read from threads, and
// the findings section only adds a count of posts that have no thread yet.
var prStatusHKSections = []string{"state", "checks", "threads", "gate"}

// prStatusHKFreshness is how old an open PR's section may be before the node
// reads mergewake instead. state is the one hk's poll rewrites on a cadence, so
// it is the heartbeat and has the short bound; the others are written when
// something changes, so an idle PR may hold an old checks or threads section
// and still be right. A merged or closed PR has no bound: nothing changes it.
var prStatusHKFreshness = map[string]time.Duration{
	"state":   15 * time.Minute,
	"checks":  time.Hour,
	"threads": time.Hour,
	"gate":    time.Hour,
}

// PRStatusSection is one section's freshness as hk records it. Snapshots built
// from hk carry one per section; mergewake snapshots carry none.
type PRStatusSection struct {
	ObservedAt string `json:"observed_at"`
	Source     string `json:"source"`
}

// PRStatusStale names one section the node did not trust and why. The
// timestamps are hk's own; Problem is one of the closed words above.
type PRStatusStale struct {
	Section      string `json:"section"`
	ObservedAt   string `json:"observed_at,omitempty"`
	Source       string `json:"source,omitempty"`
	Problem      string `json:"problem"`
	BoundSeconds int    `json:"bound_seconds,omitempty"`
}

type hkChangeOne struct {
	Change *struct {
		Repo       string `json:"repo"`
		PRNumber   int    `json:"pr_number"`
		PR         string `json:"pr"`
		State      string `json:"state"`
		Draft      bool   `json:"draft"`
		Title      string `json:"title"`
		Author     string `json:"author"`
		BaseRef    string `json:"base_ref"`
		HeadSHA    string `json:"head_sha"`
		BaseSHA    string `json:"base_sha"`
		OpenedAt   string `json:"opened_at"`
		MergedAt   string `json:"merged_at"`
		ObservedAt string `json:"observed_at"`
		Source     string `json:"source"`
	} `json:"change"`
	Gate *struct {
		State     string   `json:"state"`
		HeadSHA   string   `json:"head_sha"`
		Current   *bool    `json:"current"`
		Reasons   []string `json:"reasons"`
		DecidedAt string   `json:"decided_at"`
		Source    string   `json:"source"`
	} `json:"gate"`
	Checks []struct {
		Name       string `json:"name"`
		State      string `json:"state"`
		Conclusion string `json:"conclusion"`
		// Required is null until a mergewake pass that knows branch rules writes
		// it; a webhook write carries only the check itself.
		Required *bool `json:"required"`
		Gone     bool  `json:"gone"`
	} `json:"checks"`
	Threads []struct {
		ThreadID   string `json:"thread_id"`
		URL        string `json:"url"`
		Path       string `json:"path"`
		Line       *int   `json:"line"`
		AuthorKind string `json:"author_kind"`
		Severity   string `json:"severity"`
		Resolved   bool   `json:"resolved"`
		Replied    bool   `json:"replied"`
		Excerpt    string `json:"excerpt"`
	} `json:"threads"`
	Findings []struct {
		ThreadID string `json:"thread_id"`
		Status   string `json:"status"`
	} `json:"findings"`
	Reviews []struct {
		ReviewedHead string `json:"reviewed_head"`
		Pass         int    `json:"pass"`
		Lens         string `json:"lens"`
		Family       string `json:"family"`
	} `json:"reviews"`
	Freshness map[string]*struct {
		ObservedAt string `json:"observed_at"`
		Source     string `json:"source"`
	} `json:"freshness"`
}

// prStatusHKRecord is the bounded projection of hk's response the node returns
// as nodePRStatusHK.Change: the change, the gate and the per-section freshness,
// without the thread excerpts that make the raw response tens of kilobytes.
type prStatusHKRecord struct {
	Change    prStatusHKChange           `json:"change"`
	Gate      *prStatusHKGate            `json:"gate"`
	Freshness map[string]PRStatusSection `json:"freshness"`
}

type prStatusHKChange struct {
	Repo       string `json:"repo"`
	PRNumber   int    `json:"pr_number"`
	State      string `json:"state"`
	Draft      bool   `json:"draft"`
	Title      string `json:"title"`
	Author     string `json:"author"`
	BaseRef    string `json:"base_ref"`
	HeadSHA    string `json:"head_sha"`
	BaseSHA    string `json:"base_sha"`
	OpenedAt   string `json:"opened_at,omitempty"`
	MergedAt   string `json:"merged_at,omitempty"`
	ObservedAt string `json:"observed_at"`
	Source     string `json:"source"`
}

type prStatusHKGate struct {
	State     string   `json:"state"`
	HeadSHA   string   `json:"head_sha"`
	Current   bool     `json:"current"`
	Reasons   []string `json:"reasons"`
	DecidedAt string   `json:"decided_at"`
	Source    string   `json:"source"`
}

var (
	prStatusMarkupPattern = regexp.MustCompile(`<[^>]*(>|$)`)
	prStatusEmphasis      = strings.NewReplacer("**", "", "__", "", "`", "")
)

// prStatusExcerptSummary is the first line of a thread's excerpt that says
// something once HTML and emphasis markers are gone. hk keeps the start of the
// comment, and a bot's comment opens with a marker comment and an icon.
func prStatusExcerptSummary(excerpt string) string {
	for _, line := range strings.Split(excerpt, "\n") {
		line = oneLine(prStatusEmphasis.Replace(prStatusMarkupPattern.ReplaceAllString(line, "")))
		if line != "" {
			return utf8PrefixWithin(line, prStatusMaxSummary)
		}
	}
	return ""
}

func parsedHKTime(value string) (time.Time, bool) {
	at, err := time.Parse(time.RFC3339, value)
	return at, err == nil
}

// prStatusHKTerminal reports a PR that no event can change any more.
func prStatusHKTerminal(state string) bool {
	return state == "merged" || state == "closed"
}

// prStatusHKStaleSections lists every section the snapshot needs that is
// absent, unreadable, or (for an open PR) older than its bound.
func prStatusHKStaleSections(one hkChangeOne, now time.Time) []PRStatusStale {
	var stale []PRStatusStale
	for _, name := range prStatusHKSections {
		entry := one.Freshness[name]
		if entry == nil {
			stale = append(stale, PRStatusStale{Section: name, Problem: prStatusSectionMissing})
			continue
		}
		section := PRStatusStale{Section: name, ObservedAt: utf8PrefixWithin(entry.ObservedAt, prStatusMaxShort), Source: utf8PrefixWithin(entry.Source, prStatusMaxShort)}
		observed, ok := parsedHKTime(entry.ObservedAt)
		if !ok || (name == "gate" && one.Gate == nil) {
			section.Problem = prStatusSectionMissing
			stale = append(stale, section)
			continue
		}
		if bound := prStatusHKFreshness[name]; !prStatusHKTerminal(one.Change.State) && now.Sub(observed) > bound {
			section.Problem, section.BoundSeconds = prStatusSectionStale, int(bound/time.Second)
			stale = append(stale, section)
		}
	}
	return stale
}

// prStatusHKReadable checks that the response is about the PR that was asked
// for and carries what a snapshot cannot do without.
func prStatusHKReadable(one hkChangeOne, args nodePRStatusArgs) bool {
	change := one.Change
	return change != nil && strings.EqualFold(change.Repo, args.Repo) && change.PRNumber == args.PR && change.State != "" && change.HeadSHA != ""
}

func prStatusHKRecordOf(one hkChangeOne) prStatusHKRecord {
	change := one.Change
	record := prStatusHKRecord{
		Change: prStatusHKChange{
			Repo: utf8PrefixWithin(change.Repo, prStatusMaxLocation), PRNumber: change.PRNumber, State: utf8PrefixWithin(change.State, prStatusMaxShort), Draft: change.Draft,
			Title: utf8PrefixWithin(change.Title, prStatusMaxSummary), Author: utf8PrefixWithin(change.Author, prStatusMaxShort), BaseRef: utf8PrefixWithin(change.BaseRef, prStatusMaxShort),
			HeadSHA: utf8PrefixWithin(change.HeadSHA, prStatusMaxShort), BaseSHA: utf8PrefixWithin(change.BaseSHA, prStatusMaxShort),
			OpenedAt: utf8PrefixWithin(change.OpenedAt, prStatusMaxShort), MergedAt: utf8PrefixWithin(change.MergedAt, prStatusMaxShort),
			ObservedAt: utf8PrefixWithin(change.ObservedAt, prStatusMaxShort), Source: utf8PrefixWithin(change.Source, prStatusMaxShort),
		},
		Freshness: map[string]PRStatusSection{},
	}
	if gate := one.Gate; gate != nil {
		record.Gate = &prStatusHKGate{
			State: utf8PrefixWithin(gate.State, prStatusMaxShort), HeadSHA: utf8PrefixWithin(gate.HeadSHA, prStatusMaxShort), Current: gate.Current == nil || *gate.Current,
			Reasons: boundedStrings(gate.Reasons, prStatusMaxReasons, prStatusMaxShort), DecidedAt: utf8PrefixWithin(gate.DecidedAt, prStatusMaxShort), Source: utf8PrefixWithin(gate.Source, prStatusMaxShort),
		}
	}
	for name, entry := range one.Freshness {
		if entry != nil && len(record.Freshness) < prStatusMaxStale*2 {
			record.Freshness[utf8PrefixWithin(name, prStatusMaxShort)] = PRStatusSection{ObservedAt: utf8PrefixWithin(entry.ObservedAt, prStatusMaxShort), Source: utf8PrefixWithin(entry.Source, prStatusMaxShort)}
		}
	}
	return record
}

// prStatusSnapshotFromHK maps a fresh hk record onto the fields mergewake's own
// snapshot has. Three of them are derived, because hk stores facts and not
// mergewake's verdict on them:
//
//   - state is the gate's verdict for an open PR and merged or closed for a
//     finished one, so a merged PR is never reported as ready;
//   - checks are the current head's checks that are required or not yet known to be
//     optional, so a webhook-written row (required unset) is shown, not hidden;
//   - a thread blocks unless a bot wrote it, and findings are the unresolved
//     threads, which is what mergewake's PRRT_ finding ids are.
//
// The gate's reasons stay authoritative for why a PR is blocked.
func prStatusSnapshotFromHK(one hkChangeOne) (PRStatusSnapshot, int) {
	change := one.Change
	snapshot := PRStatusSnapshot{
		Repo: change.Repo, PR: change.PRNumber, Head: change.HeadSHA, GateHead: one.Gate.HeadSHA, State: one.Gate.State,
		Reasons: one.Gate.Reasons, Checks: []PRStatusCheck{}, Review: PRStatusReview{Passes: []PRStatusPass{}}, Findings: []PRStatusFinding{},
		Fresh: json.RawMessage("null"), Source: prStatusSourceHK, Sections: map[string]PRStatusSection{},
	}
	if prStatusHKTerminal(change.State) {
		snapshot.State = change.State
	}
	if snapshot.Reasons == nil {
		snapshot.Reasons = []string{}
	}
	for _, check := range one.Checks {
		if (check.Required == nil || *check.Required) && !check.Gone {
			snapshot.Checks = append(snapshot.Checks, PRStatusCheck{Name: check.Name, State: ghCheckState(check.Conclusion, check.State)})
		}
	}
	for _, review := range one.Reviews {
		if review.ReviewedHead == change.HeadSHA {
			snapshot.Review.Passes = append(snapshot.Review.Passes, PRStatusPass{Pass: review.Pass, Lens: review.Lens, Family: review.Family})
		}
	}
	snapshot.Review.Done = len(snapshot.Review.Passes) > 0
	unresolved := 0
	for _, thread := range one.Threads {
		if thread.Resolved {
			continue
		}
		unresolved++
		blocking := thread.AuthorKind != "bot"
		if blocking {
			snapshot.Threads.Blocking++
		} else {
			snapshot.Threads.Other++
		}
		location := thread.Path
		if thread.Line != nil {
			location += ":" + strconv.Itoa(*thread.Line)
		}
		snapshot.Findings = append(snapshot.Findings, PRStatusFinding{
			ID: thread.ThreadID, Location: location, Severity: thread.Severity, Blocking: blocking,
			Summary: prStatusExcerptSummary(thread.Excerpt), URL: thread.URL, AuthorReplied: thread.Replied,
		})
	}
	snapshot.Threads.Unresolved = &unresolved
	unposted := 0
	for _, finding := range one.Findings {
		if finding.ThreadID == "" && (finding.Status == "" || finding.Status == "open") {
			unposted++
		}
	}
	oldest := ""
	var oldestAt time.Time
	for _, name := range prStatusHKSections {
		entry := one.Freshness[name]
		snapshot.Sections[name] = PRStatusSection{ObservedAt: entry.ObservedAt, Source: entry.Source}
		if at, _ := parsedHKTime(entry.ObservedAt); oldest == "" || at.Before(oldestAt) {
			oldest, oldestAt = entry.ObservedAt, at
		}
	}
	if entry := one.Freshness["findings"]; entry != nil {
		snapshot.Sections["findings"] = PRStatusSection{ObservedAt: entry.ObservedAt, Source: entry.Source}
	}
	snapshot.ObservedAt = oldest
	return boundPRStatusSnapshot(snapshot), unposted
}
