package fleet

import (
	"context"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

// recordMergedOutcome writes the one model-outcome record for a spawned job
// whose PR hk reports merged. The trigger is the change record the pane sweep
// already reads, not a new mergewake call. The origin id is derived from the
// job, so a second merged signal is an idempotent resend.
func (a *App) recordMergedOutcome(ctx context.Context, j Job, task string, status TaskStatus, change PRChange) error {
	if task == "" || outcomeExcluded(j) {
		return nil
	}
	pr, head := joinedPR(j)
	if !mergedChangeMatches(change, pr, head) {
		return nil
	}
	store, err := a.benchStore()
	if err != nil {
		return err
	}
	var rec map[string]any
	if jobStore, err := a.jobStore(); err == nil {
		rec, _ = jobStore.Get(ctx, j.ID)
	}
	receipt := j.Receipt
	profile := firstNonempty(j.Profile, str(receipt, "profile"))
	effort := firstNonempty(j.Effort, str(receipt, "effort"))
	model := firstNonempty(str(receipt, "model_id"), str(rec, "model"))
	harness := outcomeHarness(receipt, rec)
	grade, tier := outcomeGrade(status.Title)
	if tier == "" {
		tier = firstNonempty(j.T, "T0")
	}
	rounds := outcomeRounds(j)
	findings := outcomeFindings(rec["result"])
	if findings == nil {
		findings = a.outcomeFindingsFromDisk(j.ID)
	}
	mergedAt, mergedErr := parseManualTime(change.MergedAt)
	openedAt, openedErr := parseManualTime(change.OpenedAt)
	recordedAt := a.Now().UTC()
	var mergeSeconds *float64
	if mergedErr == nil {
		recordedAt = mergedAt.UTC()
		if openedErr == nil && mergedAt.After(openedAt) {
			s := mergedAt.Sub(openedAt).Seconds()
			mergeSeconds = &s
		}
	}
	completed, merged := 1, true
	blockers := 0
	for severity, count := range findings {
		if severeRepFinding(severity) {
			blockers += count
		}
	}
	var criticalPtr, highPtr *int
	if findings != nil {
		critical, high := 0, 0
		for severity, count := range findings {
			switch normalizeRepKey(severity) {
			case "critical", "blocker", "blocking", "p0", "s0":
				critical += count
			case "high", "major", "p1", "s1":
				high += count
			}
		}
		criticalPtr, highPtr = &critical, &high
	}
	var spawnToMerge *int
	if mergeSeconds != nil {
		n := int(*mergeSeconds)
		spawnToMerge = &n
	}
	if spawned, ok := jobSpawnedAt(j); ok && mergedErr == nil && mergedAt.After(spawned) {
		n := int(mergedAt.Sub(spawned).Seconds())
		spawnToMerge = &n
	}
	notes, _ := json.Marshal(map[string]any{
		"source": "panewire", "schema": 1, "job": j.ID, "task": task,
		"profile": profile, "model": model, "effort": effort, "harness": harness,
		"grade": grade, "tier": tier, "rounds": rounds,
		"pr": change.Number, "repo": change.Repo, "head": change.Head,
		"merged_at": change.MergedAt,
	})
	rep := Rep{
		OriginID: stableRepOriginID("panewire-outcome:" + j.ID),
		Profile:  profile, ModelID: model, TaskRef: "hk" + task, Tier: tier,
		Role: outcomeRole(j), Rounds: rounds, BlockersFound: blockers,
		Completed: &completed, RecordedAt: recordedAt.Format(time.RFC3339Nano),
		Effort: effort, Grade: grade, Notes: string(notes),
		FindingsBySeverity: findings, TimeToMergeS: mergeSeconds,
		Merged: &merged, CriticalFindings: criticalPtr, HighFindings: highPtr,
		SpawnToMergeS: spawnToMerge,
	}
	if catalog, err := a.benchCatalog(ctx); err == nil {
		rep.TableGrade = repTableGrade(catalog, profile, effort)
	}
	_, recorded, err := store.AddOutcome(ctx, rep)
	if err != nil {
		return err
	}
	if !recorded {
		return nil
	}
	return nil
}

// joinedPR is the newest PR the job itself joined, which proves the merged
// change belongs to it. A fix pass or reviewer PR for the same task never
// matches without that record.
func jobHasJoinedPR(j Job) bool {
	pr, head := joinedPR(j)
	return pr != "" || head != ""
}

func joinedPR(j Job) (pr, head string) {
	for _, ev := range j.Events {
		if ev.Kind != "job.joined" {
			continue
		}
		if p := str(ev.Payload, "pr"); p != "" {
			pr = p
		}
		if h := str(ev.Payload, "head"); h != "" {
			head = h
		}
	}
	return pr, head
}

func mergedChangeMatches(change PRChange, pr, head string) bool {
	if !strings.EqualFold(change.State, "merged") {
		return false
	}
	if head != "" && change.Head != "" && strings.EqualFold(head, change.Head) {
		return true
	}
	if n := prNumber(pr); n > 0 && n == change.Number {
		return true
	}
	return false
}

func prNumber(pr string) int {
	pr = strings.TrimSuffix(strings.TrimSpace(pr), "/")
	if i := strings.LastIndex(pr, "/pull/"); i >= 0 {
		if n, err := strconv.Atoi(pr[i+6:]); err == nil {
			return n
		}
	}
	if i := strings.LastIndex(pr, "#"); i >= 0 {
		if n, err := strconv.Atoi(pr[i+1:]); err == nil {
			return n
		}
	}
	return 0
}

func outcomeHarness(receipt, rec map[string]any) string {
	if session, ok := receipt["harness_session"].(map[string]any); ok {
		if kind := str(session, "kind"); kind != "" {
			return kind
		}
	}
	return str(rec, "harness")
}

func outcomeGrade(title string) (grade, tier string) {
	if m := taskMarker.FindStringSubmatch(title); m != nil {
		return m[1], m[2]
	}
	return "", ""
}

func jobSpawnedAt(j Job) (time.Time, bool) {
	for _, ev := range j.Events {
		if ev.Kind == "job.spawned" {
			return ev.when, true
		}
	}
	return time.Time{}, false
}

func outcomeRounds(j Job) int {
	rounds := 0
	for _, ev := range j.Events {
		switch ev.Kind {
		case "job.completed":
			rounds++
		case "job.escalate":
			rounds++
		}
	}
	if rounds == 0 {
		rounds = 1
	}
	return rounds
}

func outcomeRole(j Job) string {
	switch strings.ToLower(firstNonempty(j.SpawnRole, j.Role)) {
	case "reviewer", "verify":
		return "verify"
	case "fix":
		return "fix"
	case "orch", "lead":
		return "orch"
	}
	return "impl"
}

// outcomeFindings counts result findings by severity. Unknown is nil, never an
// empty map, so a missing result stays distinguishable from zero findings.
func outcomeFindings(value any) map[string]int {
	switch v := value.(type) {
	case map[string]any:
		return countOutcomeFindings(v["findings"])
	case []any:
		return countOutcomeFindings(v)
	case json.RawMessage:
		return outcomeFindingsBytes(v)
	case string:
		return outcomeFindingsBytes([]byte(v))
	}
	return nil
}

func outcomeFindingsBytes(data []byte) map[string]int {
	var object map[string]any
	if json.Unmarshal(data, &object) != nil {
		return nil
	}
	return countOutcomeFindings(object["findings"])
}

func countOutcomeFindings(value any) map[string]int {
	items, ok := value.([]any)
	if !ok {
		return nil
	}
	out := map[string]int{}
	for _, item := range items {
		finding, ok := item.(map[string]any)
		if !ok {
			continue
		}
		severity := normalizeRepKey(str(finding, "severity"))
		if severity == "" {
			if boolean(finding, "blocking", false) {
				severity = "blocking"
			} else {
				continue
			}
		}
		out[severity]++
	}
	return out
}

func (a *App) outcomeFindingsFromDisk(id string) map[string]int {
	if !validID(id) {
		return nil
	}
	for _, name := range []string{"findings.json", "result.json"} {
		file, err := os.Open(filepath.Join(a.Jobs, id, name))
		if err != nil {
			continue
		}
		data, readErr := io.ReadAll(io.LimitReader(file, 24<<10+1))
		file.Close()
		if readErr != nil || len(data) > 24<<10 {
			continue
		}
		if findings := outcomeFindingsBytes(data); findings != nil {
			return findings
		}
	}
	return nil
}
