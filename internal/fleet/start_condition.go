package fleet

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"
)

type repeatedStringFlag []string

func (f *repeatedStringFlag) String() string { return strings.Join(*f, ",") }
func (f *repeatedStringFlag) Set(value string) error {
	*f = append(*f, value)
	return nil
}

// StartCondition is one parsed --after predicate. TargetTaskID is set only for
// blockers predicates and identifies the task whose blocking tasks are
// followed.
type StartCondition struct {
	Kind         string    `json:"kind"`
	TaskID       string    `json:"task_id,omitempty"`
	State        string    `json:"state,omitempty"`
	At           time.Time `json:"at,omitempty"`
	TargetTaskID string    `json:"target_task_id,omitempty"`
}

// TaskStateSnapshotChunk is the most task ids handoffkeep's snapshot route
// accepts in one call; a read over more ids is split into calls of this size.
const TaskStateSnapshotChunk = 200

// TaskState is one task's row in a batched handoffkeep snapshot read: its
// state and the open tasks that block it.
type TaskState struct {
	State    string
	Blockers []string
}

// TaskStateSnapshot is one snapshot of the tasks a batch names. Every
// requested id is in Tasks or Missing.
type TaskStateSnapshot struct {
	Tasks   map[string]TaskState
	Missing []string
}

type StartConditionResult struct {
	Status  string   `json:"status"`
	Reasons []string `json:"reasons,omitempty"`
	Reason  string   `json:"reason,omitempty"`
}

// ParseStartConditions validates and canonicalizes repeatable --after values.
func ParseStartConditions(values []string, taskID string) ([]StartCondition, error) {
	conditions := make([]StartCondition, 0, len(values))
	for _, value := range values {
		var condition StartCondition
		switch {
		case strings.HasPrefix(value, "task:"):
			id, state, ok := strings.Cut(strings.TrimPrefix(value, "task:"), "=")
			if !ok || !numericTaskID(id) || !validTargetState(state) {
				return nil, fmt.Errorf("invalid --after %q: expected task:N=merged|deployed|done", value)
			}
			condition = StartCondition{Kind: "task", TaskID: id, State: state}
		case strings.HasPrefix(value, "blockers"):
			state := "merged"
			if strings.HasPrefix(value, "blockers=") {
				state = strings.TrimPrefix(value, "blockers=")
			} else if value != "blockers" {
				return nil, fmt.Errorf("invalid --after %q: expected blockers[=merged|deployed]", value)
			}
			if state != "merged" && state != "deployed" {
				return nil, fmt.Errorf("invalid --after %q: expected blockers[=merged|deployed]", value)
			}
			if !numericTaskID(taskID) {
				return nil, fmt.Errorf("--after %q requires --task TASK_ID", value)
			}
			condition = StartCondition{Kind: "blockers", State: state, TargetTaskID: taskID}
		case strings.HasPrefix(value, "time:"):
			at, err := time.Parse(time.RFC3339, strings.TrimPrefix(value, "time:"))
			if err != nil {
				return nil, fmt.Errorf("invalid --after %q: expected time:RFC3339", value)
			}
			condition = StartCondition{Kind: "time", At: at.UTC()}
		default:
			return nil, fmt.Errorf("invalid --after %q: expected task:N=merged|deployed|done, blockers[=merged|deployed], or time:RFC3339", value)
		}
		conditions = append(conditions, condition)
	}
	return conditions, nil
}

func numericTaskID(id string) bool {
	if id == "" || id[0] == '0' {
		return false
	}
	for _, digit := range id {
		if digit < '0' || digit > '9' {
			return false
		}
	}
	_, err := strconv.ParseInt(id, 10, 64)
	return err == nil
}

func validTargetState(state string) bool {
	return state == "merged" || state == "deployed" || state == "done"
}

func startConditionStateRank(state string) int {
	switch state {
	case "merged":
		return 1
	case "deployed":
		return 2
	case "completed":
		return 3
	default:
		return 0
	}
}

func startConditionTargetRank(state string) int {
	if state == "done" {
		return startConditionStateRank("completed")
	}
	return startConditionStateRank(state)
}

func startConditionsEqual(a, b []StartCondition) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i].Kind != b[i].Kind || a[i].TaskID != b[i].TaskID || a[i].State != b[i].State || a[i].At.Equal(b[i].At) == false || a[i].TargetTaskID != b[i].TargetTaskID {
			return false
		}
	}
	return true
}

// ValidateStartConditions checks the stored typed form against the same
// grammar as the CLI parser.
func ValidateStartConditions(conditions []StartCondition, taskID string) bool {
	values := make([]string, len(conditions))
	for i, condition := range conditions {
		switch condition.Kind {
		case "task":
			values[i] = "task:" + condition.TaskID + "=" + condition.State
		case "blockers":
			if condition.TargetTaskID != taskID {
				return false
			}
			if condition.State == "merged" {
				values[i] = "blockers"
			} else {
				values[i] = "blockers=" + condition.State
			}
		case "time":
			values[i] = "time:" + condition.At.Format(time.RFC3339Nano)
		default:
			return false
		}
	}
	parsed, err := ParseStartConditions(values, taskID)
	return err == nil && startConditionsEqual(parsed, conditions)
}

// StartConditionTaskIDs lists the task ids a batch of predicates names
// directly: each task predicate's id and each blockers predicate's target.
// Blocker ids are added by the caller after the first read.
func StartConditionTaskIDs(requests [][]StartCondition) []string {
	seen := make(map[string]bool)
	for _, conditions := range requests {
		for _, condition := range conditions {
			switch condition.Kind {
			case "task":
				seen[condition.TaskID] = true
			case "blockers":
				seen[condition.TargetTaskID] = true
			}
		}
	}
	ids := make([]string, 0, len(seen))
	for id := range seen {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids
}

// EvaluateStartConditionSnapshot is pure: a referenced task that names no task
// is a refusal, a dropped or duplicate one cancels the request, and otherwise
// the request waits until every predicate holds.
func EvaluateStartConditionSnapshot(conditions []StartCondition, snapshot TaskStateSnapshot, now time.Time) StartConditionResult {
	missing := make(map[string]bool, len(snapshot.Missing))
	for _, id := range snapshot.Missing {
		missing[id] = true
	}
	referenced := func(id string) (TaskState, *StartConditionResult) {
		if missing[id] {
			return TaskState{}, &StartConditionResult{Status: "refused", Reason: "task:" + id + "=missing"}
		}
		task, ok := snapshot.Tasks[id]
		if !ok {
			return TaskState{}, &StartConditionResult{Status: "waiting", Reasons: []string{"start_condition_incomplete"}}
		}
		if task.State == "dropped" || task.State == "duplicate" {
			return TaskState{}, &StartConditionResult{Status: "cancelled", Reason: "task:" + id + "=" + task.State}
		}
		return task, nil
	}
	waiting := make([]string, 0)
	for _, condition := range conditions {
		switch condition.Kind {
		case "time":
			if now.Before(condition.At) {
				waiting = append(waiting, "time:"+condition.At.Format(time.RFC3339Nano))
			}
		case "task":
			task, stop := referenced(condition.TaskID)
			if stop != nil {
				return *stop
			}
			if startConditionStateRank(task.State) < startConditionTargetRank(condition.State) {
				waiting = append(waiting, "task:"+condition.TaskID+"="+condition.State)
			}
		case "blockers":
			target, stop := referenced(condition.TargetTaskID)
			if stop != nil {
				return *stop
			}
			for _, id := range target.Blockers {
				blocker, stop := referenced(id)
				if stop != nil {
					return *stop
				}
				if startConditionStateRank(blocker.State) < startConditionTargetRank(condition.State) {
					waiting = append(waiting, "task:"+id+"="+condition.State)
				}
			}
		}
	}
	if len(waiting) != 0 {
		return StartConditionResult{Status: "waiting", Reasons: waiting}
	}
	return StartConditionResult{Status: "met"}
}

// ReadTaskStateSnapshot reads ids through handoffkeep's snapshot route in one
// pass of calls of at most TaskStateSnapshotChunk ids. Every id lands in Tasks
// or Missing; a call that fails fails the read, so a caller never decides a
// condition from a partial answer. The read is one consistent snapshot per
// call and hk rejects more than TaskStateSnapshotChunk ids rather than
// truncating them.
func ReadTaskStateSnapshot(ctx context.Context, ids []string) (TaskStateSnapshot, error) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	seen := make(map[string]bool, len(ids))
	for _, id := range ids {
		seen[id] = true
	}
	ordered := make([]string, 0, len(seen))
	for id := range seen {
		ordered = append(ordered, id)
	}
	sort.Strings(ordered)
	out := TaskStateSnapshot{Tasks: make(map[string]TaskState, len(ordered))}
	if len(ordered) == 0 {
		return out, nil
	}
	c, err := newHKClient()
	if err != nil {
		return TaskStateSnapshot{}, err
	}
	for start := 0; start < len(ordered); start += TaskStateSnapshotChunk {
		chunk := ordered[start:min(start+TaskStateSnapshotChunk, len(ordered))]
		numeric := make([]int64, len(chunk))
		for i, id := range chunk {
			value, err := strconv.ParseInt(id, 10, 64)
			if err != nil || value < 1 {
				return TaskStateSnapshot{}, fmt.Errorf("invalid task id %q", id)
			}
			numeric[i] = value
		}
		snapshot, err := c.TaskStatesSnapshot(ctx, numeric, "")
		if err != nil {
			return TaskStateSnapshot{}, err
		}
		for _, entry := range snapshot.Tasks {
			blockers := make([]string, len(entry.Blockers))
			for i, blocker := range entry.Blockers {
				blockers[i] = strconv.FormatInt(blocker, 10)
			}
			out.Tasks[strconv.FormatInt(entry.ID, 10)] = TaskState{State: entry.State, Blockers: blockers}
		}
		for _, id := range snapshot.Missing {
			out.Missing = append(out.Missing, strconv.FormatInt(id, 10))
		}
	}
	sort.Strings(out.Missing)
	return out, nil
}
