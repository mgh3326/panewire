package fleet

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/mgh3326/handoffkeep/client"
)

type repeatedStringFlag []string

func (f *repeatedStringFlag) String() string { return strings.Join(*f, ",") }
func (f *repeatedStringFlag) Set(value string) error {
	*f = append(*f, value)
	return nil
}

// StartCondition is one parsed --after predicate. TargetTaskID is set only for
// blockers predicates and identifies the task whose incoming blocks relations
// are followed.
type StartCondition struct {
	Kind         string    `json:"kind"`
	TaskID       string    `json:"task_id,omitempty"`
	State        string    `json:"state,omitempty"`
	At           time.Time `json:"at,omitempty"`
	TargetTaskID string    `json:"target_task_id,omitempty"`
}

type TaskRelation struct {
	Kind  string `json:"kind"`
	SrcID string `json:"src_id"`
	DstID string `json:"dst_id"`
}

type TaskSnapshot struct {
	ID        string         `json:"id"`
	State     string         `json:"state"`
	Relations []TaskRelation `json:"relations,omitempty"`
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

// EvaluateStartConditions is pure: every referenced snapshot must be present;
// missing reads fail closed as waiting/hk_unavailable.
func EvaluateStartConditions(conditions []StartCondition, tasks map[string]TaskSnapshot, now time.Time) StartConditionResult {
	waiting := make([]string, 0)
	for _, condition := range conditions {
		var ids []string
		if condition.Kind == "task" {
			ids = []string{condition.TaskID}
		}
		if condition.Kind == "blockers" {
			ids = []string{condition.TargetTaskID}
		}
		for _, id := range ids {
			task, ok := tasks[id]
			if !ok {
				return StartConditionResult{Status: "waiting", Reasons: []string{"hk_unavailable"}}
			}
			if task.State == "dropped" || task.State == "duplicate" {
				return StartConditionResult{Status: "cancelled", Reason: "task:" + id + "=" + task.State}
			}
		}
		switch condition.Kind {
		case "time":
			if now.Before(condition.At) {
				waiting = append(waiting, "time:"+condition.At.Format(time.RFC3339Nano))
			}
		case "task":
			if tasks[condition.TaskID].State != condition.State {
				waiting = append(waiting, "task:"+condition.TaskID+"="+condition.State)
			}
		case "blockers":
			target := tasks[condition.TargetTaskID]
			for _, relation := range target.Relations {
				if relation.Kind != "blocks" || relation.DstID != condition.TargetTaskID {
					continue
				}
				blocker, ok := tasks[relation.SrcID]
				if !ok {
					return StartConditionResult{Status: "waiting", Reasons: []string{"hk_unavailable"}}
				}
				if blocker.State == "dropped" || blocker.State == "duplicate" {
					return StartConditionResult{Status: "cancelled", Reason: "task:" + blocker.ID + "=" + blocker.State}
				}
				if blocker.State != condition.State {
					waiting = append(waiting, "task:"+blocker.ID+"="+condition.State)
				}
			}
		}
	}
	if len(waiting) != 0 {
		return StartConditionResult{Status: "waiting", Reasons: waiting}
	}
	return StartConditionResult{Status: "met"}
}

// ReadStartConditionTasks deduplicates target ids, reads their current
// relations, then fetches every incoming blocker within one bounded pass.
func ReadStartConditionTasks(ctx context.Context, conditions []StartCondition) (map[string]TaskSnapshot, error) {
	return ReadStartConditionTaskBatch(ctx, [][]StartCondition{conditions})
}

// ReadStartConditionTaskBatch resolves predicates for multiple queued
// requests together, deduplicating every task id across the batch.
func ReadStartConditionTaskBatch(ctx context.Context, requests [][]StartCondition) (map[string]TaskSnapshot, error) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	ids := make(map[string]bool)
	for _, conditions := range requests {
		for _, condition := range conditions {
			if condition.Kind == "task" {
				ids[condition.TaskID] = true
			}
			if condition.Kind == "blockers" {
				ids[condition.TargetTaskID] = true
			}
		}
	}
	snapshots := make(map[string]TaskSnapshot, len(ids))
	for id := range ids {
		snapshot, err := readStartConditionTask(ctx, id)
		if err != nil {
			return nil, err
		}
		snapshots[id] = snapshot
	}
	for _, snapshot := range snapshots {
		for _, relation := range snapshot.Relations {
			if relation.Kind == "blocks" && ids[relation.DstID] {
				ids[relation.SrcID] = true
			}
		}
	}
	for id := range ids {
		if _, ok := snapshots[id]; ok {
			continue
		}
		snapshot, err := readStartConditionTask(ctx, id)
		if err != nil {
			return nil, err
		}
		snapshots[id] = snapshot
	}
	return snapshots, nil
}

func readStartConditionTask(ctx context.Context, taskID string) (TaskSnapshot, error) {
	id, err := strconv.ParseInt(taskID, 10, 64)
	if err != nil || id < 1 {
		return TaskSnapshot{}, fmt.Errorf("invalid task id")
	}
	body, answered, err := hkAnswer(func(c client.Client) error {
		_, found, callErr := c.GetTask(ctx, id)
		if callErr == nil && !found {
			return errHKNotFound
		}
		return callErr
	})
	if err != nil || !answered {
		return TaskSnapshot{}, fmt.Errorf("handoffkeep task read unavailable")
	}
	var task struct {
		State     string `json:"state"`
		Relations []struct {
			Kind  string `json:"kind"`
			SrcID int64  `json:"src_id"`
			DstID int64  `json:"dst_id"`
		} `json:"relations"`
	}
	if json.Unmarshal(body, &task) != nil || task.State == "" {
		return TaskSnapshot{}, fmt.Errorf("handoffkeep task response unreadable")
	}
	relations := make([]TaskRelation, 0, len(task.Relations))
	for _, relation := range task.Relations {
		relations = append(relations, TaskRelation{Kind: relation.Kind, SrcID: strconv.FormatInt(relation.SrcID, 10), DstID: strconv.FormatInt(relation.DstID, 10)})
	}
	return TaskSnapshot{ID: taskID, State: task.State, Relations: relations}, nil
}
