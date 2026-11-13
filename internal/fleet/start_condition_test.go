package fleet

import (
	"context"
	"encoding/json"
	"testing"
	"time"
)

func TestParseStartConditions(t *testing.T) {
	for _, tc := range []struct {
		value string
		task  string
		kind  string
	}{
		{"task:42=merged", "", "task"},
		{"task:42=deployed", "", "task"},
		{"task:42=done", "", "task"},
		{"blockers", "77", "blockers"},
		{"blockers=merged", "77", "blockers"},
		{"blockers=deployed", "77", "blockers"},
		{"time:2026-11-14T09:00:00+09:00", "", "time"},
	} {
		t.Run(tc.value, func(t *testing.T) {
			conditions, err := ParseStartConditions([]string{tc.value}, tc.task)
			if err != nil || len(conditions) != 1 || conditions[0].Kind != tc.kind {
				t.Fatalf("ParseStartConditions() = %#v, %v", conditions, err)
			}
		})
	}
	for _, value := range []string{"task:0=merged", "task:+42=merged", "task:x=merged", "task:42=closed", "blockers=done", "blockers=merged", "time:yesterday", "other"} {
		t.Run("invalid/"+value, func(t *testing.T) {
			task := ""
			if value == "blockers=merged" {
				task = ""
			}
			if _, err := ParseStartConditions([]string{value}, task); err == nil {
				t.Fatalf("ParseStartConditions(%q) succeeded", value)
			}
		})
	}
}

func TestParseSpawnAfterRepeatableAndUsageErrors(t *testing.T) {
	spawn, err := parseSpawn([]string{"--after", "task:42=merged", "--after", "blockers=deployed", "--task", "77"})
	if err != nil || len(spawn.After) != 2 || spawn.After[0] != "task:42=merged" || spawn.After[1] != "blockers=deployed" {
		t.Fatalf("parseSpawn() = %#v, %v", spawn, err)
	}
	for _, args := range [][]string{
		{"--after", "task:42=closed"},
		{"--after", "time:nope"},
		{"--after", "blockers"},
	} {
		if _, err := parseSpawn(args); exitCode(err) != exitUsage {
			t.Fatalf("parseSpawn(%q) error = %v, want usage error", args, err)
		}
	}
}

func TestReadStartConditionTasksDeduplicatesAndLoadsIncomingBlockers(t *testing.T) {
	hk := useFakeHK(t)
	hk.SetTask("77", `{"id":77,"state":"ready","relations":[{"kind":"blocks","src_id":42,"dst_id":77}]}`)
	hk.SetTask("42", `{"id":42,"state":"merged","relations":[]}`)
	conditions := []StartCondition{
		{Kind: "blockers", TargetTaskID: "77", State: "merged"},
		{Kind: "task", TaskID: "77", State: "done"},
	}
	snapshots, err := ReadStartConditionTasks(context.Background(), conditions)
	if err != nil {
		t.Fatal(err)
	}
	if len(snapshots) != 2 || snapshots["77"].Relations[0].SrcID != "42" || snapshots["77"].Relations[0].DstID != "77" {
		t.Fatalf("snapshots = %#v", snapshots)
	}
	routes := hk.Routes()
	if len(routes) != 2 || routes[0] != "GET /v1/tasks/77" || routes[1] != "GET /v1/tasks/42" {
		t.Fatalf("routes = %q, want deduplicated target and blocker reads", routes)
	}
}

func TestReadStartConditionTaskBatchKeepsIndependentReadsAndOnlyFollowsBlockers(t *testing.T) {
	hk := useFakeHK(t)
	hk.SetTask("42", `{"id":42,"state":"deployed","relations":[{"kind":"blocks","src_id":99,"dst_id":42}]}`)
	hk.Handle(func(req hkRequest) (int, string, bool) {
		if req.Path == "/v1/tasks/404" {
			return 404, `{"error":"not_found"}`, true
		}
		return 0, "", false
	})
	snapshots, err := ReadStartConditionTaskBatch(context.Background(), [][]StartCondition{
		{{Kind: "task", TaskID: "42", State: "merged"}},
		{{Kind: "task", TaskID: "404", State: "merged"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if got := EvaluateStartConditions([]StartCondition{{Kind: "task", TaskID: "42", State: "merged"}}, snapshots, time.Now()); got.Status != "met" {
		t.Fatalf("independent successful read = %#v, want met", got)
	}
	if got := EvaluateStartConditions([]StartCondition{{Kind: "task", TaskID: "404", State: "merged"}}, snapshots, time.Now()); got.Status != "waiting" || len(got.Reasons) != 1 || got.Reasons[0] != "hk_unavailable" {
		t.Fatalf("failed dependency = %#v, want waiting/hk_unavailable", got)
	}
	routes := hk.Routes()
	if len(routes) != 2 || routes[0] != "GET /v1/tasks/404" || routes[1] != "GET /v1/tasks/42" {
		t.Fatalf("task predicates unexpectedly followed relations: %q", routes)
	}
}

func TestEvaluateStartConditions(t *testing.T) {
	now := time.Date(2026, 11, 13, 0, 0, 0, 0, time.UTC)
	blocker := StartCondition{Kind: "blockers", TargetTaskID: "77", State: "merged"}
	blockedBy := TaskSnapshot{ID: "77", State: "ready", Relations: []TaskRelation{{Kind: "blocks", SrcID: "42", DstID: "77"}}}
	for _, tc := range []struct {
		name       string
		conditions []StartCondition
		tasks      map[string]TaskSnapshot
		want       string
	}{
		{"met", []StartCondition{{Kind: "task", TaskID: "42", State: "merged"}}, map[string]TaskSnapshot{"42": {ID: "42", State: "merged"}}, "met"},
		{"deployed satisfies merged", []StartCondition{{Kind: "task", TaskID: "42", State: "merged"}}, map[string]TaskSnapshot{"42": {ID: "42", State: "deployed"}}, "met"},
		{"completed satisfies deployed", []StartCondition{{Kind: "task", TaskID: "42", State: "deployed"}}, map[string]TaskSnapshot{"42": {ID: "42", State: "completed"}}, "met"},
		{"done maps to completed", []StartCondition{{Kind: "task", TaskID: "42", State: "done"}}, map[string]TaskSnapshot{"42": {ID: "42", State: "completed"}}, "met"},
		{"AND met", []StartCondition{{Kind: "task", TaskID: "42", State: "merged"}, {Kind: "time", At: now}}, map[string]TaskSnapshot{"42": {ID: "42", State: "merged"}}, "met"},
		{"AND waits", []StartCondition{{Kind: "task", TaskID: "42", State: "merged"}, {Kind: "time", At: now.Add(time.Hour)}}, map[string]TaskSnapshot{"42": {ID: "42", State: "merged"}}, "waiting"},
		{"waiting", []StartCondition{{Kind: "task", TaskID: "42", State: "merged"}}, map[string]TaskSnapshot{"42": {ID: "42", State: "ready"}}, "waiting"},
		{"cancelled dropped", []StartCondition{{Kind: "task", TaskID: "42", State: "merged"}}, map[string]TaskSnapshot{"42": {ID: "42", State: "dropped"}}, "cancelled"},
		{"cancelled duplicate", []StartCondition{{Kind: "task", TaskID: "42", State: "merged"}}, map[string]TaskSnapshot{"42": {ID: "42", State: "duplicate"}}, "cancelled"},
		{"hk unavailable", []StartCondition{{Kind: "task", TaskID: "42", State: "merged"}}, nil, "waiting"},
		{"relation counts", []StartCondition{blocker}, map[string]TaskSnapshot{"77": blockedBy, "42": {ID: "42", State: "merged"}}, "met"},
		{"completed blocker satisfies deployed", []StartCondition{{Kind: "blockers", TargetTaskID: "77", State: "deployed"}}, map[string]TaskSnapshot{"77": blockedBy, "42": {ID: "42", State: "completed"}}, "met"},
		{"relation added later counts", []StartCondition{blocker}, map[string]TaskSnapshot{"77": blockedBy, "42": {ID: "42", State: "developing"}}, "waiting"},
		{"time waiting", []StartCondition{{Kind: "time", At: now.Add(time.Hour)}}, map[string]TaskSnapshot{}, "waiting"},
		{"time met", []StartCondition{{Kind: "time", At: now}}, map[string]TaskSnapshot{}, "met"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := EvaluateStartConditions(tc.conditions, tc.tasks, now)
			if got.Status != tc.want {
				t.Fatalf("EvaluateStartConditions() = %#v, want status %q", got, tc.want)
			}
			if tc.name == "hk unavailable" && (len(got.Reasons) != 1 || got.Reasons[0] != "hk_unavailable") {
				t.Fatalf("missing unavailable reason: %#v", got)
			}
		})
	}
}

func TestSubmissionStartConditionsRoundTripAndLegacyJSON(t *testing.T) {
	legacy := Submission{RequestID: "11111111-1111-4111-8111-111111111111", Args: []string{"--task", "42"}, Brief: "brief"}
	raw, err := json.Marshal(legacy)
	if err != nil {
		t.Fatal(err)
	}
	if string(raw) != `{"request_id":"11111111-1111-4111-8111-111111111111","args":["--task","42"],"brief":"brief","repository":{"name":"","origin":"","relative":"","worktree":"","local_root":""},"origin_machine":"","deadline_seconds":0}` {
		t.Fatalf("legacy submission JSON changed: %s", raw)
	}
	conditions, err := ParseStartConditions([]string{"task:42=merged", "time:2026-11-14T00:00:00Z"}, "")
	if err != nil {
		t.Fatal(err)
	}
	request := legacy
	request.StartConditions = conditions
	raw, err = json.Marshal(request)
	if err != nil {
		t.Fatal(err)
	}
	var decoded Submission
	if err := json.Unmarshal(raw, &decoded); err != nil {
		t.Fatal(err)
	}
	if !startConditionsEqual(decoded.StartConditions, conditions) {
		t.Fatalf("round trip = %#v, want %#v", decoded.StartConditions, conditions)
	}
}
