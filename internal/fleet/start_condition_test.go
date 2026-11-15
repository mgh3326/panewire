package fleet

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
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

// snapshotFakeHK answers hk's snapshot route with a task per requested id and
// names ids in missing instead of dropping them, as the live route does.
func snapshotFakeHK(t *testing.T, states map[string]string, missing map[string]bool) *spawnHK {
	t.Helper()
	hk := useFakeHK(t)
	hk.Handle(func(req hkRequest) (int, string, bool) {
		if req.Path != "/v1/tasks/states/snapshot" {
			return 0, "", false
		}
		query, err := url.ParseQuery(req.Query)
		if err != nil {
			return 400, `{"error":"invalid_context"}`, true
		}
		var tasks, absent []string
		for _, id := range strings.Split(query.Get("ids"), ",") {
			if missing[id] {
				absent = append(absent, id)
				continue
			}
			state, ok := states[id]
			if !ok {
				state = "ready"
			}
			tasks = append(tasks, fmt.Sprintf(`{"id":%s,"state":%q,"category":"open","blockers":[]}`, id, state))
		}
		return http.StatusOK, fmt.Sprintf(`{"as_of":"2026-11-15T00:00:00Z","tasks":[%s],"missing":[%s]}`, strings.Join(tasks, ","), strings.Join(absent, ",")), true
	})
	return hk
}

func TestReadTaskStateSnapshotChunksAndNamesMissing(t *testing.T) {
	ids := make([]string, 250)
	states := make(map[string]string, len(ids))
	for i := range ids {
		ids[i] = strconv.Itoa(1000 + i)
		states[ids[i]] = "merged"
	}
	snapshotFakeHK(t, states, map[string]bool{"9999": true})
	ids = append(ids, "9999", "1000")
	hk := useFakeHK(t)

	got, err := ReadTaskStateSnapshot(context.Background(), ids)
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Tasks) != 250 || got.Tasks["1000"].State != "merged" {
		t.Fatalf("snapshot tasks = %d, want every requested task once", len(got.Tasks))
	}
	if len(got.Missing) != 1 || got.Missing[0] != "9999" {
		t.Fatalf("snapshot missing = %v, want the one absent id", got.Missing)
	}
	routes := hk.Routes()
	if len(routes) != 2 || routes[0] != "GET /v1/tasks/states/snapshot" || routes[1] != routes[0] {
		t.Fatalf("routes = %q, want two snapshot calls for 251 ids", routes)
	}
}

func TestEvaluateStartConditionSnapshot(t *testing.T) {
	now := time.Date(2026, 11, 13, 0, 0, 0, 0, time.UTC)
	blocker := StartCondition{Kind: "blockers", TargetTaskID: "77", State: "merged"}
	blockedBy := TaskState{State: "ready", Blockers: []string{"42"}}
	for _, tc := range []struct {
		name       string
		conditions []StartCondition
		snapshot   TaskStateSnapshot
		want       string
		wantReason string
	}{
		{"met", []StartCondition{{Kind: "task", TaskID: "42", State: "merged"}}, TaskStateSnapshot{Tasks: map[string]TaskState{"42": {State: "merged"}}}, "met", ""},
		{"deployed satisfies merged", []StartCondition{{Kind: "task", TaskID: "42", State: "merged"}}, TaskStateSnapshot{Tasks: map[string]TaskState{"42": {State: "deployed"}}}, "met", ""},
		{"completed satisfies deployed", []StartCondition{{Kind: "task", TaskID: "42", State: "deployed"}}, TaskStateSnapshot{Tasks: map[string]TaskState{"42": {State: "completed"}}}, "met", ""},
		{"done maps to completed", []StartCondition{{Kind: "task", TaskID: "42", State: "done"}}, TaskStateSnapshot{Tasks: map[string]TaskState{"42": {State: "completed"}}}, "met", ""},
		{"AND met", []StartCondition{{Kind: "task", TaskID: "42", State: "merged"}, {Kind: "time", At: now}}, TaskStateSnapshot{Tasks: map[string]TaskState{"42": {State: "merged"}}}, "met", ""},
		{"AND waits", []StartCondition{{Kind: "task", TaskID: "42", State: "merged"}, {Kind: "time", At: now.Add(time.Hour)}}, TaskStateSnapshot{Tasks: map[string]TaskState{"42": {State: "merged"}}}, "waiting", ""},
		{"waiting", []StartCondition{{Kind: "task", TaskID: "42", State: "merged"}}, TaskStateSnapshot{Tasks: map[string]TaskState{"42": {State: "ready"}}}, "waiting", ""},
		{"cancelled dropped", []StartCondition{{Kind: "task", TaskID: "42", State: "merged"}}, TaskStateSnapshot{Tasks: map[string]TaskState{"42": {State: "dropped"}}}, "cancelled", "task:42=dropped"},
		{"cancelled duplicate", []StartCondition{{Kind: "task", TaskID: "42", State: "merged"}}, TaskStateSnapshot{Tasks: map[string]TaskState{"42": {State: "duplicate"}}}, "cancelled", "task:42=duplicate"},
		{"missing is a refusal", []StartCondition{{Kind: "task", TaskID: "42", State: "merged"}}, TaskStateSnapshot{Missing: []string{"42"}}, "refused", "task:42=missing"},
		{"incomplete read waits", []StartCondition{{Kind: "task", TaskID: "42", State: "merged"}}, TaskStateSnapshot{}, "waiting", ""},
		{"relation counts", []StartCondition{blocker}, TaskStateSnapshot{Tasks: map[string]TaskState{"77": blockedBy, "42": {State: "merged"}}}, "met", ""},
		{"completed blocker satisfies deployed", []StartCondition{{Kind: "blockers", TargetTaskID: "77", State: "deployed"}}, TaskStateSnapshot{Tasks: map[string]TaskState{"77": blockedBy, "42": {State: "completed"}}}, "met", ""},
		{"unmet blocker waits", []StartCondition{blocker}, TaskStateSnapshot{Tasks: map[string]TaskState{"77": blockedBy, "42": {State: "ready"}}}, "waiting", ""},
		{"missing blocker is a refusal", []StartCondition{blocker}, TaskStateSnapshot{Tasks: map[string]TaskState{"77": blockedBy}, Missing: []string{"42"}}, "refused", "task:42=missing"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := EvaluateStartConditionSnapshot(tc.conditions, tc.snapshot, now)
			if got.Status != tc.want {
				t.Fatalf("EvaluateStartConditionSnapshot() = %#v, want %s", got, tc.want)
			}
			if tc.wantReason != "" && got.Reason != tc.wantReason {
				t.Fatalf("reason = %q, want %q", got.Reason, tc.wantReason)
			}
		})
	}
}

func TestStartConditionTaskIDsDeduplicates(t *testing.T) {
	ids := StartConditionTaskIDs([][]StartCondition{
		{{Kind: "task", TaskID: "42", State: "merged"}, {Kind: "time", At: time.Now()}},
		{{Kind: "task", TaskID: "7", State: "deployed"}, {Kind: "blockers", TargetTaskID: "77", State: "merged"}},
	})
	sort.Strings(ids)
	if strings.Join(ids, ",") != "42,7,77" {
		t.Fatalf("ids = %v", ids)
	}
}
