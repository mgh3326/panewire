package fleet

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
)

func TestHarnessSessionSpawn(t *testing.T) {
	for _, tc := range []struct{ kind, profile, value, refKind string }{
		{"claude", "sonnet", "claude-id", "id"},
		{"codex", "codex-terra61", "codex-id", "id"},
		{"omp", "omp-opus", "/sessions/omp.jsonl", "path"},
		{"claude", "sonnet", "", ""},
	} {
		t.Run(tc.kind+tc.value, func(t *testing.T) {
			var a *App
			var s Spawn
			var f *fakeRunner
			if tc.kind == "omp" {
				a, s, f = ompSpawnFixture(t)
			} else {
				a, s, f, _ = cmdFixture(t)
				s.Profile, s.Effort = tc.profile, "high"
				q := a.Quota.(*fakeQuota)
				q.GateDo = func(QuotaRequest) (string, error) { return "pool=example profile=" + tc.profile, nil }
				q.LaunchValue = Launch{Profile: tc.profile, ModelID: "gpt-6.1-example", Effort: "high", Grade: "A+", Gate: "default"}
			}
			original := f.Do
			started, submitted := false, false
			f.Do = func(c Command) Result {
				args := strings.Join(c.Args, " ")
				if tc.kind != "omp" {
					switch {
					case strings.HasPrefix(args, "agent start"):
						started = true
						return Result{}
					case started && strings.HasPrefix(args, "pane process-info"):
						return Result{Out: fmt.Sprintf(`{"process_info":{"shell_pid":1,"foreground_processes":[{"pid":2,"argv0":%q}]}}`, tc.kind)}
					case started && strings.HasPrefix(args, "agent prompt"):
						submitted = true
						return Result{}
					case started && (strings.HasPrefix(args, "pane read") || strings.HasPrefix(args, "agent read")):
						return Result{Out: "GPT-6.1 Example high · ~/tree\n"}
					}
				}
				if strings.HasPrefix(args, "agent list") {
					base := original(c)
					if tc.kind != "omp" {
						status := "idle"
						if submitted {
							status = "working"
						}
						base.Out = fmt.Sprintf(`{"agents":[{"pane_id":"w:p1","agent":%q,"agent_status":%q}]}`, tc.kind, status)
					}
					var obj map[string]any
					if err := json.Unmarshal([]byte(base.Out), &obj); err != nil {
						t.Fatal(err)
					}
					rows := obj["agents"].([]any)
					if tc.value != "" {
						rows[0].(map[string]any)["agent_session"] = map[string]string{"agent": tc.kind, "kind": tc.refKind, "value": tc.value, "source": "herdr:" + tc.kind}
					}
					b, _ := json.Marshal(obj)
					base.Out = string(b)
					return base
				}
				return original(c)
			}
			if err := a.Spawn(context.Background(), s); err != nil {
				t.Fatal(err)
			}
			job, err := a.job(s.Job)
			if err != nil {
				t.Fatal(err)
			}
			got, ok := job.Receipt["harness_session"].(map[string]any)
			if !ok {
				t.Fatalf("missing harness_session: %v", job.Receipt)
			}
			id, source := tc.value, "herdr:"+tc.kind
			if id == "" {
				id, source = "unknown", "unavailable"
			}
			if got["kind"] != tc.kind || got["id"] != id || got["source"] != source {
				t.Fatalf("session = %v", got)
			}
		})
	}
}

func TestHarnessSessionMalformedMetadataDoesNotBreakAgentList(t *testing.T) {
	for _, raw := range []string{`"none"`, `42`, `{"agent":"codex","kind":"id","value":123,"source":"herdr:codex"}`} {
		t.Run(raw, func(t *testing.T) {
			agents, err := ParseAgents([]byte(`{"agents":[{"pane_id":"w:p1","agent":"codex","agent_status":"idle","agent_session":` + raw + `}]}`))
			if err != nil || len(agents) != 1 || agents[0].Status != "idle" {
				t.Fatalf("agent list: %+v, %v", agents, err)
			}
			got := FindHarnessSession(agents, "w:p1", "codex")
			if got.ID != "unknown" || got.Source != "unavailable" {
				t.Fatalf("session: %+v", got)
			}
		})
	}
}
