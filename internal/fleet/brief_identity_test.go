package fleet

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestBriefIdentityMatchesSpawnReceipt(t *testing.T) {
	for _, tc := range []struct{ name, profile, canon, family, kind string }{
		{"codex", "codex-terra61", "codex-terra61", "openai", "codex"},
		{"claude", "sonnet", "sonnet", "anthropic", "claude"},
		{"unknown-family", "sonnet", "sonnet", "", "claude"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			a, s, f, _ := cmdFixture(t)
			s.Profile = tc.profile
			s.Task = "3113"
			s.Effort = "high"
			seat := a.Policy.Profiles[s.Profile]
			seat.Canon, seat.Family = tc.canon, tc.family
			a.Policy.Profiles[s.Profile] = seat
			q := a.Quota.(*fakeQuota)
			q.GateDo = func(QuotaRequest) (string, error) { return "pool=example profile=" + tc.profile, nil }
			q.LaunchValue = Launch{Profile: tc.profile, ModelID: "gpt-6.1-example", Effort: "high", Grade: "A+", Gate: "default"}
			original := f.Do
			started, submitted := false, false
			f.Do = func(c Command) Result {
				args := strings.Join(c.Args, " ")
				switch {
				case c.Name == "handoffkeep" && strings.HasPrefix(args, "tasks show"):
					return Result{Out: `{"id":3113,"title":"work (grade C, T1)","state":"ready","refs":{}}`}
				case strings.HasPrefix(args, "agent start"):
					started = true
					return Result{}
				case started && strings.HasPrefix(args, "pane process-info"):
					return Result{Out: fmt.Sprintf(`{"process_info":{"shell_pid":1,"foreground_processes":[{"pid":2,"argv0":%q}]}}`, tc.kind)}
				case started && strings.HasPrefix(args, "agent prompt"):
					submitted = true
					return Result{}
				case started && strings.HasPrefix(args, "agent list"):
					status := "idle"
					if submitted {
						status = "working"
					}
					return Result{Out: fmt.Sprintf(`{"agents":[{"pane_id":"w:p1","tab_id":"w:t1","name":"worker-a","agent":%q,"agent_status":%q,"cwd":%q,"state_change_seq":1}]}`, tc.kind, status, s.CWD)}
				case started && (strings.HasPrefix(args, "pane read") || strings.HasPrefix(args, "agent read")):
					return Result{Out: "  >_ OpenAI Codex (v0.166.0)\n  directory: ~/tree\n  model: gpt-6.1-example high\n› Ask Codex to do anything\n\n  GPT-6.1 Example high · ~/tree\n  ? for shortcuts\n"}
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
			family, _ := job.Receipt["family"].(string)
			profile, _ := job.Receipt["launch_profile"].(string)
			if family == "" {
				family = "unknown"
			}
			if tc.canon == "" {
				profile = "unknown"
			}
			want := fmt.Sprintf("Launch identity: family=%s launch_profile=%s (use these for the Mergewake-Author-Family and Mergewake-Author-Profile trailers)", family, profile)
			b, err := os.ReadFile(filepath.Join(a.Jobs, s.Job, "brief-delivered.md"))
			if err != nil {
				t.Fatal(err)
			}
			if !strings.Contains(string(b), want+"\n\n") {
				t.Fatalf("missing receipt identity %q in brief", want)
			}
			if !strings.Contains(string(b), "Mergewake-Task: 3113\nMergewake-Grade: C\nMergewake-Tier: T1\n\n") {
				t.Fatalf("missing validated task trailers in brief: %s", b)
			}
		})
	}
}

func TestBriefIdentityUnknownProfile(t *testing.T) {
	a := testApp(t)
	got, err := a.prepareBrief(Spawn{}, Seat{Family: "anthropic"}, Launch{Effort: "high"}, "BODY")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(got, "Launch identity: family=anthropic launch_profile=unknown ") {
		t.Fatalf("unexpected brief: %s", got)
	}
}

func TestBriefMergewakeTrailers(t *testing.T) {
	a := testApp(t)
	identity := "Launch identity: family=anthropic launch_profile=opus@high (use these for the Mergewake-Author-Family and Mergewake-Author-Profile trailers)\n\n"
	block := "Mergewake trailers (paste at the end of the PR body):\nMergewake-Author-Family: anthropic\nMergewake-Author-Profile: opus@high\nMergewake-Task: 3113\nMergewake-Grade: C\nMergewake-Tier: T1\n\nBODY"
	s := Spawn{Task: "3113", TaskGrade: "C", T: "T1"}
	seat := Seat{Family: "anthropic", Canon: "opus"}
	got, err := a.prepareBrief(s, seat, Launch{Effort: "high"}, "BODY")
	if err != nil || !strings.HasSuffix(got, identity+block) {
		t.Fatalf("brief = %q, error = %v; want suffix %q", got, err, identity+block)
	}
	s.Task = ""
	got, err = a.prepareBrief(s, seat, Launch{Effort: "high"}, "BODY")
	if err != nil || !strings.HasSuffix(got, identity+"BODY") {
		t.Fatalf("brief without task = %q, error = %v", got, err)
	}
	s.Task, seat.Family, seat.Canon = "3113", "", ""
	got, err = a.prepareBrief(s, seat, Launch{}, "BODY")
	if err != nil || strings.Contains(got, "Mergewake-Author-Family:") || strings.Contains(got, "Mergewake-Author-Profile:") || strings.Contains(got, ": unknown") {
		t.Fatalf("brief with unknown identity = %q, error = %v", got, err)
	}
}
