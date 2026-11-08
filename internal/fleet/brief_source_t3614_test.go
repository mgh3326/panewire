package fleet

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// hk 3614: `-p hk:doc/<key>` reads the brief body from the hk document <key>
// over the svc-panewire client instead of the filesystem. A key that hk does
// not have, an unconfigured credential, a 2xx answer that cannot be read and an
// empty body all refuse before any side effect, naming the key. A plain path
// reads the file as before.

const briefDocTestKey = "brief/2026-11-09/job-a"

// briefSubmitHub is the hub fixture that also accepts submissions, recording
// each one.
type briefSubmitHub struct {
	*fakeHub
	got []Submission
}

func (h *briefSubmitHub) Submit(_ context.Context, r Submission) (Accepted, error) {
	h.got = append(h.got, r)
	return Accepted{Job: "job-a", Request: r.RequestID}, nil
}

// briefSubmitFixture is a pushed linked worktree ready to submit job-a with
// -p prompt through a recording hub.
func briefSubmitFixture(t *testing.T, prompt string) (*App, Spawn, *briefSubmitHub, *spawnHK) {
	t.Helper()
	a, s, _, hub, _ := hubFixture(t, gitState{pushed: true}, "wk/feat", hubHosts())
	a.Config.Local["machine_id"] = "source-a"
	if err := os.MkdirAll(s.CWD, 0700); err != nil {
		t.Fatal(err)
	}
	submit := &briefSubmitHub{fakeHub: hub}
	a.Hub = submit
	s.Prompt = prompt
	s.Label, s.T, s.Owner, s.Host, s.OverrideReason, s.Containment = "job-a", "T1", "parent", "h1", "fixture", "hk:12"
	return a, s, submit, useFakeHK(t)
}

// docReads counts the document reads the fake hk received.
func docReads(hk *spawnHK) int {
	n := 0
	for _, r := range hk.Requests() {
		if r.Method == http.MethodGet && strings.HasPrefix(r.Path, "/v1/documents/") {
			n++
		}
	}
	return n
}

// argValue is the value after flag in args, "" when absent.
func argValue(args []string, flag string) string {
	for i := 0; i+1 < len(args); i++ {
		if args[i] == flag {
			return args[i+1]
		}
	}
	return ""
}

func TestSubmitBriefFromHKDoc(t *testing.T) {
	a, s, hub, hk := briefSubmitFixture(t, "hk:doc/"+briefDocTestKey)
	hk.SetDoc(briefDocTestKey, "brief from hk marker\n")
	if err := a.SubmitSpawn(t.Context(), s); err != nil {
		t.Fatal(err)
	}
	if len(hub.got) != 1 {
		t.Fatalf("submissions: %d, want 1", len(hub.got))
	}
	got := hub.got[0]
	if got.Brief != "brief from hk marker\n" {
		t.Fatalf("brief=%q, want the hk document body", got.Brief)
	}
	if p := argValue(got.Args, "-p"); p != "hk:doc/"+briefDocTestKey {
		t.Fatalf("submitted -p %q, want the hk key", p)
	}
	if want := "GET /v1/documents/brief%2F2026-11-09%2Fjob-a"; !strings.Contains(strings.Join(hk.Routes(), "\n"), want) {
		t.Fatalf("hk routes %v, want %s", hk.Routes(), want)
	}
	// submission.json records where the brief lives: the hk key in its -p.
	raw, err := os.ReadFile(filepath.Join(a.Jobs, "job-a", "submission.json"))
	if err != nil {
		t.Fatal(err)
	}
	var saved Submission
	if err := json.Unmarshal(raw, &saved); err != nil {
		t.Fatal(err)
	}
	if p := argValue(saved.Args, "-p"); p != "hk:doc/"+briefDocTestKey || saved.Brief != got.Brief {
		t.Fatalf("submission.json -p=%q brief=%q", p, saved.Brief)
	}
	if out := a.Out.(interface{ String() string }).String(); !strings.Contains(out, "brief_doc="+briefDocTestKey+" source=hk") {
		t.Fatalf("brief_doc line missing: %q", out)
	}
}

func TestSubmitBriefFromHKDocRefusesBeforeSideEffects(t *testing.T) {
	for _, tc := range []struct {
		name  string
		setup func(*testing.T, *spawnHK)
		code  int
		want  string
	}{
		{"not found", func(*testing.T, *spawnHK) {}, exitUsage, "not found"},
		{"unconfigured credential", func(t *testing.T, _ *spawnHK) {
			t.Setenv("HANDOFFKEEP_URL", "")
			t.Setenv("HANDOFFKEEP_TOKEN_SVC_PANEWIRE", "")
		}, exitConfig, "unconfigured"},
		{"unreadable answer", func(_ *testing.T, hk *spawnHK) {
			hk.Handle(hkAnswers(map[string]hkAnswerFixture{docRoute(http.MethodGet, briefDocTestKey): hkOK("not json")}))
		}, exitStateConflict, "unreadable"},
		{"answer for another key", func(_ *testing.T, hk *spawnHK) {
			hk.Handle(hkAnswers(map[string]hkAnswerFixture{docRoute(http.MethodGet, briefDocTestKey): hkOK(docAnswer("brief/other", "note", "x"))}))
		}, exitStateConflict, "unreadable"},
		{"hk outage", func(_ *testing.T, hk *spawnHK) {
			hk.Handle(hkAnswers(map[string]hkAnswerFixture{docRoute(http.MethodGet, briefDocTestKey): hkOutage}))
		}, exitStateConflict, "unavailable"},
		{"empty body", func(_ *testing.T, hk *spawnHK) { hk.SetDoc(briefDocTestKey, " \n\t\n") }, exitUsage, "empty"},
		{"over the 64 KiB brief limit", func(_ *testing.T, hk *spawnHK) {
			hk.SetDoc(briefDocTestKey, strings.Repeat("a", hubMaxBriefBytes+1))
		}, exitUsage, "64 KiB"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			a, s, hub, hk := briefSubmitFixture(t, "hk:doc/"+briefDocTestKey)
			tc.setup(t, hk)
			err := a.SubmitSpawn(t.Context(), s)
			if err == nil {
				t.Fatal("submitted")
			}
			if exitCode(err) != tc.code || !strings.Contains(err.Error(), tc.want) || !strings.Contains(err.Error(), briefDocTestKey) {
				t.Fatalf("exit=%d err=%v, want exit %d naming %q and the key", exitCode(err), err, tc.code, tc.want)
			}
			if strings.Contains(err.Error(), hkSvcTestToken) || strings.Contains(err.Error(), "\n") {
				t.Fatalf("refusal must be one line without the credential: %q", err)
			}
			if len(hub.got) != 0 || len(hub.spawns) != 0 {
				t.Fatalf("a refused brief reached the hub: %d submissions", len(hub.got))
			}
			if _, err := os.Stat(filepath.Join(a.Jobs, "job-a")); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("a refused brief created the job dir: %v", err)
			}
		})
	}
}

func TestSubmitBriefHKDocKeyGrammar(t *testing.T) {
	for _, prompt := range []string{"hk:doc/", "hk:doc//brief/x", "hk:doc/brief/a\x01b", "hk:doc/brief/a\nb", "hk:doc/brief/a\tb", "hk:doc/brief/a\x7fb"} {
		t.Run(strings.ReplaceAll(prompt, "/", "_"), func(t *testing.T) {
			a, s, hub, hk := briefSubmitFixture(t, prompt)
			err := a.SubmitSpawn(t.Context(), s)
			if exitCode(err) != exitUsage || err == nil || !strings.Contains(err.Error(), "hk:doc/KEY") {
				t.Fatalf("exit=%d err=%v, want a usage refusal naming the hk:doc/KEY grammar", exitCode(err), err)
			}
			if strings.Contains(err.Error(), "\n") {
				t.Fatalf("refusal must be one line: %q", err)
			}
			if docReads(hk) != 0 || len(hub.got) != 0 {
				t.Fatalf("an invalid key reached hk (%d reads) or the hub (%d)", docReads(hk), len(hub.got))
			}
		})
	}
}

func TestSubmitBriefPlainPathUnchanged(t *testing.T) {
	for name, rel := range map[string]string{"plain": "brief.md", "hk-like name": "hk:docs.md"} {
		t.Run(name, func(t *testing.T) {
			a, s, hub, hk := briefSubmitFixture(t, "")
			s.Prompt = filepath.Join(a.Home, rel)
			if err := os.WriteFile(s.Prompt, []byte("file brief marker"), 0600); err != nil {
				t.Fatal(err)
			}
			if err := a.SubmitSpawn(t.Context(), s); err != nil {
				t.Fatal(err)
			}
			if len(hub.got) != 1 || hub.got[0].Brief != "file brief marker" || argValue(hub.got[0].Args, "-p") != s.Prompt {
				t.Fatalf("submissions %+v", hub.got)
			}
			if docReads(hk) != 0 {
				t.Fatalf("a file brief read hk: %v", hk.Routes())
			}
			if strings.Contains(a.Out.(interface{ String() string }).String(), "brief_doc=") {
				t.Fatal("a file brief printed a brief_doc line")
			}
		})
	}
	t.Run("missing file keeps its refusal", func(t *testing.T) {
		a, s, _, _ := briefSubmitFixture(t, "")
		s.Prompt = filepath.Join(a.Home, "absent.md")
		err := a.SubmitSpawn(t.Context(), s)
		if exitCode(err) != exitUsage || err == nil || err.Error() != "pass -p with a nonempty UTF-8 brief of at most 64 KiB" {
			t.Fatalf("exit=%d err=%v", exitCode(err), err)
		}
	})
}

func TestSpawnDryRunBriefFromHKDoc(t *testing.T) {
	a, s, f, _ := cmdFixture(t)
	useFakeHK(t).SetDoc(briefDocTestKey, "dry-run brief from hk marker")
	s.Prompt, s.DryRun = "hk:doc/"+briefDocTestKey, true
	if err := a.Spawn(context.Background(), s); err != nil {
		t.Fatal(err)
	}
	out := a.Out.(interface{ String() string }).String()
	if !strings.Contains(out, "dry-run brief from hk marker") || !strings.Contains(out, "brief_doc="+briefDocTestKey+" source=hk") {
		t.Fatalf("dry-run output: %q", out)
	}
	for _, c := range f.Calls {
		if strings.HasPrefix(strings.Join(c.Args, " "), "tab create") {
			t.Fatalf("dry run created a pane: %v", c.Args)
		}
	}
}

func TestSpawnDryRunBriefFromHKDocNotFound(t *testing.T) {
	a, s, f, _ := cmdFixture(t)
	s.Prompt, s.DryRun = "hk:doc/"+briefDocTestKey, true
	err := a.Spawn(context.Background(), s)
	if exitCode(err) != exitUsage || err == nil || !strings.Contains(err.Error(), briefDocTestKey) || !strings.Contains(err.Error(), "not found") {
		t.Fatalf("exit=%d err=%v", exitCode(err), err)
	}
	for _, c := range f.Calls {
		if c.Name == "herdr" {
			t.Fatalf("refused brief ran herdr: %v", c.Args)
		}
	}
	if _, err := os.Stat(filepath.Join(a.Jobs, "job-a")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("a refused brief created the job dir: %v", err)
	}
}

// panewire work takes -p from the task's refs.brief_path through the same
// parser, so a task whose brief lives in hk names it there.
func TestWorkBriefFromHKDocRef(t *testing.T) {
	a, s, _, _ := cmdFixture(t)
	useFakeHK(t).SetTask("42", `{"id":42,"title":"implement (grade B, T1)","state":"ready","refs":{"brief_path":"hk:doc/`+briefDocTestKey+`"}}`).
		SetDoc(briefDocTestKey, "work brief from hk marker")
	err := a.Run(context.Background(), []string{"work", "42", "-c", s.CWD, "-m", "cmd-dsflash41", "-w", "workspace", "-l", "worker-a", "--job", "job-a", "--host", "local", "--owner", "parent", "--override-reason", "fixture", "--containment", "hk:12", "--dry-run"})
	if err != nil {
		t.Fatal(err)
	}
	if out := a.Out.(interface{ String() string }).String(); !strings.Contains(out, "work brief from hk marker") || !strings.Contains(out, "brief_doc="+briefDocTestKey) {
		t.Fatalf("work dry-run output: %q", out)
	}
}
