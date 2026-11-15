package fleet

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

func serveOMPRouting(t *testing.T, a *App) {
	t.Helper()
	p, err := parseRoutingJSON([]byte(routingBlockV1))
	if err != nil {
		t.Fatal(err)
	}
	for name, rule := range p.Roles {
		rule.Allowed = []string{"*"}
		p.Roles[name] = rule
	}
	b, _ := json.Marshal(p)
	serveRouting(t, routingDoc(string(b)))
}

func ompRow(profile, model, pool string) CatalogRow {
	return CatalogRow{Profile: profile, Effort: "high", Grade: "A", ModelID: model, Pool: pool, Gate: "default"}
}

func ompSpawnFixture(t *testing.T) (*App, Spawn, *fakeRunner) {
	t.Helper()
	a := testApp(t)
	cwd := filepath.Join(a.Home, "tree")
	if err := os.MkdirAll(cwd, 0700); err != nil {
		t.Fatal(err)
	}
	prompt := filepath.Join(a.Home, "brief.md")
	if err := os.WriteFile(prompt, []byte("Implement the fixture change."), 0600); err != nil {
		t.Fatal(err)
	}
	s := Spawn{CWD: cwd, Profile: "omp-opus", Prompt: prompt, Workspace: "workspace", Label: "worker-a", Job: "job-a", T: "T1", Task: "42", Role: "implementer", Host: "local", OverrideReason: "fixture", Containment: "hk:12"}
	a.Quota = &fakeQuota{LaunchValue: Launch{Profile: "omp-opus", ModelID: "anthropic/claude-opus-5-5", Effort: "high", Grade: "S+", Pool: "anthropic-api", Gate: "default"}, GateDo: gatePools(map[string]string{"omp-opus": "anthropic-api"})}
	started, submitted := false, false
	token := ""
	useFakeHK(t).SetTask("42", `{"id":42,"title":"work (grade A, T1)","state":"ready","refs":{}}`).Handle(hkClaimAnswer)
	fr := &fakeRunner{Do: func(c Command) Result {
		args := strings.Join(c.Args, " ")
		switch {
		case args == "-C "+cwd+" rev-parse --abbrev-ref HEAD":
			return Result{Out: "wk/fixture\n"}
		case args == "-C "+cwd+" rev-parse --path-format=absolute --git-dir --git-common-dir":
			return Result{Out: filepath.Join(cwd, ".git", "worktrees", "tree") + "\n" + filepath.Join(a.Home, "panewire", ".git") + "\n"}
		case args == "-C "+cwd+" rev-parse --show-toplevel":
			return Result{Out: cwd + "\n"}
		case strings.HasPrefix(args, "tab create"):
			return Result{Out: `{"result":{"root_pane":{"pane_id":"w:p1"},"tab_id":"w:t1"}}`}
		case strings.HasPrefix(args, "pane process-info"):
			if started {
				return Result{Out: `{"process_info":{"shell_pid":1,"foreground_processes":[{"pid":2,"argv0":"omp"}]}}`}
			}
			return Result{Out: `{"process_info":{"shell_pid":1,"foreground_processes":[{"pid":1,"argv0":"zsh"}]}}`}
		case strings.HasPrefix(args, "pane run"):
			token = c.Args[len(c.Args)-1]
		case strings.HasPrefix(args, "agent start"):
			started = true
		case strings.HasPrefix(args, "agent prompt"):
			submitted = true
		case strings.HasPrefix(args, "agent list"):
			status := "idle"
			if submitted {
				status = "working"
			}
			return Result{Out: fmt.Sprintf(`{"agents":[{"pane_id":"w:p1","tab_id":"w:t1","name":"worker-a","agent":"omp","agent_status":%q}]}`, status)}
		case strings.HasPrefix(args, "pane read"), strings.HasPrefix(args, "agent read"):
			if started {
				return Result{Out: ompCapture(t, "screen_opus_high_worktree.txt")}
			}
			return Result{Out: token + "\n"}
		case c.Name == "omp" && args == "models --json":
			return Result{Out: ompCapture(t, "models.json")}
		}
		return Result{}
	}}
	a.Runner = fr
	serveOMPRouting(t, a)
	return a, s, fr
}

func TestOMPSpawnLaunchAndProof(t *testing.T) {
	a, s, fr := ompSpawnFixture(t)
	if err := a.Spawn(context.Background(), s); err != nil {
		t.Fatal(err)
	}
	var starts, prompts int
	for _, c := range fr.Calls {
		args := strings.Join(c.Args, " ")
		if strings.HasPrefix(args, "agent start") {
			starts++
			if !strings.Contains(args, "--kind omp") || !strings.HasSuffix(args, "-- --model anthropic/claude-opus-5-5:high") || slices.Contains(c.Args, "--no-mcp") {
				t.Fatalf("OMP launch argv: %v", c.Args)
			}
		}
		if strings.HasPrefix(args, "agent prompt") {
			prompts++
		}
	}
	if starts != 1 || prompts != 1 {
		t.Fatalf("want one launch and brief, got %d %d", starts, prompts)
	}
}

func TestOMPReviewerLaunchUsesExtendedContextOverlay(t *testing.T) {
	for _, role := range []string{"reviewer", "consult"} {
		t.Run(role, func(t *testing.T) {
			a, s, fr := ompSpawnFixture(t)
			s.Role = role
			if role == "reviewer" {
				s.AuthorFamily = "openai"
			}
			base := fr.Do
			var configPath string
			var config string
			fr.Do = func(c Command) Result {
				if c.Name == "herdr" && strings.HasPrefix(strings.Join(c.Args, " "), "agent start") {
					index := slices.Index(c.Args, "--config")
					if index < 0 || index+1 == len(c.Args) {
						t.Fatalf("%s launch has no config overlay: %v", role, c.Args)
					}
					configPath = c.Args[index+1]
					contents, err := os.ReadFile(configPath)
					if err != nil {
						t.Fatalf("read %s config overlay: %v", role, err)
					}
					config = string(contents)
				}
				return base(c)
			}
			if err := a.Spawn(context.Background(), s); err != nil {
				t.Fatal(err)
			}
			if config != "extendedContext: true\n" {
				t.Fatalf("%s config overlay = %q", role, config)
			}
			if _, err := os.Stat(configPath); !os.IsNotExist(err) {
				t.Fatalf("%s config overlay still exists after launch: %v", role, err)
			}
		})
	}
}

func TestOMPSpawnRefusesMismatchedScreen(t *testing.T) {
	for _, afterBrief := range []bool{false, true} {
		t.Run(fmt.Sprintf("afterBrief=%t", afterBrief), func(t *testing.T) {
			a, s, fr := ompSpawnFixture(t)
			base := fr.Do
			submitted := false
			fr.Do = func(c Command) Result {
				if strings.HasPrefix(strings.Join(c.Args, " "), "agent prompt") {
					submitted = true
				}
				r := base(c)
				if !afterBrief || submitted {
					r.Out = strings.ReplaceAll(r.Out, "◒ Opus 5.5", "◒ Sonnet 5.5")
				}
				return r
			}
			if err := a.Spawn(context.Background(), s); code(err) != exitConfig || !strings.Contains(err.Error(), "not proven") {
				t.Fatalf("wrong OMP footer must refuse before or after delivery: %v", err)
			}
		})
	}
}

func TestOMPModelProof(t *testing.T) {
	screen := ompCapture(t, "screen_opus_high_worktree.txt")
	for _, tc := range []struct {
		name, screen, effort, brief string
		want                        bool
	}{
		{"matching", screen, "high", "", true},
		{"wrong model", strings.ReplaceAll(screen, "Opus 5.5", "Sonnet 5.5"), "high", "", false},
		{"wrong effort", screen, "medium", "", false},
		{"wrong cwd", strings.ReplaceAll(screen, "panewire/tree", "panewire/other"), "high", "", false},
		{"quoted footer", screen, "high", screen, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			a := testApp(t)
			a.Home = "/home/example"
			a.Runner = &fakeRunner{Do: func(c Command) Result {
				args := strings.Join(c.Args, " ")
				switch args {
				case "-C /home/example/tree rev-parse --abbrev-ref HEAD":
					return Result{Out: "wk/fixture\n"}
				case "-C /home/example/tree rev-parse --path-format=absolute --git-dir --git-common-dir":
					return Result{Out: "/home/example/tree/.git/worktrees/tree\n/home/example/panewire/.git\n"}
				case "-C /home/example/tree rev-parse --show-toplevel":
					return Result{Out: "/home/example/tree\n"}
				}
				if c.Name == "omp" {
					return Result{Out: ompCapture(t, "models.json")}
				}
				return Result{Out: tc.screen}
			}}
			err := a.modelProof(context.Background(), a.Policy.Profiles["omp-opus"], Launch{ModelID: "anthropic/claude-opus-5-5", Effort: tc.effort}, "/home/example/tree", "w:p1", tc.brief)
			if (err == nil) != tc.want {
				t.Fatalf("want proof=%t, got %v", tc.want, err)
			}
		})
	}
}

func TestOMPConsultEligibility(t *testing.T) {
	for _, role := range []string{"consult"} {
		for _, tc := range []struct {
			profile, model, pool string
			want                 bool
		}{
			{"omp-opus", "anthropic/claude-opus-5-5", "anthropic-api", true},
			{"omp-sol", "openai/gpt-6.1-sol", "openai-api", true},
			{"omp-sonnet", "anthropic/claude-sonnet-5-5", "anthropic-api", false},
			{"omp-terra61", "openai/gpt-6.1-terra", "openai-api", false},
			{"omp-opus", "anthropic/claude-sonnet-5-5", "anthropic-api", false},
			{"agy-opus", "anthropic/claude-opus-5-5", "anthropic-api", false},
		} {
			t.Run(role+"/"+tc.profile+"/"+tc.model, func(t *testing.T) {
				a, s, q := selFixture(t, "A", []CatalogRow{ompRow(tc.profile, tc.model, tc.pool)}, nil, []string{tc.pool})
				if tc.profile == "agy-opus" {
					a.Policy.Profiles[tc.profile] = Seat{Canon: tc.profile, Kind: "agy", Family: "anthropic", Capability: "standard", Roles: []string{"implementer", "consult"}}
				}
				q.GateDo = gatePools(map[string]string{tc.profile: tc.pool})
				serveOMPRouting(t, a)
				s.RoutingRole = role
				if role == "consult" {
					s.Role = "consult"
				}
				sel, err := a.selectProfile(context.Background(), s)
				if (err == nil) != tc.want {
					t.Fatalf("want eligible=%t, got %v", tc.want, err)
				}
				if !tc.want && (code(err) != exitSeatRefused || candOutcome(sel.record(), tc.profile, "high")["reason"] != "role_filtered") {
					t.Fatalf("non-top or foreign harness did not refuse through routing: %v %+v", err, sel.record())
				}
			})
		}
	}
}

func TestOMPExplicitConsultCannotChangeModel(t *testing.T) {
	a, s, fr := ompSpawnFixture(t)
	q := a.Quota.(*fakeQuota)
	q.CatalogRows = []CatalogRow{ompRow("omp-opus", "anthropic/claude-opus-5-5", "anthropic-api")}
	q.LaunchValue.ModelID = "anthropic/claude-sonnet-5-5"
	s.Role = "consult"
	if err := a.Spawn(context.Background(), s); code(err) != exitSeatRefused {
		t.Fatalf("launch model must be checked again: %v", err)
	}
	for _, c := range fr.Calls {
		if c.Name == "herdr" && strings.HasPrefix(strings.Join(c.Args, " "), "agent start") {
			t.Fatal("non-top model reached launch")
		}
	}
}

func TestOMPCreditBesideFree(t *testing.T) {
	for _, tc := range []struct{ profile, model, pool string }{
		{"omp-opus", "anthropic/claude-opus-5-5", "anthropic-api"},
		{"omp-sol", "openai/gpt-6.1-sol", "openai-api"},
	} {
		for _, exhausted := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/exhausted=%t", tc.pool, exhausted), func(t *testing.T) {
				rows := []CatalogRow{ompRow(tc.profile, tc.model, tc.pool), ompRow("sonnet", "claude-sonnet-5-5", "claude")}
				used := 90.0
				if exhausted {
					used = 100
				}
				pools := map[string]PoolView{"claude": {Status: "ok", Class: "spend", Blocking: &used}, tc.pool: {Class: "credit"}}
				a, s, q := selFixture(t, "A", rows, pools, []string{"claude", tc.pool})
				q.GateDo = gatePools(map[string]string{"sonnet": "claude", tc.profile: tc.pool})
				serveOMPRouting(t, a)
				sel, err := a.selectProfile(context.Background(), s)
				if err != nil {
					t.Fatal(err)
				}
				// Credit ranks in the free band, so it is spent before the
				// ranked subscription pool whether or not that pool is used up.
				wantProfile, wantTier := tc.profile, "credit"
				chosen := sel.record()["chosen"].(map[string]any)
				if chosen["canon"] != wantProfile || chosen["tier"] != wantTier {
					t.Fatalf("want %s tier=%s, got %v", wantProfile, wantTier, chosen)
				}
				for _, c := range a.Runner.(*fakeRunner).Calls {
					if strings.HasPrefix(strings.Join(c.Args, " "), "refresh "+tc.pool) {
						t.Fatal("credit has no balance to refresh")
					}
				}
			})
		}
	}
}

func TestOMPCreditWithoutReadingAndExclusion(t *testing.T) {
	for _, class := range []string{"", "credit", "exclude"} {
		t.Run("class="+class, func(t *testing.T) {
			pools := map[string]PoolView{}
			if class != "" {
				pools["anthropic-api"] = PoolView{Class: class}
			}
			a, s, q := selFixture(t, "A", []CatalogRow{ompRow("omp-opus", "anthropic/claude-opus-5-5", "anthropic-api")}, pools, []string{"anthropic-api"})
			serveOMPRouting(t, a)
			calls := 0
			gate := gatePools(map[string]string{"omp-opus": "anthropic-api"})
			q.GateDo = func(r QuotaRequest) (string, error) { calls++; return gate(r) }
			sel, err := a.selectProfile(context.Background(), s)
			if class == "exclude" {
				if err == nil || calls != 0 || candOutcome(sel.record(), "omp-opus", "high")["reason"] != "pool_excluded" {
					t.Fatalf("excluded credit must not reach gate: %v calls=%d", err, calls)
				}
			} else if err != nil || calls != 1 || sel.chosen.tierName != "credit" || sel.chosen.blocking != nil {
				t.Fatalf("balance-less credit must reach gate with no invented balance: %v calls=%d", err, calls)
			}
		})
	}
}

func TestOMPCreditRetainsGateAndPresenceChecks(t *testing.T) {
	for _, missingPresence := range []bool{false, true} {
		a, s, q := selFixture(t, "A", []CatalogRow{ompRow("omp-opus", "anthropic/claude-opus-5-5", "anthropic-api")}, nil, []string{"anthropic-api"})
		serveOMPRouting(t, a)
		if missingPresence {
			a.Source.(*fakePlacementSource).presence = nil
		}
		q.GateError = fail(3, "credit gate refused")
		if _, err := a.selectProfile(context.Background(), s); err == nil {
			t.Fatalf("credit bypassed presence or gate (missingPresence=%t)", missingPresence)
		}
	}
}

func TestOMPResolveAgentHint(t *testing.T) {
	for _, role := range []string{"implementer"} {
		for _, tc := range []struct{ name, frontmatter, want string }{
			{"exact", "---\nname: fixture-opus\nmodel: anthropic/claude-opus-5-5\n---\nAgent body", "fixture-opus"},
			{"quoted", "---\nname: 'fixture-opus'\nmodel: \"anthropic/claude-opus-5-5\"\n---\n", "fixture-opus"},
			{"quoted comments", "---\nname: 'fixture-opus' # user agent\nmodel: \"anthropic/claude-opus-5-5\" # exact model\n---\n", "fixture-opus"},
			{"wrong model", "---\nname: fixture-opus\nmodel: anthropic/claude-sonnet-5-5\n---\n", ""},
			{"model suffix", "---\nname: fixture-opus\nmodel: anthropic/claude-opus-5-5:high\n---\n", ""},
			{"body only", "Body\nname: fixture-opus\nmodel: anthropic/claude-opus-5-5\n", ""},
			{"missing directory", "", ""},
		} {
			t.Run(role+"/"+tc.name, func(t *testing.T) {
				rows := []CatalogRow{ompRow("omp-opus", "anthropic/claude-opus-5-5", "anthropic-api"), ompRow("sonnet", "claude-sonnet-5-5", "claude")}
				a, _, q := selFixture(t, "A", rows, map[string]PoolView{"claude": {Status: "ok", Class: "spend", Blocking: ptrF(5)}}, []string{"claude", "anthropic-api"})
				q.GateDo = gatePools(map[string]string{"omp-opus": "anthropic-api", "sonnet": "claude"})
				serveOMPRouting(t, a)
				if tc.frontmatter != "" {
					dir := filepath.Join(a.Home, ".omp", "agent", "agents")
					if err := os.MkdirAll(dir, 0700); err != nil {
						t.Fatal(err)
					}
					if err := os.WriteFile(filepath.Join(dir, "fixture.md"), []byte(tc.frontmatter), 0600); err != nil {
						t.Fatal(err)
					}
				}
				if err := a.Run(context.Background(), []string{"resolve", "--role", role, "--harness", "omp", "--json"}); err != nil {
					t.Fatal(err)
				}
				var result map[string]any
				if err := json.Unmarshal(a.Out.(*bytes.Buffer).Bytes(), &result); err != nil {
					t.Fatal(err)
				}
				if result["profile"] != "omp-opus" || result["omp_agent"] != tc.want && (tc.want != "" || result["omp_agent"] != nil) {
					t.Fatalf("unexpected resolution: %v", result)
				}
			})
		}
	}
}

func TestOMPSimulateCreditBesideFree(t *testing.T) {
	// A credit pool with balance ranks in the free band; zero credit is excluded.
	for _, tc := range []struct {
		name               string
		projected          float64
		creditLeft         *float64
		wantSeat, wantTier string
	}{
		{"pool leaves quota unused", 90, ptrF(80), "omp-opus", "credit"},
		{"pool on pace to use up, credit without expiry", 100, ptrF(80), "omp-opus", "credit"},
		{"pool on pace to use up, no credit balance recorded", 100, nil, "omp-opus", "credit"},
		{"pool on pace to use up, credit balance zero", 100, ptrF(0), "sonnet", "ranked"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			a := testApp(t)
			projected := tc.projected
			pools := []simPool{{ID: "claude", Class: "spend", Projected: &projected}}
			if tc.creditLeft != nil {
				pools = append(pools, simPool{ID: "anthropic-api", Class: "credit", CreditLeftPct: tc.creditLeft})
			}
			doc := simDoc{Grade: "A", Pools: pools, Candidates: []simCand{
				{Profile: "omp-opus", Effort: "high", Grade: "A", ModelID: "anthropic/claude-opus-5-5", Pool: "anthropic-api", Gate: "default"},
				{Profile: "sonnet", Effort: "high", Grade: "A", ModelID: "claude-sonnet-5-5", Pool: "claude", Gate: "default"},
			}}
			path := filepath.Join(a.Home, "selection.json")
			b, _ := json.Marshal(doc)
			if err := os.WriteFile(path, b, 0600); err != nil {
				t.Fatal(err)
			}
			if err := a.Run(context.Background(), []string{"simulate", "--selection", path, "--policy-set", "waste_below_projected_pct=0"}); err != nil {
				t.Fatal(err)
			}
			var result struct {
				Chosen struct{ Seat, Tier string }
			}
			if err := json.Unmarshal(a.Out.(*bytes.Buffer).Bytes(), &result); err != nil {
				t.Fatal(err)
			}
			if result.Chosen.Seat != tc.wantSeat || result.Chosen.Tier != tc.wantTier {
				t.Fatalf("want %s tier=%s, got %+v", tc.wantSeat, tc.wantTier, result.Chosen)
			}
		})
	}
}

func TestOMPCatalogSeed(t *testing.T) {
	b, err := os.ReadFile(filepath.Join("..", "..", "docs", "catalog", "omp-credit-seed.json"))
	if err != nil {
		t.Fatal(err)
	}
	var seed struct {
		Catalog []struct {
			CatalogRow
			DecidedBy    string `json:"decided_by"`
			DeviationRef string `json:"deviation_ref"`
		} `json:"catalog"`
	}
	if err := json.Unmarshal(b, &seed); err != nil {
		t.Fatal(err)
	}
	unrated := []string{"unrated", "unrated", "unrated", "unrated", "unrated"}
	wantGrades := map[string][]string{"omp-opus": {"S", "S+", "S+", "S+", "S+"}, "omp-sonnet": {"C", "C", "A", "A+", "A+"}, "omp-sol": {"C", "C", "C", "S+", "S+"}, "omp-terra61": {"C", "C", "C", "C", "C"}, "omp-haiku": unrated, "omp-luna": unrated,
		"omp-musespark13": {"unrated", "unrated", "unrated"}, "omp-argon4": {"unrated"}, "omp-mimo26pro": {"unrated"},
		"omp-cc-opus55": {"unrated", "unrated", "unrated"}, "omp-cc-sonnet55": {"unrated", "unrated", "unrated"}, "omp-cc-sol61": {"unrated"}}
	wantModels := map[string]string{"omp-opus": "anthropic/claude-opus-5-5", "omp-sonnet": "anthropic/claude-sonnet-5-5", "omp-sol": "openai/gpt-6.1-sol", "omp-terra61": "openai/gpt-6.1-terra", "omp-haiku": "anthropic/claude-haiku-5-5", "omp-luna": "openai/gpt-6-luna",
		"omp-musespark13": "commandcode/meta/muse-spark-1.3", "omp-argon4": "commandcode/google/gemini-4-argon", "omp-mimo26pro": "commandcode/xiaomi/mimo-v2.6-pro",
		"omp-cc-opus55": "commandcode/claude-opus-5-5", "omp-cc-sonnet55": "commandcode/claude-sonnet-5-5", "omp-cc-sol61": "commandcode/gpt-6.1-sol"}
	wantEfforts := map[string][]string{"omp-opus": {"low", "medium", "high", "xhigh", "max"}, "omp-sonnet": {"low", "medium", "high", "xhigh", "max"}, "omp-sol": {"low", "medium", "high", "xhigh", "max"}, "omp-terra61": {"low", "medium", "high", "xhigh", "max"}, "omp-haiku": {"low", "medium", "high", "xhigh", "max"}, "omp-luna": {"low", "medium", "high", "xhigh", "max"}, "omp-musespark13": {"low", "high", "max"}, "omp-argon4": {"off"}, "omp-mimo26pro": {"off"}, "omp-cc-opus55": {"low", "high", "max"}, "omp-cc-sonnet55": {"low", "high", "max"}, "omp-cc-sol61": {"off"}}
	// New unrated seats carry their own decision reference; the original four
	// keep the seed's ref.
	wantRef := map[string]string{"omp-haiku": "hk:task/3875", "omp-luna": "hk:task/3875", "omp-musespark13": "hk:task/3955", "omp-argon4": "hk:task/3955", "omp-mimo26pro": "hk:task/3955", "omp-cc-opus55": "hk:task/3955", "omp-cc-sonnet55": "hk:task/3955", "omp-cc-sol61": "hk:task/3955"}
	if len(seed.Catalog) != 42 {
		t.Fatalf("want 42 catalog rows, got %d", len(seed.Catalog))
	}
	seen := map[string]bool{}
	for _, row := range seed.Catalog {
		i := slices.Index(wantEfforts[row.Profile], row.Effort)
		grades := wantGrades[row.Profile]
		key := row.Profile + "@" + row.Effort
		if len(grades) == 0 || i < 0 || seen[key] || row.Grade != grades[i] || row.ModelID != wantModels[row.Profile] || row.DecidedBy != "operator-desk" || row.DeviationRef != firstNonEmpty(wantRef[row.Profile], "hk:task/2476") {
			t.Fatalf("invalid seed row: %+v", row)
		}
		seen[key] = true
	}
	// "This model has no thinking levels" is the seat's property, so the
	// published seed carries no no_thinking field at all (hk 3974).
	var raw struct {
		Catalog []map[string]any `json:"catalog"`
	}
	if err := json.Unmarshal(b, &raw); err != nil {
		t.Fatal(err)
	}
	for i, row := range raw.Catalog {
		if _, ok := row["no_thinking"]; ok {
			t.Fatalf("seed row %d carries no_thinking; the seat owns it (hk 3974)", i)
		}
	}
}
