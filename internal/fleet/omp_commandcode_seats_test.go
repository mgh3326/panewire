package fleet

import (
	"context"
	"reflect"
	"slices"
	"strings"
	"testing"
)

var ompCommandCodeModels = map[string]struct {
	model, family string
	noThinking    bool
	efforts       []string
}{
	"omp-musespark13": {"meta/muse-spark-1.3", "meta", false, []string{"low", "high", "max"}},
	"omp-argon4":      {"google/gemini-4-argon", "google", true, []string{"off"}},
	"omp-mimo26pro":   {"xiaomi/mimo-v2.6-pro", "other", true, []string{"off"}},
}

func ompCommandCodeSeed(t *testing.T) []CatalogRow {
	t.Helper()
	var rows []CatalogRow
	for _, row := range ompSeedCatalog(t) {
		if _, ok := ompCommandCodeModels[row.Profile]; ok {
			rows = append(rows, row)
		}
	}
	return rows
}

// New seats resolve to their model, family and Command Code rows, and pass the
// model to omp using the existing model selector (without an effort suffix
// when omp has no thinking levels).
func TestOMPCommandCodeSeatsResolveAndLaunch(t *testing.T) {
	a := testApp(t)
	rows := ompCommandCodeSeed(t)
	for profile, want := range ompCommandCodeModels {
		t.Run(profile, func(t *testing.T) {
			seat, ok := a.Policy.Profiles[profile]
			if !ok || seat.Kind != "omp" || seat.Family != want.family || seat.Capability != "omp_commandcode" {
				t.Fatalf("policy seat for %s: %+v", profile, seat)
			}
			var found []CatalogRow
			for _, row := range rows {
				if row.Profile == profile {
					found = append(found, row)
				}
			}
			if len(found) != len(want.efforts) {
				t.Fatalf("seed rows %v, want efforts %v", found, want.efforts)
			}
			for _, effort := range want.efforts {
				var row CatalogRow
				for _, candidate := range found {
					if candidate.Effort == effort {
						row = candidate
					}
				}
				if row.ModelID != want.model || row.Pool != commandcodePool || row.Family != want.family || row.Grade != gradeUnrated || row.NoThinking != want.noThinking {
					t.Fatalf("%s@%s row=%+v", profile, effort, row)
				}
			}
			launchEffort := "high"
			if want.noThinking {
				launchEffort = "off"
			}
			catalog := overlayLaunchCatalog(found, false)
			resolved, effort, why := catalog.resolveRow(admissionRequest{Profile: profile, Effort: launchEffort, Role: "worker", E6Arm: profile + "@" + launchEffort}, true)
			if why != "" || resolved.ModelID != want.model || resolved.Pool != commandcodePool || resolved.Family != want.family || resolved.NoThinking != want.noThinking || effort != launchEffort {
				t.Fatalf("resolved row=%+v effort=%q why=%q", resolved, effort, why)
			}
			argv, env, err := a.launchArgs(seat, Launch{ModelID: resolved.ModelID, Effort: effort, NoThinking: resolved.NoThinking})
			if err != nil {
				t.Fatal(err)
			}
			modelArg := want.model + ":" + launchEffort
			if want.noThinking {
				modelArg = want.model
			}
			if !reflect.DeepEqual(argv, []string{"--model", modelArg}) || !reflect.DeepEqual(env, []string{"OMP_SKIP_SETUP=1"}) {
				t.Fatalf("launch args %v env %v", argv, env)
			}
		})
	}
}

func TestOMPCommandCodeEffortAdmission(t *testing.T) {
	rows := admissionRows(ompCommandCodeSeed(t))
	for profile, spec := range ompCommandCodeModels {
		for _, effort := range []string{"low", "high", "max", "medium", "xhigh", "off"} {
			want := slices.Contains(spec.efforts, effort)
			t.Run(profile+"@"+effort, func(t *testing.T) {
				in := admissionInputs{Rows: rows}
				req := admissionRequest{Profile: profile, Effort: effort, Role: "worker", E6Arm: profile + "@" + effort}
				_, _, _, admissionErr := in.resolveRow(req, true)
				catalog := overlayLaunchCatalog(ompCommandCodeSeed(t), false)
				_, _, launchErr := catalog.resolveRow(req, true)
				if (admissionErr == nil) != want || (launchErr == "") != want {
					t.Fatalf("admission err=%v launch err=%q, want admitted=%t", admissionErr, launchErr, want)
				}
			})
		}
	}
	in := admissionInputs{Rows: append(rows, admissionRow{Profile: "omp-dsflash41", Effort: "off", ModelID: "deepseek/deepseek-v4.1-flash", Pool: commandcodePool, Grade: gradeUnrated, Gate: defaultGate})}
	if _, _, _, err := in.resolveRow(admissionRequest{Profile: "omp-dsflash41", Effort: "off", Role: "worker", E6Arm: "omp-dsflash41@off"}, true); err == nil {
		t.Fatal("omp-dsflash41@off was admitted")
	}
	legacy := launchCatalog{Rows: []launchRow{launchRowFrom(CatalogRow{Profile: "omp-dsflash41", Effort: "off", ModelID: "deepseek/deepseek-v4.1-flash", Pool: commandcodePool, Grade: gradeUnrated, Gate: defaultGate})}}
	if _, _, err := legacy.resolveRow(admissionRequest{Profile: "omp-dsflash41", Effort: "off", Role: "worker", E6Arm: "omp-dsflash41@off"}, true); err == "" {
		t.Fatal("launch lookup admitted omp-dsflash41@off")
	}
}

func TestOMPCommandCodeUnratedSeatsNotAutoSelected(t *testing.T) {
	rows := ompCommandCodeSeed(t)
	if len(rows) != 5 {
		t.Fatalf("catalog has %d new unrated Command Code rows, want 5", len(rows))
	}
	rows = append(rows, ompRow("sonnet", "claude-sonnet-5-5", "claude"))
	pools := map[string]PoolView{
		"claude":        {Status: "ok", Class: "spend", Blocking: ptrF(0)},
		commandcodePool: {Class: "credit"},
	}
	a, s, q := selFixture(t, "A", rows, pools, []string{"claude", commandcodePool})
	q.GateDo = gatePools(map[string]string{"sonnet": "claude", "omp-musespark13": commandcodePool, "omp-argon4": commandcodePool, "omp-mimo26pro": commandcodePool})
	serveOMPRouting(t, a)
	s.Profile = "auto"
	sel, err := a.selectProfile(context.Background(), s)
	if err != nil {
		t.Fatal(err)
	}
	if chosen := sel.record()["chosen"].(map[string]any)["canon"]; chosen != "sonnet" {
		t.Fatalf("auto selected %v, want sonnet", chosen)
	}
}

func TestOMPCommandCodeFooterWithoutThinking(t *testing.T) {
	const cwd = "/home/user/work/tree"
	screen := "π > Gemini 4 Argon > 🌳 project/tree ▶─1%──…─1M─"
	if !OMPFooterProof(screen, "Gemini 4 Argon", "off", cwd, "/home/user", "", true) {
		t.Fatal("no-thinking footer not accepted for an off rung")
	}
	if OMPFooterProof(screen, "Gemini 4 Argon", "high", cwd, "/home/user", "") {
		t.Fatal("no-thinking footer accepted for a high rung")
	}
	if OMPFooterProof(strings.Replace(screen, "Gemini 4 Argon", "MiMo V2.6 Pro", 1), "Gemini 4 Argon", "off", cwd, "/home/user", "", true) {
		t.Fatal("footer with another model name was accepted")
	}
	if OMPFooterProof(screen, "Gemini 4 Argon", "off", cwd, "/home/user", "") {
		t.Fatal("unmarked off rung accepted a no-thinking footer")
	}
}

func TestOMPAndCommandCodeExistingSeatsUnchanged(t *testing.T) {
	a := testApp(t)
	for _, tc := range []struct {
		profile, model, pool, family string
	}{
		{"omp-opus", "anthropic/claude-opus-5-5", "anthropic-api", "anthropic"},
		{"omp-sonnet", "anthropic/claude-sonnet-5-5", "anthropic-api", "anthropic"},
		{"omp-sol", "openai/gpt-6.1-sol", "openai-api", "openai"},
		{"omp-terra61", "openai/gpt-6.1-terra", "openai-api", "openai"},
		{"omp-haiku", "anthropic/claude-haiku-5-5", "anthropic-api", "anthropic"},
		{"omp-luna", "openai/gpt-6-luna", "openai-api", "openai"},
		{"omp-dsflash41", "deepseek/deepseek-v4.1-flash", commandcodePool, "deepseek"},
	} {
		seat := a.Policy.Profiles[tc.profile]
		if seat.Family != tc.family || seat.Effort != "high" {
			t.Fatalf("existing seat %s changed: %+v", tc.profile, seat)
		}
		if tc.profile == "omp-dsflash41" && seat.Capability != "omp_commandcode" {
			t.Fatalf("existing Command Code capability changed: %+v", seat)
		}
		argv, env, err := a.launchArgs(seat, Launch{ModelID: tc.model, Effort: seat.Effort})
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(argv, []string{"--model", tc.model + ":high"}) || !reflect.DeepEqual(env, []string{"OMP_SKIP_SETUP=1"}) {
			t.Fatalf("existing %s argv/env changed: %v %v", tc.profile, argv, env)
		}
		if tc.profile != "omp-dsflash41" {
			for effort, row := range ompSeedRowsFor(t, tc.profile) {
				if row.ModelID != tc.model || row.Pool != tc.pool || row.Family != tc.family || row.NoThinking || effort == "off" {
					t.Fatalf("existing seed row %s@%s changed: %+v", tc.profile, effort, row)
				}
			}
		}
	}
	for _, effort := range []string{"low", "high", "max"} {
		in := admissionInputs{Rows: []admissionRow{{Profile: "omp-dsflash41", Effort: effort, ModelID: "deepseek/deepseek-v4.1-flash", Pool: commandcodePool, Grade: gradeUnrated, Gate: defaultGate}}}
		if _, _, _, err := in.resolveRow(admissionRequest{Profile: "omp-dsflash41", Effort: effort, Role: "worker", E6Arm: "omp-dsflash41@" + effort}, true); err != nil {
			t.Fatalf("existing omp-dsflash41@%s refused: %v", effort, err)
		}
	}
	for _, effort := range []string{"medium", "xhigh"} {
		in := admissionInputs{Rows: []admissionRow{{Profile: "omp-dsflash41", Effort: effort, ModelID: "deepseek/deepseek-v4.1-flash", Pool: commandcodePool, Grade: gradeUnrated, Gate: defaultGate}}}
		if _, _, _, err := in.resolveRow(admissionRequest{Profile: "omp-dsflash41", Effort: effort, Role: "worker", E6Arm: "omp-dsflash41@" + effort}, true); err == nil {
			t.Fatalf("existing omp-dsflash41@%s admitted", effort)
		}
	}
}
