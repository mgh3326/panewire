package fleet

import (
	"context"
	"reflect"
	"slices"
	"testing"
)

var ompCommandCodeModels = map[string]struct {
	model, family, defaultEffort string
	noThinking                   bool
	efforts                      []string
}{
	"omp-musespark13": {"commandcode/meta/muse-spark-1.3", "meta", "high", false, []string{"low", "high", "max"}},
	"omp-argon4":      {"commandcode/google/gemini-4-argon", "google", "off", true, []string{"off"}},
	"omp-mimo26pro":   {"commandcode/xiaomi/mimo-v2.6-pro", "other", "off", true, []string{"off"}},
	"omp-cc-opus55":   {"commandcode/claude-opus-5-5", "anthropic", "high", false, []string{"low", "high", "max"}},
	"omp-cc-sonnet55": {"commandcode/claude-sonnet-5-5", "anthropic", "high", false, []string{"low", "high", "max"}},
	"omp-cc-sol61":    {"commandcode/gpt-6.1-sol", "openai", "off", true, []string{"off"}},
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
			if !ok || seat.Kind != "omp" || seat.Family != want.family || seat.Capability != "omp_commandcode" || seat.Effort != want.defaultEffort || seat.NoThinking != want.noThinking {
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
				if row.ModelID != want.model || row.Pool != commandcodePool || row.Family != want.family || row.Grade != gradeUnrated {
					t.Fatalf("%s@%s row=%+v", profile, effort, row)
				}
			}
			launchEffort := "high"
			if want.noThinking {
				launchEffort = "off"
			}
			catalog := overlayLaunchCatalog(found, false)
			resolved, effort, why := catalog.resolveRow(admissionRequest{Profile: profile, Effort: launchEffort, Role: "implementer", E6Arm: profile + "@" + launchEffort}, true, a.Policy.Profiles)
			if why != "" || resolved.ModelID != want.model || resolved.Pool != commandcodePool || resolved.Family != want.family || effort != launchEffort {
				t.Fatalf("resolved row=%+v effort=%q why=%q", resolved, effort, why)
			}
			argv, env, err := a.launchArgs(seat, Launch{ModelID: resolved.ModelID, Effort: effort})
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

func TestLaunchProfileCreditRetainsLegacyProfiles(t *testing.T) {
	for _, profile := range []string{"pi-unlisted", "cmd-dsflash41"} {
		if !(launchCatalog{}).profileCredit(profile) {
			t.Errorf("empty catalog lost credit identity for %s", profile)
		}
	}
}

func TestOMPCommandCodeEffortAdmission(t *testing.T) {
	a := testApp(t)
	seats := a.Policy.Profiles
	rows := admissionRows(ompCommandCodeSeed(t))
	for profile, spec := range ompCommandCodeModels {
		for _, effort := range []string{"low", "high", "max", "medium", "xhigh", "off"} {
			want := slices.Contains(spec.efforts, effort)
			t.Run(profile+"@"+effort, func(t *testing.T) {
				in := admissionInputs{Rows: rows, Seats: seats}
				req := admissionRequest{Profile: profile, Effort: effort, Role: "implementer", E6Arm: profile + "@" + effort}
				_, _, _, admissionErr := in.resolveRow(req, true)
				catalog := overlayLaunchCatalog(ompCommandCodeSeed(t), false)
				_, _, launchErr := catalog.resolveRow(req, true, seats)
				if (admissionErr == nil) != want || (launchErr == "") != want {
					t.Fatalf("admission err=%v launch err=%q, want admitted=%t", admissionErr, launchErr, want)
				}
			})
		}
	}
	in := admissionInputs{Rows: append(rows, admissionRow{Profile: "omp-dsflash41", Effort: "off", ModelID: "deepseek/deepseek-v4.1-flash", Pool: commandcodePool, Grade: gradeUnrated, Gate: defaultGate}), Seats: seats}
	if _, _, _, err := in.resolveRow(admissionRequest{Profile: "omp-dsflash41", Effort: "off", Role: "implementer", E6Arm: "omp-dsflash41@off"}, true); err == nil {
		t.Fatal("omp-dsflash41@off was admitted")
	}
	legacy := launchCatalog{Rows: []launchRow{launchRowFrom(CatalogRow{Profile: "omp-dsflash41", Effort: "off", ModelID: "deepseek/deepseek-v4.1-flash", Pool: commandcodePool, Grade: gradeUnrated, Gate: defaultGate})}}
	if _, _, err := legacy.resolveRow(admissionRequest{Profile: "omp-dsflash41", Effort: "off", Role: "implementer", E6Arm: "omp-dsflash41@off"}, true, seats); err == "" {
		t.Fatal("launch lookup admitted omp-dsflash41@off")
	}
}

func TestOMPCommandCodeUnratedSeatsNotAutoSelected(t *testing.T) {
	rows := ompCommandCodeSeed(t)
	if len(rows) != 12 {
		t.Fatalf("catalog has %d new unrated Command Code rows, want 12", len(rows))
	}
	rows = append(rows, ompRow("sonnet", "claude-sonnet-5-5", "claude"))
	pools := map[string]PoolView{
		"claude":        {Status: "ok", Class: "spend", Blocking: ptrF(0)},
		commandcodePool: {Class: "credit"},
	}
	a, s, q := selFixture(t, "A", rows, pools, []string{"claude", commandcodePool})
	q.GateDo = gatePools(map[string]string{"sonnet": "claude", "omp-musespark13": commandcodePool, "omp-argon4": commandcodePool, "omp-mimo26pro": commandcodePool, "omp-cc-opus55": commandcodePool, "omp-cc-sonnet55": commandcodePool, "omp-cc-sol61": commandcodePool})
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
	argonScreen := "π > ⬢ Gemini 4 Argon > 🌳 project/tree ▶─1%──…─1M─"
	mimoScreen := "π > ⬢ MiMo V2.6 Pro > 🌳 project/tree ▶─1%──…─1M─"
	bareScreen := "π > Gemini 4 Argon > 🌳 project/tree ▶─1%──…─1M─"
	glyphScreen := "π > ◒ Gemini 4 Argon > 🌳 project/tree ▶─1%──…─1M─"

	// (a) the argon4 and mimo26pro footers prove an off no-thinking rung
	if !OMPFooterProof(argonScreen, "Gemini 4 Argon", "off", cwd, "/home/user", "", true) {
		t.Fatal("argon4 ⬢ footer not accepted for an off no-thinking rung")
	}
	if !OMPFooterProof(mimoScreen, "MiMo V2.6 Pro", "off", cwd, "/home/user", "", true) {
		t.Fatal("mimo26pro ⬢ footer not accepted for an off no-thinking rung")
	}

	// (b) the same footers refuse a high rung of the same model, and a no-thinking off rung refuses
	// a footer with an effort glyph or with no icon
	if OMPFooterProof(argonScreen, "Gemini 4 Argon", "high", cwd, "/home/user", "") {
		t.Fatal("⬢ footer accepted for a high rung")
	}
	if OMPFooterProof(argonScreen, "Gemini 4 Argon", "high", cwd, "/home/user", "", true) {
		t.Fatal("⬢ footer accepted for a high rung even when no-thinking rung flag is true")
	}
	if OMPFooterProof(glyphScreen, "Gemini 4 Argon", "off", cwd, "/home/user", "", true) {
		t.Fatal("no-thinking off rung accepted a footer with an effort glyph")
	}
	if OMPFooterProof(bareScreen, "Gemini 4 Argon", "off", cwd, "/home/user", "", true) {
		t.Fatal("no-thinking off rung accepted a bare footer with no icon")
	}
	if OMPFooterProof(bareScreen, "Gemini 4 Argon", "off", cwd, "/home/user", "") {
		t.Fatal("unmarked off rung accepted a bare footer with no icon")
	}

	// (c) a ⬢ footer naming another model refuses
	if OMPFooterProof(mimoScreen, "Gemini 4 Argon", "off", cwd, "/home/user", "", true) {
		t.Fatal("⬢ footer with another model name was accepted")
	}
	if OMPFooterProof(argonScreen, "Gemini 4 Argon", "off", cwd, "/home/user", "") {
		t.Fatal("unmarked off rung accepted a no-thinking ⬢ footer")
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
		{"omp-dsflash41", "commandcode/deepseek/deepseek-v4.1-flash", commandcodePool, "deepseek"},
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
				if row.ModelID != tc.model || row.Pool != tc.pool || row.Family != tc.family || effort == "off" {
					t.Fatalf("existing seed row %s@%s changed: %+v", tc.profile, effort, row)
				}
			}
		}
	}
	for _, effort := range []string{"low", "high", "max"} {
		in := admissionInputs{Rows: []admissionRow{{Profile: "omp-dsflash41", Effort: effort, ModelID: "commandcode/deepseek/deepseek-v4.1-flash", Pool: commandcodePool, Grade: gradeUnrated, Gate: defaultGate}}, Seats: a.Policy.Profiles}
		if _, _, _, err := in.resolveRow(admissionRequest{Profile: "omp-dsflash41", Effort: effort, Role: "implementer", E6Arm: "omp-dsflash41@" + effort}, true); err != nil {
			t.Fatalf("existing omp-dsflash41@%s refused: %v", effort, err)
		}
	}
	for _, effort := range []string{"medium", "xhigh"} {
		in := admissionInputs{Rows: []admissionRow{{Profile: "omp-dsflash41", Effort: effort, ModelID: "commandcode/deepseek/deepseek-v4.1-flash", Pool: commandcodePool, Grade: gradeUnrated, Gate: defaultGate}}, Seats: a.Policy.Profiles}
		if _, _, _, err := in.resolveRow(admissionRequest{Profile: "omp-dsflash41", Effort: effort, Role: "implementer", E6Arm: "omp-dsflash41@" + effort}, true); err == nil {
			t.Fatalf("existing omp-dsflash41@%s admitted", effort)
		}
	}
	for _, pair := range []struct{ old, commandcode string }{{"omp-opus", "omp-cc-opus55"}, {"omp-sonnet", "omp-cc-sonnet55"}, {"omp-sol", "omp-cc-sol61"}} {
		oldRows, ccRows := ompSeedRowsFor(t, pair.old), ompSeedRowsFor(t, pair.commandcode)
		if len(oldRows) != 5 || len(ccRows) != len(ompCommandCodeModels[pair.commandcode].efforts) {
			t.Fatalf("seat rows %s=%d, %s=%d", pair.old, len(oldRows), pair.commandcode, len(ccRows))
		}
		for effort, oldRow := range oldRows {
			if oldRow.Pool == commandcodePool || oldRow.ModelID == ccRows[effort].ModelID {
				t.Fatalf("existing %s@%s collided with %s: old=%+v new=%+v", pair.old, effort, pair.commandcode, oldRow, ccRows[effort])
			}
		}
		for effort, row := range ccRows {
			if row.Pool != commandcodePool || row.ModelID != ompCommandCodeModels[pair.commandcode].model {
				t.Fatalf("new Command Code row %s@%s: %+v", pair.commandcode, effort, row)
			}
		}
	}
}
