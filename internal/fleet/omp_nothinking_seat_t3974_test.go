package fleet

import (
	"context"
	"fmt"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/mgh3326/panewire/internal/quota/shared"
)

// hk 3974: "this model has no thinking levels" is a property of the embedded
// seat, not of a catalog row. handoffkeep does not store the field, so a row
// read back from it never carries one. These tests judge the catalog exactly
// as handoffkeep returns it: the omp seed rows (docs/catalog/omp-credit-seed.json),
// which no longer name the property.

const noThinkingAccountFP = "0123456789abcdef"

// ompNoThinkingSeats is each no-thinking seat's model and omp footer name.
var ompNoThinkingSeats = map[string]struct{ model, footer string }{
	"omp-argon4":    {"commandcode/google/gemini-4-argon", "Gemini 4 Argon"},
	"omp-mimo26pro": {"commandcode/xiaomi/mimo-v2.6-pro", "MiMo V2.6 Pro"},
	"omp-cc-sol61":  {"commandcode/gpt-6.1-sol", "GPT-6.1 Sol"},
}

// noThinkingSpawnApp is an App whose launch lookup and gate read the embedded
// seat policy and a handoffkeep-shaped catalog, and whose host holds one fresh
// commandcode reading. Its quota is the production admissionQuota, so a test
// exercises the same launch and gate steps a pinned spawn runs.
func noThinkingSpawnApp(t *testing.T, extra ...CatalogRow) *App {
	t.Helper()
	rows := append(ompSeedCatalog(t), extra...)
	a := testApp(t)
	a.modelFamilies = func(context.Context) (ModelFamiliesPolicy, error) { return launchTestModelFamilies(), nil }
	a.Config.Local = map[string]any{"omp_commandcode": true}
	a.Config.Quota = map[string]any{"accounts": map[string]any{"commandcode": noThinkingAccountFP}}
	a.BenchCatalog = func(context.Context) ([]CatalogRow, error) { return rows, nil }
	now := a.Now()
	a.SharedLatest = func(_ context.Context, key string) (shared.Latest, error) {
		if key != "commandcode:"+noThinkingAccountFP {
			return shared.Latest{}, fmt.Errorf("unexpected account %q", key)
		}
		reset := now.Add(20 * 24 * time.Hour)
		return shared.Latest{Observation: &shared.Observation{
			Kind:       shared.KindAuto,
			MeasuredAt: now.Add(-time.Minute),
			Plan:       "credit",
			Buckets:    []shared.Bucket{{Window: "30d", UsedPct: 10, ResetsAt: &reset, Scope: "account"}},
		}}, nil
	}
	runner := &fakeRunner{Do: func(c Command) Result {
		return Result{Code: 1, Err: "unexpected command " + c.Name}
	}}
	a.Runner = runner
	a.Quota = admissionQuota{CLIQuota{Runner: runner, Binary: "pacelever"}, a}
	return a
}

// pinnedWorkerSpawn is a pinned worker spawn of one seat, with the marker
// named by PANEWIRE_E6_ARM as the operator passes it.
func pinnedWorkerSpawn(a *App, profile string) Spawn {
	return Spawn{Profile: profile, Role: "implementer", T: "T1", Task: "3974", Effort: "off", Containment: "hk:3974", CWD: a.Home}
}

// (a) With handoffkeep's row carrying no no_thinking field, a pinned worker
// spawn at effort off admits for each no-thinking seat: the seat supplies the
// property the row used to carry.
func TestOMPNoThinkingSeatAdmitsPinnedSpawn(t *testing.T) {
	a := noThinkingSpawnApp(t)
	for profile, want := range ompNoThinkingSeats {
		t.Run(profile, func(t *testing.T) {
			t.Setenv("PANEWIRE_E6_ARM", profile+"@off")
			launch, _, err := a.admitLaunch(context.Background(), pinnedWorkerSpawn(a, profile), a.Policy.Profiles[profile], "off", "", "B", "", false)
			if err != nil {
				t.Fatalf("pinned %s@off refused: %v", profile, err)
			}
			if launch.Profile != profile || launch.Effort != "off" || launch.ModelID != want.model || launch.Pool != commandcodePool {
				t.Fatalf("launch = %+v", launch)
			}
		})
	}
}

// (b) The launch argv carries no effort for these seats, and the landing proof
// accepts the footer without an effort glyph only for a no-thinking seat.
func TestOMPNoThinkingSeatLaunchAndProof(t *testing.T) {
	a := testApp(t)
	for profile, want := range ompNoThinkingSeats {
		seat := a.Policy.Profiles[profile]
		if !seat.NoThinking || seat.Effort != "off" {
			t.Fatalf("seat %s = %+v, want a no-thinking off seat", profile, seat)
		}
		argv, env, err := a.launchArgs(seat, Launch{ModelID: want.model, Effort: "off"})
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(argv, []string{"--model", want.model}) || !reflect.DeepEqual(env, []string{"OMP_SKIP_SETUP=1"}) {
			t.Fatalf("%s argv %v env %v", profile, argv, env)
		}
	}
	const cwd = "/home/user/work/tree"
	screen := "π > ⬢ Gemini 4 Argon > 🌳 project/tree ▶─1%──…─1M─"
	if !OMPFooterProof(screen, "Gemini 4 Argon", "off", cwd, "/home/user", "", a.Policy.Profiles["omp-argon4"].NoThinking) {
		t.Fatal("the no-thinking seat's footer with ⬢ was refused")
	}
	if OMPFooterProof(screen, "Gemini 4 Argon", "off", cwd, "/home/user", "", a.Policy.Profiles["omp-musespark13"].NoThinking) {
		t.Fatal("a thinking seat accepted the footer with ⬢")
	}
}

// The proof path a spawn runs reads the seat: the real ⬢ footer proves
// a no-thinking seat and is refused for a thinking one.
func TestOMPNoThinkingSeatProofReadsSeat(t *testing.T) {
	for profile, want := range ompNoThinkingSeats {
		t.Run(profile, func(t *testing.T) {
			a, s, fr := ompSpawnFixture(t)
			s.Profile, s.Effort, s.E6Arm = profile, "off", profile+"@off"
			a.Config.Local = map[string]any{"omp_commandcode": true}
			q := a.Quota.(*fakeQuota)
			q.LaunchValue = Launch{Profile: profile, ModelID: want.model, Effort: "off", Grade: gradeUnrated, Pool: commandcodePool, Gate: defaultGate}
			q.GateDo = gatePools(map[string]string{profile: commandcodePool})
			ompModelCatalog(fr, want.model, want.footer)
			ompSeat(fr, "", func(int) string { return "π > ⬢ " + want.footer + " > 🌳 panewire/tree ▶─1%──…─1M─" })
			if err := a.Spawn(context.Background(), s); err != nil {
				t.Fatalf("⬢ footer did not prove %s@off: %v", profile, err)
			}
		})
	}
	t.Run("thinking seat", func(t *testing.T) {
		a, s, fr := ompSpawnFixture(t)
		s.Profile, s.Effort, s.E6Arm = "omp-musespark13", "high", "omp-musespark13@high"
		a.Config.Local = map[string]any{"omp_commandcode": true}
		q := a.Quota.(*fakeQuota)
		q.LaunchValue = Launch{Profile: "omp-musespark13", ModelID: "commandcode/meta/muse-spark-1.3", Effort: "high", Grade: gradeUnrated, Pool: commandcodePool, Gate: defaultGate}
		q.GateDo = gatePools(map[string]string{"omp-musespark13": commandcodePool})
		ompModelCatalog(fr, "commandcode/meta/muse-spark-1.3", "Muse Spark 1.3")
		ompSeat(fr, "", func(int) string { return "π > ⬢ Muse Spark 1.3 > 🌳 panewire/tree ▶─1%──…─1M─" })
		err := a.Spawn(context.Background(), s)
		if err == nil || !strings.Contains(err.Error(), "omp model/effort not proven") {
			t.Fatalf("a thinking seat proved with ⬢ icon instead of an effort glyph: %v", err)
		}
	})
}

// ompModelCatalog answers omp's own model list for one selector.
func ompModelCatalog(fr *fakeRunner, model, footerName string) {
	base := fr.Do
	fr.Do = func(c Command) Result {
		if c.Name == "omp" && strings.Join(c.Args, " ") == "models --json" {
			return Result{Out: fmt.Sprintf(`{"models":[{"id":%q,"selector":%q,"name":%q}]}`, model, model, footerName)}
		}
		return base(c)
	}
}

// (c) A seat without the property is refused at effort off: another Command
// Code rung (omp-dsflash41) and a thinking seat with graded rungs
// (omp-musespark13).
func TestOMPThinkingSeatsRefuseOff(t *testing.T) {
	// omp-dsflash41's rungs are not in the omp seed; handoffkeep carries them.
	a := noThinkingSpawnApp(t, CatalogRow{Profile: "omp-dsflash41", Effort: "off", ModelID: "commandcode/deepseek/deepseek-v4.1-flash", Pool: commandcodePool, Grade: gradeUnrated, Gate: defaultGate})
	for _, profile := range []string{"omp-dsflash41", "omp-musespark13"} {
		t.Run(profile, func(t *testing.T) {
			if a.Policy.Profiles[profile].NoThinking {
				t.Fatalf("%s is marked no-thinking", profile)
			}
			t.Setenv("PANEWIRE_E6_ARM", profile+"@off")
			_, _, err := a.admitLaunch(context.Background(), pinnedWorkerSpawn(a, profile), a.Policy.Profiles[profile], "off", "", "B", "", false)
			if err == nil || !strings.Contains(err.Error(), "credit effort is not placed") {
				t.Fatalf("%s@off was admitted: %v", profile, err)
			}
		})
	}
}

// (d) Every other omp and Command Code seat keeps its argv, pool and effort
// rules, and no other seat claims the no-thinking property.
func TestOMPNoThinkingSeatLeavesOtherRungsUnchanged(t *testing.T) {
	a := testApp(t)
	for profile, seat := range a.Policy.Profiles {
		if seat.Kind == "omp" && seat.NoThinking {
			if _, ok := ompNoThinkingSeats[profile]; !ok {
				t.Fatalf("seat %s gained no_thinking", profile)
			}
		}
	}
	seed := ompSeedCatalog(t)
	for _, tc := range []struct {
		profile, model, pool, effort string
		efforts                      []string
	}{
		{"omp-musespark13", "commandcode/meta/muse-spark-1.3", commandcodePool, "high", []string{"low", "high", "max"}},
		{"omp-cc-opus55", "commandcode/claude-opus-5-5", commandcodePool, "high", []string{"low", "high", "max"}},
		{"omp-cc-sonnet55", "commandcode/claude-sonnet-5-5", commandcodePool, "high", []string{"low", "high", "max"}},
		{"omp-opus", "anthropic/claude-opus-5-5", "anthropic-api", "high", []string{"low", "medium", "high", "xhigh", "max"}},
	} {
		t.Run(tc.profile, func(t *testing.T) {
			seat := a.Policy.Profiles[tc.profile]
			if seat.NoThinking || seat.Effort != tc.effort {
				t.Fatalf("seat = %+v", seat)
			}
			argv, _, err := a.launchArgs(seat, Launch{ModelID: tc.model, Effort: seat.Effort})
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(argv, []string{"--model", tc.model + ":" + seat.Effort}) {
				t.Fatalf("argv = %v", argv)
			}
			var got []string
			for _, row := range seed {
				if row.Profile == tc.profile {
					got = append(got, row.Effort)
					if row.Pool != tc.pool || row.ModelID != tc.model {
						t.Fatalf("row %s@%s = %+v", tc.profile, row.Effort, row)
					}
				}
			}
			slices.Sort(got)
			want := slices.Clone(tc.efforts)
			slices.Sort(want)
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("rungs = %v, want %v", got, tc.efforts)
			}
			for _, effort := range tc.efforts {
				in := admissionInputs{Rows: admissionRows(seed), Seats: a.Policy.Profiles}
				if _, _, _, err := in.resolveRow(admissionRequest{Profile: tc.profile, Effort: effort, Role: "implementer", E6Arm: tc.profile + "@" + effort}, true); err != nil {
					t.Fatalf("%s@%s refused: %v", tc.profile, effort, err)
				}
			}
		})
	}
	// omp-dsflash41 keeps its rungs and its argv; its rows are handoffkeep's,
	// not the omp seed's.
	t.Run("omp-dsflash41", func(t *testing.T) {
		const model = "commandcode/deepseek/deepseek-v4.1-flash"
		seat := a.Policy.Profiles["omp-dsflash41"]
		if seat.NoThinking || seat.Effort != "high" {
			t.Fatalf("seat = %+v", seat)
		}
		argv, _, err := a.launchArgs(seat, Launch{ModelID: model, Effort: seat.Effort})
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(argv, []string{"--model", model + ":high"}) {
			t.Fatalf("argv = %v", argv)
		}
		var rows []admissionRow
		for _, effort := range []string{"low", "high", "max"} {
			rows = append(rows, admissionRow{Profile: "omp-dsflash41", Effort: effort, ModelID: model, Pool: commandcodePool, Grade: gradeUnrated, Gate: defaultGate})
		}
		in := admissionInputs{Rows: rows, Seats: a.Policy.Profiles}
		for _, effort := range []string{"low", "high", "max"} {
			if _, _, _, err := in.resolveRow(admissionRequest{Profile: "omp-dsflash41", Effort: effort, Role: "implementer", E6Arm: "omp-dsflash41@" + effort}, true); err != nil {
				t.Fatalf("omp-dsflash41@%s refused: %v", effort, err)
			}
		}
	})
}
