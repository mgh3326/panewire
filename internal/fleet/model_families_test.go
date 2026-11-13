package fleet

import (
	"context"
	"reflect"
	"strings"
	"testing"
)

const modelFamiliesJSON = `{
  "version": 1,
  "counted_by": "pool",
  "families": {
    "claude": {"harness":"claude", "billing":"subscription", "models":["haiku","sonnet","opus","fable"], "effort_via":"flag", "efforts":["low","medium","high","xhigh","max"], "ladder_cap":"high", "ladder_cap_by_model":{"haiku":"max","sonnet":"max"}, "entry_effort":"medium"},
    "openai-api": {"harness":"omp", "billing":"credit", "models":["omp-luna","omp-terra61","omp-sol"], "effort_via":"flag", "efforts":["low","medium","high","xhigh","max"], "ladder_cap":"max", "entry_effort":"medium"}
  },
  "ladder": {"trigger_reviewed_heads":3, "steps":["same_model_next_effort"]},
  "consult": {"grade":"asker_plus_one", "family":"other", "without_task":"S+", "unrated":"never"}
}`

var modelFamiliesFixture = "# policy/model-families\n\n## Machine-readable block (v1)\n```json\n" + modelFamiliesJSON + "\n```\n"

func TestModelFamiliesPolicyReadsHKDocument(t *testing.T) {
	hk := useFakeHK(t).SetDoc(modelFamiliesDocKey, modelFamiliesFixture)
	a := &App{}
	got, err := a.modelFamiliesPolicy(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if got.Version != 1 || got.CountedBy != "pool" || got.Families["claude"].EntryEffort != "medium" || got.Families["claude"].LadderCapByModel["haiku"] != "max" {
		t.Fatalf("policy fields = %+v", got)
	}
	if got.familyForPool("openai-api", "codex") != "openai-api" || got.familyForPool("kimi", "kimi") != "kimi" {
		t.Fatalf("family mapping did not prefer the policy pool or preserve an unlisted live family")
	}
	if routes := hk.Routes(); !reflect.DeepEqual(routes, []string{"GET /v1/documents/policy%2Fmodel-families"}) {
		t.Fatalf("HK document routes = %v", routes)
	}
}

func TestModelFamiliesPolicyRejectsMalformedContract(t *testing.T) {
	for name, body := range map[string]string{
		"missing block":        "# policy/model-families\n",
		"unsupported version":  strings.Replace(modelFamiliesFixture, `"version": 1`, `"version": 2`, 1),
		"unknown entry effort": strings.Replace(modelFamiliesFixture, `"entry_effort":"medium"`, `"entry_effort":"turbo"`, 1),
		"duplicate key":        strings.Replace(modelFamiliesFixture, `"version": 1`, `"version": 1, "version": 1`, 1),
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := parseModelFamiliesDoc(body); err == nil {
				t.Fatal("malformed model families policy accepted")
			}
		})
	}
}

func TestEveryLiveProfileResolvesToItsCatalogEffort(t *testing.T) {
	rows := launchRulesCatalog()
	byProfile := map[string][]CatalogRow{}
	for _, row := range rows {
		// These fixture rows model identities the launch gate deliberately refuses.
		if row.RetiredAt == nil && row.Effort != "" && row.Profile != "test-astra" && row.Profile != "test-terra" {
			byProfile[row.Profile] = append(byProfile[row.Profile], row)
		}
	}
	for profile, live := range byProfile {
		t.Run(profile, func(t *testing.T) {
			request := QuotaRequest{Profile: profile, Effort: live[0].Effort, Role: "worker"}
			if live[0].Grade == gradeUnrated {
				request.E6Arm = profile + "@" + live[0].Effort
			}
			if live[0].Gate == consultOnlyGate {
				request.OperatorRequest = "hk:doc/model-families-test"
			}
			catalog := overlayLaunchCatalog(live, false, launchTestModelFamilies())
			got, why := catalog.resolve(admissionPolicy{}, nil, launchRequest(request))
			if why != "" {
				t.Fatalf("live profile did not resolve: %s", why)
			}
			if got.Effort == "" || got.Effort != liveRowEffort(live, got) {
				t.Fatalf("launch effort %q did not come from a live row: %+v, rows=%+v", got.Effort, got, live)
			}
		})
	}
	if len(byProfile) == 0 {
		t.Fatal("fixture has no live profiles with launch efforts")
	}
}

func TestLaunchEffortUsesLiveCatalogRow(t *testing.T) {
	rows := []CatalogRow{
		{Profile: "opus", Effort: "low", ModelID: "opus", Pool: "claude", Grade: "S", Gate: defaultGate},
		{Profile: "opus", Effort: "high", ModelID: "opus", Pool: "claude", Grade: "S", Gate: defaultGate},
	}
	got, why := overlayLaunchCatalog(rows, false).resolve(admissionPolicy{}, nil, admissionRequest{Profile: "opus", Role: "worker"})
	if why != "" {
		t.Fatalf("resolve: %s", why)
	}
	if got.Effort != "low" {
		t.Fatalf("effort=%q, want the lowest selected live row effort low", got.Effort)
	}
}

func liveRowEffort(rows []CatalogRow, launch Launch) string {
	for _, row := range rows {
		if row.Profile == launch.Profile && row.ModelID == launch.ModelID && row.Effort == launch.Effort {
			return row.Effort
		}
	}
	return ""
}

func TestCatalogRowWithoutEffortCannotLaunch(t *testing.T) {
	catalog := overlayLaunchCatalog([]CatalogRow{{Profile: "no-effort", Pool: "kiro", Grade: "C", Gate: defaultGate}}, false)
	if _, why := catalog.resolve(admissionPolicy{}, nil, admissionRequest{Profile: "no-effort", Role: "worker"}); why != "catalog row has no launch effort" {
		t.Fatalf("empty catalog effort refusal = %q", why)
	}
}
