package fleet

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
)

// A successor spec checkout (hk 5118): an implementer spec on refs/heads/<branch>
// asks for a local branch at the commit, so a successor continues the pushed
// branch with a plain `git push`. A reviewer or a consult reads the same ref
// detached, and a pull ref stays detached for every role.
func TestSpecCheckoutSuccessorModeByRefAndRole5118(t *testing.T) {
	for _, test := range []struct {
		name string
		role string
		ref  string
		want *HubCheckout
	}{
		{
			name: "implementer on a branch",
			role: "implementer",
			ref:  "refs/heads/wk/x",
			want: &HubCheckout{Origin: "https://git.invalid/org/repo-a.git", Mode: "successor", Branch: "wk/x", Sha: hubHead, Dir: "job-a-checkout"},
		},
		{
			name: "reviewer on a branch stays detached",
			role: "reviewer",
			ref:  "refs/heads/wk/x",
			want: &HubCheckout{Origin: "https://git.invalid/org/repo-a.git", Mode: "detached", Sha: hubHead, Fetch: "refs/heads/wk/x", Dir: "job-a-checkout"},
		},
		{
			name: "consult on a branch stays detached",
			role: "consult",
			ref:  "refs/heads/wk/x",
			want: &HubCheckout{Origin: "https://git.invalid/org/repo-a.git", Mode: "detached", Sha: hubHead, Fetch: "refs/heads/wk/x", Dir: "job-a-checkout"},
		},
		{
			name: "implementer on a pull ref stays detached",
			role: "implementer",
			ref:  "pull/7/head",
			want: &HubCheckout{Origin: "https://git.invalid/org/repo-a.git", Mode: "detached", Sha: hubHead, Fetch: "pull/7/head", Dir: "job-a-checkout"},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			spec, name, err := Spawn{Job: "job-a", Role: test.role, CheckoutOrigin: "https://git.invalid/org/repo-a.git", CheckoutCommit: hubHead, CheckoutRef: test.ref}.checkoutSpec()
			if err != nil || name != "repo-a" || !reflect.DeepEqual(spec, test.want) {
				t.Fatalf("spec=%+v name=%q err=%v, want %+v", spec, name, err, test.want)
			}
		})
	}
}

// A sparse list belongs to a detached spec: a successor's branch checkout
// refuses the flags rather than running full.
func TestSpecCheckoutSuccessorRefusesSparseLists5118(t *testing.T) {
	_, _, err := Spawn{Job: "job-a", Role: "implementer", CheckoutOrigin: "https://git.invalid/org/repo-a.git", CheckoutCommit: hubHead, CheckoutRef: "refs/heads/wk/x", CheckoutExclude: []string{"Assets"}}.checkoutSpec()
	if err == nil || ErrorCode(err) != exitUsage || !strings.Contains(err.Error(), "detached checkout spec") {
		t.Fatalf("err=%v, want the detached-only refusal", err)
	}
}

// The hub must advertise checkout_branch_v1 before a successor spec is
// submitted: an older hub refuses the mode.
func TestSpecSpawnSuccessorNeedsTheHubFeature5118(t *testing.T) {
	checkout := &HubCheckout{Origin: "https://git.invalid/org/repo-a.git", Mode: HubCheckoutSuccessor, Branch: "wk/x", Sha: hubHead, Dir: "job-a-checkout"}
	for _, test := range []struct {
		name     string
		features []string
		accepted bool
	}{
		{"hub without the feature", []string{"start_conditions_v1", "checkout_spec_v1"}, false},
		{"hub with the feature", []string{"start_conditions_v1", "checkout_spec_v1", "checkout_branch_v1"}, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			var posted atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch {
				case r.Method == http.MethodGet && r.URL.Path == "/v1/spawns/features":
					json.NewEncoder(w).Encode(map[string]any{"features": test.features})
				case r.Method == http.MethodPost && r.URL.Path == "/v1/spawns":
					posted.Add(1)
					w.WriteHeader(http.StatusAccepted)
					json.NewEncoder(w).Encode(map[string]any{"job_id": "test-job", "request_id": "d676220b-e763-4af1-aefd-000000000001"})
				default:
					w.WriteHeader(http.StatusNotFound)
				}
			}))
			t.Cleanup(server.Close)
			c := hubHTTP{base: server.URL, token: "fixture", client: server.Client()}
			_, err := c.Submit(t.Context(), Submission{RequestID: "d676220b-e763-4af1-aefd-000000000001", CheckoutSpec: true, Repository: Repository{Checkout: checkout}})
			if test.accepted != (err == nil) || test.accepted != (posted.Load() == 1) {
				t.Fatalf("accepted=%v err=%v posted=%d", test.accepted, err, posted.Load())
			}
			if !test.accepted && (ErrorCode(err) != exitUsage || !strings.Contains(err.Error(), "--checkout-ref refs/heads/<branch>")) {
				t.Fatalf("refusal: code=%d err=%v", ErrorCode(err), err)
			}
		})
	}
}
