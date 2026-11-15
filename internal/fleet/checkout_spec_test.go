package fleet

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
)

// hk 4738: a spawn may name its worktree with --checkout-origin,
// --checkout-commit and --checkout-ref instead of holding a local -c checkout.
// The submitting host reads no checkout; the node prepares the same detached
// worktree the -c path would have produced.

func TestSpawnCheckoutSpecValidation(t *testing.T) {
	base := []string{"-l", "job-a", "--job", "job-a", "--t", "T1", "--task", "42", "-p", "/work/brief.md"}
	valid := []string{"--checkout-origin", "https://git.invalid/org/repo-a.git", "--checkout-commit", hubHead, "--checkout-ref", "pull/7/head"}
	for _, test := range []struct {
		name string
		args []string
		want string
	}{
		{"no spec", base, ""},
		{"origin alone", append(slices.Clone(base), "--checkout-origin", "https://git.invalid/org/repo-a.git"), "together"},
		{"commit alone", append(slices.Clone(base), "--checkout-commit", hubHead), "together"},
		{"ref alone", append(slices.Clone(base), "--checkout-ref", "pull/7/head"), "together"},
		{"with -c", append(append(slices.Clone(base), "-c", "/work/repo-a"), valid...), "exclusive"},
		{"commit too short", append(slices.Clone(base), "--checkout-origin", "https://git.invalid/org/repo-a.git", "--checkout-commit", hubHead[:39], "--checkout-ref", "pull/7/head"), "40-character commit id"},
		{"commit upper case", append(slices.Clone(base), "--checkout-origin", "https://git.invalid/org/repo-a.git", "--checkout-commit", strings.ToUpper(hubHead), "--checkout-ref", "pull/7/head"), "40-character commit id"},
		{"ref is a tag", append(slices.Clone(base), "--checkout-origin", "https://git.invalid/org/repo-a.git", "--checkout-commit", hubHead, "--checkout-ref", "refs/tags/v1"), "pull/<n>/head or refs/heads/<branch>"},
		{"ref is a bare branch", append(slices.Clone(base), "--checkout-origin", "https://git.invalid/org/repo-a.git", "--checkout-commit", hubHead, "--checkout-ref", "main"), "pull/<n>/head or refs/heads/<branch>"},
		{"origin carries a credential", append(slices.Clone(base), "--checkout-origin", "https://user:pw@git.invalid/org/repo-a.git", "--checkout-commit", hubHead, "--checkout-ref", "pull/7/head"), "naming owner/repo"},
		{"origin names no owner", append(slices.Clone(base), "--checkout-origin", "https://git.invalid/repo-a.git", "--checkout-commit", hubHead, "--checkout-ref", "pull/7/head"), "naming owner/repo"},
		{"origin is a local path", append(slices.Clone(base), "--checkout-origin", "/work/fleet/repo-a", "--checkout-commit", hubHead, "--checkout-ref", "pull/7/head"), "naming owner/repo"},
		{"valid spec", valid, ""},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, err := parseSpawn(test.args)
			if test.want == "" {
				if err != nil {
					t.Fatalf("refused: %v", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), test.want) || ErrorCode(err) != exitUsage {
				t.Fatalf("want a usage refusal naming %q, got %v (code %d)", test.want, err, ErrorCode(err))
			}
		})
	}

	// Every remote form the docs name is accepted, and the spec carries the
	// userinfo-free https form the -c path uploads, which the node clones.
	for _, test := range []struct{ name, origin, want string }{
		{"https", "https://git.invalid/org/repo-a.git", "https://git.invalid/org/repo-a.git"},
		{"scp with user", "git@git.invalid:org/repo-a.git", "https://git.invalid/org/repo-a.git"},
		{"scp without user", "git.invalid:org/repo-a", "https://git.invalid/org/repo-a"},
		{"ssh", "ssh://git@git.invalid/org/repo-a", "https://git.invalid/org/repo-a"},
	} {
		spec, name, err := Spawn{Job: "job-a", CheckoutOrigin: test.origin, CheckoutCommit: hubHead, CheckoutRef: "pull/7/head"}.checkoutSpec()
		if err != nil || name != "repo-a" || !reflect.DeepEqual(spec, &HubCheckout{Origin: test.want, Mode: "detached", Sha: hubHead, Fetch: "pull/7/head", Dir: "job-a-checkout"}) {
			t.Fatalf("%s: spec=%+v name=%q err=%v", test.name, spec, name, err)
		}
	}
}

// The submitter builds the pinned checkout the node prepares from the flags
// alone: no local repo is read, no -c travels, and the hub places it on h1.
func TestSpecSpawnCarriesThePinnedCheckoutWithoutACheckout(t *testing.T) {
	a, s, f, hub, _ := hubFixture(t, gitState{}, "", hubHosts())
	brief := filepath.Join(a.Home, "spec-brief.md")
	if err := os.WriteFile(brief, []byte("the brief"), 0600); err != nil {
		t.Fatal(err)
	}
	s.CWD = ""
	s.Prompt = brief
	s.Label, s.T, s.Owner, s.Role, s.Host = "job-a", "T1", "parent", "reviewer", "h1"
	s.OverrideReason, s.Containment = "fixture", "hk:12"
	s.CheckoutOrigin = "https://git.invalid/org/repo-a.git"
	s.CheckoutCommit = hubHead
	s.CheckoutRef = "pull/7/head"

	request, err := a.PrepareSubmission(t.Context(), s)
	if err != nil {
		t.Fatal(err)
	}
	want := &HubCheckout{Origin: "https://git.invalid/org/repo-a.git", Mode: "detached", Sha: hubHead, Fetch: "pull/7/head", Dir: "job-a-checkout"}
	repo := request.Repository
	if !reflect.DeepEqual(repo.Checkout, want) || repo.Name != "repo-a" || repo.Worktree != want.Dir || repo.Origin != want.Origin || repo.LocalRoot != "" || repo.Relative != "" {
		t.Fatalf("repository=%+v checkout=%+v", repo, repo.Checkout)
	}
	if slices.Contains(request.Args, "-c") || slices.Contains(request.Args, "--checkout-origin") {
		t.Fatalf("the spec must travel as the checkout field, not as args: %v", request.Args)
	}
	if !request.CheckoutSpec {
		t.Fatal("the request must mark itself so Submit asks for checkout_spec_v1")
	}
	if calls := f.Calls; len(calls) != 0 {
		t.Fatalf("a spec spawn must read no local checkout, got %v", calls)
	}
	parsed, err := ValidateSubmission(request)
	if err != nil || parsed.CWD != "" {
		t.Fatalf("hub validation: %v cwd=%q", err, parsed.CWD)
	}

	// The hub places the request with the same checkout the node prepares.
	a.Repository = &request.Repository
	handled, rec, err := a.placeAuto(t.Context(), parsed, "the brief")
	if err != nil || !handled || rec["chose"] != "h1" {
		t.Fatalf("handled=%v rec=%v err=%v", handled, rec, err)
	}
	if len(hub.spawns) != 1 || !reflect.DeepEqual(hub.spawns[0].Checkout, want) || hub.spawns[0].CWDKey != "repo-a" {
		t.Fatalf("spawn=%+v", hub.spawns)
	}
	if sshCalls(f) != 0 {
		t.Fatal("ssh was used")
	}
}

// A hub that does not advertise checkout_spec_v1 would run the job in the
// node's cwd_map path instead of the named commit, so the CLI refuses first.
func TestSpecSpawnNeedsTheHubFeature(t *testing.T) {
	for _, test := range []struct {
		name     string
		features []string
		accepted bool
	}{
		{"hub without the feature", []string{"start_conditions_v1", "spawn_expires_v1"}, false},
		{"hub with the feature", []string{"start_conditions_v1", "spawn_expires_v1", "checkout_spec_v1"}, true},
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
			_, err := c.Submit(t.Context(), Submission{RequestID: "d676220b-e763-4af1-aefd-000000000001", CheckoutSpec: true})
			if test.accepted != (err == nil) || test.accepted != (posted.Load() == 1) {
				t.Fatalf("accepted=%v err=%v posted=%d", test.accepted, err, posted.Load())
			}
			if !test.accepted && (ErrorCode(err) != exitUsage || !strings.Contains(err.Error(), "--checkout-origin")) {
				t.Fatalf("refusal: code=%d err=%v", ErrorCode(err), err)
			}
		})
	}
}
