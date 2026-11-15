package fleet

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// A host table that still lists the retired builder seat is a config error
// that names the key to edit, for a remote host and for [local].
func TestHostConfigListingBuilderIsRefusedNamingKey(t *testing.T) {
	for name, tc := range map[string]struct{ body, want string }{
		"remote host": {"[hosts.pi]\nroles = [\"implementer\", \"builder\"]\n", `delete "builder" from hosts.pi.roles`},
		"local":       {"[local]\nroles = [\"implementer\", \"builder\"]\n", `delete "builder" from local.roles`},
	} {
		t.Run(name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "hosts.toml")
			if err := os.WriteFile(path, []byte(tc.body), 0o600); err != nil {
				t.Fatal(err)
			}
			_, err := LoadConfig(path)
			if exitCode(err) != exitConfig || err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("want exit %d naming %q, got %v", exitConfig, tc.want, err)
			}
		})
	}
}

// panewire reads no lead entry. A policy with none routes implementer,
// reviewer and consult from their own entries and refuses nothing.
func TestPolicyWithoutLeadEntryRoutesEverySeatRole(t *testing.T) {
	if strings.Contains(routingBlockV1, `"lead"`) {
		t.Fatal("the routing fixture must not carry a lead entry")
	}
	for seat, want := range map[string]string{"implementer": "implementer", "reviewer": "reviewer", "consult": "consult"} {
		a := testApp(t)
		a.Runner = &fakeRunner{}
		serveRouting(t, routingDocV1)
		r, err := a.routeFor(context.Background(), seat, "")
		if err != nil || r.role != want {
			t.Fatalf("%s: want routing role %s, got %+v %v", seat, want, r, err)
		}
	}
}

// A lead entry left in the table does not stand in for a missing consult
// entry, and lead is no longer a routing role anyone can ask for.
func TestLeadEntryInPolicyIsNeverRead(t *testing.T) {
	leadOnly := strings.Replace(routingBlockV1, `"consult":     {`, `"lead":        {`, 1)
	a := testApp(t)
	a.Runner = &fakeRunner{}
	serveRouting(t, routingDoc(leadOnly))
	if _, err := a.routeFor(context.Background(), "consult", ""); exitCode(err) != exitConfig || err == nil || !strings.Contains(err.Error(), "POLICY_UNAVAILABLE") {
		t.Fatalf("consult must not fall back to a lead entry: %v", err)
	}
	if _, err := a.routeFor(context.Background(), "implementer", "lead"); exitCode(err) != exitUsage {
		t.Fatalf("a lead routing override must be a usage refusal: %v", err)
	}
	if err := a.resolveCmd(context.Background(), []string{"--role", "lead"}); exitCode(err) != exitUsage {
		t.Fatalf("resolve --role lead must be a usage refusal: %v", err)
	}
}

// Until desk deletes it (stage 2), the table still carries a lead entry, with
// the family_effort lead had. panewire ignores it without refusing the table.
func TestLeadEntryWithFamilyEffortDoesNotBreakThePolicy(t *testing.T) {
	withLead := strings.Replace(routingBlockV1, `"consult":     {`, `"lead":        {"allowed": ["opus"], "effort": "high", "on_exhaustion": "wait", "family_effort": {"codex-sol": "xhigh"}},
    "consult":     {`, 1)
	p, err := parseRoutingDoc(routingDoc(withLead))
	if err != nil {
		t.Fatalf("a retired lead entry must not make the table unreadable: %v", err)
	}
	if _, ok := p.Roles["lead"]; ok || p.Roles["consult"].Allowed == nil {
		t.Fatalf("lead must be dropped and consult kept: %v", p.Roles)
	}
}
