package fleet

import (
	"context"
	"testing"
)

func TestUnratedTopRoleReachesQuota(t *testing.T) {
	for _, role := range []string{"consult"} {
		t.Run(role, func(t *testing.T) {
			a, _, q := consultFixture(t)
			q.CatalogRows = []CatalogRow{{Profile: "codex-sol", Effort: "high", Grade: "unrated", Pool: "codex", Gate: "default", ModelID: "sol-m"}}
			gates := 0
			q.GateDo = func(QuotaRequest) (string, error) {
				gates++
				return "", fail(exitSeatRefused, "unrated %s rung reached quota", role)
			}
			err := a.resolveCmd(context.Background(), []string{"--role", role, "--json"})
			if exitCode(err) != exitSeatRefused || gates != 0 {
				t.Fatalf("unrated %s: err=%v gates=%d; an unrated rung must never reach quota", role, err, gates)
			}
		})
	}
}

func TestQuotaRequestKeepsTopRoutingRole(t *testing.T) {
	a := testApp(t)
	for _, tc := range []struct{ seatRole, routingRole, want string }{
		{"implementer", "consult", "consult"},
		{"consult", "consult", "consult"},
		{"implementer", "implementer", "implementer"}, {"implementer", "verifier", "implementer"}, {"implementer", "", "implementer"}, {"reviewer", "", "reviewer"},
	} {
		qr, ok := a.quotaRequest(Spawn{Profile: "codex-sol", Effort: "high", Role: tc.seatRole, RoutingRole: tc.routingRole})
		if !ok || qr.Role != tc.want {
			t.Errorf("seat=%s routing=%s quota role=%s want=%s", tc.seatRole, tc.routingRole, qr.Role, tc.want)
		}
	}
}
