package fleet

import (
	"context"
	"encoding/json"
	"testing"
)

func TestUnratedTopRoleReachesQuota(t *testing.T) {
	for _, role := range []string{"consult"} {
		t.Run(role, func(t *testing.T) {
			a, _, q := consultFixture(t)
			q.CatalogRows = []CatalogRow{{Profile: "codex-sol", Effort: "high", Grade: "unrated", Pool: "codex", Gate: "default", ModelID: "sol-m"}}
			admit := gatePools(map[string]string{"codex-sol": "codex"})
			q.GateDo = func(req QuotaRequest) (string, error) {
				if req.Role != role {
					return "", fail(exitSeatRefused, "unrated %s quota request received seat role %s", role, req.Role)
				}
				return admit(req)
			}
			if err := a.resolveCmd(context.Background(), []string{"--role", role, "--json"}); err != nil {
				t.Fatal(err)
			}
			var result resolveResult
			if err := json.Unmarshal([]byte(a.Out.(interface{ String() string }).String()), &result); err != nil {
				t.Fatal(err)
			}
			if result.Profile != "codex-sol" || result.SeatRole != "worker" || result.RoutingRole != role {
				t.Fatalf("routing role changed seat behavior: %+v", result)
			}
		})
	}
}

func TestQuotaRequestKeepsTopRoutingRole(t *testing.T) {
	a := testApp(t)
	for _, tc := range []struct{ seatRole, routingRole, want string }{
		{"worker", "consult", "consult"},
		{"counsel", "consult", "consult"},
		{"worker", "implementer", "worker"}, {"tester", "verifier", "tester"}, {"worker", "", "worker"},
	} {
		qr, ok := a.quotaRequest(Spawn{Profile: "codex-sol", Effort: "high", Role: tc.seatRole, RoutingRole: tc.routingRole})
		if !ok || qr.Role != tc.want {
			t.Errorf("seat=%s routing=%s quota role=%s want=%s", tc.seatRole, tc.routingRole, qr.Role, tc.want)
		}
	}
}
