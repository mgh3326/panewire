package fleet

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
)

// catalogGateQuota is the production gate answering selection's catalog read
// from the fixture rows instead of handoffkeep's HTTP catalog. Selection and
// CLIQuota.Catalog both read CatalogDetails, which the embedded CLIQuota would
// otherwise serve over HTTP.
type catalogGateQuota struct {
	admissionQuota
	rows []CatalogEntry
}

func (q *catalogGateQuota) CatalogDetails(context.Context) (CatalogData, error) {
	return CatalogData{Rows: q.rows}, nil
}

// --requested-by names the lane a failed spawn notifies; it reaches the quota
// gate only beside --operator-request, where the gate records who asked
// (hk 3080). A bare --requested-by must place exactly as no flag did.
func TestRequestedByReachesGateOnlyWithOperatorRequest(t *testing.T) {
	a, s, _, _ := gateReasonFixture(t, 40)
	s.RequestedBy = "operator"
	qr, ok := a.quotaRequest(s)
	if !ok || qr.RequestedBy != "" {
		t.Fatalf("bare --requested-by reached the gate request: %+v", qr)
	}
	seat := a.Policy.Profiles[s.Profile]
	launchQR := a.spawnQuotaRequest(s, seat, seat.Effort)
	if launchQR.RequestedBy != "" {
		t.Fatalf("bare --requested-by reached the launch gate request: %+v", launchQR)
	}
	s.OperatorRequest, s.Purpose = "hk:task/3836", "operator-request"
	qr, _ = a.quotaRequest(s)
	if qr.RequestedBy != "operator" || qr.OperatorRequest != s.OperatorRequest {
		t.Fatalf("operator request lost its requester: %+v", qr)
	}
}

// The same bare flag must not refuse the placement the gate would have
// refused on the request shape alone.
func TestRequestedByAlonePlacesThroughTheGate(t *testing.T) {
	a, s, q, _ := gateReasonFixture(t, 40)
	s.RequestedBy = "operator"
	handled, rec, err := a.placeAuto(context.Background(), s, "brief")
	if err != nil || !handled || rec == nil || rec["chose"] != "first" {
		t.Fatalf("bare --requested-by refused placement: handled=%v rec=%v err=%v", handled, rec, err)
	}
	if q.gates != 1 {
		t.Fatalf("host gate asked %d times, want 1", q.gates)
	}
}

// `-m auto` judges the same request per candidate: a bare --requested-by
// admits there too, and an operator request on an ordinary rung reports the
// gate's refusal instead of QUOTA_UNKNOWN (hk 3080).
func TestAutoSelectionRequestedByAndGateReason(t *testing.T) {
	var rows []CatalogEntry
	if err := json.Unmarshal([]byte(gateReasonCatalog), &rows); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name    string
		edit    func(*Spawn)
		wantErr string
		wantOK  bool
	}{
		{"requested-by alone admits", func(s *Spawn) { s.RequestedBy = "operator" }, "", true},
		{"operator request on ordinary rung", func(s *Spawn) {
			s.OperatorRequest, s.Purpose, s.RequestedBy = "hk:task/3836", "operator-request", "operator"
		}, "operator_request_not_applicable", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			a, s, q, _ := gateReasonFixture(t, 40)
			a.Quota = &catalogGateQuota{admissionQuota: q.admissionQuota, rows: rows}
			s.Profile, s.Grade = "auto", "A"
			tc.edit(&s)
			src := a.Source.(*fakePlacementSource)
			src.quotaDo = func(string) *PoolView { return ptrPool(riskPool(30, 8)) }
			sel, err := a.selectProfile(context.Background(), s)
			if tc.wantOK {
				if err != nil || sel == nil || sel.chosen == nil {
					t.Fatalf("selection refused: %v", err)
				}
				return
			}
			if err == nil {
				t.Fatalf("selection admitted an operator request on an ordinary rung: %+v", sel.chosen)
			}
			if !strings.Contains(err.Error(), tc.wantErr) || strings.Contains(err.Error(), "QUOTA_UNKNOWN") {
				t.Fatalf("gate refusal reason lost: %v", err)
			}
			if sel == nil || sel.outcome != outcomePolicyUnavailable {
				t.Fatalf("outcome=%s want %s", sel.outcome, outcomePolicyUnavailable)
			}
		})
	}
}
