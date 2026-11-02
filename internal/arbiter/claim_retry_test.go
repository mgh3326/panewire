package arbiter

import "testing"

func TestReleaseClaimRetryAndResources(t *testing.T) {
	s, _ := newStore(t)
	claim(t, s, "failed")
	if err := s.ReleaseClaim(ctx, "failed"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Claim(ctx, Claim{Job: "failed", Lane: "lane1", T: "T1", ReclaimReleased: true}); err != nil {
		t.Fatalf("retry: %v", err)
	}
	if _, err := s.RecordQuotaPool(ctx, QuotaRun{Job: "failed", Profile: "fixture", Gate: Gate{Pool: "example"}}); err != nil {
		t.Fatal(err)
	}
	if err := s.ReleaseClaim(ctx, "failed"); err == nil {
		t.Fatal("resource-bearing job was released")
	}
	job, err := s.JobGet(ctx, "failed")
	if err != nil || job.State != "leased" {
		t.Fatalf("%+v %v", job, err)
	}
	if _, err := s.Claim(ctx, Claim{Job: "failed", Lane: "lane1", T: "T1", ReclaimReleased: true}); err == nil {
		t.Fatal("live duplicate was reclaimed")
	}
}
