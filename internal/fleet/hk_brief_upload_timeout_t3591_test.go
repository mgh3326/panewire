package fleet

import (
	"context"
	"strings"
	"testing"
	"time"
)

// The hub brief upload has its own bound, longer than the per-call bound
// every other spawn-path hk call keeps: the hk CLI's `doc put` had no timeout
// of its own, and a large brief must not fail at the 8 s a task read gets.
// Shrunk bounds prove which one each call uses: an hk that answers after
// both the task read's bound and less than the upload's lets the upload
// through and times the task read out.
func TestHKBriefUploadHasItsOwnLongerBound(t *testing.T) {
	if hkUploadTimeout < 60*time.Second || hkCallTimeout != catalogTimeout {
		t.Fatalf("bounds: upload %v call %v, want the upload at least 60s and calls at the catalog's %v", hkUploadTimeout, hkCallTimeout, catalogTimeout)
	}
	call, upload := hkCallTimeout, hkUploadTimeout
	t.Cleanup(func() { hkCallTimeout, hkUploadTimeout = call, upload })
	hkCallTimeout, hkUploadTimeout = 150*time.Millisecond, 20*time.Second

	ctx := context.Background()
	a, hk := hkSiteApp(t)
	hk.SetTask("42", hkTaskJSON(42, "ready", "implement (grade B, T1)", "", "panewire", `{}`)).
		Handle(func(r hkRequest) (int, string, bool) {
			time.Sleep(600 * time.Millisecond)
			return 0, "", false
		})
	if key, e := a.putBrief(ctx, Spawn{Job: "job-a"}, "briefs/node-a/", "brief"); e != nil || key != "briefs/node-a/job-a.md" {
		t.Fatalf("putBrief under its own bound=%q %v", key, e)
	}
	if _, _, e := a.taskPrecheck(ctx, Spawn{Task: "42", Job: "job-a", Label: "worker-a"}); exitCode(e) != exitConfig || !strings.Contains(hkErrOf(e), "task precheck unavailable") {
		t.Fatalf("a task read slower than the call bound=%v, want it timed out", e)
	}
	hkOnly(t, hk, "PUT /v1/documents/briefs%2Fnode-a%2Fjob-a.md", "GET /v1/tasks/42")
}
