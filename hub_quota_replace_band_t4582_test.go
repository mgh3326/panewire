package panewire

import "testing"

// A paid Devin stop's replacement excludes the paid band, not the catalog pool:
// excluding devin would drop the six SWE-2 free rungs that cool apart from the
// paid window, so the successor could not use them (hk 4582). The stopped job's
// hk record still names the catalog pool.
func TestQuotaStopPaidDevinReplacementExcludesOnlyPaidRungs_T4582(t *testing.T) {
	f := newQuotaReplaceFixture(t, "reviewer", "mwr-42-p1")
	f.jobs.records[f.job]["pool"] = "devin"
	f.stopPool = "devin"
	placed := hubSpawnOutcome{RC: 0, Stage: "done", Host: quotaPlaced, Pane: "w9:p1", Profile: "sonnet"}
	result, commands := f.run(t, placed, 4)
	if !result.ok || result.job != "mwr-42-p1-q1" {
		t.Fatalf("replacement=%+v, want mwr-42-p1-q1", result)
	}
	args := f.successor(t, "mwr-42-p1-q1").Args
	if got := hubSpawnArg(args, "--exclude-pool"); got != "devin-paid" {
		t.Fatalf("a paid devin stop's replacement must exclude the paid band alone: %q (%v)", got, args)
	}
	if reason := endReasonOf(t, commands[len(commands)-1]); reason != "quota_stop pool=devin replaced_by=mwr-42-p1-q1" {
		t.Fatalf("end_reason=%q", reason)
	}
}

// A free-band stop of the same catalog pool excludes the free band, not the
// paid one. The record pool is the launch pool, devin for both bands, so the
// band must come from the stop's own pool in the job.lost payload; reading it
// from the record would spend the paid band and leave another free rung of the
// exhausted account placeable (hk 4582).
func TestQuotaStopFreeBandDevinReplacementExcludesOnlyFreeRungs_T4582(t *testing.T) {
	f := newQuotaReplaceFixture(t, "reviewer", "mwr-43-p1")
	f.jobs.records[f.job]["pool"] = "devin"
	f.stopPool = "devin-free"
	placed := hubSpawnOutcome{RC: 0, Stage: "done", Host: quotaPlaced, Pane: "w9:p1", Profile: "sonnet"}
	result, commands := f.run(t, placed, 4)
	if !result.ok || result.job != "mwr-43-p1-q1" {
		t.Fatalf("replacement=%+v, want mwr-43-p1-q1", result)
	}
	args := f.successor(t, "mwr-43-p1-q1").Args
	if got := hubSpawnArg(args, "--exclude-pool"); got != "devin-free" {
		t.Fatalf("a devin-free stop's replacement must exclude the free band alone: %q (%v)", got, args)
	}
	if reason := endReasonOf(t, commands[len(commands)-1]); reason != "quota_stop pool=devin replaced_by=mwr-43-p1-q1" {
		t.Fatalf("end_reason must keep the record pool: %q", reason)
	}
}
