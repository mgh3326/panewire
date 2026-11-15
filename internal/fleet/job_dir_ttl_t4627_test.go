package fleet

import (
	"encoding/json"
	"testing"
)

// hk 4627: a submitter-side dir's submission.json is a frozen record, written
// by the panewire of its day. mergewake's reviewer submitter dirs (3,597 on
// oci, 2026-11-14) carry --builder-family, a flag retired since; the live
// spawn parser refuses it, which made the gate call every such receipt
// unreadable and keep the dir forever. The gate now parses the receipt with
// the retired flags accepted as one-value no-ops, so the dir is decided like
// any other submitter-side dir; a flag that was never a spawn flag still
// keeps it, and so does a retired name that is another flag's value.

// mergewakeSubmissionArgs is the args of the real oci receipt
// /home/opc/work/herdr-inbox/jobs/mwr-637-d24d143-p2-1794650830/submission.json
// with its paths pointed at the test rig.
func mergewakeSubmissionArgs(r *ttlRig, id string) []string {
	return []string{
		"-c", "/state/mergewake/review-checkouts/" + id,
		"-p", r.dir(id) + "/review-prompt.md",
		"-m", "auto",
		"-w", "workers",
		"-l", id,
		"--job", id,
		"--t", "T2",
		"--owner", "mergewake-design",
		"--role", "reviewer",
		"--task", "4556",
		"--host", "auto",
		"--report-relative", id + "/report.md",
		"--exclude-host", "oci",
		"--builder-family", "openai",
		"--builder-job", "pa-ci-evidence-4556d",
		"--review-pass", "2",
	}
}

// seedSubmissionArgs builds a submitter-side dir whose submission.json carries
// the spawn args as recorded: no events/, a receipt, its brief and the report
// prompt beside it, every file at the base time.
func (r *ttlRig) seedSubmissionArgs(id string, args []string) {
	r.t.Helper()
	raw, _ := json.Marshal(Submission{
		RequestID:       "66dd23c9-e1a1-4125-b457-c82695c46589",
		Args:            args,
		Brief:           "the brief",
		OriginMachine:   "oci",
		DeadlineSeconds: 600,
	})
	r.write(id, "submission.json", raw)
	r.write(id, "review-brief.md", []byte("the brief\n"))
	r.write(id, "review-prompt.md", []byte("the prompt\n"))
	r.age(id)
}

func TestJobDirTTLRetiredSubmitFlags(t *testing.T) {
	t.Run("mergewake receipt with a retired flag", func(t *testing.T) {
		r := newTTLRig(t)
		r.seedSubmissionArgs("mwr-637-d24d143-p2-1794650830", mergewakeSubmissionArgs(r, "mwr-637-d24d143-p2-1794650830"))
		r.store.records["mwr-637-d24d143-p2-1794650830"] = map[string]any{"job_id": "mwr-637-d24d143-p2-1794650830", "state": "completed"}
		wantDelete(t, r, r.pass(true), "mwr-637-d24d143-p2-1794650830", "submission-terminal")
	})
	t.Run("consult receipt with the retired routing role", func(t *testing.T) {
		r := newTTLRig(t)
		id := "consult-hk4300-b02-rootcause"
		args := []string{
			"-c", "/work/handoffkeep.hk4060-b02-issue-create",
			"-p", r.dir(id) + "/question.md",
			"-m", "codex-sol",
			"-w", "w2",
			"-l", id,
			"--job", id,
			"--t", "T2",
			"--owner", "handoffkeep-design",
			"--effort", "max",
			"--role", "counsel",
			"--parent-job", "hk4300-b02-takeover",
			"--host", "local",
			"--report-relative", id + "/answer.md",
			"--override-reason", "panewire resolve --role consult: profile=codex-sol effort=max",
			"--containment", "hk:4300",
			"--routing-role", "consult",
			"--keep",
		}
		r.seedSubmissionArgs(id, args)
		r.store.records[id] = map[string]any{"job_id": id, "state": "completed"}
		wantKeep(t, r, r.pass(true), id, "protected")
	})
	t.Run("retired flag written with =", func(t *testing.T) {
		r := newTTLRig(t)
		id := "mwr-2-attached"
		args := []string{"-c", "/work/tree", "-m", "auto", "-l", id, "--job", id, "--task", "42", "--owner", "owner", "--builder-family=openai"}
		r.seedSubmissionArgs(id, args)
		r.store.records[id] = map[string]any{"job_id": id, "state": "completed"}
		wantDelete(t, r, r.pass(true), id, "submission-terminal")
	})
	t.Run("a retired flag name that is another flag's value", func(t *testing.T) {
		// The receipt's --override-reason is free text; a reason that reads
		// "--builder-family" is a value, not a flag, so --keep must survive
		// (mergewake round 1, medium).
		r := newTTLRig(t)
		id := "mwr-4-value"
		args := []string{"-c", "/work/tree", "-m", "auto", "-l", id, "--job", id, "--task", "42", "--owner", "owner", "--override-reason", "--builder-family", "--keep"}
		r.seedSubmissionArgs(id, args)
		r.store.records[id] = map[string]any{"job_id": id, "state": "completed"}
		wantKeep(t, r, r.pass(true), id, "protected")
	})
	t.Run("a flag that was never a spawn flag keeps the dir", func(t *testing.T) {
		r := newTTLRig(t)
		id := "mwr-1-nonsense"
		args := append(mergewakeSubmissionArgs(r, id), "--nonsense", "value")
		r.seedSubmissionArgs(id, args)
		r.store.records[id] = map[string]any{"job_id": id, "state": "completed"}
		wantKeep(t, r, r.pass(true), id, "submission-unreadable")
	})
}
