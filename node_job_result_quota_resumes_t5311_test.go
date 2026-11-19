package panewire

import (
	"io"
	"log/slog"
	"path/filepath"
	"testing"
)

// job.result reports how many quota-stop resumes the node recorded for the
// job (hk 5311). The count is read from the store the relay outbox shares.
func TestNodeJobResultQuotaResumes(t *testing.T) {
	client, _ := nodeReapJobDir(t, "job-a", "report.md")
	store, err := OpenStore(filepath.Join(t.TempDir(), "store.sqlite3"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.Close() })
	if _, err := newQuotaStopManager(store, t.TempDir(), false, slog.New(slog.NewTextHandler(io.Discard, nil))); err != nil {
		t.Fatal(err)
	}
	insert := func(event, job string, attempted int) {
		t.Helper()
		if _, err := store.db.Exec(`INSERT INTO quota_stop_decisions(event_id,job,pane,label,attempted) VALUES(?,?,?,?,?)`, event, job, "w:p1", "worker", attempted); err != nil {
			t.Fatal(err)
		}
	}
	insert("e1", "job-a", 1)
	insert("e2", "job-a", 0)
	insert("e3", "job-b", 1)

	count := func(t *testing.T, job string) *int {
		t.Helper()
		got, code := client.nodeJobResult(nodeJobResultArgs{Job: job})
		result, _ := got.(nodeJobResult)
		if code != "" {
			t.Fatalf("job %s code=%q", job, code)
		}
		return result.QuotaResumes
	}

	// Without an attached store the count is unknown, not zero.
	if got := count(t, "job-a"); got != nil {
		t.Fatalf("no store attached: quota_resumes=%d, want unknown", *got)
	}
	client.outbox = store
	if got := count(t, "job-a"); got == nil || *got != 1 {
		t.Fatalf("job-a: quota_resumes=%v, want 1 (attempted rows only)", got)
	}
	if got := count(t, "job-b"); got == nil || *got != 1 {
		t.Fatalf("job-b: quota_resumes=%v, want 1", got)
	}
	// A job the node has no directory for still reports what it recorded.
	if got := count(t, "job-c"); got == nil || *got != 0 {
		t.Fatalf("job-c: quota_resumes=%v, want 0", got)
	}
}

func TestNodeJobResultQuotaResumesValidation(t *testing.T) {
	negative := -1
	if validNodeJobResult(nodeJobResult{Found: true, FindingsError: "missing", QuotaResumes: &negative}) {
		t.Fatal("negative resume count accepted")
	}
	if validNodeJobResult(nodeJobResult{QuotaResumes: &negative}) {
		t.Fatal("negative resume count accepted for an absent job")
	}
	zero := 0
	if !validNodeJobResult(nodeJobResult{Found: true, FindingsError: "missing", QuotaResumes: &zero}) || !validNodeJobResult(nodeJobResult{QuotaResumes: &zero}) {
		t.Fatal("zero resume count rejected")
	}
}
