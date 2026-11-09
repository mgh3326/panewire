package panewire

import (
	"os"
	"path/filepath"
	"testing"
)

// hk 3702 (f): job.done leaves both files it may be given where they are,
// whether the report is report.md or the pane screen, and whether or not hk
// holds the screen. Removing the screen after done is not part of this change
// (decision C), so this passes on the base too: it pins that the job dir keeps
// its files until the TTL net or the job-end cleanup removes the job dir.
func TestNodeJobDoneRemovesNoFile_t3702(t *testing.T) {
	for _, report := range []string{nodeJobReportFile, nodeCaptureScreenTxt} {
		t.Run(report, func(t *testing.T) {
			_, _, lifecycle := nodeReapLifecycle(t, "", false)
			client, path := nodeReapJobDir(t, "job-a", report)
			dir := filepath.Dir(path)
			if err := os.WriteFile(filepath.Join(dir, nodeJobReportFile), []byte("report\n"), 0600); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(dir, nodeCaptureScreenTxt), []byte("screen\n"), 0600); err != nil {
				t.Fatal(err)
			}
			client.nodeLifecycle = lifecycle

			result, code := nodeRun(t, client, nodeCommandJobDone, nodeJobDoneArgs{Job: "job-a", Report: path})
			if code != "" || result["outcome"] != nodeJobDoneOK {
				t.Fatalf("code=%q result=%v", code, result)
			}
			for _, name := range []string{nodeJobReportFile, nodeCaptureScreenTxt} {
				if _, err := os.Lstat(filepath.Join(dir, name)); err != nil {
					t.Fatalf("job.done with %s removed %s: %v", report, name, err)
				}
			}
		})
	}
}
