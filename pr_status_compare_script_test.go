package panewire

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// The comparison harness is the operator's way to put panewire, hk and gh side
// by side (docs/pr-status.md); this runs it against fakes of all three.
func TestPRStatusCompareScript(t *testing.T) {
	for _, tool := range []string{"bash", "jq", "python3"} {
		if _, err := exec.LookPath(tool); err != nil {
			t.Skipf("the harness needs %s", tool)
		}
	}
	dir := t.TempDir()
	counter := filepath.Join(dir, "counter")
	if err := os.WriteFile(counter, []byte("100\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	write := func(name, body string) string {
		path := filepath.Join(dir, name)
		if err := os.WriteFile(path, []byte("#!/bin/sh\n"+body), 0o755); err != nil {
			t.Fatal(err)
		}
		return path
	}
	statusJSON := `{"source":"panewire","mergewake":null,"snapshot":{"source":"hk","state":"blocked","checks":[{"name":"a","state":"green"},{"name":"b","state":"red"}]}}`
	findingsJSON := `{"source":"panewire","findings":[{"id":"1"},{"id":"2"}],"findings_omitted":1}`
	fallbackJSON := `{"source":"gh","mergewake":null}`
	panewire := write("panewire", `
[ "$1" = pr ] || exit 2
case "$2:$3" in
  status:owner/name) case "$4" in 7) echo '`+statusJSON+`' ;; *) echo '`+fallbackJSON+`' ;; esac ;;
  findings:owner/name) echo '`+findingsJSON+`' ;;
esac
`)
	hkJSON := `{"change":{"state":"open","head_sha":"new"},"checks":[
	  {"head_sha":"new","name":"a","state":"completed","conclusion":"success","required":true},
	  {"head_sha":"new","name":"b","state":"completed","conclusion":"failure","required":true},
	  {"head_sha":"new","name":"c","state":"completed","conclusion":"failure","required":false},
	  {"head_sha":"new","name":"d","state":"in_progress","required":true,"gone":true},
	  {"head_sha":"old","name":"e","state":"in_progress","required":true},
	  {"head_sha":"old","name":"f","state":"completed","conclusion":"failure"}],
	 "threads":[{"resolved":false},{"resolved":false},{"resolved":false},{"resolved":true}]}`
	hk := write("handoffkeep", `
[ "$1 $2 $3 $4 $5" = "changes show --repo owner/name --pr" ] || exit 2
case "$6" in 7) echo '`+hkJSON+`' ;; *) exit 1 ;; esac
`)
	gh := write("gh", `
bump() { n=$(cat '`+counter+`'); echo $((n + 1)) > '`+counter+`'; }
case "$1 $2" in
  "api rate_limit") cat '`+counter+`' ;;
  "pr view") bump; echo '{"state":"OPEN"}' ;;
  "pr checks") bump; echo '[{"bucket":"pass"},{"bucket":"fail"}]' ;;
  "api graphql")
    bump
    case "$*" in *--paginate*) ;; *) exit 3 ;; esac
    # --paginate prints one JSON document per page: the threads are split over two.
    echo '{"data":{"repository":{"pullRequest":{"reviewThreads":{"nodes":[{"isResolved":false},{"isResolved":true}]}}}}}'
    echo '{"data":{"repository":{"pullRequest":{"reviewThreads":{"nodes":[{"isResolved":false},{"isResolved":false}]}}}}}' ;;
  *) exit 2 ;;
esac
`)
	script, err := filepath.Abs("scripts/pr-status-compare.sh")
	if err != nil {
		t.Fatal(err)
	}
	hkBin := hk
	run := func(args ...string) (string, error) {
		cmd := exec.Command("bash", append([]string{script}, args...)...)
		cmd.Env = append(os.Environ(), "PANEWIRE_BIN="+panewire, "GH_BIN="+gh, "HK_BIN="+hkBin)
		out, err := cmd.CombinedOutput()
		return string(out), err
	}
	out, err := run("owner/name", "7", "8")
	if err != nil {
		t.Fatalf("%v: %s", err, out)
	}
	rows := strings.Split(strings.TrimSuffix(out, "\n"), "\n")
	if len(rows) != 3 || !strings.HasPrefix(rows[0], "pr\tpanewire_state\t") {
		t.Fatalf("output %q", out)
	}
	cells := strings.Split(rows[1], "\t")
	// pr, states, checks, findings (2 listed + 1 omitted = 3 threads), then the
	// three gh requests the harness counted itself.
	want := []string{"7", "blocked", "OPEN", "yes", "green:1,red:1", "green:1,red:1", "yes", "3", "3", "yes"}
	if len(cells) != 22 || strings.Join(cells[:10], "|") != strings.Join(want, "|") || cells[12] != "3" || cells[13] != "panewire" || cells[14] != "hk" {
		t.Fatalf("row %q", rows[1])
	}
	// The hk columns read hk directly: the checks of the current head that are not
	// known to be optional (the not-required, the gone and the earlier-head ones are left out) and the
	// unresolved threads.
	wantHK := []string{"OPEN", "yes", "green:1,red:1", "yes", "3", "yes"}
	if got := strings.Join(cells[15:21], "|"); got != strings.Join(wantHK, "|") || cells[21] == "-" {
		t.Fatalf("hk cells %q in %q", got, rows[1])
	}
	fallback := strings.Split(rows[2], "\t")
	if fallback[0] != "8" || fallback[1] != "-" || fallback[3] != "n/a" || fallback[6] != "n/a" || fallback[9] != "n/a" || fallback[13] != "gh" || fallback[14] != "-" {
		t.Fatalf("fallback row %q", rows[2])
	}
	// hk answering an error, or no handoffkeep CLI at all, leaves its columns "-".
	if strings.Join(fallback[15:], "|") != "-|n/a|-|n/a|-|n/a|-" {
		t.Fatalf("hk error cells %q", rows[2])
	}
	hkBin = filepath.Join(dir, "absent")
	out, err = run("owner/name", "7")
	if cells = strings.Split(strings.Split(strings.TrimSuffix(out, "\n"), "\n")[1], "\t"); err != nil || strings.Join(cells[15:], "|") != "-|n/a|-|n/a|-|n/a|-" {
		t.Fatalf("without handoffkeep: %v %q", err, out)
	}
	hkBin = hk
	for _, args := range [][]string{{}, {"owner/name"}, {"noslash", "1"}, {"owner/name", "x"}} {
		if out, err := run(args...); err == nil {
			t.Errorf("args %v accepted: %s", args, out)
		}
	}
}
