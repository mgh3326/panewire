package panewire

import (
	"bytes"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// runDistVersion executes scripts/dist-version.sh. When useOverride is set the
// script reads the version from PANEWIRE_DIST_VERSION instead of git, which is
// also how release builds outside a checkout supply a tag. The variable is
// always removed from the inherited environment first so a caller's setting
// cannot leak into the git-derived path.
func runDistVersion(t *testing.T, override string, useOverride bool) (stdout, stderr string, err error) {
	t.Helper()
	cmd := exec.Command("sh", "scripts/dist-version.sh")
	env := make([]string, 0, len(os.Environ())+1)
	for _, kv := range os.Environ() {
		if !strings.HasPrefix(kv, "PANEWIRE_DIST_VERSION=") {
			env = append(env, kv)
		}
	}
	if useOverride {
		env = append(env, "PANEWIRE_DIST_VERSION="+override)
	}
	cmd.Env = env
	var outBuf, errBuf bytes.Buffer
	cmd.Stdout = &outBuf
	cmd.Stderr = &errBuf
	err = cmd.Run()
	return strings.TrimSpace(outBuf.String()), strings.TrimSpace(errBuf.String()), err
}

func captureStdout(t *testing.T, f func()) string {
	t.Helper()
	old := os.Stdout
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatalf("os.Pipe: %v", err)
	}
	os.Stdout = w
	f()
	if err := w.Close(); err != nil {
		t.Fatalf("close pipe writer: %v", err)
	}
	os.Stdout = old
	b, err := io.ReadAll(r)
	if err != nil {
		t.Fatalf("read pipe: %v", err)
	}
	return strings.TrimSpace(string(b))
}

// The version a release build can inject must always satisfy the same gate the
// hub applies to node hellos (hubVersionPattern). An out-of-pattern value would
// be silently replaced by "panewire-dev" in MainWithVersion, recreating the
// indistinguishable-node telemetry this task removes.
func TestDistVersionOutputSatisfiesHubPattern(t *testing.T) {
	version, stderr, err := runDistVersion(t, "", false)
	if err != nil {
		t.Fatalf("dist-version.sh failed: %v (stderr=%q)", err, stderr)
	}
	if version == "" || version == "panewire-dev" {
		t.Fatalf("dist-version.sh printed the placeholder %q", version)
	}
	if !hubVersionPattern.MatchString(version) {
		t.Fatalf("dist-version.sh printed %q, outside hubVersionPattern", version)
	}
}

func TestDistVersionRejectsOutOfPatternValues(t *testing.T) {
	for _, bad := range []string{
		"",
		"release/v1.0", // slash-delimited tag names are legal git refs
		"has space",
		"tag:1",
		"버전-1",
		strings.Repeat("a", 65), // over the 64-character cap
	} {
		if hubVersionPattern.MatchString(bad) {
			t.Fatalf("fixture %q unexpectedly satisfies hubVersionPattern", bad)
		}
		if _, _, err := runDistVersion(t, bad, true); err == nil {
			t.Fatalf("dist-version.sh accepted out-of-pattern version %q", bad)
		}
	}
}

func TestDistVersionPrintsValidValuesVerbatim(t *testing.T) {
	for _, good := range []string{"6a44c39", "v1.2.3", "v1.2.3-4-gabc123-dirty", "panewire-r10"} {
		if !hubVersionPattern.MatchString(good) {
			t.Fatalf("fixture %q does not satisfy hubVersionPattern", good)
		}
		got, stderr, err := runDistVersion(t, good, true)
		if err != nil {
			t.Fatalf("dist-version.sh rejected %q: %v (stderr=%q)", good, err, stderr)
		}
		if got != good {
			t.Fatalf("dist-version.sh printed %q, want %q", got, good)
		}
	}
}

// The deploy build path must actually stamp the derived version. Removing the
// -ldflags injection from build-linux.sh makes this fail: the binary then
// carries no -X main.version build setting.
func TestBuildLinuxScriptStampsDerivedVersion(t *testing.T) {
	want, stderr, err := runDistVersion(t, "", false)
	if err != nil {
		t.Fatalf("dist-version.sh failed: %v (stderr=%q)", err, stderr)
	}
	output := filepath.Join(t.TempDir(), "panewire-linux-amd64")
	if b, err := exec.Command("sh", "scripts/build-linux.sh", output).CombinedOutput(); err != nil {
		t.Fatalf("build-linux.sh failed: %v\n%s", err, b)
	}
	info, err := exec.Command("go", "version", "-m", output).CombinedOutput()
	if err != nil {
		t.Fatalf("go version -m failed: %v\n%s", err, info)
	}
	if !strings.Contains(string(info), "-X main.version="+want) {
		t.Fatalf("built binary lacks -X main.version=%s:\n%s", want, info)
	}
}

// The CLI sanitize is the second gate: even a value that slipped past the
// build-time check must never reach the node hello.
func TestVersionSubcommandSanitizesInjectedValue(t *testing.T) {
	var code int
	out := captureStdout(t, func() {
		code = MainWithVersion([]string{"version"}, "release/v1.0")
	})
	if code != ExitOK || out != "panewire-dev" {
		t.Fatalf("injected %q: code=%d version output=%q, want panewire-dev", "release/v1.0", code, out)
	}
	out = captureStdout(t, func() {
		code = MainWithVersion([]string{"version"}, "panewire-r10")
	})
	if code != ExitOK || out != "panewire-r10" {
		t.Fatalf("injected %q: code=%d version output=%q, want panewire-r10", "panewire-r10", code, out)
	}
}

// A plain `go build` (no ldflags) keeps the compiled-in placeholder, so
// development builds are unchanged.
func TestDevBuildReportsPlaceholderVersion(t *testing.T) {
	b, err := exec.Command("go", "run", "./cmd/panewire", "version").CombinedOutput()
	if err != nil {
		t.Fatalf("go run ./cmd/panewire version failed: %v\n%s", err, b)
	}
	if got := strings.TrimSpace(string(b)); got != "panewire-dev" {
		t.Fatalf("dev build reported %q, want panewire-dev", got)
	}
}
