package panewire

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

func TestT574ReleaseBuildStampsNamesAndSums(t *testing.T) {
	head, err := exec.Command("git", "rev-parse", "HEAD").Output()
	if err != nil {
		t.Fatalf("git rev-parse: %v", err)
	}
	want := "pw-" + strings.TrimSpace(string(head))[:7]
	host := runtime.GOOS + "/" + runtime.GOARCH
	targets := "linux/arm64 " + host
	if host == "linux/arm64" {
		targets = "darwin/amd64 " + host
	}
	output := t.TempDir()
	cmd := exec.Command("sh", "scripts/build-release.sh", output)
	cmd.Env = append(os.Environ(), "PANEWIRE_RELEASE_TARGETS="+targets)
	out, err := cmd.Output()
	if err != nil {
		t.Fatalf("build-release.sh: %v", err)
	}
	if got := strings.TrimSpace(string(out)); got != want {
		t.Fatalf("release version=%q, want %q", got, want)
	}
	sums, err := os.ReadFile(filepath.Join(output, "SHA256SUMS"))
	if err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(strings.TrimSpace(string(sums)), "\n")
	if len(lines) != 2 {
		t.Fatalf("SHA256SUMS=%q", sums)
	}
	for _, line := range lines {
		sum, name, found := strings.Cut(line, "  ")
		if !found {
			t.Fatalf("SHA256SUMS line %q", line)
		}
		asset, err := os.ReadFile(filepath.Join(output, name))
		if err != nil {
			t.Fatal(err)
		}
		if digest := sha256.Sum256(asset); hex.EncodeToString(digest[:]) != sum {
			t.Fatalf("%s checksum mismatch", name)
		}
		// -trimpath keeps -ldflags out of the build info, so look for the
		// stamped string itself and for the static, trimmed build settings.
		if !bytes.Contains(asset, []byte(want)) {
			t.Fatalf("%s lacks the stamped version %s", name, want)
		}
		info, err := exec.Command("go", "version", "-m", filepath.Join(output, name)).CombinedOutput()
		if err != nil || !strings.Contains(string(info), "-trimpath=true") || !strings.Contains(string(info), "CGO_ENABLED=0") {
			t.Fatalf("%s is not a static -trimpath build: %v\n%s", name, err, info)
		}
	}
	script, err := os.ReadFile("scripts/build-release.sh")
	if err != nil || !strings.Contains(string(script), `"darwin/amd64 darwin/arm64 linux/amd64 linux/arm64"`) {
		t.Fatalf("default release targets changed: %v", err)
	}
}

// hk 2550: the release workflow passes the v* tag as VERSION, so the asset
// names and `panewire version` carry the tag instead of pw-<sha>.
func TestT2550ReleaseBuildStampsTag(t *testing.T) {
	const tag = "v0.0.0-dryrun.1"
	host := runtime.GOOS + "/" + runtime.GOARCH
	output := t.TempDir()
	cmd := exec.Command("sh", "scripts/build-release.sh", output, tag)
	cmd.Env = append(os.Environ(), "PANEWIRE_RELEASE_TARGETS="+host)
	out, err := cmd.Output()
	if err != nil {
		t.Fatalf("build-release.sh: %v", err)
	}
	if got := strings.TrimSpace(string(out)); got != tag {
		t.Fatalf("release version=%q, want %q", got, tag)
	}
	asset := filepath.Join(output, "panewire_"+tag+"_"+runtime.GOOS+"_"+runtime.GOARCH)
	printed, err := exec.Command(asset, "version").Output()
	if err != nil {
		t.Fatalf("%s version: %v", asset, err)
	}
	if got := strings.TrimSpace(string(printed)); got != tag {
		t.Fatalf("panewire version=%q, want %q", got, tag)
	}
	bad := exec.Command("sh", "scripts/build-release.sh", t.TempDir(), "v1/../x")
	bad.Env = cmd.Env
	if err := bad.Run(); err == nil {
		t.Fatal("build-release.sh accepted a version outside the hub pattern")
	}
}
