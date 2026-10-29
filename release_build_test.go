package panewire

import (
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
		info, err := exec.Command("go", "version", "-m", filepath.Join(output, name)).CombinedOutput()
		if err != nil || !strings.Contains(string(info), "-X main.version="+want) {
			t.Fatalf("%s lacks -X main.version=%s: %v\n%s", name, want, err, info)
		}
	}
	script, err := os.ReadFile("scripts/build-release.sh")
	if err != nil || !strings.Contains(string(script), `"darwin/amd64 darwin/arm64 linux/amd64 linux/arm64"`) {
		t.Fatalf("default release targets changed: %v", err)
	}
}
