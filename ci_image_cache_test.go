package panewire

import (
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

// The hub image layer cache rule (docs/hub-image.md): no workflow under
// .github writes buildkit layers to the GitHub Actions cache. The layers live
// at one tag of the image package in GHCR: publish (main) reads and writes it,
// image (pull requests and main) only reads it.

const (
	imageCacheWorkflowPath = ".github/workflows/image.yml"
	imageCacheRef          = "${{ steps.image.outputs.name }}:buildcache"
	imageCacheBuildUses    = "docker/build-push-action"
)

// imageCacheGate is true when GHCR credentials are available to the job: every
// run except a fork PR (which has no token for the package). The image job's
// login and its cache import must share this gate.
const imageCacheGate = "github.event_name != 'pull_request' || github.event.pull_request.head.repo.full_name == github.repository"

// imageCacheImport is the image job's cache-from: the registry import when the
// gate holds, and an empty value (an uncached build) otherwise.
const imageCacheImport = "${{ (" + imageCacheGate + ") && format('type=registry,ref={0}:buildcache', steps.image.outputs.name) || '' }}"

type imageCacheStep struct {
	If   string         `yaml:"if"`
	Uses string         `yaml:"uses"`
	With map[string]any `yaml:"with"`
}

type imageCacheWorkflow struct {
	Jobs map[string]struct {
		Steps []imageCacheStep `yaml:"steps"`
	} `yaml:"jobs"`
}

// imageCacheOptions parses a buildx cache option string such as
// "type=registry,ref=...,mode=max" into its key/value pairs.
func imageCacheOptions(s string) map[string]string {
	opts := map[string]string{}
	for _, part := range strings.Split(s, ",") {
		k, v, _ := strings.Cut(part, "=")
		opts[strings.TrimSpace(k)] = strings.TrimSpace(v)
	}
	return opts
}

// imageCacheBuildOptions returns the cache-from and cache-to values of the one
// build step in job. A missing key yields "".
func imageCacheBuildOptions(t *testing.T, w imageCacheWorkflow, job string) (from, to string) {
	t.Helper()
	j, ok := w.Jobs[job]
	if !ok {
		t.Fatalf("%s has no job %q", imageCacheWorkflowPath, job)
	}
	var builds []imageCacheStep
	for _, step := range j.Steps {
		if strings.HasPrefix(step.Uses, imageCacheBuildUses) {
			builds = append(builds, step)
		}
	}
	if len(builds) != 1 {
		t.Fatalf("job %q has %d %s steps, want 1", job, len(builds), imageCacheBuildUses)
	}
	from, _ = builds[0].With["cache-from"].(string)
	to, _ = builds[0].With["cache-to"].(string)
	return from, to
}

// No file under .github may write buildkit layers to the Actions cache.
func TestImageCacheNoActionsCacheWrites(t *testing.T) {
	scanned := 0
	err := filepath.WalkDir(".github", func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		raw, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		scanned++
		if strings.Contains(string(raw), "type=gha") {
			t.Errorf("%s contains type=gha; buildkit layers belong in the GHCR registry cache (docs/hub-image.md)", path)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if scanned == 0 {
		t.Fatal("scanned no files under .github")
	}
}

// publish reads and writes the registry cache on the same ref, in max mode.
func TestImageCachePublishReadsAndWritesRegistry(t *testing.T) {
	var w imageCacheWorkflow
	raw, err := os.ReadFile(imageCacheWorkflowPath)
	if err != nil {
		t.Fatal(err)
	}
	if err := yaml.Unmarshal(raw, &w); err != nil {
		t.Fatalf("%s: %v", imageCacheWorkflowPath, err)
	}
	from, to := imageCacheBuildOptions(t, w, "publish")

	fromOpts := imageCacheOptions(from)
	if fromOpts["type"] != "registry" || fromOpts["ref"] != imageCacheRef {
		t.Errorf("publish cache-from = %q, want type=registry on %s", from, imageCacheRef)
	}

	toOpts := imageCacheOptions(to)
	switch {
	case toOpts["type"] != "registry":
		t.Errorf("publish cache-to = %q, want type=registry", to)
	case toOpts["ref"] != imageCacheRef:
		t.Errorf("publish cache-to ref = %q, want %s", toOpts["ref"], imageCacheRef)
	case toOpts["mode"] != "max":
		t.Errorf("publish cache-to = %q, want mode=max", to)
	}
}

// image only reads the registry cache: the import is gated on the same
// condition as its GHCR login (a fork PR builds uncached), and there is no
// cache-to.
func TestImageCacheImageJobOnlyReads(t *testing.T) {
	var w imageCacheWorkflow
	raw, err := os.ReadFile(imageCacheWorkflowPath)
	if err != nil {
		t.Fatal(err)
	}
	if err := yaml.Unmarshal(raw, &w); err != nil {
		t.Fatalf("%s: %v", imageCacheWorkflowPath, err)
	}
	from, to := imageCacheBuildOptions(t, w, "image")

	if from != imageCacheImport {
		t.Errorf("image cache-from = %q, want %q", from, imageCacheImport)
	}
	if to != "" {
		t.Errorf("image cache-to = %q, want none: the image job only reads the cache", to)
	}

	var logins []imageCacheStep
	for _, step := range w.Jobs["image"].Steps {
		if strings.HasPrefix(step.Uses, "docker/login-action") {
			logins = append(logins, step)
		}
	}
	if len(logins) != 1 {
		t.Fatalf("image job has %d docker/login-action steps, want 1", len(logins))
	}
	if logins[0].If != imageCacheGate {
		t.Errorf("image login if = %q, want the cache gate %q", logins[0].If, imageCacheGate)
	}
}

// Every checkout in image.yml leaves no token in .git/config: the build
// context includes .git and publish exports every layer, the COPY . . layer
// included, to the publicly readable :buildcache tag.
func TestImageCacheCheckoutPersistsNoCredentials(t *testing.T) {
	var w struct {
		Jobs map[string]struct {
			Steps []struct {
				Uses string         `yaml:"uses"`
				With map[string]any `yaml:"with"`
			} `yaml:"steps"`
		} `yaml:"jobs"`
	}
	raw, err := os.ReadFile(imageCacheWorkflowPath)
	if err != nil {
		t.Fatal(err)
	}
	if err := yaml.Unmarshal(raw, &w); err != nil {
		t.Fatalf("%s: %v", imageCacheWorkflowPath, err)
	}
	checkouts := 0
	for name, job := range w.Jobs {
		for _, step := range job.Steps {
			if !strings.HasPrefix(step.Uses, "actions/checkout@") {
				continue
			}
			checkouts++
			if v, ok := step.With["persist-credentials"].(bool); !ok || v {
				t.Errorf("job %q checkout persist-credentials = %v, want false", name, step.With["persist-credentials"])
			}
		}
	}
	if checkouts == 0 {
		t.Fatalf("%s has no actions/checkout step", imageCacheWorkflowPath)
	}
}
