package teststate

import (
	"encoding/json"
	"os"
	"os/exec"
)

// PinGoToolchainEnv fixes GOCACHE, GOMODCACHE, GOPATH and GOENV to what they
// resolve to right now, so a later HOME or XDG change cannot move them.
//
// Call it before TestMain moves HOME or XDG_CACHE_HOME. On Linux `go env
// GOCACHE` derives the build cache from XDG_CACHE_HOME, so a pin taken after
// the move points every go command a test runs at that test's own temp state
// dir: a fresh build cache per run, roughly 1 GB, left behind whenever the
// test binary is killed before its cleanup (hk 4733). Without go on PATH
// nothing is pinned, because no test can run go then either. A variable
// already set in the environment keeps its value.
func PinGoToolchainEnv() {
	out, err := exec.Command("go", "env", "-json", "GOCACHE", "GOMODCACHE", "GOPATH", "GOENV").Output()
	if err != nil {
		return
	}
	var values map[string]string
	if json.Unmarshal(out, &values) != nil {
		return
	}
	for key, value := range values {
		if value != "" && os.Getenv(key) == "" {
			os.Setenv(key, value)
		}
	}
}
