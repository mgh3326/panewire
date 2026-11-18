package fleet

import (
	"fmt"
	"go/ast"
	"go/token"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/mgh3326/panewire/internal/arbiter"
	"github.com/mgh3326/panewire/internal/hkcred"
	"github.com/mgh3326/panewire/internal/teststate"
)

// hermeticEnvPrefixes and hermeticEnvNames cover every environment variable
// panewire reads. TestMain clears them so an operator's shell (an armed E6
// marker, a custom CODEX_HOME, a strict lane flag) cannot change test results;
// tests that need one set it with t.Setenv.
var (
	hermeticEnvPrefixes = []string{"PANEWIRE_", "FLEET_", "WRK_", "SCOPEFUEL_", "ARBITER_", "HERDR_", "HANDOFFKEEP_", "HK_", "CLINEPASS_", "MOCK_MCP_", "PACELEVER_"}
	hermeticEnvNames    = []string{"HOME", "CODEX_HOME", "CLAUDE_CONFIG_DIR", "CLAUDE_SECURESTORAGE_CONFIG_DIR", "SENTRY_DSN_PANEWIRE", "XDG_CONFIG_HOME", "XDG_CACHE_HOME", "XDG_DATA_HOME", "XDG_STATE_HOME", "GOFLAGS", "GOENV"}
)

func hermeticEnv(name string) bool {
	if slices.Contains(hermeticEnvNames, name) {
		return true
	}
	return slices.ContainsFunc(hermeticEnvPrefixes, func(p string) bool { return strings.HasPrefix(name, p) })
}

// testLaunchHome is the home the test binary was started with, before
// TestMain moved HOME to the temp dir. The arbiter guard forbids ledger
// state under it, and tests that need the real home read it here.
var testLaunchHome string

// The prefixes of the temp dirs TestMain hands a test run. They are swept, not
// just removed, because a killed test binary leaves them behind (hk 4733).
const (
	testCachePrefix = "panewire-test-cache-"
	testHomePrefix  = "panewire-test-home-"
)

func TestMain(m *testing.M) {
	// The launch home is captured before HOME moves: it is the operator's
	// real home, which no test may open ledger state under (hk 4685).
	testLaunchHome, _ = os.UserHomeDir()
	if testLaunchHome == "" {
		testLaunchHome = os.Getenv("HOME")
	}
	// The go toolchain keeps the launch environment's caches, so a test that
	// runs go stays warm and offline. Pin them before HOME and XDG_CACHE_HOME
	// move: on Linux GOCACHE derives from XDG_CACHE_HOME, and a go command run
	// after the move would build into the temp cache dir instead (hk 4733).
	teststate.PinGoToolchainEnv()
	for _, kv := range os.Environ() {
		if name, _, _ := strings.Cut(kv, "="); hermeticEnv(name) {
			os.Unsetenv(name)
		}
	}
	// A test binary killed before its cleanup leaves these behind.
	if _, err := teststate.SweepStale(os.TempDir(), time.Now(), teststate.StaleAfter, testCachePrefix, testHomePrefix); err != nil {
		fmt.Fprintf(os.Stderr, "fleet: sweeping stale test dirs: %v\n", err)
	}
	// The bench catalog cache is written under XDG_CACHE_HOME; keep tests out
	// of the developer's real cache.
	cache, err := os.MkdirTemp("", testCachePrefix)
	if err != nil {
		panic(err)
	}
	os.Setenv("XDG_CACHE_HOME", cache)
	// HOME and the XDG roots move to a private temp home so every default
	// resolver — the job ledger's state.db, the jobs inbox, the hosts config,
	// the bench store, credential files — lands under it instead of the
	// developer's real home (hk 4685). XDG_DATA_HOME is pinned even though
	// trust() would prefer it unset: a temp value still fails trust()'s
	// canonical check, and it keeps ledger() off the real state.db for Apps
	// built with the launch home.
	home, err := os.MkdirTemp("", testHomePrefix)
	if err != nil {
		panic(err)
	}
	os.Setenv("HOME", home)
	os.Setenv("XDG_DATA_HOME", filepath.Join(home, ".local", "share"))
	os.Setenv("XDG_CONFIG_HOME", filepath.Join(home, ".config"))
	os.Setenv("XDG_STATE_HOME", filepath.Join(home, ".local", "state"))
	arbiter.ForbidHomeForTests(testLaunchHome)
	// No test may fall through to the developer's real svc-panewire file
	// (~/.config/panewire/hk-svc.env, hk 3570): point the default at a path
	// that does not exist. A test that wants the file sets PANEWIRE_HK_ENV,
	// or clears it under a temporary HOME.
	os.Setenv(hkcred.PathEnv, filepath.Join(cache, "no-hk-svc.env"))
	code := m.Run()
	for _, path := range []string{cache, home} {
		if err := teststate.RemoveAll(path); err != nil {
			fmt.Fprintf(os.Stderr, "fleet: removing %s: %v\n", path, err)
		}
	}
	os.Exit(code)
}

// TestEnvReadsAreHermetic requires every literal environment variable name the
// package reads, test sources included, to be cleared by TestMain.
func TestEnvReadsAreHermetic(t *testing.T) {
	fset, files := parsePackage(t, true)
	for _, f := range files {
		ast.Inspect(f, func(n ast.Node) bool {
			lit, ok := n.(*ast.BasicLit)
			if !ok || lit.Kind != token.STRING {
				return true
			}
			name, _ := strconv.Unquote(lit.Value)
			if isEnvRead(f, lit) && !hermeticEnv(name) {
				t.Errorf("%s: %s is read from the environment but not cleared by TestMain", fset.Position(lit.Pos()), name)
			}
			return true
		})
	}
}

// isEnvRead reports whether lit is the name argument of os.Getenv or
// os.LookupEnv.
func isEnvRead(f *ast.File, lit *ast.BasicLit) bool {
	found := false
	ast.Inspect(f, func(n ast.Node) bool {
		x, ok := n.(*ast.CallExpr)
		if !ok || len(x.Args) == 0 || x.Args[0] != lit {
			return !found
		}
		if fn, ok := x.Fun.(*ast.SelectorExpr); ok {
			found = fn.Sel.Name == "Getenv" || fn.Sel.Name == "LookupEnv"
		}
		return !found
	})
	return found
}
