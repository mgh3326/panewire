package fleet

import (
	"go/ast"
	"go/token"
	"os"
	"slices"
	"strconv"
	"strings"
	"testing"
)

// hermeticEnvPrefixes and hermeticEnvNames cover every environment variable
// panewire reads. TestMain clears them so an operator's shell (an armed E6
// marker, a custom CODEX_HOME, a strict lane flag) cannot change test results;
// tests that need one set it with t.Setenv.
var (
	hermeticEnvPrefixes = []string{"PANEWIRE_", "FLEET_", "WRK_", "SCOPEFUEL_", "ARBITER_", "HERDR_", "HANDOFFKEEP_", "HK_", "CLINEPASS_", "MOCK_MCP_"}
	hermeticEnvNames    = []string{"CODEX_HOME", "CLAUDE_CONFIG_DIR", "XDG_CONFIG_HOME", "XDG_DATA_HOME", "XDG_STATE_HOME"}
)

func hermeticEnv(name string) bool {
	if slices.Contains(hermeticEnvNames, name) {
		return true
	}
	return slices.ContainsFunc(hermeticEnvPrefixes, func(p string) bool { return strings.HasPrefix(name, p) })
}

func TestMain(m *testing.M) {
	for _, kv := range os.Environ() {
		if name, _, _ := strings.Cut(kv, "="); hermeticEnv(name) {
			os.Unsetenv(name)
		}
	}
	os.Exit(m.Run())
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
