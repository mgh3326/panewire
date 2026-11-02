package fleet

import (
	"bytes"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"
)

// parsePackage parses the package's sources; withTests includes _test.go
// files (they legitimately use literals the non-test rules forbid).
func parsePackage(t *testing.T, withTests bool) (*token.FileSet, []*ast.File) {
	t.Helper()
	fset := token.NewFileSet()
	names, e := filepath.Glob("*.go")
	if e != nil {
		t.Fatal(e)
	}
	var files []*ast.File
	for _, name := range names {
		if !withTests && strings.HasSuffix(name, "_test.go") {
			continue
		}
		f, e := parser.ParseFile(fset, name, nil, 0)
		if e != nil {
			t.Fatal(e)
		}
		files = append(files, f)
	}
	return fset, files
}

// TestExitCodesNamed keeps exit statuses symbolic: fail and candResult take
// one of the exit* constants, never a literal number.
func TestExitCodesNamed(t *testing.T) {
	fset, files := parsePackage(t, false)
	for _, f := range files {
		ast.Inspect(f, func(n ast.Node) bool {
			switch x := n.(type) {
			case *ast.CallExpr:
				if id, ok := x.Fun.(*ast.Ident); ok && id.Name == "fail" && len(x.Args) > 0 {
					if _, lit := x.Args[0].(*ast.BasicLit); lit {
						t.Errorf("%s: fail with a literal exit code; use an exit* constant", fset.Position(x.Pos()))
					}
				}
			case *ast.KeyValueExpr:
				if k, ok := x.Key.(*ast.Ident); ok && k.Name == "rc" {
					if _, lit := x.Value.(*ast.BasicLit); lit {
						t.Errorf("%s: candResult rc is a literal; use an exit* constant", fset.Position(x.Pos()))
					}
				}
			}
			return true
		})
	}
}

// TestExitCodesDocumented requires every exit* constant to have a row in the
// docs/config.md exit status table, and every row to name a known status.
func TestExitCodesDocumented(t *testing.T) {
	_, files := parsePackage(t, false)
	consts := map[int]string{}
	for _, f := range files {
		for _, d := range f.Decls {
			g, ok := d.(*ast.GenDecl)
			if !ok || g.Tok != token.CONST {
				continue
			}
			for _, spec := range g.Specs {
				v := spec.(*ast.ValueSpec)
				for i, name := range v.Names {
					if !strings.HasPrefix(name.Name, "exit") || i >= len(v.Values) {
						continue
					}
					lit, ok := v.Values[i].(*ast.BasicLit)
					if !ok {
						t.Fatalf("%s must be a literal", name.Name)
					}
					n, _ := strconv.Atoi(lit.Value)
					if prev, dup := consts[n]; dup {
						t.Errorf("exit status %d is named twice: %s and %s", n, prev, name.Name)
					}
					consts[n] = name.Name
				}
			}
		}
	}
	b, e := os.ReadFile("../../docs/config.md")
	if e != nil {
		t.Fatal(e)
	}
	section := string(b)[strings.Index(string(b), "## Exit statuses"):]
	documented := map[int]bool{}
	for _, m := range regexp.MustCompile(`(?m)^\| ([0-9 /]+) \|`).FindAllStringSubmatch(section, -1) {
		for _, f := range strings.Split(m[1], "/") {
			n, _ := strconv.Atoi(strings.TrimSpace(f))
			documented[n] = true
		}
	}
	for n, name := range consts {
		if !documented[n] {
			t.Errorf("%s (%d) has no row in docs/config.md", name, n)
		}
	}
}

func TestNewestReportIsStableOnEqualMtime(t *testing.T) {
	dir := t.TempDir()
	same := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	for _, name := range []string{"report.md", "report-b.md", "report-a.md"} {
		p := filepath.Join(dir, name)
		if e := os.WriteFile(p, []byte(name), 0600); e != nil {
			t.Fatal(e)
		}
		if e := os.Chtimes(p, same, same); e != nil {
			t.Fatal(e)
		}
	}
	for range 20 {
		if got := newestReport(dir); filepath.Base(got) != "report-a.md" {
			t.Fatalf("tie must resolve to the lexically first name, got %s", got)
		}
	}
	newer := filepath.Join(dir, "report.md")
	if e := os.Chtimes(newer, same.Add(time.Second), same.Add(time.Second)); e != nil {
		t.Fatal(e)
	}
	if got := newestReport(dir); got != newer {
		t.Fatalf("newest report must win, got %s", got)
	}
	if got := newestReport(t.TempDir()); got != "" {
		t.Fatalf("empty dir must select nothing, got %s", got)
	}
}

func TestHostsDiagnosticIsSorted(t *testing.T) {
	a := testApp(t)
	a.Config.Hosts = map[string]map[string]any{"zeta": {}, "alpha": {}, "mid": {}, "beta": {}}
	for range 10 {
		out := &bytes.Buffer{}
		a.Out = out
		if e := a.diagnostics(t.Context(), "hosts", nil); e != nil {
			t.Fatal(e)
		}
		var hosts []string
		for _, line := range strings.Split(out.String(), "\n") {
			if rest, ok := strings.CutPrefix(line, "host="); ok {
				hosts = append(hosts, strings.Fields(rest)[0])
			}
		}
		if strings.Join(hosts, ",") != "alpha,beta,mid,zeta" {
			t.Fatalf("hosts must print sorted, got %v", hosts)
		}
	}
}

func TestConfigValidationReportsFirstSortedError(t *testing.T) {
	path := filepath.Join(t.TempDir(), "hosts.toml")
	toml := "[hosts.zz]\nvia = \"bad\"\n[hosts.aa]\nvia = \"bad\"\n[hosts.mm]\nvia = \"bad\"\n"
	if e := os.WriteFile(path, []byte(toml), 0600); e != nil {
		t.Fatal(e)
	}
	for range 20 {
		_, e := LoadConfig(path)
		if e == nil || !strings.Contains(e.Error(), "hosts.aa.via") {
			t.Fatalf("validation must report the first host in name order, got %v", e)
		}
	}
}

func TestLoginRankIsSymmetric(t *testing.T) {
	if loginRank("") != loginRank(loginPresent) || loginRank(loginPresent) >= loginRank(loginUnknown) {
		t.Fatal("present and no-account logins rank ahead of unknown, equally")
	}
}
