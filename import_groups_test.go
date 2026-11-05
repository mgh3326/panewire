package panewire

import (
	"go/parser"
	"go/token"
	"io/fs"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

// TestImportGroupsPutStandardLibraryFirst enforces the goimports layout that
// gofmt does not: in every Go file of the module, standard-library imports
// form the first group, and no blank-line-separated group mixes the standard
// library with module imports. goimports -w <file> fixes a mixed group; merge
// split standard-library groups by hand.
func TestImportGroupsPutStandardLibraryFirst(t *testing.T) {
	err := filepath.WalkDir(".", func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			if name := entry.Name(); path != "." && (strings.HasPrefix(name, ".") || name == "testdata") {
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") {
			return nil
		}
		fileSet := token.NewFileSet()
		file, err := parser.ParseFile(fileSet, path, nil, parser.ImportsOnly)
		if err != nil {
			return err
		}
		var groups [][]bool // per group: is each import standard library
		lastLine := -1
		for _, spec := range file.Imports {
			line := fileSet.Position(spec.Pos()).Line
			if lastLine < 0 || line > lastLine+1 {
				groups = append(groups, nil)
			}
			lastLine = fileSet.Position(spec.End()).Line
			importPath, _ := strconv.Unquote(spec.Path.Value)
			first, _, _ := strings.Cut(importPath, "/")
			groups[len(groups)-1] = append(groups[len(groups)-1], !strings.Contains(first, "."))
		}
		for index, group := range groups {
			standard, module := 0, 0
			for _, isStandard := range group {
				if isStandard {
					standard++
				} else {
					module++
				}
			}
			if standard > 0 && module > 0 {
				t.Errorf("%s: import group %d mixes the standard library with module imports; run goimports -w", path, index+1)
			}
			if standard > 0 && index > 0 {
				t.Errorf("%s: standard-library imports are not all in the first group; merge them into it", path)
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}
