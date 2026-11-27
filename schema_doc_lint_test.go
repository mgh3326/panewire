package panewire

import (
	"io/fs"
	"os"
	"regexp"
	"sort"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

// TestSchemaDocLintCoversHubTables mirrors the post-merge tbls lint in
// .github/workflows/schema-doc.yml for every pull request: each table the hub's
// MySQL migrations create needs a table comment and exactly one viewpoint in
// .tbls.yml, and the config must name no table the migrations do not create.
// The real lint runs only after a merge, so a table added without these entries
// would reach main unseen (hk 6995).
func TestSchemaDocLintCoversHubTables(t *testing.T) {
	raw, err := os.ReadFile(".tbls.yml")
	if err != nil {
		t.Fatalf("read .tbls.yml: %v", err)
	}
	var cfg struct {
		Comments []struct {
			Table        string `yaml:"table"`
			TableComment string `yaml:"tableComment"`
		} `yaml:"comments"`
		Viewpoints []struct {
			ID     string   `yaml:"id"`
			Tables []string `yaml:"tables"`
		} `yaml:"viewpoints"`
	}
	if err := yaml.Unmarshal(raw, &cfg); err != nil {
		t.Fatalf("parse .tbls.yml: %v", err)
	}

	known := map[string]bool{}
	for _, table := range hubMySQLTableNames(t) {
		known[table] = true
	}

	commented := map[string]bool{}
	for _, c := range cfg.Comments {
		if strings.TrimSpace(c.TableComment) == "" {
			continue
		}
		if !known[c.Table] {
			t.Errorf("tableComment names %s, which no hub migration creates", c.Table)
		}
		commented[c.Table] = true
	}

	viewpoints := map[string]int{}
	for _, v := range cfg.Viewpoints {
		for _, table := range v.Tables {
			viewpoints[table]++
			if !known[table] {
				t.Errorf("viewpoint %s names %s, which no hub migration creates", v.ID, table)
			}
		}
	}

	for table := range known {
		if !commented[table] {
			t.Errorf("table %s has no tableComment in .tbls.yml", table)
		}
		if n := viewpoints[table]; n != 1 {
			t.Errorf("table %s is in %d viewpoints in .tbls.yml, want exactly 1", table, n)
		}
	}
}

// hubMySQLTableNames returns the tables the embedded MySQL migrations create,
// plus the ledger table hubMySQLLedger creates, sorted.
func hubMySQLTableNames(t *testing.T) []string {
	t.Helper()
	names, err := fs.Glob(hubMySQLMigrations, "hub_mysql_migrations/*.sql")
	if err != nil || len(names) == 0 {
		t.Fatalf("embedded migrations = %v, %v", names, err)
	}
	ddl := hubMySQLLedger
	for _, name := range names {
		body, err := fs.ReadFile(hubMySQLMigrations, name)
		if err != nil {
			t.Fatalf("read %s: %v", name, err)
		}
		ddl += "\n" + string(body)
	}
	created := regexp.MustCompile(`(?m)^\s*CREATE TABLE(?: IF NOT EXISTS)?\s+([a-z0-9_]+)`)
	seen := map[string]bool{}
	for _, m := range created.FindAllStringSubmatch(ddl, -1) {
		seen[m[1]] = true
	}
	if len(seen) == 0 {
		t.Fatalf("no CREATE TABLE found in %d migrations", len(names))
	}
	out := make([]string, 0, len(seen))
	for table := range seen {
		out = append(out, table)
	}
	sort.Strings(out)
	return out
}
