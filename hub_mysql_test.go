package panewire

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"math/big"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"
	"time"
)

// The MySQL migration files are one statement each. MySQL commits DDL as it
// runs, so a file of two statements that stops between them leaves a half
// applied file no ledger row describes (docs/hub-mysql.md, "Migrations").
func TestHubMySQLMigrationsAreOneGuardedStatement(t *testing.T) {
	names, err := fs.Glob(hubMySQLMigrations, "hub_mysql_migrations/*.sql")
	if err != nil || len(names) == 0 {
		t.Fatalf("no embedded MySQL migrations (%v)", err)
	}
	numbered := regexp.MustCompile(`^hub_mysql_migrations/(\d{4})_[a-z0-9_]+\.sql$`)
	seen := map[string]string{}
	for _, name := range names {
		m := numbered.FindStringSubmatch(name)
		if m == nil {
			t.Errorf("%s: a migration is NNNN_feature.sql", name)
			continue
		}
		if other, dup := seen[m[1]]; dup {
			t.Errorf("%s and %s share the number %s: MySQL migrations are one linear series", name, other, m[1])
		}
		seen[m[1]] = name
		body, err := hubMySQLMigrations.ReadFile(name)
		if err != nil {
			t.Fatal(err)
		}
		migration, err := parseHubMySQLMigration(string(body))
		if err != nil {
			t.Errorf("%s: %v", name, err)
			continue
		}
		if n := strings.Count(migration.statement, ";"); n != 0 {
			t.Errorf("%s: %d statement separators in a one-statement file", name, n)
		}
	}
	// The numbers run 0001, 0002, ... with no gap: a file renumbered away
	// would leave a hub that applied it with a ledger row no file matches.
	sort.Strings(names)
	for i, name := range names {
		if want := fmt.Sprintf("hub_mysql_migrations/%04d_", i+1); !strings.HasPrefix(name, want) {
			t.Errorf("file %d is %s, want a name starting %s", i+1, name, want)
			break
		}
	}
}

func TestParseHubMySQLMigrationRefusesAFileThatIsNotOneGuardedStatement(t *testing.T) {
	const guard = "-- applied-if: SELECT COUNT(*) FROM information_schema.tables WHERE table_schema=DATABASE() AND table_name='a'\n"
	for _, c := range []struct{ name, body, want string }{
		{"two statements", guard + "CREATE TABLE a (x INT);\nCREATE TABLE b (y INT);\n", "more than one statement"},
		{"two statements on a line", guard + "CREATE TABLE a (x INT); CREATE TABLE b (y INT);\n", "more than one statement"},
		{"no guard", "CREATE TABLE a (x INT);\n", "no applied-if SELECT guard"},
		{"a guard that is not a select", "-- applied-if: DROP TABLE a\nCREATE TABLE a (x INT);\n", "no applied-if SELECT guard"},
		{"two guards", guard + guard + "CREATE TABLE a (x INT);\n", "more than one applied-if guard"},
		{"a schema statement guarded by data", "-- applied-if: SELECT COUNT(*) FROM a\nCREATE TABLE a (x INT);\n", "information_schema"},
		{"no statement", guard, "no statement"},
	} {
		if _, err := parseHubMySQLMigration(c.body); err == nil || !strings.Contains(err.Error(), c.want) {
			t.Errorf("%s: err = %v, want one naming %q", c.name, err, c.want)
		}
	}
	good, err := parseHubMySQLMigration("-- a comment\n" + guard + "CREATE TABLE a (\n x INT\n);\n")
	if err != nil || good.statement != "CREATE TABLE a (\nx INT\n)" || !strings.HasPrefix(good.appliedIf, "SELECT COUNT(*)") {
		t.Errorf("one guarded statement: %+v, %v", good, err)
	}
}

// columnsOf is the column names of the CREATE TABLE and ALTER TABLE ADD COLUMN
// statements in files, by table, less the tables a later statement drops. It
// reads both dialects' DDL: it takes the first word of each comma-separated
// item of a table body that is not a constraint or an index.
func columnsOf(t *testing.T, sources []string) map[string]map[string]bool {
	t.Helper()
	create := regexp.MustCompile(`(?is)^CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?(\w+)\s*\((.*)\)[^)]*$`)
	alter := regexp.MustCompile(`(?is)^ALTER\s+TABLE\s+(\w+)\s+ADD\s+COLUMN\s+(?:IF\s+NOT\s+EXISTS\s+)?(\w+)`)
	drop := regexp.MustCompile(`(?is)^DROP\s+TABLE\s+(?:IF\s+EXISTS\s+)?(\w+)`)
	tables := map[string]map[string]bool{}
	for _, source := range sources {
		var code []string
		for _, line := range strings.Split(source, "\n") {
			if line = strings.TrimSpace(line); !strings.HasPrefix(line, "--") {
				code = append(code, line)
			}
		}
		for _, statement := range strings.Split(strings.Join(code, "\n"), ";") {
			statement = strings.TrimSpace(statement)
			switch {
			case create.MatchString(statement):
				m := create.FindStringSubmatch(statement)
				cols := map[string]bool{}
				for _, item := range splitTopLevel(m[2]) {
					word := strings.ToLower(strings.Fields(item + " ")[0])
					switch word {
					case "primary", "unique", "check", "constraint", "key", "index", "foreign":
					default:
						cols[word] = true
					}
				}
				tables[m[1]] = cols
			case alter.MatchString(statement):
				m := alter.FindStringSubmatch(statement)
				if tables[m[1]] == nil {
					t.Fatalf("ALTER of %s, which no earlier statement created", m[1])
				}
				tables[m[1]][strings.ToLower(m[2])] = true
			case drop.MatchString(statement):
				delete(tables, drop.FindStringSubmatch(statement)[1])
			}
		}
	}
	return tables
}

func splitTopLevel(s string) []string {
	var items []string
	depth, start := 0, 0
	for i, r := range s {
		switch r {
		case '(':
			depth++
		case ')':
			depth--
		case ',':
			if depth == 0 {
				items = append(items, strings.TrimSpace(s[start:i]))
				start = i + 1
			}
		}
	}
	return append(items, strings.TrimSpace(s[start:]))
}

func migrationSources(t *testing.T, fsys fs.ReadFileFS, pattern string) []string {
	t.Helper()
	names, err := fs.Glob(fsys, pattern)
	if err != nil || len(names) == 0 {
		t.Fatalf("no migrations match %s (%v)", pattern, err)
	}
	sort.Strings(names)
	var sources []string
	for _, name := range names {
		body, err := fsys.ReadFile(name)
		if err != nil {
			t.Fatal(err)
		}
		sources = append(sources, string(body))
	}
	return sources
}

// The MySQL schema has every table and column the Postgres migrations leave
// (the whole schema after phase 0), and one table more: the supersede row
// lock. A column added to a Postgres migration without a MySQL file fails here.
func TestHubMySQLMigrationsCoverThePostgresSchema(t *testing.T) {
	postgres := columnsOf(t, migrationSources(t, hubPostgresMigrations, "hub_postgres_migrations/*.sql"))
	mysqlTables := columnsOf(t, migrationSources(t, hubMySQLMigrations, "hub_mysql_migrations/*.sql"))
	if len(postgres) < 20 {
		t.Fatalf("read %d Postgres tables; the DDL reader no longer finds them", len(postgres))
	}
	for table, cols := range postgres {
		got, ok := mysqlTables[table]
		if !ok {
			t.Errorf("table %s has no MySQL migration", table)
			continue
		}
		for col := range cols {
			if !got[col] {
				t.Errorf("table %s: column %s is in Postgres and not in MySQL", table, col)
			}
		}
		for col := range got {
			if !cols[col] {
				t.Errorf("table %s: column %s is in MySQL and not in Postgres", table, col)
			}
		}
	}
	for table := range mysqlTables {
		if _, ok := postgres[table]; !ok && table != "supersede_lane_cause" {
			t.Errorf("table %s is in MySQL only", table)
		}
	}
	if _, ok := mysqlTables["quota_refresh_failures"]; ok {
		t.Error("quota_refresh_failures is dropped in phase 0 and must not be carried to MySQL")
	}
}

func TestHubMySQLConfig(t *testing.T) {
	cfg, err := hubMySQLConfig("mysql://app:p%40ss@127.0.0.1:3307/panewire_hub")
	if err != nil {
		t.Fatal(err)
	}
	if cfg.User != "app" || cfg.Passwd != "p@ss" || cfg.Addr != "127.0.0.1:3307" || cfg.DBName != "panewire_hub" {
		t.Errorf("identity = %q %q %q %q", cfg.User, cfg.Passwd, cfg.Addr, cfg.DBName)
	}
	// RowsAffected keeps its compare-and-set meaning, timestamps parse.
	if !cfg.ClientFoundRows || !cfg.ParseTime {
		t.Errorf("clientFoundRows=%v parseTime=%v, want both", cfg.ClientFoundRows, cfg.ParseTime)
	}
	if cfg.Collation != "utf8mb4_bin" {
		t.Errorf("collation = %q", cfg.Collation)
	}
	if cfg.Params["transaction_isolation"] != "'READ-COMMITTED'" {
		t.Errorf("session isolation = %q, want READ-COMMITTED", cfg.Params["transaction_isolation"])
	}
	if cfg.TLS != nil {
		t.Error("a loopback URL without tls-ca got a TLS config")
	}
	// The account is capped at 24 connections (hk 4898 a299) and a restart
	// runs two hub processes on it: two pools must fit.
	if hubMySQLMaxConns < 1 || 2*hubMySQLMaxConns > 24 {
		t.Errorf("hubMySQLMaxConns = %d: two overlapping hubs need 2x at most 24", hubMySQLMaxConns)
	}
	// A strict mode of the hub's own, without NO_BACKSLASH_ESCAPES.
	if mode := cfg.Params["sql_mode"]; !strings.Contains(mode, "STRICT_TRANS_TABLES") || strings.Contains(mode, "NO_BACKSLASH_ESCAPES") {
		t.Errorf("sql_mode = %s", mode)
	}
	if c, err := hubMySQLConfig("mysql://app@localhost/db"); err != nil || c.Addr != "localhost:3306" {
		t.Errorf("default port: %v %v", c, err)
	}
}

func TestHubMySQLConfigRefusesAnUnsafeOrMalformedURL(t *testing.T) {
	ca := writeTestCA(t, t.TempDir())
	for _, raw := range []string{
		"postgres://app@127.0.0.1/db",
		"mysql://app@127.0.0.1",
		"mysql://app@127.0.0.1/",
		"mysql://app@127.0.0.1/a/b",
		"mysql:///db",
		"mysql://app@db.example/db",                      // a remote host needs its CA pinned
		"mysql://app@127.0.0.1/db?tls=skip-verify",       // a URL cannot relax TLS
		"mysql://app@127.0.0.1/db?tls=false",             // nor turn it off
		"mysql://app@127.0.0.1/db?maxAllowedPacket=1",    // nor set anything else
		"mysql://app@127.0.0.1/db?clientFoundRows=false", // clientFoundRows is not the URL's to decide
		"mysql://app@127.0.0.1/db?interpolateParams=true",
		"mysql://app@db.example/db?tls-ca=" + filepath.Join(t.TempDir(), "missing.pem"),
		"mysql://app@127.0.0.1/db#frag",
		"mysql://app:pwsecret@[::1/db",
	} {
		if _, err := hubMySQLConfig(raw); err == nil {
			t.Errorf("%q was accepted", raw)
		} else if strings.Contains(err.Error(), "pwsecret") || strings.Contains(err.Error(), "db.example") {
			t.Errorf("%q: the error carries the URL: %v", raw, err)
		}
	}
	remote, err := hubMySQLConfig("mysql://app@db.example:3306/db?tls-ca=" + ca.path)
	if err != nil {
		t.Fatal(err)
	}
	if remote.TLS == nil || remote.TLS.VerifyConnection == nil || remote.TLS.MinVersion < tls.VersionTLS12 {
		t.Errorf("a remote URL got TLS %+v, want TLS 1.2+ with the pinned verifier", remote.TLS)
	}
}

type testCA struct {
	path string
	cert *x509.Certificate
	key  *ecdsa.PrivateKey
}

func newTestCA(t *testing.T, name string) *testCA {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	template := &x509.Certificate{
		SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: name},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour),
		IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign,
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	return &testCA{cert: cert, key: key}
}

func writeTestCA(t *testing.T, dir string) *testCA {
	t.Helper()
	ca := newTestCA(t, "instance CA")
	ca.path = filepath.Join(dir, "ca.pem")
	pemBytes := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: ca.cert.Raw})
	if err := os.WriteFile(ca.path, pemBytes, 0o600); err != nil {
		t.Fatal(err)
	}
	return ca
}

// leaf issues a server certificate with no SAN and a common name that is not
// the host: the shape of the HeatWave certificate (hk 4246).
func (ca *testCA) leaf(t *testing.T, notAfter time.Time) *x509.Certificate {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	template := &x509.Certificate{
		SerialNumber: big.NewInt(2), Subject: pkix.Name{CommonName: "MySQL_Server_8.4.0_Auto_Generated_Server_Certificate"},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: notAfter,
		KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, template, ca.cert, &key.PublicKey, ca.key)
	if err != nil {
		t.Fatal(err)
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	return cert
}

// The pinned CA is VERIFY_CA: a certificate that chains to it passes with no
// SAN and a name that is not the host's, and one that does not chain to it, has
// expired, or is absent is refused. Verification is never skipped.
func TestHubMySQLPinnedCAVerifiesTheChainAndNotTheName(t *testing.T) {
	dir := t.TempDir()
	ca := writeTestCA(t, dir)
	config, err := hubMySQLPinnedCA(ca.path)
	if err != nil {
		t.Fatal(err)
	}
	verify := config.VerifyConnection
	if verify == nil {
		t.Fatal("no VerifyConnection: Go's own check is skipped and nothing replaces it")
	}
	state := func(certs ...*x509.Certificate) tls.ConnectionState {
		return tls.ConnectionState{PeerCertificates: certs}
	}
	if err := verify(state(ca.leaf(t, time.Now().Add(time.Hour)))); err != nil {
		t.Errorf("a certificate signed by the pinned CA: %v", err)
	}
	other := newTestCA(t, "another CA")
	if err := verify(state(other.leaf(t, time.Now().Add(time.Hour)))); err == nil {
		t.Error("a certificate signed by another CA was accepted")
	}
	if err := verify(state(ca.leaf(t, time.Now().Add(-time.Minute)))); err == nil {
		t.Error("an expired certificate was accepted")
	}
	if err := verify(state()); err == nil {
		t.Error("a connection with no certificate was accepted")
	}
}

func TestHubMySQLPinnedCARefusesAnUnusableFile(t *testing.T) {
	dir := t.TempDir()
	ca := writeTestCA(t, dir)
	notCA := newTestCA(t, "x").leaf(t, time.Now().Add(time.Hour))
	write := func(name string, data []byte) string {
		path := filepath.Join(dir, name)
		if err := os.WriteFile(path, data, 0o600); err != nil {
			t.Fatal(err)
		}
		return path
	}
	for name, path := range map[string]string{
		"missing":   filepath.Join(dir, "nope.pem"),
		"directory": dir,
		"empty":     write("empty.pem", nil),
		"not pem":   write("text.pem", []byte("hello")),
		"a leaf":    write("leaf.pem", pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: notCA.Raw})),
		"too large": write("big.pem", append(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: ca.cert.Raw}), make([]byte, hubMySQLCAFileMax)...)),
	} {
		if _, err := hubMySQLPinnedCA(path); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
}

// The MySQL duplicate-key no-op is never INSERT IGNORE, which turns every other
// error into a warning (a value too long is stored truncated, a NULL in a NOT
// NULL column becomes a default). Mutant: spell insertIgnore as INSERT IGNORE.
func TestMySQLDuplicateKeyNoOpIsNotInsertIgnore(t *testing.T) {
	for _, target := range []string{"", "a", "a,b"} {
		got := dialectMySQL.insertIgnore(`INSERT INTO t(a,b) VALUES(?,?)`, target)
		if strings.Contains(strings.ToUpper(got), "IGNORE") || !strings.Contains(got, "ON DUPLICATE KEY UPDATE") {
			t.Errorf("insertIgnore(%q) = %s", target, got)
		}
	}
	for _, d := range []sqlDialect{dialectSQLite, dialectPostgres, dialectMySQL} {
		if got := d.insertOnce(`INSERT INTO t(a,b) VALUES(?,?)`, "a"); strings.Contains(strings.ToUpper(got), "IGNORE") {
			t.Errorf("insertOnce = %s", got)
		}
	}
}

// A database URL is a credential: a failed connect must not echo the password,
// user, or host, in the error or the hub log. Port 1 on loopback refuses at
// once; no database server is involved.
func TestHubDatabaseURLNeverEchoedForMySQL(t *testing.T) {
	const dsn = "mysql://pwsecretuser:pwsecretpass@127.0.0.1:1/pwsecretdb"
	var logs bytes.Buffer
	_, err := NewHubServer(HubServerConfig{Tokens: map[string]string{"operator": "op"}, InboxDatabaseURL: dsn, Logger: slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug}))})
	if err == nil {
		t.Fatal("hub started with an unreachable database")
	}
	for _, secret := range []string{"pwsecret", "127.0.0.1"} {
		if strings.Contains(err.Error(), secret) || strings.Contains(logs.String(), secret) {
			t.Fatalf("error %q or log %q echoes %q", err, logs.String(), secret)
		}
	}
	auth := filepath.Join(t.TempDir(), "auth.env")
	if err := os.WriteFile(auth, []byte("HUB_TOKEN_operator=op\n"), 0600); err != nil {
		t.Fatal(err)
	}
	t.Setenv(hubDatabaseURLEnv, dsn)
	_, _, _, err = newHubServerForCLI([]string{"--hub-auth", auth}, slog.New(slog.NewTextHandler(&logs, nil)))
	if err == nil || !strings.Contains(err.Error(), "hub database connect failed") {
		t.Fatalf("hub CLI error = %v, want the database connect failure", err)
	}
	for _, secret := range []string{"pwsecret", "127.0.0.1:1"} {
		if strings.Contains(err.Error(), secret) || strings.Contains(logs.String(), secret) {
			t.Fatalf("CLI error %q or log %q echoes %q", err, logs.String(), secret)
		}
	}
	if _, err := openLaneInboxStoreMySQL(context.Background(), "mysql://pwsecretuser:pwsecretpass@[::1"); err == nil || strings.Contains(err.Error(), "pwsecret") {
		t.Fatalf("parse error = %v", err)
	}
}

// The scheme chooses the backend and nothing else does: a mysql:// URL is
// never read as Postgres (and a Postgres URL is never read as MySQL).
func TestOpenLaneInboxStoreURLChoosesByScheme(t *testing.T) {
	for _, c := range []struct{ url, want string }{
		{"mysql://u:p@127.0.0.1:1/db", "MySQL error"},
	} {
		if _, err := openLaneInboxStoreURL(context.Background(), c.url); err == nil || !strings.Contains(err.Error(), "hub database connect failed") {
			t.Errorf("%s: err = %v", c.url, err)
		}
	}
	// A URL of another scheme goes to Postgres, whose parser refuses it
	// without a connection.
	if _, err := openLaneInboxStoreURL(context.Background(), "MYSQL://u@127.0.0.1:1/db"); err == nil || !strings.Contains(err.Error(), "hub database connect failed") {
		t.Errorf("an upper-case scheme was not read as MySQL: %v", err)
	}
	if _, err := openLaneInboxStoreURL(context.Background(), "mysql-ish://u@127.0.0.1:1/db"); !errors.Is(err, errHubDatabaseURL) {
		t.Errorf("another scheme: err = %v, want the Postgres parser's refusal", err)
	}
}

// A key longer than event_key is stored as its hash: deduplicated like any
// other, never refused or truncated. Two long keys that share their first
// 2048 bytes stay two keys, and no raw key can equal a hashed one.
func TestMySQLStoredKeyHashesAKeyThatDoesNotFit(t *testing.T) {
	stored := func(key string) []byte { return dialectMySQL.storedKey(key).([]byte) }
	if got := stored("lane.event\x00lane\x00ev-1"); string(got) != "lane.event\x00lane\x00ev-1" {
		t.Errorf("a short key changed: %q", got)
	}
	exact := strings.Repeat("a", mysqlEventKeyMax)
	if got := stored(exact); string(got) != exact {
		t.Error("a key of exactly the column's size changed")
	}
	longA := strings.Repeat("p", 3000) + "A"
	longB := strings.Repeat("p", 3000) + "B"
	a, b := stored(longA), stored(longB)
	if len(a) > mysqlEventKeyMax || len(b) > mysqlEventKeyMax {
		t.Errorf("hashed keys are %d and %d bytes, want at most %d", len(a), len(b), mysqlEventKeyMax)
	}
	if string(a) == string(b) || string(a) != string(stored(longA)) {
		t.Error("hashed keys must be distinct for distinct keys and stable for one")
	}
	prefixed := mysqlHashedKeyPrefix + "x"
	if got := stored(prefixed); string(got) == prefixed {
		t.Error("a raw key that starts like a hashed one was stored raw")
	}
}
