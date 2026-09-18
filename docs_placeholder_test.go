package panewire

import (
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

// This repository is public. A real operational literal pasted into a
// Markdown file — a live hub hostname, a home path, a tailnet address, a
// pane id — becomes a permanent disclosure, so every .md file is scanned and
// anything outside the allow-lists below fails the build. The allow-lists
// are deliberately the single place where "safe to publish" is defined:
// docs must use RFC-reserved placeholder domains (hub.example.invalid,
// hub.example, *.example.com, *.example.dev), <angle-bracket> placeholders,
// localhost/loopback, the documented CGNAT example 100.64.0.1, and the
// synthetic pane pair w1:p1/w1:p2.
//
// What this check cannot see: bare machine aliases with no dot (mac-a vs a
// real enrolled ID are indistinguishable tokens) and hosts on TLDs omitted
// from docFlaggedTLDs. Those remain review responsibilities.

// docAllowedHostSuffixes are the only flagged-TLD hosts permitted in docs.
// Each entry exists because a legitimate public reference needs it:
var docAllowedHostSuffixes = []string{
	"example.com",           // RFC 2606 documentation domain
	"example.dev",           // conventional placeholder on a real TLD
	"github.com",            // public forge: module path and release URLs
	"githubusercontent.com", // GitHub release-asset redirect host
	"modernc.org",           // Go module path for the sqlite driver
}

// docAllowedPaneIDs is the synthetic window:pane pair used by the lane docs.
// Any other pane-shaped token is treated as a real pane id.
var docAllowedPaneIDs = map[string]bool{"w1:p1": true, "w1:p2": true}

// docAllowedTailnetIP is the CGNAT-range address README explicitly declares
// documentation-only; every other 100.64.0.0/10 address fails.
const docAllowedTailnetIP = "100.64.0.1"

// docFlaggedTLDs marks a dotted token as a possible real hostname when its
// last label is in this set. It is intentionally not exhaustive: suffixes
// already used by panewire's dotted event-kind vocabulary (event, host,
// machine, store, jobs, run, subscribe, ...), file names in the docs
// (md, rs, sh, plist, service, sock, sqlite3, ...), and TLDs that collide
// with common file extensions or JSON field names (app, so, cc, pl, id,
// is, in, at, be, to, no, it, us, as, by, do, im, am, fm, mu) are left out
// so the gate does not false-positive on legitimate text.
var docFlaggedTLDs = map[string]bool{
	"com": true, "net": true, "org": true, "io": true, "dev": true,
	"co": true, "ai": true, "me": true, "info": true, "biz": true,
	"xyz": true, "cloud": true, "site": true, "online": true,
	"tech": true, "pro": true, "digital": true, "network": true,
	"systems": true, "services": true, "solutions": true, "email": true,
	"hosting": true, "world": true, "zone": true, "agency": true,
	"company": true, "today": true, "media": true, "news": true,
	"software": true, "support": true, "tools": true, "works": true,
	"team": true, "group": true, "life": true, "social": true,
	// Internal-style pseudo-TLDs used for corporate intranet names.
	"internal": true, "corp": true, "lan": true, "home": true,
	"private": true, "local": true, "intranet": true,
	// Country-code TLDs in common corporate use. Extension/field
	// colliders (pl, rs, sh, md, mu, so, cc, id, is, in, at, be, to,
	// no, it, us, am, fm, im) are deliberately omitted.
	"uk": true, "de": true, "fr": true, "jp": true, "kr": true,
	"cn": true, "au": true, "ca": true, "br": true, "es": true,
	"nl": true, "se": true, "fi": true, "dk": true, "ch": true,
	"ie": true, "nz": true, "sg": true, "hk": true, "tw": true,
	"my": true, "th": true, "ph": true, "vn": true, "za": true,
	"mx": true, "ar": true, "cl": true, "eu": true, "tr": true,
	"il": true, "ae": true, "sa": true, "pt": true, "gr": true,
	"cz": true, "hu": true, "ro": true, "lu": true, "tv": true,
	"gg": true,
}

var (
	// Dotted token possibly containing an <angle-bracket> placeholder label.
	docHostTokenPattern = regexp.MustCompile(`[a-zA-Z0-9_<>-]+(\.[a-zA-Z0-9_<>-]+)+`)
	docUsersPathPattern = regexp.MustCompile(`/Users/[A-Za-z0-9_.-]+`)
	docTailnetIPPattern = regexp.MustCompile(`\b100\.(6[4-9]|[7-9][0-9]|1[01][0-9]|12[0-7])\.[0-9]{1,3}\.[0-9]{1,3}\b`)
	docPaneIDPattern    = regexp.MustCompile(`\bw[0-9A-Za-z]+:p[0-9]+\b`)
)

func docHostAllowed(token string) bool {
	for _, suffix := range docAllowedHostSuffixes {
		if token == suffix || strings.HasSuffix(token, "."+suffix) {
			return true
		}
	}
	return false
}

// scanDocText returns one finding per offending literal in a single doc.
func scanDocText(text string) []string {
	var findings []string
	for i, line := range strings.Split(text, "\n") {
		for _, m := range docUsersPathPattern.FindAllString(line, -1) {
			findings = append(findings, fmt.Sprintf("line %d: home path %q — use ~ or $HOME", i+1, m))
		}
		for _, m := range docTailnetIPPattern.FindAllString(line, -1) {
			if m != docAllowedTailnetIP {
				findings = append(findings, fmt.Sprintf("line %d: tailnet-range IP %q — only the documented example %s is allowed", i+1, m, docAllowedTailnetIP))
			}
		}
		for _, m := range docPaneIDPattern.FindAllString(line, -1) {
			if !docAllowedPaneIDs[m] {
				findings = append(findings, fmt.Sprintf("line %d: pane-id-shaped %q — use w1:p1/w1:p2 or <wN:pN>", i+1, m))
			}
		}
		for _, m := range docHostTokenPattern.FindAllString(line, -1) {
			if strings.ContainsAny(m, "<>") {
				continue // explicit <placeholder> label, e.g. <team>.cloudflareaccess.com
			}
			last := m[strings.LastIndex(m, ".")+1:]
			if !docFlaggedTLDs[strings.ToLower(last)] || docHostAllowed(m) {
				continue
			}
			findings = append(findings, fmt.Sprintf("line %d: host-shaped %q is not an allowed placeholder", i+1, m))
		}
	}
	return findings
}

// Every Markdown file in the checkout is public; none may carry an
// operational literal. Replaced values live in the allow-lists above, never
// in this rule set, so the check itself adds no disclosure.
func TestDocsContainNoOperationalLiterals(t *testing.T) {
	var docs []string
	err := filepath.WalkDir(".", func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() && d.Name() == ".git" {
			return filepath.SkipDir
		}
		if !d.IsDir() && strings.HasSuffix(d.Name(), ".md") {
			docs = append(docs, path)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walk repo: %v", err)
	}
	if len(docs) == 0 {
		t.Fatal("found no Markdown files; the test must run from the repository root")
	}
	var failures []string
	for _, doc := range docs {
		b, err := os.ReadFile(doc)
		if err != nil {
			t.Fatalf("read %s: %v", doc, err)
		}
		for _, f := range scanDocText(string(b)) {
			failures = append(failures, doc+":"+f)
		}
	}
	if len(failures) > 0 {
		t.Fatalf("operational literals in public docs:\n%s", strings.Join(failures, "\n"))
	}
}

// The scanner's own fixtures prove each rule fires (and that the sanctioned
// placeholders pass) without having to dirty a real doc.
func TestDocLiteralScannerRules(t *testing.T) {
	for _, bad := range []string{
		"hub.mycorp.dev",                      // real-shaped host on a flagged TLD
		"wss://node.acme.io",                  // real-shaped host inside a URL
		"db.corp",                             // internal pseudo-TLD
		"nas.home",                            // internal pseudo-TLD
		"realteam.cloudflareaccess.com",       // real subdomain of an allowed-<placeholder> service
		"/Users/someone/.config/panewire/env", // macOS home path
		"100.100.23.7",                        // tailnet-range IP that is not the doc example
		"w16:p3",                              // real-shaped pane id
		"wB:p7",                               // real-shaped pane id
		"evilgithub.com",                      // suffix lookalike, not an allowed host
		"docs.example.com.evil.dev",           // allowed suffix as a prefix label still fails
	} {
		if got := scanDocText(bad); len(got) == 0 {
			t.Fatalf("scanner missed %q", bad)
		}
	}
	for _, good := range []string{
		"hub.example.invalid",
		"hub.example",
		"wss://hub.example.com --hub-url https://hub.example.dev",
		"https://<team>.cloudflareaccess.com/cdn-cgi/access/certs",
		"github.com/mgh3326/panewire/cmd/panewire@latest",
		"objects.githubusercontent.com",
		"modernc.org/sqlite",
		"127.0.0.1:9377",
		"localhost",
		docAllowedTailnetIP,
		"w1:p1", "w1:p2", "<wN:pN>",
		"~/.config/panewire/hub.env",
		"$HOME/.config/panewire/hub.env",
		"lane.event", "job.completed", "events.subscribe",
		"brief.md", "install-linux.sh", "attach.rs", "h.mu",
		"e.g. this", "v0.8.2", "Stage2.Enabled",
	} {
		if got := scanDocText(good); len(got) != 0 {
			t.Fatalf("scanner false-positive on %q: %v", good, got)
		}
	}
}
