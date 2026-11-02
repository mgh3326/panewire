package fleet

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"regexp"
	"slices"
	"sort"
	"strings"
	"sync"
)

// `panewire delegate --panel N` (N 2..4) puts the same prompt to N seats of N
// different model families, in parallel, and sets the answers side by side.
// It is the cross-check of pstack's arena and interrogate: the value is where
// independent models agree, disagree or each see something the others do not.
//
// Each member is an ordinary delegation (its own job delegate-<parent>-<n>,
// pane or --headless), so placement, quota and hk records are per answer.
// Families are chosen first, one selection per member with the families
// already picked excluded, so the panel is the N best admitted distinct
// families under the normal selection policy. When fewer than N families are
// admitted the panel runs with those and says why. A member that fails or
// times out does not stop the others: its error is reported and the rest are
// collected. The exit status is 0 when at least one member answered; with
// none it is the first failure's status.

// PanelMember is one panel answer, or why there is none.
type PanelMember struct {
	Label string `json:"label"`
	DelegateResult
	Status string `json:"status"` // answered | failed
	Exit   int    `json:"exit,omitempty"`
	Error  string `json:"error,omitempty"`
}

// PanelResult is the --json document of a panel.
type PanelResult struct {
	Role      string        `json:"role"`
	Mode      string        `json:"mode"`
	Requested int           `json:"requested"`
	Answered  int           `json:"answered"`
	Reason    string        `json:"reason,omitempty"`
	Members   []PanelMember `json:"members"`
	Table     PanelTable    `json:"table"`
}

// PanelTable is the comparison of the answered members' points.
type PanelTable struct {
	Verdicts      map[string]string `json:"verdicts,omitempty"`
	Agreements    []PanelPoint      `json:"agreements"`
	Disagreements []PanelDispute    `json:"disagreements"`
	Unique        []PanelPoint      `json:"unique"`
}
type PanelPoint struct {
	Members []string `json:"members"`
	Text    string   `json:"text"`
}
type PanelDispute struct {
	// Where is a cited file:line, or "verdict" when the overall verdicts differ.
	Where  string            `json:"where"`
	Claims map[string]string `json:"claims"`
}

func (a *App) delegatePanel(ctx context.Context, d delegateReq) error {
	n := d.o.panel
	pick := Spawn{CWD: d.cwd, Profile: "auto", T: "T0", Role: d.seatRole, Host: "auto", Grade: d.o.grade, ParentFamily: d.caller.family, Task: d.caller.task, Inherited: d.caller.parent, RoutingRole: d.routingRole()}
	if d.o.headless || d.o.patch {
		pick.Host = "local"
	}
	var families []string
	reason := ""
	for len(families) < n {
		pick.ExcludeFamilies = slices.Clone(families)
		sel, e := a.selectProfile(ctx, pick)
		if e != nil {
			if len(families) == 0 {
				return e
			}
			reason = fmt.Sprintf("%d of %d distinct families admitted; no further family: %v", len(families), n, e)
			break
		}
		families = append(families, sel.chosen.seat.Family)
	}
	if reason != "" {
		a.warn("panel: %s", reason)
	}
	ids := make([]string, len(families))
	for i := range ids {
		id, e := a.delegateJobID(d.caller.parent)
		if e != nil {
			return e
		}
		ids[i] = id
	}
	// Resolve the hk client once so the members share it.
	_, _ = a.jobStore()

	type outcome struct {
		res    DelegateResult
		err    error
		stderr bytes.Buffer
	}
	outs := make([]*outcome, len(families))
	var wg sync.WaitGroup
	for i := range families {
		outs[i] = &outcome{}
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			o := outs[i]
			member := a.fork(io.Discard, &o.stderr)
			o.res, o.err = member.delegateRun(ctx, d, ids[i], families[i], nil)
		}(i)
	}
	wg.Wait()

	r := PanelResult{Role: d.o.role, Mode: "pane", Requested: n, Reason: reason}
	if d.o.headless {
		r.Mode = "headless"
	}
	var firstErr error
	for i, o := range outs {
		m := PanelMember{Label: string(rune('A' + i)), DelegateResult: o.res, Status: "answered"}
		if m.Job == "" {
			m.Job, m.Role, m.Mode = ids[i], d.o.role, r.Mode
		}
		if m.Family == "" {
			m.Family = families[i]
		}
		a.Err.Write(o.stderr.Bytes())
		if o.err != nil {
			m.Status, m.Exit, m.Error = "failed", exitCode(o.err), o.err.Error()
			a.warn("panel member %s (%s, family %s) failed: %v", m.Label, m.Job, m.Family, o.err)
			if firstErr == nil {
				firstErr = o.err
			}
		} else {
			r.Answered++
		}
		r.Members = append(r.Members, m)
	}
	if r.Answered == 0 {
		return firstErr
	}
	r.Table = panelTable(r.Members)
	return a.printPanel(r, d.o.json)
}

// fork is a copy of the app for one parallel member: its own output writers
// and warning memo, everything else shared.
func (a *App) fork(out, errw io.Writer) *App {
	b := *a
	b.Out, b.Err, b.warned = out, errw, nil
	return &b
}

var (
	panelLoc   = regexp.MustCompile(`[A-Za-z0-9_./-]+\.[A-Za-z0-9]+:\d+`)
	panelWord  = regexp.MustCompile(`[a-z0-9_]{3,}`)
	panelBullt = regexp.MustCompile(`^\s*(?:[-*+•]|\d+[.)])\s+`)
)

// panelSimilar is the Jaccard similarity above which two points are the same
// claim.
const panelSimilar = 0.5

type panelItem struct {
	member string
	text   string
	loc    string
	words  map[string]bool
}

// panelPoints lists a member's claims: the "points" it filed, else the lines
// of its answer.
func panelPoints(m PanelMember) []string {
	var pts []string
	if raw, ok := m.Result["points"].([]any); ok {
		for _, v := range raw {
			if s, ok := v.(string); ok && strings.TrimSpace(s) != "" {
				pts = append(pts, strings.TrimSpace(s))
			}
		}
	}
	if len(pts) > 0 {
		return pts
	}
	for _, line := range strings.Split(m.Answer, "\n") {
		line = strings.TrimSpace(panelBullt.ReplaceAllString(line, ""))
		if len(panelWord.FindAllString(strings.ToLower(line), -1)) >= 3 {
			pts = append(pts, line)
		}
	}
	return pts
}

func jaccard(a, b map[string]bool) float64 {
	if len(a) == 0 || len(b) == 0 {
		return 0
	}
	both := 0
	for w := range a {
		if b[w] {
			both++
		}
	}
	return float64(both) / float64(len(a)+len(b)-both)
}

// panelTable compares the members' points. Points that two or more members
// make (same words, by Jaccard similarity) are agreements. Points that cite
// the same file:line without being the same claim are disagreements, as are
// differing verdicts. What is left is unique to one member.
func panelTable(members []PanelMember) PanelTable {
	t := PanelTable{Agreements: []PanelPoint{}, Disagreements: []PanelDispute{}, Unique: []PanelPoint{}}
	var items []*panelItem
	verdicts := map[string]string{}
	for _, m := range members {
		if m.Status != "answered" {
			continue
		}
		if v := strings.ToLower(strings.TrimSpace(str(m.Result, "verdict"))); v != "" {
			verdicts[m.Label] = v
		}
		for _, p := range panelPoints(m) {
			it := &panelItem{member: m.Label, text: p, loc: panelLoc.FindString(p), words: map[string]bool{}}
			for _, w := range panelWord.FindAllString(strings.ToLower(panelLoc.ReplaceAllString(p, "")), -1) {
				it.words[w] = true
			}
			items = append(items, it)
		}
	}
	if len(verdicts) > 0 {
		t.Verdicts = verdicts
		distinct := map[string]bool{}
		for _, v := range verdicts {
			distinct[v] = true
		}
		if len(distinct) > 1 {
			claims := map[string]string{}
			for l, v := range verdicts {
				claims[l] = v
			}
			t.Disagreements = append(t.Disagreements, PanelDispute{Where: "verdict", Claims: claims})
		}
	}
	// Union points of different members that say the same thing.
	parent := make([]int, len(items))
	for i := range parent {
		parent[i] = i
	}
	var find func(int) int
	find = func(i int) int {
		for parent[i] != i {
			parent[i] = parent[parent[i]]
			i = parent[i]
		}
		return i
	}
	for i := range items {
		for j := i + 1; j < len(items); j++ {
			if items[i].member != items[j].member && jaccard(items[i].words, items[j].words) >= panelSimilar {
				parent[find(i)] = find(j)
			}
		}
	}
	clusters := map[int][]*panelItem{}
	var roots []int
	for i, it := range items {
		r := find(i)
		if _, seen := clusters[r]; !seen {
			roots = append(roots, r)
		}
		clusters[r] = append(clusters[r], it)
	}
	memberSet := func(c []*panelItem) []string {
		var ms []string
		for _, it := range c {
			if !slices.Contains(ms, it.member) {
				ms = append(ms, it.member)
			}
		}
		sort.Strings(ms)
		return ms
	}
	// A location cited by clusters of different members is disputed unless one
	// cluster already holds them all.
	disputed := map[int]bool{}
	byLoc := map[string][]int{}
	var locs []string
	for _, r := range roots {
		for _, it := range clusters[r] {
			if it.loc == "" {
				continue
			}
			if _, ok := byLoc[it.loc]; !ok {
				locs = append(locs, it.loc)
			}
			if !slices.Contains(byLoc[it.loc], r) {
				byLoc[it.loc] = append(byLoc[it.loc], r)
			}
		}
	}
	for _, loc := range locs {
		rs := byLoc[loc]
		if len(rs) < 2 {
			continue
		}
		claims := map[string]string{}
		for _, r := range rs {
			for _, it := range clusters[r] {
				if it.loc == loc {
					if _, ok := claims[it.member]; !ok {
						claims[it.member] = it.text
					}
				}
			}
		}
		if len(claims) < 2 {
			continue
		}
		for _, r := range rs {
			disputed[r] = true
		}
		t.Disagreements = append(t.Disagreements, PanelDispute{Where: loc, Claims: claims})
	}
	for _, r := range roots {
		c := clusters[r]
		ms := memberSet(c)
		switch {
		case len(ms) >= 2:
			t.Agreements = append(t.Agreements, PanelPoint{Members: ms, Text: c[0].text})
		case !disputed[r]:
			t.Unique = append(t.Unique, PanelPoint{Members: ms, Text: c[0].text})
		}
	}
	return t
}

// printPanel writes the panel: each member's answer, then the comparison
// table last. --json prints PanelResult alone.
func (a *App) printPanel(r PanelResult, asJSON bool) error {
	if asJSON {
		return writeJSON(a.Out, r)
	}
	fmt.Fprintf(a.Out, "panel role=%s mode=%s requested=%d answered=%d\n", r.Role, r.Mode, r.Requested, r.Answered)
	if r.Reason != "" {
		fmt.Fprintf(a.Out, "reason: %s\n", r.Reason)
	}
	for _, m := range r.Members {
		fmt.Fprintf(a.Out, "\n== %s job=%s profile=%s family=%s status=%s", m.Label, m.Job, m.Profile, m.Family, m.Status)
		if m.Status == "failed" {
			fmt.Fprintf(a.Out, " exit=%d ==\n%s\n", m.Exit, m.Error)
			continue
		}
		fmt.Fprintln(a.Out, " ==")
		if m.Patch != "" {
			fmt.Fprintf(a.Out, "patch=%s\n", m.Patch)
		}
		fmt.Fprintln(a.Out, m.Answer)
	}
	t := r.Table
	fmt.Fprintf(a.Out, "\n== side by side ==\n")
	if len(t.Verdicts) > 0 {
		var ls []string
		for l := range t.Verdicts {
			ls = append(ls, l)
		}
		sort.Strings(ls)
		var vs []string
		for _, l := range ls {
			vs = append(vs, l+"="+t.Verdicts[l])
		}
		fmt.Fprintf(a.Out, "verdicts: %s\n", strings.Join(vs, " "))
	}
	section := func(title string, n int, each func(i int) string) {
		fmt.Fprintf(a.Out, "%s (%d)\n", title, n)
		for i := 0; i < n; i++ {
			fmt.Fprintf(a.Out, "  - %s\n", each(i))
		}
	}
	section("agreements", len(t.Agreements), func(i int) string {
		return fmt.Sprintf("[%s] %s", strings.Join(t.Agreements[i].Members, ","), t.Agreements[i].Text)
	})
	section("disagreements", len(t.Disagreements), func(i int) string {
		d := t.Disagreements[i]
		var ls []string
		for l := range d.Claims {
			ls = append(ls, l)
		}
		sort.Strings(ls)
		var cs []string
		for _, l := range ls {
			cs = append(cs, fmt.Sprintf("%s: %s", l, d.Claims[l]))
		}
		return d.Where + " | " + strings.Join(cs, " | ")
	})
	section("unique", len(t.Unique), func(i int) string {
		return fmt.Sprintf("[%s] %s", strings.Join(t.Unique[i].Members, ","), t.Unique[i].Text)
	})
	return nil
}
