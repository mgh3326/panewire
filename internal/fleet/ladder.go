package fleet

import (
	"context"
	_ "embed"
	"encoding/json"
	"fmt"
	"os"
	"slices"
	"strings"
)

// The escalation ladder (hk:doc policy/model-families §The escalation ladder,
// operator decisions 137, 146 and 148). families.json is that document's
// machine-readable block, copied whole so a reader can diff the two: the
// per-family model order, effort scale, ladder cap and entry effort. panewire
// reads it for the round-5 step (`panewire escalate-author`); the policy stays
// canonical, so a change there changes this file in the same wave.
//
// The three steps, in order:
//  1. same model, effort one step up, up to the family's ladder cap;
//  2. at the cap, the next model of the same family at its entry effort;
//  3. at the top of the family, one try on a seat of another family at the
//     same task grade (resolved by the ordinary selection core).
//
// A step never goes to a rung graded below the task, and never to a
// consult-only rung.
//
//go:embed families.json
var defaultFamilies []byte

// LadderStepEndReason is the end_reason a replaced author's hk record takes:
// the successor's pointer is the last key=value token, as mergewake reads it.
// The record that carries it is the proof that the step reached its pane
// close, so a replay recognises its own half-done step by it.
func LadderStepEndReason(round int, successor string) string {
	return fmt.Sprintf("%s%d replaced_by=%s", ladderStepEndReasonPrefix, round, successor)
}

const ladderStepEndReasonPrefix = "ladder step round="

// ladderStepReplayed reports whether record is a job a ladder step of this
// round already ended.
func ladderStepReplayed(record map[string]any, round int) bool {
	state, _ := record["state"].(string)
	reason, _ := record["end_reason"].(string)
	return state == "lost" && strings.HasPrefix(reason, fmt.Sprintf("%s%d replaced_by=", ladderStepEndReasonPrefix, round))
}

// LadderFamily is one pool family in the policy block.
type LadderFamily struct {
	Harness    string            `json:"harness"`
	Billing    string            `json:"billing"`
	Ends       string            `json:"ends,omitempty"`
	FreeUntil  any               `json:"free_until,omitempty"`
	Models     []string          `json:"models"`
	Efforts    []string          `json:"efforts,omitempty"`
	LadderCap  string            `json:"ladder_cap,omitempty"`
	CapByModel map[string]string `json:"ladder_cap_by_model,omitempty"`
	Entry      string            `json:"entry_effort,omitempty"`
}

// Families is the policy's machine-readable block.
type Families struct {
	Version   int                     `json:"version"`
	CountedBy string                  `json:"counted_by"`
	Families  map[string]LadderFamily `json:"families"`
	Ladder    struct {
		TriggerReviewedHeads int      `json:"trigger_reviewed_heads"`
		Steps                []string `json:"steps"`
	} `json:"ladder"`
}

// LoadFamilies decodes the embedded policy block. An invalid block is a
// configuration refusal, never a silent empty ladder.
func LoadFamilies() (Families, error) {
	var f Families
	if err := json.Unmarshal(defaultFamilies, &f); err != nil {
		return f, fail(exitConfig, "invalid families policy: %v", err)
	}
	if f.Version != 1 || len(f.Families) == 0 {
		return f, fail(exitConfig, "invalid families policy version or empty families")
	}
	return f, nil
}

// LadderPick is one seat rung: what a spawn pins and what a ladder step moves
// from. Pool names the ladder family (claude, codex, ...); Family is the
// seat's routing family from the seat policy (anthropic, openai, ...), which
// step 3 excludes.
type LadderPick struct {
	Profile string
	Effort  string
	Pool    string
	Family  string
	ModelID string
	Grade   string
}

// LadderStep names which policy step produced the pick, so the caller records
// the kind that was taken.
type LadderStep struct {
	Kind string
	Pick LadderPick
}

// The step names the policy block lists.
const (
	ladderSameModelNextEffort  = "same_model_next_effort"
	ladderNextModelAtEntry     = "next_model_at_entry_effort"
	ladderOtherFamilySameGrade = "other_family_same_grade_once"
)

// NextLadderRung computes the next rung for a seat that has not passed a
// review round. ok false with no error is the policy's "no step left"; an
// error is a configuration or catalog failure.
func (a *App) NextLadderRung(ctx context.Context, from LadderPick, grade, t string) (LadderStep, bool, error) {
	if a.Quota == nil {
		return LadderStep{}, false, fail(exitConfig, "POLICY_UNAVAILABLE: the ladder needs the quota catalog")
	}
	families, err := LoadFamilies()
	if err != nil {
		return LadderStep{}, false, err
	}
	rows, err := a.Quota.Catalog(ctx)
	if err != nil {
		return LadderStep{}, false, fail(exitConfig, "POLICY_UNAVAILABLE: bench catalog unreadable: %v", err)
	}
	fam, known := families.Families[from.Pool]
	if known && slices.Contains(fam.Models, from.Profile) {
		if pick, ok := ladderNextEffort(rows, from, fam, grade); ok {
			return LadderStep{Kind: ladderSameModelNextEffort, Pick: pick}, true, nil
		}
		if pick, ok := ladderNextModel(rows, from, fam, grade); ok {
			return LadderStep{Kind: ladderNextModelAtEntry, Pick: pick}, true, nil
		}
	}
	pick, err := a.OtherFamilySeat(ctx, from.Family, grade, t)
	if err != nil {
		if ladderNoCandidate(err) {
			return LadderStep{}, false, nil
		}
		return LadderStep{}, false, err
	}
	return LadderStep{Kind: ladderOtherFamilySameGrade, Pick: pick.pick()}, true, nil
}

// ladderNextEffort is step 1: the same model one effort step up, up to the
// family's cap for that model.
func ladderNextEffort(rows []CatalogRow, from LadderPick, fam LadderFamily, grade string) (LadderPick, bool) {
	efforts := fam.Efforts
	if len(efforts) == 0 {
		return LadderPick{}, false
	}
	current := from.Effort
	if current == "" {
		current = "medium"
	}
	cap := fam.LadderCap
	if c, ok := fam.CapByModel[from.Profile]; ok {
		cap = c
	}
	start := slices.Index(efforts, current)
	if start < 0 {
		return LadderPick{}, false
	}
	limit := len(efforts)
	if cap != "" {
		if at := slices.Index(efforts, cap); at >= 0 {
			limit = at + 1
		}
	}
	// A job already at or above the cap has no step here; the ladder goes on
	// to the next model.
	if start+1 >= min(limit, len(efforts)) {
		return LadderPick{}, false
	}
	for _, effort := range efforts[start+1 : min(limit, len(efforts))] {
		if row, ok := ladderRow(rows, from.Profile, effort, grade); ok {
			return ladderPick(row, from.Family), true
		}
	}
	return LadderPick{}, false
}

// ladderNextModel is step 2: at the cap, the next model of the same family at
// its entry effort.
func ladderNextModel(rows []CatalogRow, from LadderPick, fam LadderFamily, grade string) (LadderPick, bool) {
	entry := fam.Entry
	if entry == "" && len(fam.Efforts) > 0 {
		entry = "medium"
	}
	at := slices.Index(fam.Models, from.Profile)
	if at < 0 {
		return LadderPick{}, false
	}
	for _, model := range fam.Models[at+1:] {
		if row, ok := ladderRow(rows, model, entry, grade); ok {
			return ladderPick(row, from.Family), true
		}
	}
	return LadderPick{}, false
}

// ladderRow is the live catalog rung a step may take: not retired, an ordinary
// or escalation gate (never consult-only), and a grade that covers the task.
func ladderRow(rows []CatalogRow, profile, effort, grade string) (CatalogRow, bool) {
	for _, row := range rows {
		if row.Profile != profile || row.Effort != effort || row.RetiredAt != nil {
			continue
		}
		if row.Gate != defaultGate && row.Gate != escalationGate {
			continue
		}
		if !ladderGradeCovers(row.Grade, grade) {
			continue
		}
		return row, true
	}
	return CatalogRow{}, false
}

// ladderPick projects a catalog rung onto the pick a spawn pins.
func ladderPick(row CatalogRow, family string) LadderPick {
	return LadderPick{Profile: row.Profile, Effort: row.Effort, Pool: row.Pool, Family: family, ModelID: row.ModelID, Grade: row.Grade}
}

// ladderGradeCovers reports a rung graded at or above the task grade. A task
// without a grade (or an unrated rung) is never a step: the policy forbids
// both.
func ladderGradeCovers(rung, task string) bool {
	r, ok := gradeOrder[rung]
	if !ok || rung == gradeUnrated {
		return false
	}
	if task == "" {
		return true
	}
	n, ok := gradeOrder[task]
	return ok && r >= n
}

// ladderNoCandidate reports the selection refusal that means no seat passes
// the role, retirement, grade, gate and seat rules, as opposed to one the
// quota gate refused.
func ladderNoCandidate(err error) bool {
	return strings.Contains(err.Error(), "no profile passes role, retirement, grade, gate and seat rules")
}

// SeatChoice is one resolved seat: the profile and effort to pin.
type SeatChoice struct {
	Profile, Effort, Pool, Family, ModelID, Grade, Seat string
}

func (c SeatChoice) pick() LadderPick {
	return LadderPick{Profile: c.Profile, Effort: c.Effort, Pool: c.Pool, Family: c.Family, ModelID: c.ModelID, Grade: c.Grade}
}

// ConsultSeat resolves the consult seat for an asker of askerGrade in lane,
// in another family than builderFamily: the consult floor of
// hk:doc policy/model-families §Consult (one grade above the asker, capped at
// SS, a design lane asking SS), never an unrated rung.
func (a *App) ConsultSeat(ctx context.Context, askerGrade, lane, builderFamily, t string) (SeatChoice, error) {
	gradeSource := "flag"
	if askerGrade == "" {
		gradeSource = "without_task"
	}
	return a.pickSeat(ctx, "consult", askerGrade, gradeSource, lane, builderFamily, t, nil)
}

// OtherFamilySeat resolves a seat of another routing family than
// excludeFamily at grade: ladder step 3. It is the ordinary selection core
// with the family excluded, so pool order and the quota gates still decide.
func (a *App) OtherFamilySeat(ctx context.Context, excludeFamily, grade, t string) (SeatChoice, error) {
	var exclude []string
	if excludeFamily != "" {
		exclude = []string{excludeFamily}
	}
	return a.pickSeat(ctx, "implementer", grade, "flag", "", "", t, exclude)
}

// pickSeat runs the selection core for one routing role and returns the chosen
// seat. It places no host, spawns nothing and writes no job record.
func (a *App) pickSeat(ctx context.Context, routingRole, grade, gradeSource, lane, builderFamily, t string, excludeFamilies []string) (SeatChoice, error) {
	if a.Quota == nil {
		return SeatChoice{}, fail(exitConfig, "POLICY_UNAVAILABLE: seat selection needs the quota catalog")
	}
	seatRole := "worker"
	if routingRole == "reviewer" || routingRole == "judge" {
		seatRole = "reviewer"
	}
	route, err := a.routeFor(ctx, seatRole, routingRole)
	if err != nil {
		return SeatChoice{}, err
	}
	cwd, _ := os.Getwd()
	s := Spawn{CWD: cwd, Profile: "auto", Host: "auto", Role: seatRole, T: t, ParentFamily: builderFamily, RoutingRole: routingRole, AllowEscalation: true, DryRun: true, ExcludeFamilies: excludeFamilies}
	req := selReq{grade: grade, lane: lane, gradeSource: gradeSource, t: t, role: seatRole, allowEscalation: true, builderFamily: builderFamily, excludeFamilies: excludeFamilies, urgency: urgencyNormal, urgencySource: "default", route: route}
	sel, err := a.selectFor(ctx, s, req)
	if err != nil {
		return SeatChoice{}, err
	}
	c := sel.chosen
	effort := route.effort(c.row)
	if effort == "" {
		effort = c.seat.Effort
	}
	return SeatChoice{Profile: c.seat.Canon, Effort: effort, Pool: c.row.Pool, Family: c.seat.Family, ModelID: c.row.ModelID, Grade: c.row.Grade, Seat: c.seatName}, nil
}

// TaskMarker reads a task's (grade X, Tn) title marker over handoffkeep.
func TaskMarker(ctx context.Context, task string) (grade, scale string, err error) {
	t, err := hkGetTask(ctx, task)
	if err != nil {
		return "", "", fail(exitConfig, "task %s unreadable: %v", task, err)
	}
	m := taskMarker.FindStringSubmatch(t.Title)
	if m == nil {
		return "", "", fail(exitSeatRefused, "task %s title has no (grade X, Tn) marker", task)
	}
	return strings.ToUpper(m[1]), strings.ToUpper(m[2]), nil
}
