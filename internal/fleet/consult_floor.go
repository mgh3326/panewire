package fleet

import (
	"path"
	"slices"
	"strings"

	"github.com/mgh3326/panewire/internal/grade"
)

// consultDesignLane matches the hub's current relay.design_lanes list:
// ["*-design", "routing-policy", "fleet-policy"]. The policy's
// consult.design_session floor is SS (hk:doc policy/model-families).
func consultDesignLane(lane string) bool {
	if lane == "" || lane == "operator-desk" || strings.HasSuffix(lane, "-operator-desk") {
		return false
	}
	patternMatch, _ := path.Match("*-design", lane)
	return patternMatch || lane == "routing-policy" || lane == "fleet-policy"
}

// consultFloorOf is the lowest grade a consult rung may launch at for an asker
// of grade askerGrade: one grade above the asker, capped at SS. A session
// without a task grade (askerGrade "") has floor S+. See docs/config.md
// (consult grade floor).
func consultFloorOf(askerGrade string) string {
	if askerGrade == "" {
		return "S+"
	}
	grades := grade.Grades()
	return grades[min(gradeOrder[askerGrade]+1, len(grades)-1)]
}

// consultFloorFor applies the policy floor for a design-session asker and the
// ordinary asker+1 floor for every other lane.
func consultFloorFor(askerGrade, lane string) string {
	if consultDesignLane(lane) {
		return "SS"
	}
	return consultFloorOf(askerGrade)
}

// rungIndex groups catalog rows by profile and effort, so a consult candidate
// can be judged on the rung it launches at.
type rungIndex map[[2]string][]CatalogRow

func indexRungs(rows []CatalogRow) rungIndex {
	ix := make(rungIndex, len(rows))
	for _, row := range rows {
		k := [2]string{row.Profile, row.Effort}
		ix[k] = append(ix[k], row)
	}
	return ix
}

// hasLaunchEffort reports whether the profile has a live row naming an effort.
// Admission resolves a rung-less row's effort to the profile's lowest ordinary
// named rung, so a profile without one resolves to no effort at all and
// refuses the launch ("catalog row has no launch effort").
func (ix rungIndex) hasLaunchEffort(profile string) bool {
	for key, rows := range ix {
		if key[0] != profile || key[1] == "" {
			continue
		}
		for _, row := range rows {
			if row.RetiredAt == nil {
				return true
			}
		}
	}
	return false
}

// launchGrade is the catalog grade of the rung row launches at. ok is false
// when the catalog has no such rung. A row that launches at its own rung is
// judged on its own grade; otherwise the rung in the row's pool wins over any
// other pool's rung.
func (ix rungIndex) launchGrade(row CatalogRow, launch string) (string, bool) {
	if launch == row.Effort {
		return row.Grade, true
	}
	found, ok := "", false
	for _, r := range ix[[2]string{row.Profile, launch}] {
		if r.Pool == row.Pool {
			return r.Grade, true
		}
		if !ok {
			found, ok = r.Grade, true
		}
	}
	return found, ok
}

// topGradeDrop judges a consult candidate on its launch rung: an unrated row or
// rung is never chosen, and a launch rung that is missing from the catalog or
// rated below floor is grade_insufficient. floor "" judges no floor, which the
// fallback probe uses to see the rungs the floor alone removed.
func topGradeDrop(row CatalogRow, launch, floor string, rungs rungIndex) string {
	launchGrade, ok := rungs.launchGrade(row, launch)
	switch {
	case row.Grade == gradeUnrated || launchGrade == gradeUnrated:
		return "grade_unrated"
	case !ok:
		return "grade_insufficient"
	case floor != "" && gradeOrder[launchGrade] < gradeOrder[floor]:
		return "grade_insufficient"
	}
	return ""
}

// reasonNoLaunchEffort drops a rung-less row whose profile has no named rung:
// admission has no effort to resolve for it, so choosing it would fail the
// plan with "catalog row has no launch effort" instead of trying the next
// candidate.
const reasonNoLaunchEffort = "no_launch_effort"

// staticDrop applies the rules a candidate row meets before presence and quota,
// in the order selectFor has always used. floor is the consult grade floor.
// It returns the reason the row drops ("" when it passes), and the seat name
// and seat it passes under.
func (a *App) staticDrop(row CatalogRow, req selReq, floor string, rungs rungIndex, cooling bool) (string, string, *Seat) {
	switch {
	case row.RetiredAt != nil:
		return "retired", "", nil
	case row.Profile == "" || row.ModelID == "" || row.Pool == "":
		return "catalog_incomplete", "", nil
	case !req.route.admits(row.Profile):
		return "role_filtered", "", nil
	case !validCatalogGrade(row.Grade):
		return "grade_invalid", "", nil
	}
	if !req.route.top() {
		switch {
		case row.Grade == gradeUnrated:
			return "grade_unrated", "", nil
		case gradeOrder[row.Grade] < gradeOrder[req.grade]:
			return "grade_insufficient", "", nil
		}
	}
	roleCap := req.route.roleEffortFor(row)
	switch {
	case row.Gate != "default" && row.Gate != "escalation" && row.Gate != "consult_only":
		return "gate_unknown", "", nil
	case row.Gate != "default" && !req.allowEscalation && !req.route.familyAdmitsGate(row):
		return "gate_class", "", nil
	case aboveEffort(row.Effort, roleCap) && !req.allowEscalation:
		return reasonAboveRoleEffort, "", nil
	case req.route.minEffort() != "" && rankEffort(a.rowLaunchEffort(row)) < rankEffort(req.route.minEffort()):
		return reasonBelowRoleMinEffort, "", nil
	case req.effort != "" && row.Effort != req.effort:
		return "effort_mismatch", "", nil
	case !validID(row.Pool):
		return "pool_invalid", "", nil
	case slices.Contains(req.excludePools, row.Pool):
		return "pool_excluded", "", nil
	case cooling:
		return "pool_cooling", "", nil
	}
	name, seat, reason := a.seatFor(row, req)
	if seat == nil {
		return reason, "", nil
	}
	if row.Effort == "" && !rungs.hasLaunchEffort(row.Profile) {
		// Dropped after the seat rules so a row they refuse keeps its own
		// reason, and before the launch itself: admission resolves no effort
		// for this profile, so selecting it would end the whole plan.
		return reasonNoLaunchEffort, "", nil
	}
	if req.route.top() {
		// Judged on the rung the spawn launches: spawn pins the launch to the
		// chosen row's own effort, not the role-effort rung. After the seat
		// rules, so a row they drop keeps its own reason, as before the floor
		// existed.
		if reason := topGradeDrop(row, a.rowLaunchEffort(row), floor, rungs); reason != "" {
			return reason, "", nil
		}
	}
	return "", name, seat
}

// consultStamp records how a consult selection judged its rungs: the asker's
// grade and family ("unknown" when the spawn named none), where the grade came
// from, the floor asked and the floor used, and the fallback taken, if any.
func (p *selPlan) consultStamp() map[string]any {
	family := p.req.authorFamily
	if family == "" {
		family = "unknown"
	}
	floor := consultFloorFor(p.req.grade, p.req.lane)
	used := p.req.usedFloor
	if used == "" {
		used = floor
	}
	return map[string]any{
		"asker_grade":  p.req.grade,
		"asker_family": family,
		"grade_source": p.req.gradeSource,
		"floor":        floor,
		"used_floor":   used,
		"fallback":     p.req.fallback,
	}
}
