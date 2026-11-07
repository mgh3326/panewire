package fleet

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
)

// hkProjectOwner is handoffkeep's project-owner answer
// (GET /v1/tasks/projects/{project}/owner).
type hkProjectOwner struct {
	DesignLane     string `json:"design_lane"`
	EffectiveOwner string `json:"effective_owner"`
}

// resolveOwner fills s.Owner from the task's project owner in handoffkeep when
// the caller omitted --owner. A task is required: a spawn with neither --task
// nor --parent-job keeps its previous behaviour. hk answering nothing is a
// refusal, never a guess and never a silent operator-desk fallback.
func (a *App) resolveOwner(ctx context.Context, s *Spawn) error {
	if s.Owner != "" {
		return nil
	}
	task := s.Task
	if task == "" && s.ParentJob != "" {
		parent, err := a.resolveParent(ctx, s.ParentJob)
		if err != nil {
			return fail(exitStateConflict, "parent metadata unreadable: %v", err)
		}
		task = parent.Task
	}
	if task == "" {
		return nil
	}
	r := a.call(ctx, "handoffkeep", "tasks", "show", task)
	if r.Code != 0 {
		return fail(exitUsage, "cannot read task %s from hk to resolve the owner (%s); pass --owner LANE explicitly", task, strings.TrimSpace(r.Err))
	}
	var t Task
	if json.Unmarshal([]byte(r.Out), &t) != nil || (t.ID != nil && scalar(t.ID) != task) {
		return fail(exitUsage, "unreadable task %s from hk to resolve the owner; pass --owner LANE explicitly", task)
	}
	if t.Project == "" {
		return fail(exitUsage, "task %s names no project, so hk cannot resolve an owner; pass --owner LANE explicitly", task)
	}
	r = a.call(ctx, "handoffkeep", "tasks", "owner", t.Project)
	if r.Code != 0 {
		return fail(exitUsage, "cannot resolve the hk owner of project %s (%s); pass --owner LANE explicitly", t.Project, strings.TrimSpace(r.Err))
	}
	var owner hkProjectOwner
	if json.Unmarshal([]byte(r.Out), &owner) != nil || strings.TrimSpace(owner.EffectiveOwner) == "" {
		return fail(exitUsage, "hk returned no effective owner for project %s; pass --owner LANE explicitly", t.Project)
	}
	s.Owner = owner.EffectiveOwner
	fmt.Fprintf(a.Out, "owner=%s source=hk project=%s\n", s.Owner, t.Project)
	return nil
}
