package fleet

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/mgh3326/handoffkeep/client"
)

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
	t, err := hkGetTask(ctx, task)
	if err != nil && !errors.Is(err, errHKUnreadable) {
		return fail(exitUsage, "cannot read task %s from hk to resolve the owner (%s); pass --owner LANE explicitly", task, hkErrText(err))
	}
	if err != nil || !hkTaskIDIs(t, task) {
		return fail(exitUsage, "unreadable task %s from hk to resolve the owner; pass --owner LANE explicitly", task)
	}
	project := hkTaskProject(t)
	if project == "" {
		return fail(exitUsage, "task %s names no project, so hk cannot resolve an owner; pass --owner LANE explicitly", task)
	}
	owner, err := hkProjectOwner(ctx, project)
	if err != nil && !errors.Is(err, errHKUnreadable) {
		return fail(exitUsage, "cannot resolve the hk owner of project %s (%s); pass --owner LANE explicitly", project, hkErrText(err))
	}
	if err != nil || strings.TrimSpace(owner.EffectiveOwner) == "" {
		return fail(exitUsage, "hk returned no effective owner for project %s; pass --owner LANE explicitly", project)
	}
	s.Owner = owner.EffectiveOwner
	fmt.Fprintf(a.Out, "owner=%s source=hk project=%s\n", s.Owner, project)
	return nil
}

// hkProjectOwner reads the project's owner over hk HTTP. A project hk does
// not know reads "no such project", as the hk CLI printed it. The answer is
// read as the CLI path read `handoffkeep tasks owner` output — its two lanes
// only, so a timestamp hk's typed client cannot decode does not matter — and
// one that path could not read is errHKUnreadable.
func hkProjectOwner(ctx context.Context, project string) (client.ProjectOwner, error) {
	body, answered, err := hkAnswer(func(c client.Client) error {
		_, e := c.ProjectOwner(ctx, project)
		return e
	})
	if client.ErrorCode(err) == "not_found" {
		return client.ProjectOwner{}, errors.New("no such project: " + project)
	}
	if !answered {
		return client.ProjectOwner{}, err
	}
	var owner struct {
		DesignLane     string `json:"design_lane"`
		EffectiveOwner string `json:"effective_owner"`
	}
	if json.Unmarshal(body, &owner) != nil {
		return client.ProjectOwner{}, errHKUnreadable
	}
	return client.ProjectOwner{Name: project, DesignLane: owner.DesignLane, EffectiveOwner: owner.EffectiveOwner}, nil
}
