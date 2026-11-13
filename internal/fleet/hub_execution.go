package fleet

import (
	"context"
	"errors"
	"io"
	"os"
)

// ExecuteSubmission runs the existing placement and admission engine in the hub.
// Only provider/node commands remain external; no launcher subprocess or SSH leg
// is available. The submitting filesystem is represented by validated evidence.
func (a *App) ExecuteSubmission(ctx context.Context, request Submission) error {
	s, cleanup, err := a.hubSpawn(request)
	if err != nil {
		return err
	}
	defer cleanup()
	return a.Spawn(ctx, s)
}

// hubSpawn binds a validated submission to this App as the hub's coordinator
// and writes its brief to a private file the spawn reads. The returned cleanup
// removes that file. Execution and dry-run planning both start here, so a plan
// places with exactly the config a real spawn would.
func (a *App) hubSpawn(request Submission) (Spawn, func(), error) {
	s, err := ValidateSubmission(request)
	if err != nil {
		return Spawn{}, nil, err
	}
	if err = a.CheckTrust(); err != nil {
		return Spawn{}, nil, err
	}
	a.Repository = &request.Repository
	a.RequestID = request.RequestID
	if a.Config.Hosts == nil {
		a.Config.Hosts = map[string]map[string]any{}
	}
	localID := a.machine()
	if localID != "local" && validID(localID) && boolean(a.Config.Local, "spawn", true) {
		if _, exists := a.Config.Hosts[localID]; !exists {
			host := map[string]any{}
			for key, value := range a.Config.Local {
				host[key] = value
			}
			a.Config.Hosts[localID] = host
			a.Config.HostOrder = append(a.Config.HostOrder, localID)
		}
	}
	a.Config.Local = map[string]any{"machine_id": "hub-coordinator", "spawn": false}
	for _, host := range a.Config.Hosts {
		host["via"] = "hub"
	}
	f, err := os.CreateTemp("", "panewire-hub-brief-*")
	if err != nil {
		return Spawn{}, nil, err
	}
	cleanup := func() { _ = os.Remove(f.Name()) }
	if err = f.Chmod(0600); err == nil {
		_, err = io.WriteString(f, request.Brief)
	}
	err = errors.Join(err, f.Close())
	if err != nil {
		cleanup()
		return Spawn{}, nil, err
	}
	s.Prompt = f.Name()
	return s, cleanup, nil
}

func ParseHubHosts(raw string) ([]HostView, error) { return parseHubHosts(raw) }
