package panewire

import (
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/mgh3326/panewire/internal/fleet"
)

// jobHarnessSession refreshes the newest spawn's reference, retaining its
// landing value if the pane or herdr is no longer available.
func jobHarnessSession(eventsDir, pane string) *fleet.HarnessSession {
	entries, err := os.ReadDir(eventsDir)
	if err != nil {
		return nil
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].Name() > entries[j].Name() })
	for _, entry := range entries {
		if !strings.HasSuffix(entry.Name(), "-job.spawned.json") {
			continue
		}
		data, err := os.ReadFile(filepath.Join(eventsDir, entry.Name()))
		if err != nil {
			return nil
		}
		var event struct {
			Payload struct {
				Pane           string                `json:"pane_id"`
				HerdrSession   string                `json:"herdr_session"`
				HarnessSession *fleet.HarnessSession `json:"harness_session"`
			} `json:"payload"`
		}
		if json.Unmarshal(data, &event) != nil || event.Payload.HarnessSession == nil || event.Payload.Pane != pane {
			return nil
		}
		session := event.Payload.HarnessSession
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		binary := os.Getenv("HERDR_BIN")
		if binary == "" {
			binary = "herdr"
		}
		cmd := exec.CommandContext(ctx, binary, "agent", "list")
		cmd.Env = hubSpawnEnvironment(os.Environ(), event.Payload.HerdrSession)
		output, err := cmd.Output()
		if err == nil {
			agents, err := fleet.ParseAgents(output)
			if err == nil {
				refreshed := fleet.FindHarnessSession(agents, pane, session.Kind)
				if refreshed.Source != "unavailable" {
					return &refreshed
				}
			}
		}
		return session
	}
	return nil
}
