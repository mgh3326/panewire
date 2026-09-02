package panewire

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
)

// scanHubActiveJobs reads only structured local event metadata. It is capped
// before it reaches a wire payload; brief text is never copied.
func scanHubActiveJobs(inboxRoot string) []HubActiveJob {
	if inboxRoot == "" {
		return nil
	}
	entries, err := os.ReadDir(filepath.Join(inboxRoot, "jobs"))
	if err != nil {
		return nil
	}
	jobs := make([]HubActiveJob, 0, 32)
	for _, entry := range entries {
		if len(jobs) == 32 {
			break
		}
		if !entry.IsDir() || !hubJobIDPattern.MatchString(entry.Name()) {
			continue
		}
		if active, ok := scanHubJobEvents(filepath.Join(inboxRoot, "jobs", entry.Name(), "events"), entry.Name()); ok {
			jobs = append(jobs, active)
		}
	}
	sort.Slice(jobs, func(i, j int) bool { return jobs[i].JobID < jobs[j].JobID })
	return jobs
}

func scanHubJobEvents(eventsDir, jobID string) (HubActiveJob, bool) {
	entries, err := os.ReadDir(eventsDir)
	if err != nil {
		return HubActiveJob{}, false
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].Name() < entries[j].Name() })
	var active HubActiveJob
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".json") {
			continue
		}
		contents, readErr := os.ReadFile(filepath.Join(eventsDir, entry.Name()))
		if readErr != nil || len(contents) > 16<<10 {
			continue
		}
		var event struct {
			Type       string `json:"type"`
			Kind       string `json:"kind"`
			Event      string `json:"event"`
			AgentLabel string `json:"agent_label"`
			PushSHA    string `json:"push_sha"`
			Epoch      uint64 `json:"epoch"`
		}
		if json.Unmarshal(contents, &event) != nil {
			continue
		}
		kind := event.Type
		if kind == "" {
			kind = event.Kind
		}
		if kind == "" {
			kind = event.Event
		}
		seq, seqOK := hubEventSequence(entry.Name())
		switch kind {
		case "job.claimed", "job.claim":
			candidate := HubActiveJob{JobID: jobID, AgentLabel: event.AgentLabel, LastEventSeq: seq, PushSHA: event.PushSHA, Epoch: event.Epoch}
			if seqOK && validHubActiveJob(candidate) {
				active = candidate
			}
		case "job.completed", "job.completion", "job.revoked":
			active = HubActiveJob{}
		}
	}
	return active, validHubActiveJob(active)
}

func hubEventSequence(name string) (uint64, bool) {
	prefix, _, found := strings.Cut(name, "-")
	if !found || len(prefix) == 0 || len(prefix) > 20 {
		return 0, false
	}
	value, err := strconv.ParseUint(prefix, 10, 64)
	return value, err == nil
}

func writeHubRevocation(inboxRoot string, revoked hubJobRevokedEvent) error {
	if inboxRoot == "" || !hubJobIDPattern.MatchString(revoked.JobID) || revoked.Epoch == 0 {
		return fmt.Errorf("invalid job revocation")
	}
	eventsDir := filepath.Join(inboxRoot, "jobs", revoked.JobID, "events")
	if err := os.MkdirAll(eventsDir, 0700); err != nil {
		return err
	}
	entries, err := os.ReadDir(eventsDir)
	if err != nil {
		return err
	}
	var highest uint64
	for _, entry := range entries {
		if seq, ok := hubEventSequence(entry.Name()); ok && seq > highest {
			highest = seq
		}
	}
	contents, err := json.Marshal(struct {
		Type  string `json:"type"`
		JobID string `json:"job_id"`
		Epoch uint64 `json:"epoch"`
	}{Type: "job.revoked", JobID: revoked.JobID, Epoch: revoked.Epoch})
	if err != nil {
		return err
	}
	path := filepath.Join(eventsDir, fmt.Sprintf("%05d-job.revoked.json", highest+1))
	temporary, err := os.CreateTemp(eventsDir, ".revocation-*")
	if err != nil {
		return err
	}
	name := temporary.Name()
	defer os.Remove(name)
	if err := temporary.Chmod(0600); err != nil {
		_ = temporary.Close()
		return err
	}
	if _, err := temporary.Write(contents); err != nil {
		_ = temporary.Close()
		return err
	}
	if err := temporary.Close(); err != nil {
		return err
	}
	return os.Rename(name, path)
}
