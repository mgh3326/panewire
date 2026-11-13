package panewire

import (
	"net/http"
	"testing"

	"github.com/mgh3326/panewire/internal/fleet"
)

// consultRequest is a counsel spawn bound to task 42 under its own request id
// and job. A consult pair is two such seats on one task.
func consultRequest(n int, job string) fleet.Submission {
	request := durableSpawnRequest()
	request.RequestID = "d676220b-e763-4af1-aefd-00000000020" + string(rune('0'+n))
	request.Args = []string{"-c", "/work/project", "-p", "/work/brief.md", "-m", "auto", "-l", job, "--job", job, "--t", "T1", "--task", "42", "--owner", "parent", "--role", "counsel", "--host", "auto"}
	return request
}

// A consult pair shares one task: two counsel seats bound to task 42 are both
// accepted and neither takes a "42:counsel" reservation, because a consult
// works beside the task's claimant and never holds its one-holder slot (hk 4568).
func TestConsultPairSharesTaskReservation(t *testing.T) {
	hub, server, _ := reviewPassHub(t, nil)
	fable := reviewPassSubmit(t, server, consultRequest(1, "fable-job"))
	astra := reviewPassSubmit(t, server, consultRequest(2, "astra-job"))
	if fable.status != http.StatusAccepted || astra.status != http.StatusAccepted {
		t.Fatalf("fable=%d %s astra=%d %s; want both accepted", fable.status, fable.body, astra.status, astra.body)
	}
	if owner := reviewPassOwner(t, hub, "42:counsel"); owner != "" {
		t.Fatalf("consult seat holds 42:counsel reservation %s; want none", owner)
	}
}

// The one-holder rule stays for a worker: a second worker on task 42 is still
// refused while the first holds "42:worker", and a consult can join beside it.
func TestConsultJoinsWorkerTaskWithoutDisturbingOneHolder(t *testing.T) {
	hub, server, _ := reviewPassHub(t, nil)
	if result := reviewPassSubmit(t, server, durableSpawnRequest()); result.status != http.StatusAccepted {
		t.Fatalf("worker status=%d body=%s", result.status, result.body)
	}
	if result := reviewPassSubmit(t, server, consultRequest(1, "fable-job")); result.status != http.StatusAccepted {
		t.Fatalf("consult beside worker status=%d body=%s", result.status, result.body)
	}
	second := durableSpawnRequest()
	second.RequestID = "d676220b-e763-4af1-aefd-000000000299"
	second.Args = []string{"-c", "/work/project", "-p", "/work/brief.md", "-m", "auto", "-l", "worker-2", "--job", "worker-2", "--t", "T1", "--task", "42", "--owner", "parent", "--role", "worker", "--host", "auto"}
	if result := reviewPassSubmit(t, server, second); result.status != http.StatusConflict {
		t.Fatalf("second worker status=%d body=%s; want 409", result.status, result.body)
	}
	if _, job, _, err := hub.inbox.store.spawnRoleOwner(t.Context(), "42:worker"); err != nil || job != "test-job" {
		t.Fatalf("worker key held by %q (%v), want test-job", job, err)
	}
}
