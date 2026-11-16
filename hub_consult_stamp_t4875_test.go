package panewire

import (
	"errors"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/mgh3326/panewire/internal/fleet"
)

// consultStampWire is a planned consult's stamp, as --consult-stamp carries it.
const consultStampWire = "eyJhc2tlcl9mYW1pbHkiOiJhbnRocm9waWMiLCJhc2tlcl9ncmFkZSI6IkEiLCJmYWxsYmFjayI6IiIsImZsb29yIjoiQSsiLCJncmFkZV9zb3VyY2UiOiJmbGFnIiwidXNlZF9mbG9vciI6IkErIn0"

// A node's spawn argument rule decides whether the planned consult's stamp may
// travel: a node that predates the flag refuses the unknown argument and would
// drop the whole spawn, so the hub binds it the spawn without the stamp
// (hk 4875).
func TestHubBindsConsultStampOnlyForCapableNode_t4875(t *testing.T) {
	for _, tc := range []struct {
		name         string
		capabilities []string
		want         bool
	}{
		{"node without the capability", []string{"spawn_v2", "spawn_ops_v1"}, false},
		{"node advertising consult_stamp_v1", []string{"spawn_v2", "spawn_ops_v1", hubCapabilityConsultStamp}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := asyncSpawnHub(t)
			server := httptest.NewServer(h.Handler())
			defer server.Close()
			startProofNode(t, h, server, "machine-a", tc.capabilities)
			request := reviewerReleaseRequest("consult", false)
			for i := range request.Args {
				if request.Args[i] == "auto" {
					request.Args[i] = "codex-sol"
				}
			}
			request.Args = append(request.Args, "--override-reason", "consult seat codex-sol", "--containment", "hk:42", fleet.ConsultStampArg, consultStampWire)
			task, _, err := h.inbox.store.claimSpawn(t.Context(), request, "test-job", "parent", time.Now())
			if err != nil {
				t.Fatal(err)
			}
			transport := &directSpawnTransport{h: h, task: task}
			if _, err := transport.Spawn(boundedContext(t), fleet.HubSpawnRequest{
				RequestID: t14RequestID(908), Machine: "machine-a", CWDKey: "project",
				Brief: fleet.HubBrief{Doc: "briefs/test-job.md"}, Checkout: request.Repository.Checkout,
				Args: request.Args, WaitSeconds: 60,
			}); !errors.Is(err, fleet.ErrHubPlacementBound) {
				t.Fatalf("bind: %v", err)
			}
			saved, err := h.inbox.store.spawnTask(t.Context(), request.RequestID)
			if err != nil || saved.Node == nil {
				t.Fatalf("bound request: node=%v err=%v", saved.Node, err)
			}
			if at := indexArg(saved.Node.Args, fleet.ConsultStampArg); (at >= 0) != tc.want {
				t.Fatalf("bound args carry the stamp: %v (want %v)", saved.Node.Args, tc.want)
			}
		})
	}
}

func indexArg(args []string, flag string) int {
	for i, arg := range args {
		if arg == flag {
			return i
		}
	}
	return -1
}
