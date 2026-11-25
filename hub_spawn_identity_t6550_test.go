package panewire

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestLaneSpawnProofReadsStoredArgvWithRetiredFlags_t6550(t *testing.T) {
	hub, _ := takeoverHub(t, `{"lanes":{}}`, nil)
	request := durableSpawnRequest()
	request.RequestID = uuid.NewString()
	request.Args = []string{"-c", "/work/project", "-p", "/work/brief.md", "-m", "auto", "-l", "stored-job", "--job", "stored-job", "--t", "T1", "--owner", "mergewake", "--role", "implementer", "--host", "auto", "--parent-job", "chain-root"}
	task, _, err := hub.inbox.store.claimSpawn(t.Context(), request, "stored-job", "mergewake", time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := hub.inbox.store.bindSpawn(t.Context(), request.RequestID, task.Attempt, hubSpawnRequest{
		RequestID: uuid.NewString(), Machine: "machine-a", Args: request.Args,
	}, time.Now()); err != nil {
		t.Fatal(err)
	}
	task, err = hub.inbox.store.latestSpawnForJob(t.Context(), "stored-job")
	if err != nil {
		t.Fatal(err)
	}
	task.Request.Args = append([]string{"--builder-family", "anthropic", "--routing-role", "consult"}, task.Request.Args...)
	payload, err := json.Marshal(task.Request)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := hub.inbox.store.db.ExecContext(t.Context(), `UPDATE spawn_operations SET payload=? WHERE request_id=?`, string(payload), request.RequestID); err != nil {
		t.Fatal(err)
	}

	proof := hub.laneSpawnProof("machine-a", hubLaneWriteRequest{Job: "stored-job", Pane: "w1:p1"})
	if !proof.valid || proof.task.Job != "stored-job" {
		t.Fatalf("lane spawn proof = %+v, want valid proof for stored-job", proof)
	}
}

func TestHubSpawnOperationsDocumentsSpawnFlagGrammar_t6550(t *testing.T) {
	doc, err := os.ReadFile(filepath.Join("docs", "hub-spawn-operations.md"))
	if err != nil {
		t.Fatal(err)
	}
	section := strings.SplitN(string(doc), "### Spawn identity", 2)
	if len(section) != 2 {
		t.Fatal("Spawn identity section is missing")
	}
	section = strings.SplitN(section[1], "### ", 2)
	const grammar = "a flag's value is never read as a flag; a repeated flag keeps its last value."
	paragraph := strings.Join(strings.Fields(section[0]), " ")
	if !strings.Contains(paragraph, grammar) {
		t.Fatalf("Spawn identity section does not document flag grammar %q", grammar)
	}
}
