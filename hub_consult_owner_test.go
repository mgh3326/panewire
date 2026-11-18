package panewire

import (
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
)

// claimAuthorJob records an author job owned by owner, the way a hub spawn of
// that job would, so the consult owner rule can read it back.
func (f *consultFixture) claimAuthorJob(t *testing.T, job, owner string) {
	t.Helper()
	request := consultAuthorRequest()
	request.RequestID = uuid.NewString()
	// The fixture's author job holds task 42's implementer role; this job
	// takes task 43 so its own claim does not collide with that reservation.
	for i, arg := range request.Args {
		if arg == "--task" {
			request.Args[i+1] = "43"
		}
	}
	if _, _, err := f.hub.inbox.store.claimSpawn(t.Context(), request, job, owner, time.Now()); err != nil {
		t.Fatal(err)
	}
}

// consultOwnerOf posts one consult and returns its stored row and the owner
// of the spawn operation the row started.
func consultOwnerOf(t *testing.T, f *consultFixture, body map[string]string) (rowOwner, spawnOwner string) {
	t.Helper()
	code, out := f.post(t, "", t14OperatorToken, body)
	if code != http.StatusCreated || out["state"] != consultQueued {
		t.Fatalf("create = %d %v, want 201 queued", code, out)
	}
	row, err := f.hub.inbox.store.consultByID(t.Context(), fmt.Sprint(out["id"]))
	if err != nil {
		t.Fatal(err)
	}
	task, err := f.hub.inbox.store.spawnTask(t.Context(), row.RequestID)
	if err != nil {
		t.Fatal(err)
	}
	return row.Owner, task.Owner
}

// An author job whose owner lane is registered owns the consult, whoever asks.
func TestConsultOwnedByAuthorJobOwnerLane(t *testing.T) {
	f := newConsultFixture(t)
	f.claimAuthorJob(t, "design-author", "panewire-design")
	body := consultRequestBody("42", "example/project#7", strings.Repeat("a", 40))
	body["author_job"] = "design-author"
	body["requester"] = "mergewake/example/project#7"

	rowOwner, spawnOwner := consultOwnerOf(t, f, body)
	if rowOwner != "panewire-design" || spawnOwner != "panewire-design" {
		t.Fatalf("owner = row %q spawn %q, want panewire-design for both", rowOwner, spawnOwner)
	}
}

// A respawn keeps the stored owner: the second attempt's spawn follows the
// same lane as the first, so its notices do not move to the desk.
func TestConsultRespawnKeepsStoredOwner(t *testing.T) {
	f := newConsultFixture(t)
	f.claimAuthorJob(t, "design-author", "panewire-design")
	body := consultRequestBody("42", "example/project#7", strings.Repeat("a", 40))
	body["author_job"] = "design-author"
	body["requester"] = "mergewake/example/project#7"
	_, out := f.post(t, "", t14OperatorToken, body)
	first, err := f.hub.inbox.store.consultByID(t.Context(), fmt.Sprint(out["id"]))
	if err != nil {
		t.Fatal(err)
	}
	f.setSpawnStatus(t, first.RequestID, "failed")
	f.hub.reconcileConsults(t.Context())

	second, err := f.hub.inbox.store.consultByID(t.Context(), first.ID)
	if err != nil || second.Respawns != 1 || second.RequestID == first.RequestID {
		t.Fatalf("respawn = %+v err=%v, want a second attempt", second, err)
	}
	task, err := f.hub.inbox.store.spawnTask(t.Context(), second.RequestID)
	if err != nil {
		t.Fatal(err)
	}
	if second.Owner != "panewire-design" || task.Owner != "panewire-design" {
		t.Fatalf("respawn owner = row %q spawn %q, want panewire-design", second.Owner, task.Owner)
	}
}

// A lane requester with no author job owns its consult.
func TestConsultLaneRequesterOwnsItself(t *testing.T) {
	f := newConsultFixture(t)
	body := consultRequestBody("42", "example/project#7", strings.Repeat("a", 40))
	delete(body, "author_job")

	rowOwner, spawnOwner := consultOwnerOf(t, f, body)
	if rowOwner != "parent" || spawnOwner != "parent" {
		t.Fatalf("owner = row %q spawn %q, want the requesting lane parent", rowOwner, spawnOwner)
	}
}

// Fail-safe: an author job the hub does not know falls back to the next rule
// and still spawns. Neither the owner nor the repository evidence may drop it.
func TestConsultUnknownAuthorJobFallsBackAndSpawns(t *testing.T) {
	cases := []struct {
		name      string
		requester string
		want      string
	}{
		{"tool requester owns the desk", "mergewake/example/project#7", consultToolLane},
		{"lane requester owns itself", "parent", "parent"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := newConsultFixture(t)
			body := consultRequestBody("42", "example/project#7", strings.Repeat("a", 40))
			body["author_job"] = "ghost-job"
			body["requester"] = tc.requester

			rowOwner, spawnOwner := consultOwnerOf(t, f, body)
			if rowOwner != tc.want || spawnOwner != tc.want {
				t.Fatalf("owner = row %q spawn %q, want %q", rowOwner, spawnOwner, tc.want)
			}
		})
	}
}

// Fail-safe: an author job whose owner lane has no route on this hub is not
// trusted with the consult; the requester rule applies.
func TestConsultUnregisteredAuthorOwnerFallsBack(t *testing.T) {
	f := newConsultFixture(t)
	f.claimAuthorJob(t, "retired-author", "retired-lane")
	body := consultRequestBody("42", "example/project#7", strings.Repeat("a", 40))
	body["author_job"] = "retired-author"
	body["requester"] = "mergewake/example/project#7"

	rowOwner, spawnOwner := consultOwnerOf(t, f, body)
	if rowOwner != consultToolLane || spawnOwner != consultToolLane {
		t.Fatalf("owner = row %q spawn %q, want %q", rowOwner, spawnOwner, consultToolLane)
	}
}

// A row from before the owner column reads the requester rule, so an old
// consult keeps the owner it was given.
func TestConsultRowWithoutStoredOwnerReadsRequesterRule(t *testing.T) {
	lane := hubConsult{Requester: "parent"}
	if got := lane.ownerLane(); got != "parent" {
		t.Fatalf("lane owner = %q, want parent", got)
	}
	tool := hubConsult{Requester: "mergewake/example/project#7"}
	if got := tool.ownerLane(); got != consultToolLane {
		t.Fatalf("tool owner = %q, want %q", got, consultToolLane)
	}
	stored := hubConsult{Requester: "parent", Owner: "panewire-design"}
	if got := stored.ownerLane(); got != "panewire-design" {
		t.Fatalf("stored owner = %q, want panewire-design", got)
	}
}
