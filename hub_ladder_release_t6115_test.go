package panewire

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/mgh3326/panewire/internal/fleet"
)

// The operator's release of an unresolved ladder stage (hk 6115). The stage is
// staged by the same fixture the reconciler tests use; the release goes through
// POST /v1/ladder/stages/{request_id}/release.

// stageLiveSuccessorWithClose stages the successor of the live author. A non-nil
// close is frozen on the stage (a legacy marker binding, for one); ladder=false
// stages a successor with no ladder step at all. It returns the stage's request id.
func (f *ladderCloseFixture) stageLiveSuccessorWithClose(t *testing.T, close *fleet.LadderCloseBinding, ladder bool) string {
	t.Helper()
	next, err := escalateAuthorSubmission(escalateHolderRequest(f.job), f.request(), f.job+"-l1")
	if err != nil {
		t.Fatal(err)
	}
	setEscalateAuthorHost(&next, "machine-a")
	next.Repository.Checkout.Sha = strings.Repeat("b", 40)
	next.Repository.Checkout.VerifyReuse = true
	if ladder {
		next.LadderStep = &fleet.LadderStepIdentity{Parent: f.job, Round: 5, PR: f.request().PR, Profile: "sonnet", Effort: "xhigh"}
	}
	holder, err := f.hub.inbox.store.latestSpawnForJob(t.Context(), f.job)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := f.hub.inbox.store.claimSpawnReplacing(t.Context(), next, f.job+"-l1", "parent", time.Now().Add(-10*time.Minute), holder.Request.RequestID, "42:implementer", "staged", ""); err != nil {
		t.Fatal(err)
	}
	if close != nil {
		if _, err := f.hub.inbox.store.bindStagedClose(t.Context(), next.RequestID, *close); err != nil {
			t.Fatal(err)
		}
	}
	return next.RequestID
}

// releaseLadderHTTP posts one release for requestID with token. machine, when
// set, names the node that token belongs to.
func releaseLadderHTTP(t *testing.T, h *HubServer, requestID, token, machine, reason string) *httptest.ResponseRecorder {
	t.Helper()
	body, err := json.Marshal(map[string]string{"reason": reason})
	if err != nil {
		t.Fatal(err)
	}
	request := httptest.NewRequest(http.MethodPost, "/v1/ladder/stages/"+requestID+"/release", bytes.NewReader(body))
	request.Header.Set(hubAuthorizationHeader, "Bearer "+token)
	if machine != "" {
		request.Header.Set(hubMachineIDHeader, machine)
	}
	recorder := httptest.NewRecorder()
	h.Handler().ServeHTTP(recorder, request)
	return recorder
}

func releaseLadderBody(t *testing.T, recorder *httptest.ResponseRecorder) hubLadderReleaseBody {
	t.Helper()
	var body hubLadderReleaseBody
	if err := json.Unmarshal(recorder.Body.Bytes(), &body); err != nil {
		t.Fatalf("body %q: %v", recorder.Body.String(), err)
	}
	return body
}

// stagedRowCount reports whether the stage's row and its reservation still exist.
func stagedRowCount(t *testing.T, h *HubServer, requestID string) (rows, reservations int) {
	t.Helper()
	if err := h.inbox.store.db.QueryRow(`SELECT count(*) FROM spawn_operations WHERE request_id=?`, requestID).Scan(&rows); err != nil {
		t.Fatal(err)
	}
	if err := h.inbox.store.db.QueryRow(`SELECT count(*) FROM spawn_reservations WHERE request_id=?`, requestID).Scan(&reservations); err != nil {
		t.Fatal(err)
	}
	return rows, reservations
}

// A legacy marker stage is released through the normal release: the stage's row
// and reservation go, the holder's role comes back, and no node is sent a
// command (a marker stage has no K to fence).
func TestLadderReleaseMarkerStageRestoresTheRole(t *testing.T) {
	f := newLadderCloseFixture(t, "pw-task-6115-marker", true)
	stageID := f.stageLiveSuccessorWithClose(t, &fleet.LadderCloseBinding{Protocol: fleet.LadderCloseProtocolMarker}, true)
	holder, err := f.hub.inbox.store.latestSpawnForJob(t.Context(), f.job)
	if err != nil {
		t.Fatal(err)
	}
	stageRole := reservationTaskRole(t, f.hub, stageID)
	if !stageRole.Valid {
		t.Fatalf("the stage holds no role before the release: %+v", stageRole)
	}
	f.take()
	recorder := releaseLadderHTTP(t, f.hub, stageID, t14OperatorToken, "", "author pane verified gone")
	if recorder.Code != http.StatusOK {
		t.Fatalf("release status=%d body=%s", recorder.Code, recorder.Body.String())
	}
	if ops := commandOps(f.take()); len(ops) != 0 {
		t.Fatalf("a marker release sent node commands: %v", ops)
	}
	if rows, reservations := stagedRowCount(t, f.hub, stageID); rows != 0 || reservations != 0 {
		t.Fatalf("stage rows=%d reservations=%d; want both gone", rows, reservations)
	}
	if got := reservationTaskRole(t, f.hub, holder.Request.RequestID); got != stageRole {
		t.Fatalf("holder role after release=%+v; want the role the stage held, %+v", got, stageRole)
	}
	f.awaitInfo(t, "released the unresolved staged successor", "Reason: author pane verified gone")
}

// A v1 stage whose node answers unknown is fenced first, then released. The
// fence is the one read sent, and no close runs for the K.
func TestLadderReleaseV1UnknownFencesThenReleases(t *testing.T) {
	f := newLadderCloseFixture(t, "pw-task-6115-v1", true)
	f.fenceAnswer = nodeLadderUnknown
	f.stageBoundLiveSuccessor(t)
	stage, err := f.hub.inbox.store.latestSpawnForJob(t.Context(), f.job+"-l1")
	if err != nil {
		t.Fatal(err)
	}
	f.take()
	recorder := releaseLadderHTTP(t, f.hub, stage.Request.RequestID, t14OperatorToken, "", "unknown close outcome; operator checked the pane")
	if recorder.Code != http.StatusOK {
		t.Fatalf("release status=%d body=%s", recorder.Code, recorder.Body.String())
	}
	ops := commandOps(f.take())
	if !slices.Contains(ops, nodeCommandLadderFence) || slices.Contains(ops, nodeCommandLadderClose) || slices.Contains(ops, nodeCommandPaneClose) {
		t.Fatalf("ops=%v; want the fence and no close", ops)
	}
	if rows, reservations := stagedRowCount(t, f.hub, stage.Request.RequestID); rows != 0 || reservations != 0 {
		t.Fatalf("stage rows=%d reservations=%d; want both gone", rows, reservations)
	}
}

// A v1 stage whose verdict is settled is not released: the reconciler owns it.
func TestLadderReleaseRefusesASettledVerdict(t *testing.T) {
	for _, state := range []string{nodeLadderClosed, nodeLadderGone, nodeLadderRefused, nodeLadderFenced} {
		t.Run(state, func(t *testing.T) {
			f := newLadderCloseFixture(t, "pw-task-6115-settled", true)
			f.fenceAnswer = state
			f.stageBoundLiveSuccessor(t)
			stage, err := f.hub.inbox.store.latestSpawnForJob(t.Context(), f.job+"-l1")
			if err != nil {
				t.Fatal(err)
			}
			f.take()
			recorder := releaseLadderHTTP(t, f.hub, stage.Request.RequestID, t14OperatorToken, "", "operator release attempt")
			body := releaseLadderBody(t, recorder)
			if recorder.Code != http.StatusConflict || body.Error != "verdict_resolved" || body.State != state {
				t.Fatalf("status=%d body=%+v; want 409 verdict_resolved state=%s", recorder.Code, body, state)
			}
			if ops := commandOps(f.take()); slices.Contains(ops, nodeCommandLadderClose) || slices.Contains(ops, nodeCommandPaneClose) {
				t.Fatalf("a refused release sent a close: %v", ops)
			}
			if rows, _ := stagedRowCount(t, f.hub, stage.Request.RequestID); rows != 1 {
				t.Fatalf("a refused release removed the stage: rows=%d", rows)
			}
		})
	}
}

// Each other refusal answers its own code and leaves the stage alone.
func TestLadderReleaseRefusals(t *testing.T) {
	t.Run("activated", func(t *testing.T) {
		f := newLadderCloseFixture(t, "pw-task-6115-activated", true)
		f.stageBoundLiveSuccessor(t)
		stage, err := f.hub.inbox.store.latestSpawnForJob(t.Context(), f.job+"-l1")
		if err != nil {
			t.Fatal(err)
		}
		if err := f.hub.inbox.store.activateStagedSpawn(t.Context(), stage.Request.RequestID, time.Now(), 600); err != nil {
			t.Fatal(err)
		}
		recorder := releaseLadderHTTP(t, f.hub, stage.Request.RequestID, t14OperatorToken, "", "too late")
		body := releaseLadderBody(t, recorder)
		if recorder.Code != http.StatusConflict || body.Error != "not_staged" || body.Status != "accepted" {
			t.Fatalf("status=%d body=%+v; want 409 not_staged accepted", recorder.Code, body)
		}
	})
	t.Run("not a ladder stage", func(t *testing.T) {
		f := newLadderCloseFixture(t, "pw-task-6115-plain", true)
		stageID := f.stageLiveSuccessorWithClose(t, nil, false)
		recorder := releaseLadderHTTP(t, f.hub, stageID, t14OperatorToken, "", "not a ladder step")
		if body := releaseLadderBody(t, recorder); recorder.Code != http.StatusConflict || body.Error != "not_ladder_stage" {
			t.Fatalf("status=%d body=%+v; want 409 not_ladder_stage", recorder.Code, body)
		}
		if rows, _ := stagedRowCount(t, f.hub, stageID); rows != 1 {
			t.Fatalf("the refused stage was removed: rows=%d", rows)
		}
	})
	t.Run("unknown request", func(t *testing.T) {
		f := newLadderCloseFixture(t, "pw-task-6115-unknown", true)
		id, err := fleet.NewRequestID()
		if err != nil {
			t.Fatal(err)
		}
		recorder := releaseLadderHTTP(t, f.hub, id, t14OperatorToken, "", "no such stage")
		if body := releaseLadderBody(t, recorder); recorder.Code != http.StatusNotFound || body.Error != "not_found" {
			t.Fatalf("status=%d body=%+v; want 404 not_found", recorder.Code, body)
		}
	})
	t.Run("empty reason", func(t *testing.T) {
		f := newLadderCloseFixture(t, "pw-task-6115-reason", true)
		stageID := f.stageLiveSuccessorWithClose(t, &fleet.LadderCloseBinding{Protocol: fleet.LadderCloseProtocolMarker}, true)
		if recorder := releaseLadderHTTP(t, f.hub, stageID, t14OperatorToken, "", ""); recorder.Code != http.StatusBadRequest {
			t.Fatalf("status=%d; want 400 for an empty reason", recorder.Code)
		}
		if rows, _ := stagedRowCount(t, f.hub, stageID); rows != 1 {
			t.Fatalf("an invalid release removed the stage: rows=%d", rows)
		}
	})
}

// Only the operator may release a stage. A node's own token is refused and the
// stage stays staged.
func TestLadderReleaseRequiresTheOperatorToken(t *testing.T) {
	f := newLadderCloseFixture(t, "pw-task-6115-auth", true)
	stageID := f.stageLiveSuccessorWithClose(t, &fleet.LadderCloseBinding{Protocol: fleet.LadderCloseProtocolMarker}, true)
	recorder := releaseLadderHTTP(t, f.hub, stageID, "node-token", "machine-a", "a node asks for a release")
	if recorder.Code != http.StatusUnauthorized {
		t.Fatalf("a node's release status=%d; want 401", recorder.Code)
	}
	if rows, _ := stagedRowCount(t, f.hub, stageID); rows != 1 {
		t.Fatalf("a refused release removed the stage: rows=%d", rows)
	}
}

// A repeat of a release that landed answers the first release's result; the
// stage is gone, so no second release runs.
func TestLadderReleaseReplayAnswersTheSameResult(t *testing.T) {
	f := newLadderCloseFixture(t, "pw-task-6115-replay", true)
	stageID := f.stageLiveSuccessorWithClose(t, &fleet.LadderCloseBinding{Protocol: fleet.LadderCloseProtocolMarker}, true)
	first := releaseLadderHTTP(t, f.hub, stageID, t14OperatorToken, "", "first release")
	if first.Code != http.StatusOK {
		t.Fatalf("first release status=%d body=%s", first.Code, first.Body.String())
	}
	want := releaseLadderBody(t, first)
	for _, reason := range []string{"first release", "a different reason"} {
		again := releaseLadderHTTP(t, f.hub, stageID, t14OperatorToken, "", reason)
		if got := releaseLadderBody(t, again); again.Code != http.StatusOK || got != want {
			t.Fatalf("replay (%q) status=%d body=%+v; want 200 %+v", reason, again.Code, got, want)
		}
	}
	var count int
	if err := f.hub.inbox.store.db.QueryRow(`SELECT count(*) FROM spawn_notifications WHERE event_id=?`, ladderReleaseEventPrefix+stageID).Scan(&count); err != nil || count != 1 {
		t.Fatalf("release records=%d err=%v; want exactly one", count, err)
	}
}

// The CLI's usage and its refusals for a bad invocation, before any hub call.
func TestHubLadderCLIInvocation(t *testing.T) {
	var stdout, stderr bytes.Buffer
	if code := runHubLadderCLI(nil, &stdout, &stderr, hubCLIDeps{}); code != ExitUsage || !strings.Contains(stderr.String(), "panewire hub ladder release STAGE_REQUEST_ID") {
		t.Fatalf("no subcommand: code=%d stderr=%q", code, stderr.String())
	}
	stderr.Reset()
	if code := runHubLadderCLI([]string{"release"}, &stdout, &stderr, hubCLIDeps{}); code != ExitUsage || !strings.Contains(stderr.String(), "a valid REQUEST_ID is required") {
		t.Fatalf("no request id: code=%d stderr=%q", code, stderr.String())
	}
	stderr.Reset()
	id, err := fleet.NewRequestID()
	if err != nil {
		t.Fatal(err)
	}
	missing := filepath.Join(t.TempDir(), "absent.env")
	args := []string{"release", id, "--reason", "operator check", "--hub-url", "https://hub.example.test", "--hub-token-env", missing}
	if code := runHubLadderCLI(args, &stdout, &stderr, hubCLIDeps{}); code != ExitConditionInvalid || !strings.Contains(stderr.String(), "hub ladder release rejected: invalid operator token env") {
		t.Fatalf("missing operator env: code=%d stderr=%q", code, stderr.String())
	}
}
