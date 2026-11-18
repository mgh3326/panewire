package panewire

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestPoolHandoverMissingJobNotifiesWhileKeepingFence(t *testing.T) {
	for _, reason := range []string{"close attempt unavailable", "release not proved"} {
		t.Run(reason, func(t *testing.T) {
			const job = "missing-old-job"
			f := handoverFixture(t, job, 1, 0)
			sweepHandover(t, f, window5h(), handoverReadReply(job))
			action := readOnlyPoolAction(t, f.hub)
			if reason == "close attempt unavailable" {
				checkout := action.Data.Handover.Successor.Repository.Checkout
				action.Data.Args = &nodePaneCloseOpArgs{RequestID: action.ID, Job: job, Epoch: 1, Attempt: "00002-job.spawned.json", PaneID: roleHeldPane, Name: job, AgentStatus: "idle", Cwd: "/work/project.checkout", EndReason: action.Data.EndReason, Handover: &nodeHandoverCloseGuard{Origin: checkout.Origin, Branch: checkout.Branch, Commit: checkout.Sha}}
				if err := f.hub.inbox.store.savePoolAction(t.Context(), action); err != nil {
					t.Fatal(err)
				}
			}
			inbox := t.TempDir()
			if err := os.MkdirAll(filepath.Join(inbox, "jobs"), 0700); err != nil {
				t.Fatal(err)
			}
			node := &HubClient{jobsInboxRoot: inbox}
			ref := nodeCloseOpRefArgs{RequestID: action.ID, Job: job}
			status, code := node.runCloseOpRef(ref, false)
			if code != "" {
				t.Fatalf("status code=%q", code)
			}
			release, code := node.runCloseOpRef(ref, true)
			if code != "" {
				t.Fatalf("release code=%q", code)
			}
			asMap := func(value any) map[string]any {
				data, err := json.Marshal(value)
				if err != nil {
					t.Fatal(err)
				}
				var result map[string]any
				if err := json.Unmarshal(data, &result); err != nil {
					t.Fatal(err)
				}
				return result
			}
			start := time.Date(2026, 11, 19, 0, 0, 0, 0, time.UTC)
			now := start
			f.hub.now = func() time.Time { return now }
			step := func() {
				t.Helper()
				row := readOnlyPoolAction(t, f.hub)
				replies := []poolActionReply{{op: nodeCommandPaneCloseStatus, result: asMap(status)}}
				if reason == "release not proved" {
					replies = append(replies, poolActionReply{op: nodeCommandPaneCloseRelease, result: asMap(release)})
				}
				if err := runPoolActionStep(t, f.hub, f.node, row, replies...); err == nil || !strings.Contains(err.Error(), reason) {
					t.Fatalf("reconcile error=%v, want %q", err, reason)
				}
				row = readOnlyPoolAction(t, f.hub)
				owner, err := f.hub.inbox.store.poolActionOwns(t.Context(), job)
				if err != nil || row.Phase != poolActionPending || owner != action.ID {
					t.Fatalf("phase=%s owner=%q err=%v", row.Phase, owner, err)
				}
			}
			step()
			restartPoolActionHub(t, f)
			f.hub.now = func() time.Time { return now }
			now = start.Add(30*time.Minute - time.Second)
			step()
			if lines := ownerLines(t, f.hub, "pool-action-stuck:"); len(lines) != 0 {
				t.Fatalf("early notice=%q", lines)
			}
			now = start.Add(30 * time.Minute)
			step()
			lines := ownerLines(t, f.hub, "pool-action-stuck:")
			if len(lines) != 1 {
				t.Fatalf("notices after 30 minutes=%q, want one", lines)
			}
			for _, want := range []string{job, "machine-a", reason, action.ID, "holds", "check", "hand"} {
				if !strings.Contains(lines[0], want) {
					t.Fatalf("notice omits %q: %q", want, lines[0])
				}
			}
			restartPoolActionHub(t, f)
			f.hub.now = func() time.Time { return now }
			now = start.Add(31 * time.Minute)
			step()
			if lines := ownerLines(t, f.hub, "pool-action-stuck:"); len(lines) != 1 {
				t.Fatalf("replayed notices=%q, want one", lines)
			}
			items, err := f.hub.inbox.store.list("parent", false)
			if err != nil {
				t.Fatal(err)
			}
			found := false
			for _, item := range items {
				if item.Event.EventID == "pool-action-stuck:"+action.ID {
					if item.Event.NotifyType != "info" {
						t.Fatalf("notice type=%q, want info", item.Event.NotifyType)
					}
					found = true
				}
			}
			if !found {
				t.Fatalf("no fixed event ID for row %s", action.ID)
			}
		})
	}
}

func TestPoolActionMissingJobNotifiesWhileKeepingFence(t *testing.T) {
	f, action := poolActionFixture(t)
	start := time.Date(2026, 11, 19, 0, 0, 0, 0, time.UTC)
	now := start
	f.hub.now = func() time.Time { return now }
	step := func() {
		t.Helper()
		row := readOnlyPoolAction(t, f.hub)
		err := runPoolActionStep(t, f.hub, f.node, row,
			poolActionReply{op: nodeCommandPaneCloseStatus, result: map[string]any{"state": nodeCloseOpNone, "attempt": ""}},
			poolActionReply{op: nodeCommandPaneCloseOp, result: map[string]any{"outcome": nodeCaptureNotAJob}},
			poolActionReply{op: nodeCommandPaneCloseRelease, result: map[string]any{"state": nodeCloseOpNone, "released": false}},
		)
		if err == nil || !strings.Contains(err.Error(), "release not proved") {
			t.Fatalf("reconcile error=%v", err)
		}
		assertPoolActionFence(t, f, action, false)
	}
	step()
	now = start.Add(30 * time.Minute)
	step()
	if lines := ownerLines(t, f.hub, "pool-action-stuck:"); len(lines) != 1 || !strings.Contains(lines[0], action.ID) {
		t.Fatalf("shared close notice=%q", lines)
	}
	step()
	if lines := ownerLines(t, f.hub, "pool-action-stuck:"); len(lines) != 1 {
		t.Fatalf("replayed shared close notice=%q", lines)
	}
}

func TestPoolHandoverNoticeWaitsForContinuousFailure(t *testing.T) {
	const job = "resumed-old-job"
	f := handoverFixture(t, job, 1, 0)
	sweepHandover(t, f, window5h(), handoverReadReply(job))
	action := readOnlyPoolAction(t, f.hub)
	checkout := action.Data.Handover.Successor.Repository.Checkout
	action.Data.Args = &nodePaneCloseOpArgs{RequestID: action.ID, Job: job, Epoch: 1, Attempt: "00002-job.spawned.json", PaneID: roleHeldPane, Name: job, AgentStatus: "idle", Cwd: "/work/project.checkout", EndReason: action.Data.EndReason, Handover: &nodeHandoverCloseGuard{Origin: checkout.Origin, Branch: checkout.Branch, Commit: checkout.Sha}}
	if err := f.hub.inbox.store.savePoolAction(t.Context(), action); err != nil {
		t.Fatal(err)
	}
	start := time.Date(2026, 11, 19, 0, 0, 0, 0, time.UTC)
	now := start
	f.hub.now = func() time.Time { return now }
	missing := poolActionReply{op: nodeCommandPaneCloseStatus, result: map[string]any{"state": nodeCloseOpNone, "attempt": ""}}
	stepMissing := func() {
		t.Helper()
		err := runPoolActionStep(t, f.hub, f.node, readOnlyPoolAction(t, f.hub), missing)
		if err == nil || !strings.Contains(err.Error(), "close attempt unavailable") {
			t.Fatalf("reconcile error=%v", err)
		}
	}
	stepMissing()
	now = start.Add(20 * time.Minute)
	if err := runPoolActionStep(t, f.hub, f.node, readOnlyPoolAction(t, f.hub), poolStatus("closed", false, false)); err != nil {
		t.Fatal(err)
	}
	now = start.Add(31 * time.Minute)
	stepMissing()
	if lines := ownerLines(t, f.hub, "pool-action-stuck:"); len(lines) != 0 {
		t.Fatalf("notice counted an earlier failure period: %q", lines)
	}
	now = start.Add(61 * time.Minute)
	stepMissing()
	if lines := ownerLines(t, f.hub, "pool-action-stuck:"); len(lines) != 1 {
		t.Fatalf("notice after continuous failure=%q", lines)
	}
}

func TestPoolActionRetryStartsANewNoticeInterval(t *testing.T) {
	f, action := poolActionFixture(t)
	start := time.Date(2026, 11, 19, 0, 0, 0, 0, time.UTC)
	now := start
	f.hub.now = func() time.Time { return now }
	missingStatus := poolActionReply{op: nodeCommandPaneCloseStatus, result: map[string]any{"state": nodeCloseOpNone, "attempt": ""}}
	missingRelease := poolActionReply{op: nodeCommandPaneCloseRelease, result: map[string]any{"state": nodeCloseOpNone, "released": false}}
	if err := runPoolActionStep(t, f.hub, f.node, action, missingStatus,
		poolActionReply{op: nodeCommandPaneCloseOp, result: map[string]any{"outcome": nodeCaptureNotAJob}}, missingRelease); err == nil {
		t.Fatal("missing release was accepted")
	}
	now = start.Add(31 * time.Minute)
	action = readOnlyPoolAction(t, f.hub)
	runUnknownRetry(t, f, action, action.Data.Args.Attempt)
	rows, err := f.hub.inbox.store.activePoolActions(t.Context())
	if err != nil || len(rows) != 2 {
		t.Fatalf("retry rows=%+v err=%v", rows, err)
	}
	var retry poolAction
	for _, row := range rows {
		if row.ID != action.ID {
			retry = row
		}
	}
	restartPoolActionHub(t, f)
	f.hub.now = func() time.Time { return now }
	stepRetry := func() {
		t.Helper()
		rows, err := f.hub.inbox.store.activePoolActions(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		for _, row := range rows {
			if row.ID == retry.ID {
				if err := runPoolActionStep(t, f.hub, f.node, row, missingStatus, missingRelease); err == nil || !strings.Contains(err.Error(), "release not proved") {
					t.Fatalf("retry error=%v", err)
				}
				return
			}
		}
		t.Fatal("retry row disappeared")
	}
	stepRetry()
	if lines := ownerLines(t, f.hub, "pool-action-stuck:"); len(lines) != 0 {
		t.Fatalf("new retry inherited an earlier notice interval: %q", lines)
	}
	now = now.Add(30 * time.Minute)
	stepRetry()
	if lines := ownerLines(t, f.hub, "pool-action-stuck:"); len(lines) != 1 || !strings.Contains(lines[0], retry.ID) {
		t.Fatalf("retry notice=%q", lines)
	}
}
