package panewire

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/mgh3326/panewire/internal/durable"
	"github.com/mgh3326/panewire/internal/fleet"
)

// A transfer is recorded before the hk CAS. If the hub stops after the CAS,
// the successor's next heartbeat can recover its grant without the old node.
type hubOwnerTransfer struct {
	Job   string `json:"job"`
	From  string `json:"from"`
	To    string `json:"to"`
	Epoch uint64 `json:"epoch"`
}

func defaultHubOwnerTransferDir() string {
	if state := panewireStateDir(); state != "" {
		return filepath.Join(state, "job-owner-transfers")
	}
	return ""
}

func (t hubOwnerTransfer) valid() bool {
	return hubJobIDPattern.MatchString(t.Job) && machineIDPattern.MatchString(t.From) && machineIDPattern.MatchString(t.To) && t.From != t.To && t.Epoch > 0 && t.Epoch+1 > t.Epoch
}

func (h *HubServer) ownerTransferPath(t hubOwnerTransfer) (string, error) {
	if h.ownerTransferDir == "" || !t.valid() {
		return "", errors.New("job transfer journal unavailable")
	}
	return filepath.Join(h.ownerTransferDir, t.To, fmt.Sprintf("%s--%d.json", t.Job, t.Epoch)), nil
}

func readHubOwnerTransfer(path string) (hubOwnerTransfer, error) {
	var stored hubOwnerTransfer
	file, err := os.Open(path)
	if err != nil {
		return stored, err
	}
	defer file.Close()
	body, err := io.ReadAll(io.LimitReader(file, 4097))
	if err != nil {
		return stored, err
	}
	if len(body) > 4096 || json.Unmarshal(body, &stored) != nil || !stored.valid() {
		return hubOwnerTransfer{}, errors.New("invalid job transfer journal")
	}
	return stored, nil
}

func (h *HubServer) hasOwnerTransfer(t hubOwnerTransfer) (bool, error) {
	path, err := h.ownerTransferPath(t)
	if err != nil {
		return false, err
	}
	stored, err := readHubOwnerTransfer(path)
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if stored != t {
		return false, errors.New("invalid job transfer journal")
	}
	return true, nil
}

func (h *HubServer) saveOwnerTransfer(t hubOwnerTransfer) error {
	path, err := h.ownerTransferPath(t)
	if err != nil {
		return err
	}
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0700); err != nil {
		return err
	}
	durable.SyncDirectoryBestEffort(filepath.Dir(h.ownerTransferDir))
	durable.SyncDirectoryBestEffort(h.ownerTransferDir)
	body, err := json.Marshal(t)
	if err != nil {
		return err
	}
	return durable.ReplaceFileAtomically(path, ".transfer-*", body, 0600)
}

func (h *HubServer) removeOwnerTransfer(t hubOwnerTransfer) error {
	path, err := h.ownerTransferPath(t)
	if err != nil {
		return err
	}
	if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	durable.SyncDirectoryBestEffort(filepath.Dir(path))
	return nil
}

// recoverOwnerTransfers runs before heartbeat directives. It trusts hk, not
// the journal, to say whether the intended CAS actually committed.
func (h *HubServer) recoverOwnerTransfers(machine string) {
	if h.ownerTransferDir == "" || !machineIDPattern.MatchString(machine) {
		return
	}
	dir := filepath.Join(h.ownerTransferDir, machine)
	entries, err := os.ReadDir(dir)
	if errors.Is(err, os.ErrNotExist) {
		return
	}
	if err != nil {
		h.logger.Warn("job transfer journal unavailable")
		return
	}
	ctx, cancel := context.WithTimeout(h.closeCtx, nodeLoopHKTimeout)
	defer cancel()
	for _, entry := range entries {
		if ctx.Err() != nil {
			return
		}
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".json") {
			continue
		}
		intent, err := readHubOwnerTransfer(filepath.Join(dir, entry.Name()))
		if err != nil || intent.To != machine {
			continue
		}
		path, _ := h.ownerTransferPath(intent)
		if filepath.Base(path) != entry.Name() {
			continue
		}
		unlock, err := h.poolActionLocks.lock(ctx, intent.Job)
		if err != nil {
			return
		}
		h.recoverOwnerTransfer(ctx, intent)
		unlock()
	}
}

func (h *HubServer) recoverOwnerTransfer(ctx context.Context, intent hubOwnerTransfer) {
	next := fleet.JobOwner{Node: intent.To, Epoch: intent.Epoch + 1}
	h.mu.Lock()
	current := h.jobs[intent.Job]
	if current != nil && current.Node == next.Node && current.Epoch == next.Epoch {
		// A heartbeat at the granted epoch proves the node persisted the grant.
		node := h.nodes[next.Node]
		active, present := HubActiveJob{}, false
		if node != nil {
			active, present = node.activeJobs[intent.Job]
		}
		h.mu.Unlock()
		if present && active.Epoch == next.Epoch {
			_ = h.removeOwnerTransfer(intent)
		}
		return
	}
	h.mu.Unlock()
	owner, supported, exists, err := h.readJobOwner(ctx, intent.Job)
	if err != nil || !exists || !supported {
		return
	}
	if owner != next {
		// Epochs only increase. Once hk reached this epoch elsewhere, the
		// old conditional transfer can no longer commit.
		if owner.Epoch >= next.Epoch {
			_ = h.removeOwnerTransfer(intent)
		}
		return
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	current = h.jobs[intent.Job]
	if current == nil {
		current = &hubJobRecord{HubActiveJob: HubActiveJob{JobID: intent.Job, Epoch: next.Epoch}, Node: next.Node, LastSeen: h.now(), FencedNodes: make(map[string]uint64)}
		h.jobs[intent.Job] = current
	} else if current.Node == intent.From && current.Epoch == intent.Epoch && !current.Reassigning {
		current.Node, current.Epoch, current.Orphaned = next.Node, next.Epoch, false
	} else {
		return
	}
	if current.FencedNodes == nil {
		current.FencedNodes = make(map[string]uint64)
	}
	current.FencedNodes[intent.From] = next.Epoch
	h.enqueueRevocationLocked(intent.From, hubJobRevokedEvent{Type: "job.revoked", JobID: intent.Job, Epoch: next.Epoch})
}
