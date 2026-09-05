package panewire

import (
	"context"
	"encoding/json"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"time"

	"github.com/fsnotify/fsnotify"
)

// InboxWatcher observes the inbox tree in one of two modes. fsnotify keeps one
// kqueue watch per directory, which macOS cannot recurse; that is why
// inboxWatchMode defaults darwin to the descriptor-free poll mode instead.
type InboxWatcher struct {
	watcher  *fsnotify.Watcher
	root     string
	store    *Store
	mode     string
	poll     time.Duration
	baseline map[string]inboxFileState
}

func NewInboxWatcher(root string, store *Store) (*InboxWatcher, error) {
	if root == "" {
		return nil, fmt.Errorf("empty inbox root")
	}
	if err := os.MkdirAll(root, 0700); err != nil {
		return nil, err
	}
	iw := &InboxWatcher{root: root, store: store, mode: inboxWatchModeFromEnv(), poll: inboxPollInterval()}
	if iw.mode == inboxWatchPoll {
		iw.takeBaseline()
		return iw, nil
	}
	w, err := fsnotify.NewWatcher()
	if err != nil {
		return nil, err
	}
	iw.watcher = w
	if err := iw.addTree(root); err != nil {
		w.Close()
		return nil, err
	}
	return iw, nil
}
func (iw *InboxWatcher) addTree(root string) error {
	return filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return iw.watcher.Add(path)
		}
		return nil
	})
}
func (iw *InboxWatcher) Run(ctx context.Context) error {
	if iw.mode == inboxWatchPoll {
		return iw.runPoll(ctx)
	}
	defer iw.watcher.Close()
	for {
		select {
		case <-ctx.Done():
			return nil
		case err, ok := <-iw.watcher.Errors:
			if !ok {
				return nil
			}
			_ = err
		case ev, ok := <-iw.watcher.Events:
			if !ok {
				return nil
			}
			if ev.Op&fsnotify.Create != 0 {
				if info, e := os.Stat(ev.Name); e == nil && info.IsDir() {
					_ = iw.addTree(ev.Name)
				}
			}
			if ev.Op&(fsnotify.Create|fsnotify.Write|fsnotify.Rename) != 0 {
				kind := "inbox.file_changed"
				if ev.Op&fsnotify.Create != 0 {
					kind = "inbox.file_created"
				}
				if info, e := os.Stat(ev.Name); e == nil && !info.IsDir() {
					payload := json.RawMessage(fmt.Sprintf(`{"size":%d,"mtime_ms":%d}`, info.Size(), info.ModTime().UnixMilli()))
					_ = iw.store.RecordEvent(ctx, Event{Source: "inbox", Kind: kind, Path: ev.Name, Payload: payload})
				}
			}
		}
	}
}
func recordHerdrEvent(ctx context.Context, s *Store, ev HerdrEvent, protocol, schema int) {
	p := json.RawMessage(fmt.Sprintf(`{"kind":"%s","pane_id":"%s","workspace_id":"%s","revision":%d}`, ev.Kind, ev.PaneID, ev.WorkspaceID, ev.Revision))
	_ = s.RecordEvent(ctx, Event{Source: "herdr", Kind: ev.Kind, PaneID: ev.PaneID, WorkspaceID: ev.WorkspaceID, AgentStatus: ev.AgentStatus, Revision: ev.Revision, Protocol: int64(protocol), SchemaVersion: int64(schema), Payload: p, Unknown: ev.UnknownFields})
}
