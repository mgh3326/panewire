package panewire

import (
	"context"
	"encoding/json"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
	"time"
)

const (
	inboxWatchFsnotify       = "fsnotify"
	inboxWatchPoll           = "poll"
	defaultInboxPollInterval = 5 * time.Second
)

// inboxWatchMode is a pure decision so the darwin default stays testable from
// any build host. macOS kqueue cannot recurse, so a recursive fsnotify watch
// opens one descriptor per job directory; that exhausted a whole machine in
// production, and polling is the safe default there.
func inboxWatchMode(goos, env string) string {
	switch env {
	case inboxWatchFsnotify, inboxWatchPoll:
		return env
	}
	if goos == "darwin" {
		return inboxWatchPoll
	}
	return inboxWatchFsnotify
}

func inboxWatchModeFromEnv() string {
	return inboxWatchMode(runtime.GOOS, os.Getenv("PANEWIRE_INBOX_WATCH"))
}

func inboxPollInterval() time.Duration {
	if value, err := time.ParseDuration(os.Getenv("PANEWIRE_INBOX_POLL")); err == nil && value > 0 {
		return value
	}
	return defaultInboxPollInterval
}

// takeBaseline records what already exists. It runs in the constructor, the
// way fsnotify registers its watches there, so a file written after
// NewInboxWatcher returns is reliably seen as new.
func (iw *InboxWatcher) takeBaseline() {
	iw.baseline = make(map[string]inboxFileState)
	iw.collect(iw.baseline, nil, nil)
}

type inboxFileState struct {
	size    int64
	modTime time.Time
}

// runPoll walks the tree on an interval instead of registering a watch per
// directory. It records the same two event kinds as the fsnotify path so
// downstream consumers cannot tell the two modes apart.
func (iw *InboxWatcher) runPoll(ctx context.Context) error {
	known := iw.baseline
	if known == nil {
		known = make(map[string]inboxFileState)
		iw.collect(known, nil, nil)
	}
	ticker := time.NewTicker(iw.poll)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			current := make(map[string]inboxFileState, len(known))
			var created, changed []string
			iw.collect(current, &created, &changed, known)
			for _, path := range created {
				iw.recordPollEvent(ctx, "inbox.file_created", path, current[path])
			}
			for _, path := range changed {
				iw.recordPollEvent(ctx, "inbox.file_changed", path, current[path])
			}
			known = current
		}
	}
}

func (iw *InboxWatcher) collect(into map[string]inboxFileState, created, changed *[]string, previous ...map[string]inboxFileState) {
	var known map[string]inboxFileState
	if len(previous) == 1 {
		known = previous[0]
	}
	_ = filepath.WalkDir(iw.root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			// A directory removed mid-walk is ordinary inbox churn.
			return nil
		}
		if d.IsDir() {
			return nil
		}
		info, err := d.Info()
		if err != nil {
			return nil
		}
		state := inboxFileState{size: info.Size(), modTime: info.ModTime()}
		into[path] = state
		if known == nil {
			return nil
		}
		if before, existed := known[path]; !existed {
			if created != nil {
				*created = append(*created, path)
			}
		} else if before.size != state.size || !before.modTime.Equal(state.modTime) {
			if changed != nil {
				*changed = append(*changed, path)
			}
		}
		return nil
	})
}

func (iw *InboxWatcher) recordPollEvent(ctx context.Context, kind, path string, state inboxFileState) {
	payload := json.RawMessage(fmt.Sprintf(`{"size":%d,"mtime_ms":%d}`, state.size, state.modTime.UnixMilli()))
	_ = iw.store.RecordEvent(ctx, Event{Source: "inbox", Kind: kind, Path: path, Payload: payload})
}
