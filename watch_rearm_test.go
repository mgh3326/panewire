package panewire

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"syscall"
	"testing"

	"github.com/mgh3326/panewire/internal/fleet"
)

func TestNodeStartupRearmsWatchers(t *testing.T) {
	root := t.TempDir()
	root, err := filepath.EvalSymlinks(root)
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"live", "locked", "completed", "reaped", "no-pane"} {
		dir := filepath.Join(root, id, "events")
		if err := os.MkdirAll(dir, 0700); err != nil {
			t.Fatal(err)
		}
		events := []map[string]any{{"kind": "job.spawned", "payload": map[string]any{"pane_id": "pane"}}}
		if id == "no-pane" {
			events[0]["payload"] = map[string]any{}
		}
		if id == "completed" {
			events = append(events, map[string]any{"kind": "job.completed"})
		}
		if id == "reaped" {
			events = append(events, map[string]any{"kind": "job.reaped"})
		}
		for i, ev := range events {
			b, err := json.Marshal(ev)
			if err != nil {
				t.Fatal(err)
			}
			name := []string{"01.json", "02.json"}[i]
			if err := os.WriteFile(filepath.Join(dir, name), b, 0600); err != nil {
				t.Fatal(err)
			}
		}
	}
	lock, err := os.OpenFile(filepath.Join(root, "locked", ".panewire-watch.lock"), os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		t.Fatal(err)
	}
	defer lock.Close()
	if err := syscall.Flock(int(lock.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		t.Fatal(err)
	}
	var started, logged []string
	app := &fleet.App{Jobs: root}
	client := &HubClient{spawnConfigPath: filepath.Join(t.TempDir(), "missing"), spawnJournalDir: filepath.Join(t.TempDir(), "journal"), rearmHook: func() {
		if err := app.RearmWatchers(func(id string) error { started = append(started, id); return nil }, func(id string) { logged = append(logged, id) }); err != nil {
			t.Fatal(err)
		}
	}}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	client.Run(ctx)
	if !reflect.DeepEqual(started, []string{"live"}) || !reflect.DeepEqual(logged, started) {
		t.Fatalf("started=%v logged=%v", started, logged)
	}
}
