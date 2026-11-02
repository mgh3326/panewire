package fleet

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// Seed only the selected workspace's trust, using the CLI's existing format.
// Stores are merged under a stable lock and atomically replaced; malformed or
// symlinked stores are never overwritten.
func (a *App) workspaceTrust(ctx context.Context, s Spawn, seat Seat, env []string) error {
	cwd, e := filepath.EvalSymlinks(s.CWD)
	if e != nil {
		return e
	}
	if cwd == a.Home || cwd == "/" {
		return fail(exitConfig, "refusing broad workspace trust")
	}
	switch seat.Kind {
	case "claude":
		dir := firstNonempty(os.Getenv("CLAUDE_CONFIG_DIR"), a.Home)
		path := filepath.Join(dir, ".claude.json")
		return a.mergeStore(ctx, path, false, func(data map[string]any) error {
			projects, ok := data["projects"].(map[string]any)
			if data["projects"] != nil && !ok {
				return fail(exitConfig, "Claude projects is not an object")
			}
			if projects == nil {
				projects = map[string]any{}
				data["projects"] = projects
			}
			entry, ok := projects[cwd].(map[string]any)
			if projects[cwd] != nil && !ok {
				return fail(exitConfig, "Claude project is not an object")
			}
			if entry == nil {
				entry = map[string]any{}
				projects[cwd] = entry
			}
			entry["hasTrustDialogAccepted"] = true
			return nil
		})
	case "devin":
		dataHome := firstNonempty(os.Getenv("XDG_DATA_HOME"), filepath.Join(a.Home, ".local", "share"))
		path := filepath.Join(dataHome, "devin", "cli", "trusted_workspaces.json")
		return a.mergeStore(ctx, path, true, func(data map[string]any) error {
			paths, ok := data["trusted_paths"].([]any)
			if !ok {
				return fail(exitConfig, "Devin trusted_paths is not a list")
			}
			for _, path := range paths {
				base, ok := path.(string)
				if !ok {
					return fail(exitConfig, "invalid Devin trusted path")
				}
				if base == cwd || within(base, cwd) {
					return nil
				}
			}
			data["trusted_paths"] = append(paths, cwd)
			return nil
		})
	case "kimi":
		home := firstNonempty(os.Getenv("KIMI_CODE_HOME"), filepath.Join(a.Home, ".kimi-code"))
		for _, entry := range env {
			if h, ok := strings.CutPrefix(entry, "KIMI_CODE_HOME="); ok {
				home = h
			}
		}
		name := strings.ToLower(filepath.Base(cwd))
		if len(name) > 40 {
			name = name[:40]
		}
		name = strings.TrimRightFunc(name, func(c rune) bool { return !((c >= 'a' && c <= 'z') || (c >= '0' && c <= '9')) })
		sum := sha256.Sum256([]byte(cwd))
		path := filepath.Join(home, "workspace-trust", "wd_"+name+"_"+hex.EncodeToString(sum[:])[:12])
		if e = noSymlinks(path); e != nil {
			return e
		}
		b, _ := encode(map[string]any{"root": cwd, "trustedAt": a.Now().UnixMilli()})
		return atomicFile(path, b, 0600)
	case "codex":
		home := firstNonempty(os.Getenv("CODEX_HOME"), filepath.Join(a.Home, ".codex"))
		for _, entry := range env {
			if h, ok := strings.CutPrefix(entry, "CODEX_HOME="); ok {
				home = h
			}
		}
		path := filepath.Join(home, "config.toml")
		if e = noSymlinks(path); e != nil {
			return e
		}
		if e = os.MkdirAll(home, 0700); e != nil {
			return e
		}
		// The .wrk.lock suffix is shared with the retired spawner so
		// concurrently deployed binaries still serialize on the same file;
		// rename only after every host has drained its old processes.
		lock, e := secureOpen(path+".wrk.lock", true)
		if e != nil {
			return e
		}
		defer lock.Close()
		if e = a.waitLock(ctx, lock); e != nil {
			return e
		}
		raw, e := os.ReadFile(path)
		if e != nil && !os.IsNotExist(e) {
			return e
		}
		var data map[string]any
		if len(raw) > 0 {
			if e = unmarshalTOML(raw, &data); e != nil {
				return e
			}
		}
		if projects, ok := data["projects"].(map[string]any); ok && projects[cwd] != nil {
			return nil
		}
		key, _ := json.Marshal(cwd)
		blob := append(raw, []byte("\n[projects."+string(key)+"]\ntrust_level = \"trusted\"\n")...)
		var validated map[string]any
		if e = unmarshalTOML(blob, &validated); e != nil {
			return e
		}
		return atomicFile(path, blob, 0600)
	}
	return nil
}
func (a *App) waitLock(ctx context.Context, f *os.File) error {
	deadline := a.Now().Add(10 * time.Second)
	for !tryLock(f) {
		if !a.Now().Before(deadline) {
			return fail(exitStateConflict, "workspace trust lock timed out")
		}
		if e := a.Sleep(ctx, 50*time.Millisecond); e != nil {
			return e
		}
	}
	return nil
}
func (a *App) mergeStore(ctx context.Context, path string, required bool, mutate func(map[string]any) error) error {
	if e := noSymlinks(path); e != nil {
		return e
	}
	if e := os.MkdirAll(filepath.Dir(path), 0700); e != nil {
		return e
	}
	// See above: the lock name stays .wrk.lock for one release.
	lock, e := secureOpen(path+".wrk.lock", true)
	if e != nil {
		return e
	}
	defer lock.Close()
	if e = a.waitLock(ctx, lock); e != nil {
		return e
	}
	raw, e := os.ReadFile(path)
	if e != nil && (required || !os.IsNotExist(e)) {
		return e
	}
	data := map[string]any{}
	if len(raw) > 0 {
		if e = uniqueJSON(raw); e != nil {
			return e
		}
		if e = json.Unmarshal(raw, &data); e != nil || data == nil {
			return fail(exitConfig, "malformed workspace trust store")
		}
	}
	if e = mutate(data); e != nil {
		return e
	}
	b, e := encode(data)
	if e != nil {
		return e
	}
	return atomicFile(path, b, 0600)
}
func atomicFile(path string, b []byte, mode os.FileMode) error {
	if e := noSymlinks(path); e != nil {
		return e
	}
	if e := os.MkdirAll(filepath.Dir(path), 0700); e != nil {
		return e
	}
	f, e := os.CreateTemp(filepath.Dir(path), ".panewire-*")
	if e != nil {
		return e
	}
	defer os.Remove(f.Name())
	if e = f.Chmod(mode); e != nil {
		f.Close()
		return e
	}
	if _, e = f.Write(b); e != nil {
		f.Close()
		return e
	}
	if e = f.Sync(); e != nil {
		f.Close()
		return e
	}
	if e = f.Close(); e != nil {
		return e
	}
	return os.Rename(f.Name(), path)
}
