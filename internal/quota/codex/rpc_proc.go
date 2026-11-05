package codex

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"os/exec"
	"strings"
	"sync"
	"time"
)

const (
	closeReapWait = 2 * time.Second
	rpcLineLimit  = 4 << 20
)

// procTransport is one running `codex app-server`, in its own process group.
type procTransport struct {
	cmd    *exec.Cmd
	in     io.WriteCloser
	out    *bufio.Reader
	waited chan struct{}
	once   sync.Once
}

// startAppServer spawns the app-server with cwd = the codex home. The only
// variable it sets is CODEX_HOME, over an inherited environment; stderr is
// discarded and no credential is passed, on argv or otherwise.
func startAppServer(_ context.Context, binary, dir, codexHome string) (Transport, error) {
	cmd := exec.Command(binary, "-s", "read-only", "-a", "never", "app-server")
	cmd.Dir = dir
	cmd.Env = withCodexHome(os.Environ(), codexHome)
	setProcessGroup(cmd)
	in, err := cmd.StdinPipe()
	if err != nil {
		return nil, err
	}
	out, err := cmd.StdoutPipe()
	if err != nil {
		return nil, err
	}
	if err = cmd.Start(); err != nil {
		return nil, err
	}
	t := &procTransport{cmd: cmd, in: in, out: bufio.NewReader(out), waited: make(chan struct{})}
	go func() {
		_ = cmd.Wait()
		close(t.waited)
	}()
	return t, nil
}

// withCodexHome is env with CODEX_HOME set to dir and no other copy of it.
func withCodexHome(env []string, dir string) []string {
	out := make([]string, 0, len(env)+1)
	for _, entry := range env {
		if !strings.HasPrefix(entry, "CODEX_HOME=") {
			out = append(out, entry)
		}
	}
	return append(out, "CODEX_HOME="+dir)
}

func (t *procTransport) Send(b []byte) error {
	_, err := t.in.Write(append(b[:len(b):len(b)], '\n'))
	return err
}

// Recv reads one line, at most rpcLineLimit bytes.
func (t *procTransport) Recv() ([]byte, error) {
	var line bytes.Buffer
	for {
		chunk, err := t.out.ReadSlice('\n')
		line.Write(chunk)
		if line.Len() > rpcLineLimit {
			return nil, errors.New("app-server line too long")
		}
		if !errors.Is(err, bufio.ErrBufferFull) {
			return line.Bytes(), err
		}
	}
}

// Close kills the app-server's whole process group and waits for it to be
// reaped, at most closeReapWait. Every start must be closed so probes never
// leak `codex app-server` processes.
func (t *procTransport) Close() {
	t.once.Do(func() {
		select {
		case <-t.waited: // already reaped; its pid may be reused
		default:
			killProcessGroup(t.cmd)
		}
		_ = t.in.Close()
		select {
		case <-t.waited:
		case <-time.After(closeReapWait):
		}
	})
}
