package envfile

import (
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"
)

func writeFile(t *testing.T, path, body string, mode os.FileMode) {
	t.Helper()
	if err := os.WriteFile(path, []byte(body), mode); err != nil {
		t.Fatal(err)
	}
	// WriteFile's mode passes through the umask; pin the one the case needs.
	if err := os.Chmod(path, mode); err != nil {
		t.Fatal(err)
	}
}

func TestLoadReadsARegularMode0600File(t *testing.T) {
	path := filepath.Join(t.TempDir(), "hk.env")
	writeFile(t, path, "# comment\nexport A=1\nB=\"two words\"\nA=3\n", 0o600)
	values, err := Load(path)
	if err != nil || values["A"] != "3" || values["B"] != "two words" {
		t.Fatalf("values=%v err=%v", values, err)
	}
}

func TestLoadRefusesALooseFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "hk.env")
	writeFile(t, path, "A=1\n", 0o644)
	if _, err := Load(path); err == nil {
		t.Fatal("a mode-0644 file must be refused")
	}
}

// A symlink at the leaf is refused even when its target is a valid file.
func TestLoadRefusesALeafSymlink(t *testing.T) {
	dir := t.TempDir()
	target := filepath.Join(dir, "target.env")
	writeFile(t, target, "A=1\n", 0o600)
	link := filepath.Join(dir, "hk.env")
	if err := os.Symlink(target, link); err != nil {
		t.Fatal(err)
	}
	if _, err := Load(link); err == nil {
		t.Fatal("a leaf symlink must be refused")
	}
}

// A FIFO is refused, and refusing it never waits for a writer.
func TestLoadRefusesAFIFOWithoutBlocking(t *testing.T) {
	path := filepath.Join(t.TempDir(), "hk.env")
	if err := syscall.Mkfifo(path, 0o600); err != nil {
		t.Skipf("mkfifo unavailable here: %v", err)
	}
	done := make(chan error, 1)
	go func() {
		_, err := Load(path)
		done <- err
	}()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("a FIFO must be refused")
		}
	case <-time.After(5 * time.Second):
		// Unblock the reader so the goroutine ends with the test.
		if writer, err := os.OpenFile(path, os.O_WRONLY|syscall.O_NONBLOCK, 0); err == nil {
			writer.Close()
		}
		t.Fatal("Load blocked on a FIFO")
	}
}

// The checked object is the one read. A swapper replaces the leaf, again and
// again, between a valid mode-0600 file and a symlink to a readable
// mode-0644 file that carries a marker. A loader that checks the path and
// then opens it again can follow the symlink in between and return the
// marker; a loader that opens without following the leaf and checks the
// open descriptor never can.
func TestLoadReadsOnlyTheCheckedDescriptor(t *testing.T) {
	dir := t.TempDir()
	good := filepath.Join(dir, "good.env")
	writeFile(t, good, "A=checked\n", 0o600)
	planted := filepath.Join(dir, "planted.env")
	writeFile(t, planted, "A=planted-marker\n", 0o644)
	path := filepath.Join(dir, "hk.env")
	if err := os.Link(good, path); err != nil {
		t.Fatal(err)
	}

	var stop atomic.Bool
	swapped := make(chan struct{})
	go func() {
		defer close(swapped)
		next := filepath.Join(dir, "next")
		for i := 0; !stop.Load(); i++ {
			_ = os.Remove(next)
			var err error
			if i%2 == 0 {
				err = os.Symlink(planted, next)
			} else {
				err = os.Link(good, next)
			}
			if err == nil {
				_ = os.Rename(next, path)
			}
		}
	}()
	defer func() {
		stop.Store(true)
		<-swapped
	}()

	deadline := time.Now().Add(2 * time.Second)
	reads := 0
	for time.Now().Before(deadline) {
		values, err := Load(path)
		if err == nil {
			reads++
		}
		if strings.Contains(values["A"], "planted") {
			t.Fatalf("Load returned the planted file's value after %d loads: it read an object it never checked", reads)
		}
	}
	if reads == 0 {
		t.Log("no load hit the regular file while the swapper ran; the case proved only the refusals")
	}
}
