package herdrpanes

import (
	"context"
	"errors"
	"strings"
	"testing"
)

// A socket reply cut at the boundary is typed by SocketCut, and Read retries
// a SocketCutError once: the second, whole answer is returned.
func TestReadRetriesSocketCutOnce(t *testing.T) {
	cut := cutAnswer()
	fetches := 0
	out, err := Read(t.Context(), func(context.Context) ([]byte, error) {
		fetches++
		if fetches == 1 {
			return nil, SocketCut(len(cut), strictDecode(cut))
		}
		return []byte(wholeAnswer), nil
	}, strictDecode)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if fetches != 2 || string(out) != wholeAnswer {
		t.Fatalf("fetches=%d out=%q, want 2 and the whole answer", fetches, out)
	}
}

// A socket cut that stays cut after the retry is ErrCut, never an empty list.
func TestReadSocketCutTwiceIsErrCut(t *testing.T) {
	cut := cutAnswer()
	fetches := 0
	_, err := Read(t.Context(), func(context.Context) ([]byte, error) {
		fetches++
		return nil, SocketCut(len(cut), strictDecode(cut))
	}, strictDecode)
	if !errors.Is(err, ErrCut) || fetches != 2 {
		t.Fatalf("fetches=%d err=%v, want two fetches and ErrCut", fetches, err)
	}
}

// SocketCut types only a boundary cut: an ordinary end of connection is not a cut.
func TestSocketCutOnlyAtBoundary(t *testing.T) {
	if err := SocketCut(100, strictDecode([]byte(`{"result":`))); err != nil {
		t.Fatalf("SocketCut at 100 bytes = %v, want nil", err)
	}
	var typed *SocketCutError
	err := SocketCut(4096, strictDecode([]byte(strings.Repeat("{", 4096))))
	if !errors.As(err, &typed) || typed.Size != 4096 {
		t.Fatalf("SocketCut at 4096 bytes = %v, want a SocketCutError of 4096", err)
	}
}
