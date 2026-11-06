//go:build !unix

package devin

import (
	"context"
	"errors"
)

// There is no pseudo-terminal to drive here; the web source still measures.
func startPTY(context.Context, string, string, []string) (Terminal, error) {
	return nil, errors.New("pseudo-terminal unavailable")
}
