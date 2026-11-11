//go:build !linux

package agy

import (
	"context"
	"errors"
)

// secretServiceItem is the Linux secret service read; other platforms have
// none.
func secretServiceItem(context.Context) (string, error) {
	return "", errors.New("the secret service is read on linux only")
}
