//go:build linux

package agy

import (
	"context"
	"errors"
	"strings"

	"github.com/godbus/dbus/v5"
)

// The Secret Service names are fixed. The item's attributes are the CLI's
// fixed service and user names, which hold no secret.
const (
	secretServiceName      = "org.freedesktop.secrets"
	secretServicePath      = "/org/freedesktop/secrets"
	secretServiceInterface = "org.freedesktop.Secret.Service"
	secretItemInterface    = "org.freedesktop.Secret.Item"
)

// secretServiceItem reads the CLI's login item from the Linux secret service:
// the go-keyring item with attributes service=gemini and username=antigravity,
// in whichever collection holds it. A missing or locked item is an error, not
// a prompt, and every call carries the caller's deadline. The connection never
// starts a bus or a service.
func secretServiceItem(ctx context.Context) (string, error) {
	conn, err := dbus.SessionBusPrivateNoAutoStartup(dbus.WithContext(ctx))
	if err != nil {
		return "", err
	}
	defer conn.Close()
	if err := conn.Auth(nil); err != nil {
		return "", err
	}
	if err := conn.Hello(); err != nil {
		return "", err
	}
	service := conn.Object(secretServiceName, secretServicePath)
	var algorithm dbus.Variant
	var session dbus.ObjectPath
	if err := service.CallWithContext(ctx, secretServiceInterface+".OpenSession", dbus.FlagNoAutoStart, "plain", dbus.MakeVariant("")).Store(&algorithm, &session); err != nil {
		return "", err
	}
	var unlocked, locked []dbus.ObjectPath
	attributes := map[string]string{"service": "gemini", "username": "antigravity"}
	if err := service.CallWithContext(ctx, secretServiceInterface+".SearchItems", dbus.FlagNoAutoStart, attributes).Store(&unlocked, &locked); err != nil {
		return "", err
	}
	if len(unlocked) == 0 {
		if len(locked) > 0 {
			return "", errors.New("agy login item is locked")
		}
		return "", errors.New("agy login item is absent")
	}
	var secret struct {
		Session     dbus.ObjectPath
		Parameters  []byte
		Value       []byte
		ContentType string
	}
	if err := conn.Object(secretServiceName, unlocked[0]).CallWithContext(ctx, secretItemInterface+".GetSecret", dbus.FlagNoAutoStart, session).Store(&secret); err != nil {
		return "", err
	}
	return strings.TrimRight(string(secret.Value), "\x00"), nil
}
