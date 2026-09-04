package panewire

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"time"
)

var hubRequestIDPattern = regexp.MustCompile(`^[A-Za-z0-9._-]{1,96}$`)

func validHubRequestID(value string) bool { return hubRequestIDPattern.MatchString(value) }
func validHubSHA256(value string) bool {
	if len(value) != 64 {
		return false
	}
	_, err := hex.DecodeString(value)
	return err == nil
}
func validHubUpdateURL(value string) bool {
	u, err := url.Parse(value)
	return err == nil && u.Scheme == "https" && u.Host != "" && u.User == nil && u.RawQuery == "" && u.Fragment == ""
}

// applyHubUpdate downloads to a sibling temporary file, verifies it before
// touching the executable, then leaves a timestamped rollback copy. It is
// intentionally fail-closed: every error before the final rename leaves the
// running executable unchanged.
func applyHubUpdate(ctx context.Context, httpClient *http.Client, executable, assetURL, expectedSHA string) error {
	if !validHubUpdateURL(assetURL) || !validHubSHA256(expectedSHA) {
		return errors.New("update rejected")
	}
	if executable == "" {
		var err error
		executable, err = os.Executable()
		if err != nil {
			return errors.New("update unavailable")
		}
	}
	info, err := os.Stat(executable)
	if err != nil || !info.Mode().IsRegular() {
		return errors.New("update unavailable")
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, assetURL, nil)
	if err != nil {
		return errors.New("update unavailable")
	}
	response, err := httpClient.Do(request)
	if err != nil || response.StatusCode != http.StatusOK {
		if response != nil {
			response.Body.Close()
		}
		return errors.New("update unavailable")
	}
	defer response.Body.Close()
	temporary, err := os.CreateTemp(filepath.Dir(executable), ".panewire-update-")
	if err != nil {
		return errors.New("update unavailable")
	}
	temporaryName := temporary.Name()
	defer os.Remove(temporaryName)
	hash := sha256.New()
	if _, err = io.Copy(io.MultiWriter(temporary, hash), io.LimitReader(response.Body, 128<<20)); err != nil || temporary.Close() != nil {
		return errors.New("update unavailable")
	}
	if hex.EncodeToString(hash.Sum(nil)) != strings.ToLower(expectedSHA) {
		return errors.New("update checksum mismatch")
	}
	if err = os.Chmod(temporaryName, info.Mode().Perm()); err != nil {
		return errors.New("update unavailable")
	}
	backup := executable + ".bak-" + time.Now().UTC().Format("20060102T150405Z")
	if err = os.Rename(executable, backup); err != nil {
		return errors.New("update unavailable")
	}
	if err = os.Rename(temporaryName, executable); err != nil {
		_ = os.Rename(backup, executable)
		return errors.New("update unavailable")
	}
	return nil
}

func (client *HubClient) handleHubUpdate(message hubOutboundMessage) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := applyHubUpdate(ctx, client.updateHTTPClient, client.executablePath, message.URL, message.SHA256); err != nil {
		client.warn("hub update rejected")
		return
	}
	client.restart()
}

func (client *HubClient) handleHubQuota(ctx context.Context, peer *hubClientConnection, message hubOutboundMessage) {
	command, err := exec.LookPath("scopefuel")
	payload := ""
	if err == nil {
		runContext, cancel := context.WithTimeout(ctx, 15*time.Second)
		out, runErr := exec.CommandContext(runContext, command, "--json").Output()
		cancel()
		if runErr != nil {
			err = errors.New("scopefuel failed")
		} else {
			payload = string(out)
		}
	}
	response := struct {
		Type      string `json:"type"`
		RequestID string `json:"request_id"`
		Payload   string `json:"payload,omitempty"`
		Error     string `json:"error,omitempty"`
	}{Type: "quota.report", RequestID: message.RequestID, Payload: payload}
	if err != nil {
		if strings.Contains(err.Error(), "executable") {
			response.Error = "unsupported"
		} else {
			response.Error = "scopefuel failed"
		}
	}
	_ = peer.write(ctx, response)
}

// Keep json imported in this file so payload formation stays deliberately
// explicit when this code is changed; quota values are opaque stdout.
var _ = json.RawMessage{}
