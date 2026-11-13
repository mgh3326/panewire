package fleet

import (
	"context"
	"errors"
	"fmt"

	"github.com/mgh3326/handoffkeep/client"

	"github.com/mgh3326/panewire/internal/hkcred"
	"github.com/mgh3326/panewire/internal/textbound"
)

// A job's saved pane screen also goes to hk (hk 3617), so the evidence a
// close leaves behind outlives the node's job directory: the task pane sweep
// (capturePaneScreen, then storePaneScreen) and the pane.capture node command upload it after the
// local file is written. See docs/pane-sweep.md and docs/node-commands.md.

// PaneScreenDocMaxBytes is hk's document body cap (handoffkeep
// internal/store/store.go DocumentMaxBytes, v0.0.130).
const PaneScreenDocMaxBytes = 512 << 10

// PaneScreenDocKey is the hk document a job's pane screen is uploaded as.
func PaneScreenDocKey(job string) string { return "screens/" + job + "/" + PaneScreenFile }

// UploadPaneScreen writes screen to hk as the evidence document
// PaneScreenDocKey(job), with the job, origin class job and — when task is a
// numeric hk task id — the task, under the upload bound. It returns the key
// once hk stored it. A screen over hk's cap is sent as paneScreenDocBody
// cuts it; the caller's local file always keeps the whole screen. A failure
// is a paneScreenUploadError, whose text is safe to log.
func UploadPaneScreen(ctx context.Context, job, task string, screen []byte) (string, error) {
	key := PaneScreenDocKey(job)
	doc := client.Document{Key: key, Kind: "evidence", Job: job, Body: paneScreenDocBody(screen), OriginClass: "job"}
	if id, err := hkTaskID(task); err == nil {
		doc.TaskID = &id
	}
	if err := hkPutDocument(ctx, doc); err != nil {
		return "", paneScreenUploadError(err)
	}
	return key, nil
}

// paneScreenUploadError names why an upload failed without hk's endpoint, the
// credential file's path or the screen's text: hk's error code when hk
// refused it (secret_like_content for a guard refusal), else unconfigured, or
// unreachable for a transport failure or an answer hk's client could not
// read.
func paneScreenUploadError(err error) error {
	var unconfigured *hkcred.UnconfiguredError
	switch code := client.ErrorCode(err); {
	case code != "":
		return fmt.Errorf("hk refused the screen document: %s", code)
	case errors.As(err, &unconfigured):
		return errors.New("hk is not configured on this node")
	}
	return errors.New("hk unreachable or its answer unreadable")
}

// paneScreenDocBody is the screen as hk stores it: unchanged when it fits
// hk's cap, otherwise a first line saying it was cut followed by the
// screen's last bytes, cut on a rune boundary, so the whole body fits. The
// tail is kept because the end of a screen — the last output, the prompt the
// pane stopped at — is what a reader of a closed pane's evidence needs.
func paneScreenDocBody(screen []byte) string {
	if len(screen) <= PaneScreenDocMaxBytes {
		return string(screen)
	}
	header := fmt.Sprintf("[panewire: pane screen cut to its last bytes; the full %d-byte screen stays in the job directory]\n", len(screen))
	return header + textbound.SuffixWithin(string(screen), PaneScreenDocMaxBytes-len(header))
}
