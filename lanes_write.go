package panewire

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"time"

	"golang.org/x/sys/unix"
)

const (
	lanesFileMaxBytes         = 64 << 10
	lanesWriteRequestMaxBytes = 8 << 10
	lanesBackupsKept          = 10
)

var (
	laneNamePattern = regexp.MustCompile(`^[a-z0-9-]{1,32}$`)
	lanePanePattern = regexp.MustCompile(`^w[A-Za-z0-9]+:p[A-Za-z0-9]+$`)

	errLanesWriteUnconfigured = errors.New("lanes path is not configured")
	errLanesWriteInvalid      = errors.New("lanes file is invalid")
	errLanesWriteFailed       = errors.New("lanes file write failed")
	errLaneNotFound           = errors.New("lane was not found")
	errLaneProtected          = errors.New("lane is protected")
)

type hubLaneWriteRequest struct {
	Machine string `json:"machine"`
	Pane    string `json:"pane"`
	Parent  string `json:"parent"`
	Sink    bool   `json:"sink"`
}

func (body *hubLaneWriteRequest) UnmarshalJSON(data []byte) error {
	if bytes.Equal(bytes.TrimSpace(data), []byte("null")) {
		return errors.New("lane request must be an object")
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(data, &fields); err != nil || fields == nil {
		return errors.New("lane request must be an object")
	}
	for field := range fields {
		switch field {
		case "machine", "pane", "parent", "sink":
		default:
			return fmt.Errorf("unknown lane request field %q", field)
		}
	}
	decodeString := func(name string, destination *string) error {
		value, present := fields[name]
		if !present || bytes.Equal(bytes.TrimSpace(value), []byte("null")) {
			if present {
				return fmt.Errorf("lane request field %q must be a string", name)
			}
			return nil
		}
		if err := json.Unmarshal(value, destination); err != nil {
			return fmt.Errorf("lane request field %q must be a string", name)
		}
		return nil
	}
	if err := decodeString("machine", &body.Machine); err != nil {
		return err
	}
	if err := decodeString("pane", &body.Pane); err != nil {
		return err
	}
	if err := decodeString("parent", &body.Parent); err != nil {
		return err
	}
	if value, present := fields["sink"]; present {
		if bytes.Equal(bytes.TrimSpace(value), []byte("null")) || json.Unmarshal(value, &body.Sink) != nil {
			return errors.New("lane request field sink must be a boolean")
		}
	}
	return nil
}

type hubLaneDeleteResponse struct {
	Lane    string `json:"lane"`
	Removed bool   `json:"removed"`
}

type lanesFileSnapshot struct {
	Bytes  []byte
	Routes map[string]reportRelayRoute
	Exists bool
}

// lanesWriteOps keeps filesystem failure injection local to package tests
// without weakening the production path or replacing its OS file lock.
type lanesWriteOps struct {
	createBackup func(path string, contents []byte, now time.Time) error
	createTemp   func(directory, pattern string) (*os.File, error)
	rename       func(oldPath, newPath string) error
	remove       func(path string) error
}

func (h *HubServer) handlePutLane(writer http.ResponseWriter, request *http.Request) {
	if !h.authorizeOperator(request) {
		hubUnauthorized(writer)
		return
	}
	lane := request.PathValue("lane")
	if !laneNamePattern.MatchString(lane) {
		writeLaneJSONError(writer, http.StatusBadRequest, "invalid_lane")
		return
	}
	var body hubLaneWriteRequest
	decoder := json.NewDecoder(http.MaxBytesReader(writer, request.Body, lanesWriteRequestMaxBytes))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&body) != nil {
		writeLaneJSONError(writer, http.StatusBadRequest, "invalid_lane_request")
		return
	}
	var trailing any
	if decoder.Decode(&trailing) != io.EOF {
		writeLaneJSONError(writer, http.StatusBadRequest, "invalid_lane_request")
		return
	}

	projection, created, err := h.putLane(lane, body)
	if err != nil {
		if errors.Is(err, errLaneRequestInvalid) {
			writeLaneJSONError(writer, http.StatusBadRequest, "invalid_lane_request")
			return
		}
		writeLanesWriteError(writer, err)
		return
	}
	writer.Header().Set("Content-Type", "application/json")
	if created {
		writer.WriteHeader(http.StatusCreated)
	}
	_ = json.NewEncoder(writer).Encode(projection)
}

func (h *HubServer) handleDeleteLane(writer http.ResponseWriter, request *http.Request) {
	if !h.authorizeOperator(request) {
		hubUnauthorized(writer)
		return
	}
	lane := request.PathValue("lane")
	if !laneNamePattern.MatchString(lane) {
		writeLaneJSONError(writer, http.StatusBadRequest, "invalid_lane")
		return
	}
	if err := h.deleteLane(lane); err != nil {
		switch {
		case errors.Is(err, errLaneNotFound):
			writeLaneJSONError(writer, http.StatusNotFound, "lane_not_found")
		case errors.Is(err, errLaneProtected):
			writeLaneJSONError(writer, http.StatusConflict, "lane_protected")
		default:
			writeLanesWriteError(writer, err)
		}
		return
	}
	writer.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(writer).Encode(hubLaneDeleteResponse{Lane: lane, Removed: true})
}

var errLaneRequestInvalid = errors.New("lane request is invalid")

func (h *HubServer) putLane(lane string, body hubLaneWriteRequest) (hubLaneProjection, bool, error) {
	if h.reportRelayPath == "" {
		return hubLaneProjection{}, false, errLanesWriteUnconfigured
	}
	var projection hubLaneProjection
	var created bool
	err := withLanesFileLock(h.reportRelayPath, func() error {
		snapshot, err := readLanesFileForWrite(h.reportRelayPath)
		if err != nil {
			return err
		}
		existing, exists := snapshot.Routes[lane]
		if err := validateLaneWriteRequest(h, lane, body, snapshot.Routes); err != nil {
			return err
		}
		route := reportRelayRoute{Machine: body.Machine, Pane: body.Pane, Parent: body.Parent, Deliver: existing.Deliver, Protected: existing.Protected}
		if body.Sink {
			route.Sink = true
			route.Machine = ""
			route.Pane = ""
		}
		if snapshot.Routes == nil {
			snapshot.Routes = make(map[string]reportRelayRoute)
		}
		snapshot.Routes[lane] = route
		if err := h.replaceLanesFile(h.reportRelayPath, snapshot, lanesBackupNow(h)); err != nil {
			return err
		}
		created = !exists
		projection = hubLaneProjection{Lane: lane, Machine: route.Machine, Pane: route.Pane, Parent: route.Parent, Sink: route.Sink}
		return nil
	})
	if err != nil {
		return hubLaneProjection{}, false, err
	}
	h.broadcastLanesChanged(lane, map[bool]string{true: "add", false: "update"}[created])
	return projection, created, nil
}

func (h *HubServer) deleteLane(lane string) error {
	if h.reportRelayPath == "" {
		return errLanesWriteUnconfigured
	}
	err := withLanesFileLock(h.reportRelayPath, func() error {
		snapshot, err := readLanesFileForWrite(h.reportRelayPath)
		if err != nil {
			return err
		}
		route, exists := snapshot.Routes[lane]
		if !exists {
			return errLaneNotFound
		}
		if route.Protected {
			return errLaneProtected
		}
		delete(snapshot.Routes, lane)
		return h.replaceLanesFile(h.reportRelayPath, snapshot, lanesBackupNow(h))
	})
	if err != nil {
		return err
	}
	h.broadcastLanesChanged(lane, "remove")
	return nil
}

func validateLaneWriteRequest(h *HubServer, lane string, body hubLaneWriteRequest, routes map[string]reportRelayRoute) error {
	if !laneNamePattern.MatchString(lane) {
		return errLaneRequestInvalid
	}
	if body.Parent != "" {
		if !laneNamePattern.MatchString(body.Parent) || body.Parent == lane {
			return errLaneRequestInvalid
		}
		if _, exists := routes[body.Parent]; !exists {
			return errLaneRequestInvalid
		}
	}
	if body.Sink {
		// The loader gives an explicit sink precedence over transport fields.
		// Keep that compatibility: machine and pane are normalized away below.
		return nil
	}
	if body.Machine == hubOperatorMachineID || !machineIDPattern.MatchString(body.Machine) || !h.knownLaneMachine(body.Machine, routes) {
		return errLaneRequestInvalid
	}
	if !validLanePane(body.Pane) {
		return errLaneRequestInvalid
	}
	return nil
}

func validLanePane(pane string) bool {
	return pane != "" && len(pane) <= 128 && lanePanePattern.MatchString(pane)
}

func (h *HubServer) knownLaneMachine(machine string, routes map[string]reportRelayRoute) bool {
	if _, configured := h.tokens[machine]; configured {
		return machine != hubOperatorMachineID
	}
	for _, route := range routes {
		if !route.Sink && route.Machine == machine {
			return true
		}
	}
	h.mu.Lock()
	_, connected := h.nodes[machine]
	h.mu.Unlock()
	return connected
}

func (h *HubServer) broadcastLanesChanged(lane, operation string) {
	payload, _ := json.Marshal(struct {
		Lane string `json:"lane"`
		Op   string `json:"op"`
	}{Lane: lane, Op: operation})
	h.broadcast(hubEvent{Kind: "lanes.changed", Payload: payload, Received: lanesBackupNow(h)})
}

func writeLaneJSONError(writer http.ResponseWriter, status int, code string) {
	writer.Header().Set("Content-Type", "application/json")
	writer.WriteHeader(status)
	_ = json.NewEncoder(writer).Encode(struct {
		Error string `json:"error"`
	}{Error: code})
}

func writeLanesWriteError(writer http.ResponseWriter, err error) {
	code := "lanes_write_failed"
	if errors.Is(err, errLanesWriteInvalid) {
		code = "lanes_invalid"
	} else if errors.Is(err, errLanesWriteUnconfigured) {
		code = "lanes_unconfigured"
	}
	writeLaneJSONError(writer, http.StatusInternalServerError, code)
}

func readLanesFileForWrite(path string) (lanesFileSnapshot, error) {
	info, err := os.Stat(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return lanesFileSnapshot{Routes: make(map[string]reportRelayRoute)}, nil
		}
		return lanesFileSnapshot{}, fmt.Errorf("%w: stat lanes file: %v", errLanesWriteFailed, err)
	}
	if !info.Mode().IsRegular() {
		return lanesFileSnapshot{}, fmt.Errorf("%w: lanes path is not a regular file", errLanesWriteFailed)
	}
	file, err := os.Open(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return lanesFileSnapshot{Routes: make(map[string]reportRelayRoute)}, nil
		}
		return lanesFileSnapshot{}, fmt.Errorf("%w: open lanes file: %v", errLanesWriteFailed, err)
	}
	contents, readErr := io.ReadAll(io.LimitReader(file, lanesFileMaxBytes+1))
	closeErr := file.Close()
	if readErr != nil {
		return lanesFileSnapshot{}, fmt.Errorf("%w: read lanes file: %v", errLanesWriteFailed, readErr)
	}
	if closeErr != nil {
		return lanesFileSnapshot{}, fmt.Errorf("%w: close lanes file: %v", errLanesWriteFailed, closeErr)
	}
	if len(contents) > lanesFileMaxBytes {
		return lanesFileSnapshot{}, errLanesWriteInvalid
	}
	routes, err := parseReportRelayRoutes(contents)
	if err != nil {
		return lanesFileSnapshot{}, fmt.Errorf("%w: %v", errLanesWriteInvalid, err)
	}
	if routes == nil {
		routes = make(map[string]reportRelayRoute)
	}
	return lanesFileSnapshot{Bytes: contents, Routes: routes, Exists: true}, nil
}

func withLanesFileLock(path string, operation func() error) error {
	lock, err := os.OpenFile(path+".lock", os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return fmt.Errorf("%w: open lanes lock: %v", errLanesWriteFailed, err)
	}
	defer lock.Close()
	if err := lock.Chmod(0600); err != nil {
		return fmt.Errorf("%w: secure lanes lock: %v", errLanesWriteFailed, err)
	}
	if err := unix.Flock(int(lock.Fd()), unix.LOCK_EX); err != nil {
		return fmt.Errorf("%w: lock lanes file: %v", errLanesWriteFailed, err)
	}
	operationErr := operation()
	unlockErr := unix.Flock(int(lock.Fd()), unix.LOCK_UN)
	if operationErr != nil {
		return operationErr
	}
	if unlockErr != nil {
		return fmt.Errorf("%w: unlock lanes file: %v", errLanesWriteFailed, unlockErr)
	}
	return nil
}

func (h *HubServer) replaceLanesFile(path string, snapshot lanesFileSnapshot, now time.Time) error {
	contents, err := json.MarshalIndent(struct {
		Lanes map[string]reportRelayRoute `json:"lanes"`
	}{Lanes: snapshot.Routes}, "", "  ")
	if err != nil {
		return fmt.Errorf("%w: encode lanes file: %v", errLanesWriteFailed, err)
	}
	contents = append(contents, '\n')
	if len(contents) > lanesFileMaxBytes {
		return errLanesWriteInvalid
	}
	if snapshot.Exists {
		createBackup := h.lanesWriteOps.createBackup
		if createBackup == nil {
			createBackup = createLanesBackup
		}
		if err := createBackup(path, snapshot.Bytes, now); err != nil {
			return err
		}
	}

	directory := filepath.Dir(path)
	createTemp := h.lanesWriteOps.createTemp
	if createTemp == nil {
		createTemp = os.CreateTemp
	}
	temporary, err := createTemp(directory, "."+filepath.Base(path)+".tmp-*")
	if err != nil {
		return fmt.Errorf("%w: create lanes temp: %v", errLanesWriteFailed, err)
	}
	temporaryPath := temporary.Name()
	removeTemporary := true
	remove := h.lanesWriteOps.remove
	if remove == nil {
		remove = os.Remove
	}
	defer func() {
		if removeTemporary {
			_ = remove(temporaryPath)
		}
	}()
	if err := temporary.Chmod(0600); err != nil {
		_ = temporary.Close()
		return fmt.Errorf("%w: secure lanes temp: %v", errLanesWriteFailed, err)
	}
	if err := writeAll(temporary, contents); err != nil {
		_ = temporary.Close()
		return fmt.Errorf("%w: write lanes temp: %v", errLanesWriteFailed, err)
	}
	if err := temporary.Sync(); err != nil {
		_ = temporary.Close()
		return fmt.Errorf("%w: sync lanes temp: %v", errLanesWriteFailed, err)
	}
	if err := temporary.Close(); err != nil {
		return fmt.Errorf("%w: close lanes temp: %v", errLanesWriteFailed, err)
	}
	rename := h.lanesWriteOps.rename
	if rename == nil {
		rename = os.Rename
	}
	if err := rename(temporaryPath, path); err != nil {
		return fmt.Errorf("%w: rename lanes temp: %v", errLanesWriteFailed, err)
	}
	removeTemporary = false
	// Directory sync is best effort across the supported filesystems. The
	// rename above is the visibility boundary; a directory fsync failure must
	// not turn a successfully replaced, parseable file into a reported failure.
	if directoryFile, err := os.Open(directory); err == nil {
		_ = directoryFile.Sync()
		_ = directoryFile.Close()
	}
	return nil
}

func createLanesBackup(path string, contents []byte, now time.Time) error {
	if now.IsZero() {
		now = time.Now().UTC()
	}
	stamp := now.UTC().Format("20060102T150405.000000000Z")
	var backupPath string
	var backup *os.File
	for attempt := 0; attempt < 10000; attempt++ {
		suffix := stamp + "-" + fmt.Sprintf("%06d", attempt)
		candidate := path + ".bak-" + suffix
		file, err := os.OpenFile(candidate, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
		if errors.Is(err, os.ErrExist) {
			continue
		}
		if err != nil {
			return fmt.Errorf("%w: create lanes backup: %v", errLanesWriteFailed, err)
		}
		backupPath, backup = candidate, file
		break
	}
	if backup == nil {
		return fmt.Errorf("%w: no unique lanes backup name", errLanesWriteFailed)
	}
	removeBackup := true
	defer func() {
		if removeBackup {
			_ = os.Remove(backupPath)
		}
	}()
	if err := backup.Chmod(0600); err != nil {
		_ = backup.Close()
		return fmt.Errorf("%w: secure lanes backup: %v", errLanesWriteFailed, err)
	}
	if err := writeAll(backup, contents); err != nil {
		_ = backup.Close()
		return fmt.Errorf("%w: write lanes backup: %v", errLanesWriteFailed, err)
	}
	if err := backup.Sync(); err != nil {
		_ = backup.Close()
		return fmt.Errorf("%w: sync lanes backup: %v", errLanesWriteFailed, err)
	}
	if err := backup.Close(); err != nil {
		return fmt.Errorf("%w: close lanes backup: %v", errLanesWriteFailed, err)
	}
	removeBackup = false
	if err := rotateLanesBackups(path); err != nil {
		return err
	}
	return nil
}

func rotateLanesBackups(path string) error {
	directory := filepath.Dir(path)
	prefix := filepath.Base(path) + ".bak-"
	entries, err := os.ReadDir(directory)
	if err != nil {
		return fmt.Errorf("%w: list lanes backups: %v", errLanesWriteFailed, err)
	}
	backups := make([]string, 0, len(entries))
	for _, entry := range entries {
		if !strings.HasPrefix(entry.Name(), prefix) {
			continue
		}
		info, err := entry.Info()
		if err != nil {
			return fmt.Errorf("%w: inspect lanes backup: %v", errLanesWriteFailed, err)
		}
		if info.Mode().IsRegular() {
			backups = append(backups, entry.Name())
		}
	}
	sort.Sort(sort.Reverse(sort.StringSlice(backups)))
	if len(backups) <= lanesBackupsKept {
		return nil
	}
	for _, name := range backups[lanesBackupsKept:] {
		if err := os.Remove(filepath.Join(directory, name)); err != nil && !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("%w: rotate lanes backups: %v", errLanesWriteFailed, err)
		}
	}
	return nil
}

func writeAll(writer io.Writer, contents []byte) error {
	for len(contents) > 0 {
		written, err := writer.Write(contents)
		if err != nil {
			return err
		}
		if written <= 0 || written > len(contents) {
			return io.ErrShortWrite
		}
		contents = contents[written:]
	}
	return nil
}

func lanesBackupNow(h *HubServer) time.Time {
	if h.now != nil {
		return h.now().UTC()
	}
	return time.Now().UTC()
}
