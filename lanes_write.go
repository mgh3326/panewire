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
	// lanesNodeFileReserveBytes is the tail of the lanes file that only
	// operator writes may grow into: a node write never leaves the file
	// larger than lanesNodeFileMaxBytes, so one node token can never consume
	// the space operator administration needs.
	lanesNodeFileReserveBytes = 8 << 10
	lanesNodeFileMaxBytes     = lanesFileMaxBytes - lanesNodeFileReserveBytes
	// defaultLanesNodeRowCap bounds the lane rows one node credential may
	// own. Sixty-four rows is far above the pane count one machine hosts,
	// yet even all-maximum-length fields keep a node's footprint under half
	// the node byte budget.
	defaultLanesNodeRowCap = 64
)

var (
	laneNamePattern = regexp.MustCompile(`^[a-z0-9-]{1,32}$`)
	lanePanePattern = regexp.MustCompile(`^w[A-Za-z0-9]+:p[A-Za-z0-9]+$`)

	errLanesWriteUnconfigured = errors.New("lanes path is not configured")
	errLanesWriteInvalid      = errors.New("lanes file is invalid")
	errLanesWriteFailed       = errors.New("lanes file write failed")
	errLanesFileFull          = errors.New("lanes file is full")
	errLaneQuotaExceeded      = errors.New("node lane quota is exhausted")
	errLaneNotFound           = errors.New("lane was not found")
	errLaneProtected          = errors.New("lane is protected")
	errLaneMachineMismatch    = errors.New("lane machine does not match the authenticated node")
)

type hubLaneWriteRequest struct {
	Machine        string              `json:"machine"`
	Pane           string              `json:"pane"`
	Parent         string              `json:"parent"`
	Sink           bool                `json:"sink"`
	Standby        *reportRelayStandby `json:"standby"`
	standbyPresent bool
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
		case "machine", "pane", "parent", "sink", "standby":
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
	if value, present := fields["standby"]; present {
		body.standbyPresent = true
		if !bytes.Equal(bytes.TrimSpace(value), []byte("null")) {
			var standby reportRelayStandby
			if err := json.Unmarshal(value, &standby); err != nil {
				return errors.New("lane request field standby must be an object")
			}
			body.Standby = &standby
		}
	}
	return nil
}

type hubLaneDeleteResponse struct {
	Lane    string `json:"lane"`
	Removed bool   `json:"removed"`
}

type lanesFileSnapshot struct {
	Bytes   []byte
	Routes  map[string]reportRelayRoute
	Control lanesFileControl
	Exists  bool
}

// lanesWriteOps keeps filesystem failure injection local to package tests
// without weakening the production path or replacing its OS file lock.
type lanesWriteOps struct {
	createBackup func(path string, contents []byte, now time.Time) error
	createTemp   func(directory, pattern string) (*os.File, error)
	rename       func(oldPath, newPath string) error
	remove       func(path string) error
}

// authorizeLanesCaller authenticates a /v1/lanes request. The operator
// bearer keeps the whole surface and reports an empty machine id. Otherwise
// the request must satisfy authorizeAgent — the node bearer plus the matching
// X-Panewire-Machine-ID header — and the returned machine id scopes every
// route lookup to lanes that machine owns.
func (h *HubServer) authorizeLanesCaller(request *http.Request) (nodeMachine string, ok bool) {
	if h.authorizeOperator(request) {
		return "", true
	}
	return h.authorizeAgent(request)
}

func (h *HubServer) handlePutLane(writer http.ResponseWriter, request *http.Request) {
	nodeMachine, ok := h.authorizeLanesCaller(request)
	if !ok {
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

	projection, created, err := h.putLane(lane, body, nodeMachine)
	if err != nil {
		if errors.Is(err, errLaneMachineMismatch) {
			writeLaneJSONError(writer, http.StatusForbidden, "lane_machine_mismatch")
			return
		}
		if errors.Is(err, errLaneRequestInvalid) {
			writeLaneJSONError(writer, http.StatusBadRequest, "invalid_lane_request")
			return
		}
		if errors.Is(err, errAuthorityLaneDirectWrite) {
			writeAuthorityLaneError(writer, "authority_lane_direct_write")
			return
		}
		if errors.Is(err, errAuthorityLanePolicyUnavailable) {
			writeAuthorityLaneError(writer, "authority_lane_policy_unavailable")
			return
		}
		if errors.Is(err, errLaneQuotaExceeded) {
			writeLaneJSONError(writer, http.StatusTooManyRequests, "lane_quota_exceeded")
			return
		}
		if errors.Is(err, errLanesFileFull) {
			writeLaneJSONError(writer, http.StatusRequestEntityTooLarge, "lanes_file_full")
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
	nodeMachine, ok := h.authorizeLanesCaller(request)
	if !ok {
		hubUnauthorized(writer)
		return
	}
	lane := request.PathValue("lane")
	if !laneNamePattern.MatchString(lane) {
		writeLaneJSONError(writer, http.StatusBadRequest, "invalid_lane")
		return
	}
	if err := h.deleteLane(lane, nodeMachine); err != nil {
		switch {
		case errors.Is(err, errLaneMachineMismatch):
			writeLaneJSONError(writer, http.StatusForbidden, "lane_machine_mismatch")
		case errors.Is(err, errAuthorityLaneDirectWrite):
			writeAuthorityLaneError(writer, "authority_lane_direct_write")
		case errors.Is(err, errAuthorityLanePolicyUnavailable):
			writeAuthorityLaneError(writer, "authority_lane_policy_unavailable")
		case errors.Is(err, errLaneNotFound):
			writeLaneJSONError(writer, http.StatusNotFound, "lane_not_found")
		case errors.Is(err, errLaneProtected):
			writeLaneJSONError(writer, http.StatusConflict, "lane_protected")
		case errors.Is(err, errLanesFileFull):
			writeLaneJSONError(writer, http.StatusRequestEntityTooLarge, "lanes_file_full")
		default:
			writeLanesWriteError(writer, err)
		}
		return
	}
	writer.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(writer).Encode(hubLaneDeleteResponse{Lane: lane, Removed: true})
}

var errLaneRequestInvalid = errors.New("lane request is invalid")
var errAuthorityLaneDirectWrite = errors.New("authority lane direct write is disabled")

// nodeLaneFieldScope decides field-level scope for a node-authenticated write
// before request validation. The body machine and the standby machine may
// only be the node's own, so every other value — a foreign machine, a machine
// that exists only in some route, or an unknown one — answers the same
// refusal. A sink route has no machine and can never belong to a node. The
// parent must be a lane on the node's machine: its shape errors keep the
// shared 400, while an existing foreign parent and a missing one are the same
// 403, so the field cannot reveal which lanes exist.
func nodeLaneFieldScope(nodeMachine, lane string, body hubLaneWriteRequest, routes map[string]reportRelayRoute) error {
	if body.Sink || body.Machine != nodeMachine {
		return errLaneMachineMismatch
	}
	if body.standbyPresent && body.Standby != nil && body.Standby.Machine != nodeMachine {
		return errLaneMachineMismatch
	}
	if body.Parent != "" {
		if !validReportRelayLaneName(body.Parent) || body.Parent == lane {
			return errLaneRequestInvalid
		}
		if parent, found := routes[body.Parent]; !found || parent.Machine != nodeMachine {
			return errLaneMachineMismatch
		}
	}
	return nil
}

// laneHasForeignChild reports whether any route owned by a machine other than
// the node's — including a sink, which belongs to no machine — names lane as
// its parent. Deleting or re-creating that name would orphan or redirect the
// foreign lane's upward reports.
func laneHasForeignChild(routes map[string]reportRelayRoute, lane, nodeMachine string) bool {
	for _, route := range routes {
		if route.Parent == lane && route.Machine != nodeMachine {
			return true
		}
	}
	return false
}

// laneHasForeignJob reports whether any machine other than the caller's has
// a live job whose owner lane is the lane. Job reports resolve their route
// by owner lane name (resolveRelayRoute), so taking or dropping the name
// redirects reports that belong to another machine. The h.jobs scan covers
// the reconnect window: connect() empties a node's activeJobs until its
// first heartbeat while its still-running jobs stay registered in h.jobs.
// Callers run inside the lanes file lock; this takes h.mu the way
// controlPlaneAuthorityDecision does.
func (h *HubServer) laneHasForeignJob(lane, nodeMachine string) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	for machine, record := range h.nodes {
		if machine == nodeMachine || record == nil {
			continue
		}
		for _, job := range record.activeJobs {
			if job.OwnerLane == lane {
				return true
			}
		}
	}
	for _, job := range h.jobs {
		if job == nil || job.Completed || job.Node == nodeMachine {
			continue
		}
		if job.OwnerLane == lane {
			return true
		}
	}
	return false
}

// laneHasForeignReference joins the two ways a lane name already carries a
// foreign machine's reports: a route that parents to it and a live job that
// reports to it.
func (h *HubServer) laneHasForeignReference(routes map[string]reportRelayRoute, lane, nodeMachine string) bool {
	return laneHasForeignChild(routes, lane, nodeMachine) || h.laneHasForeignJob(lane, nodeMachine)
}

func (h *HubServer) putLane(lane string, body hubLaneWriteRequest, nodeMachine string) (hubLaneProjection, bool, error) {
	if h.reportRelayPath == "" {
		return hubLaneProjection{}, false, errLanesWriteUnconfigured
	}
	var projection hubLaneProjection
	var created bool
	err := withLanesFileLock(h.reportRelayPath, func() error {
		if nodeMachine == "" {
			// The authority decision comes from hub configuration, never from
			// the lanes file, so it is answered before the source is read.
			// Deciding after the read would let a malformed lanes file report
			// lanes_invalid for a write this hub refuses outright.
			if err := h.controlPlaneAuthorityDecision(lane); err != nil {
				return err
			}
		}
		snapshot, err := readLanesFileForWrite(h.reportRelayPath)
		if err != nil {
			return err
		}
		existing, exists := snapshot.Routes[lane]
		if nodeMachine != "" {
			// Field scope runs before validation so the machine, standby and
			// parent fields answer identically for a foreign, a missing and
			// an owned lane.
			if err := nodeLaneFieldScope(nodeMachine, lane, body, snapshot.Routes); err != nil {
				return err
			}
			// The remaining fields validate before the row-scope checks below:
			// the same 400 answers a malformed body whatever the lane, so a
			// PUT is not an existence oracle. The node parent check already
			// ran as scope, so validation sees the parent field cleared.
			remaining := body
			remaining.Parent = ""
			if err := validateLaneWriteRequest(h, lane, remaining, snapshot.Routes); err != nil {
				return err
			}
			// Row scope: an existing row must already belong to the node, and
			// a new row may take no name another machine's reports already
			// route through — neither a lane that names it parent nor a live
			// job that reports to it.
			if exists && existing.Machine != nodeMachine {
				return errLaneMachineMismatch
			}
			if !exists && h.laneHasForeignReference(snapshot.Routes, lane, nodeMachine) {
				return errLaneMachineMismatch
			}
			// Only a lane the node already owns may reveal that it is
			// protected. A foreign or missing authority name returns the same
			// refusal as any other write the node may not make.
			if err := h.controlPlaneAuthorityDecision(lane); err != nil {
				if errors.Is(err, errAuthorityLaneDirectWrite) && !(exists && existing.Machine == nodeMachine) {
					return errLaneMachineMismatch
				}
				return err
			}
		} else {
			if err := validateLaneWriteRequest(h, lane, body, snapshot.Routes); err != nil {
				return err
			}
		}
		route := reportRelayRoute{Machine: body.Machine, Pane: body.Pane, Parent: body.Parent, Deliver: existing.Deliver, Protected: existing.Protected, Standby: existing.Standby}
		// Failover swaps only machine/pane; preserve an omitted standby so a later
		// reverse swap still has its alternate destination.
		if body.standbyPresent {
			route.Standby = body.Standby
		}
		if body.Sink {
			route.Sink = true
			route.Machine = ""
			route.Pane = ""
			route.Standby = nil
		}
		// The quota checks run only after every scope, validation and
		// authority decision above: a write that reaches this line was
		// otherwise valid, so the refusal reveals nothing about foreign or
		// authority names. Updating a row the node already owns is allowed at
		// the cap — the cap bounds creates, not maintenance of existing rows.
		if nodeMachine != "" && !exists && countMachineLanes(snapshot.Routes, nodeMachine) >= h.lanesNodeRowCap {
			return errLaneQuotaExceeded
		}
		if snapshot.Routes == nil {
			snapshot.Routes = make(map[string]reportRelayRoute)
		}
		snapshot.Routes[lane] = route
		contents, err := encodeLanesFile(snapshot)
		if err != nil {
			return err
		}
		// A node write may never grow the shared file into the operator
		// reserve. One that does not enlarge the file is unaffected, so a
		// node already past the line can still shrink its rows back under it.
		if nodeMachine != "" && len(contents) > lanesNodeFileMaxBytes && len(contents) > len(snapshot.Bytes) {
			return errLanesFileFull
		}
		if err := h.replaceLanesFile(h.reportRelayPath, snapshot, contents, lanesBackupNow(h)); err != nil {
			return err
		}
		created = !exists
		projection = hubLaneProjection{Lane: lane, Machine: route.Machine, Pane: route.Pane, Parent: route.Parent, Sink: route.Sink, Standby: route.Standby}
		return nil
	})
	if err != nil {
		return hubLaneProjection{}, false, err
	}
	h.broadcastLanesChanged(lane, map[bool]string{true: "add", false: "update"}[created])
	return projection, created, nil
}

func (h *HubServer) deleteLane(lane, nodeMachine string) error {
	if h.reportRelayPath == "" {
		return errLanesWriteUnconfigured
	}
	err := withLanesFileLock(h.reportRelayPath, func() error {
		if nodeMachine == "" {
			if err := h.controlPlaneAuthorityDecision(lane); err != nil {
				return err
			}
		}
		snapshot, err := readLanesFileForWrite(h.reportRelayPath)
		if err != nil {
			return err
		}
		route, exists := snapshot.Routes[lane]
		if nodeMachine != "" {
			// One refusal for missing and foreign lanes alike: a node cannot
			// probe which it was. The authority guard still applies to a lane
			// the node owns. A lane a foreign machine still routes through —
			// a route that names it parent or a live job that reports to it —
			// is refused the same way: removing it strands those reports and
			// frees the name for another node to claim.
			if !exists || route.Machine != nodeMachine || h.laneHasForeignReference(snapshot.Routes, lane, nodeMachine) {
				return errLaneMachineMismatch
			}
			if err := h.controlPlaneAuthorityDecision(lane); err != nil {
				return err
			}
		}
		if !exists {
			return errLaneNotFound
		}
		if route.Protected {
			return errLaneProtected
		}
		delete(snapshot.Routes, lane)
		contents, err := encodeLanesFile(snapshot)
		if err != nil {
			return err
		}
		return h.replaceLanesFile(h.reportRelayPath, snapshot, contents, lanesBackupNow(h))
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
		if !validReportRelayLaneName(body.Parent) || body.Parent == lane {
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
	if body.standbyPresent && body.Standby != nil {
		if !validReportRelayStandby(*body.Standby) {
			return errLaneRequestInvalid
		}
		// The write path must not persist a standby it would later refuse to
		// promote: require the same machine/pane checks the primary route is
		// held to below, so a failover PUT of standby into machine/pane always
		// succeeds. The hot loader (parseReportRelayRoutes) deliberately keeps
		// the looser validReportRelayStandby-only check above so an operator's
		// hand-edited lanes file is never silently dropped on read.
		if body.Standby.Machine == hubOperatorMachineID || !h.knownLaneMachine(body.Standby.Machine, routes) {
			return errLaneRequestInvalid
		}
		if !validLanePane(body.Standby.Pane) {
			return errLaneRequestInvalid
		}
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

// countMachineLanes counts the rows one machine owns — the same machine
// field the node scope checks enforce — so the cap sees exactly the rows a
// node could have written itself. A sink has no machine and counts for no
// one.
func countMachineLanes(routes map[string]reportRelayRoute, machine string) int {
	count := 0
	for _, route := range routes {
		if route.Machine == machine {
			count++
		}
	}
	return count
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

// Guidance for each authority refusal. A refusal that does not say where to go
// instead pushes an operator toward editing the lanes file by hand, which is
// the one path this API cannot reach — so each code names the next step rather
// than a generic destination.
var authorityLaneErrorGuidance = map[string]string{
	"authority_lane_direct_write": "POST /v1/control-plane/transfer",
	// Transfer is refused in this state too, so pointing at it would send the
	// operator somewhere that is also closed.
	"authority_lane_policy_unavailable": "restore the --control-plane-lanes policy file",
	"authority_bundle_mismatch":         "transfer every configured authority lane in one request",
}

// writeAuthorityLaneError adds a "use" field beside the unchanged "error" key.
func writeAuthorityLaneError(writer http.ResponseWriter, code string) {
	writer.Header().Set("Content-Type", "application/json")
	writer.WriteHeader(http.StatusConflict)
	_ = json.NewEncoder(writer).Encode(struct {
		Error string `json:"error"`
		Use   string `json:"use"`
	}{Error: code, Use: authorityLaneErrorGuidance[code]})
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
	routes, control, err := parseReportRelayRoutesForWrite(contents)
	if err != nil {
		return lanesFileSnapshot{}, fmt.Errorf("%w: %v", errLanesWriteInvalid, err)
	}
	if routes == nil {
		routes = make(map[string]reportRelayRoute)
	}
	return lanesFileSnapshot{Bytes: contents, Routes: routes, Control: control, Exists: true}, nil
}

// parseReportRelayRoutesForWrite adds a loss-prevention precondition to the
// best-effort hot loader. Reads may continue omitting semantically invalid
// operator entries, but an unrelated write must never serialize that filtered
// projection over the source file and silently erase them.
func parseReportRelayRoutesForWrite(contents []byte) (map[string]reportRelayRoute, lanesFileControl, error) {
	var source reportRelayRoutes
	if err := json.Unmarshal(contents, &source); err != nil {
		return nil, lanesFileControl{}, err
	}
	control, err := controlFromLanesFile(source.Control)
	if err != nil {
		return nil, lanesFileControl{}, err
	}
	authoritative := source.Routes
	if source.Lanes != nil {
		authoritative = source.Lanes
	}
	routes, err := parseReportRelayRoutes(contents)
	if err != nil {
		return nil, lanesFileControl{}, err
	}
	if len(routes) != len(authoritative) {
		return nil, lanesFileControl{}, errReportRelayRoutesInvalid
	}
	for lane := range authoritative {
		if _, retained := routes[lane]; !retained {
			return nil, lanesFileControl{}, errReportRelayRoutesInvalid
		}
	}
	return routes, control, nil
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

// encodeLanesFile serializes the route set exactly the way replaceLanesFile
// persists it, so a size check sees the same bytes the file would contain.
func encodeLanesFile(snapshot lanesFileSnapshot) ([]byte, error) {
	var control *lanesFileControl
	if snapshot.Control.shouldPersist() {
		copied := snapshot.Control
		control = &copied
	}
	contents, err := json.MarshalIndent(struct {
		Lanes   map[string]reportRelayRoute `json:"lanes"`
		Control *lanesFileControl           `json:"control,omitempty"`
	}{Lanes: snapshot.Routes, Control: control}, "", "  ")
	if err != nil {
		return nil, fmt.Errorf("%w: encode lanes file: %v", errLanesWriteFailed, err)
	}
	return append(contents, '\n'), nil
}

func (h *HubServer) replaceLanesFile(path string, snapshot lanesFileSnapshot, contents []byte, now time.Time) error {
	// A write whose result would not fit is a refusal, not a malfunction:
	// the caller is over its budget and the current file stays untouched.
	if len(contents) > lanesFileMaxBytes {
		return errLanesFileFull
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
