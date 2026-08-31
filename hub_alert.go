package panewire

import (
	"context"
	"sort"
	"strings"
	"time"
)

const (
	defaultHubGracePeriod       = 2 * time.Minute
	defaultHubAlertObservations = 2
	hubAlertReasonDisconnected  = "disconnected"
	hubAlertReasonStale         = "stale"
	hubAlertReasonCheckFailed   = "check_failed"
	hubAlertNoCheck             = "none"
)

// HubAlert carries only the three operator-visible identifiers.  Check status,
// timing, command output, and every credential remain outside this boundary.
type HubAlert struct {
	Recovery  bool
	MachineID string
	Reason    string
	Check     string
}

// HubNotifier is the optional server-side notification boundary.
type HubNotifier interface {
	Send(context.Context, HubAlert) error
}

type hubAlertState struct {
	badSince          time.Time
	badRuns           int
	clearRuns         int
	active            bool
	reason            string
	check             string
	incidentDelivered bool
	incidentPending   bool
	recoveryNeeded    bool
	recoveryPending   bool
}

type hubNotification struct {
	key      string
	incident bool
	alert    HubAlert
}

func (h *HubServer) observeNodeAlertLocked(now time.Time, record *hubNodeRecord) []hubNotification {
	if record == nil || !h.watchesAlerts(record.machineID) {
		return nil
	}
	if record.state == "connected" {
		return h.observeHubAlertLocked(now, "node:"+record.machineID, false, time.Time{}, "", hubAlertNoCheck, h.gracePeriod)
	}
	reason := hubAlertReasonDisconnected
	if record.state == "stale" {
		reason = hubAlertReasonStale
	}
	return h.observeHubAlertLocked(now, "node:"+record.machineID, true, record.stateSince, reason, hubAlertNoCheck, h.gracePeriod)
}

func (h *HubServer) observeHeartbeatAlerts(machineID string, heartbeat hubHeartbeatPayload) {
	if !h.watchesAlerts(machineID) {
		return
	}
	now := h.now().UTC()
	notifications := make([]hubNotification, 0, len(heartbeat.Checks))
	h.mu.Lock()
	if _, exists := h.nodes[machineID]; !exists {
		h.mu.Unlock()
		return
	}
	names := make([]string, 0, len(heartbeat.Checks))
	for name := range heartbeat.Checks {
		names = append(names, name)
	}
	sort.Strings(names)
	seen := make(map[string]struct{}, len(names))
	for _, name := range names {
		seen[name] = struct{}{}
		notifications = append(notifications, h.observeHubAlertLocked(now, "check:"+machineID+":"+name, heartbeat.Checks[name] == HubCheckFail, time.Time{}, hubAlertReasonCheckFailed, name, 0)...)
	}
	prefix := "check:" + machineID + ":"
	for key, state := range h.alerts {
		if !strings.HasPrefix(key, prefix) {
			continue
		}
		name := strings.TrimPrefix(key, prefix)
		if _, present := seen[name]; present {
			continue
		}
		notifications = append(notifications, h.observeHubAlertLocked(now, key, false, time.Time{}, hubAlertReasonCheckFailed, state.check, 0)...)
	}
	h.mu.Unlock()
	h.dispatchHubNotifications(notifications)
}

// observeHubAlertLocked applies the shared two-observation debounce.  For a
// presence issue, problemSince is the actual disconnect/stale transition; for
// a check it is captured at the first failing heartbeat.
func (h *HubServer) observeHubAlertLocked(now time.Time, key string, problem bool, problemSince time.Time, reason, check string, grace time.Duration) []hubNotification {
	state := h.alerts[key]
	if state == nil {
		state = &hubAlertState{}
		h.alerts[key] = state
	}
	if problem {
		state.clearRuns = 0
		state.recoveryNeeded = false
		if !state.active {
			if state.badSince.IsZero() {
				state.badSince = problemSince
				if state.badSince.IsZero() {
					state.badSince = now
				}
			}
			if now.Sub(state.badSince) >= grace {
				state.badRuns++
			}
			if state.badRuns >= h.alertObservations {
				state.active = true
				state.badRuns = 0
				state.badSince = time.Time{}
				state.reason = reason
				state.check = check
				state.incidentDelivered = false
			}
		}
	} else {
		state.badSince = time.Time{}
		state.badRuns = 0
		if state.active {
			state.clearRuns++
			if state.clearRuns >= h.alertObservations {
				state.active = false
				state.clearRuns = 0
				if state.incidentDelivered {
					state.recoveryNeeded = true
				}
			}
		}
	}
	return h.pendingHubNotificationLocked(key, state)
}

func (h *HubServer) pendingHubNotificationLocked(key string, state *hubAlertState) []hubNotification {
	if h.notifier == nil {
		return nil
	}
	if state.active && !state.incidentDelivered && !state.incidentPending {
		state.incidentPending = true
		return []hubNotification{{
			key: key, incident: true,
			alert: HubAlert{MachineID: hubAlertMachineID(key), Reason: state.reason, Check: state.check},
		}}
	}
	if !state.active && state.recoveryNeeded && !state.recoveryPending {
		state.recoveryPending = true
		return []hubNotification{{
			key: key, incident: false,
			alert: HubAlert{Recovery: true, MachineID: hubAlertMachineID(key), Reason: state.reason, Check: state.check},
		}}
	}
	return nil
}

func hubAlertMachineID(key string) string {
	if machineID, found := strings.CutPrefix(key, "node:"); found {
		return machineID
	}
	if remainder, found := strings.CutPrefix(key, "check:"); found {
		machineID, _, _ := strings.Cut(remainder, ":")
		return machineID
	}
	return "unknown"
}

func (h *HubServer) dispatchHubNotifications(notifications []hubNotification) {
	for _, notification := range notifications {
		notifier := h.notifier
		if notifier == nil {
			continue
		}
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		err := notifier.Send(ctx, notification.alert)
		cancel()
		if err != nil {
			h.logger.Warn("hub Telegram notification failed")
		}
		h.mu.Lock()
		state := h.alerts[notification.key]
		if state != nil {
			if notification.incident {
				state.incidentPending = false
				if err == nil && state.active {
					state.incidentDelivered = true
				}
			} else {
				state.recoveryPending = false
				if err == nil && !state.active && state.recoveryNeeded {
					state.recoveryNeeded = false
					state.incidentDelivered = false
					state.reason = ""
					state.check = ""
				}
			}
		}
		h.mu.Unlock()
	}
}

func validHubAlertReason(reason string) bool {
	return reason == hubAlertReasonDisconnected || reason == hubAlertReasonStale || reason == hubAlertReasonCheckFailed
}

func formatHubAlert(alert HubAlert) string {
	machineID := alert.MachineID
	if !machineIDPattern.MatchString(machineID) {
		machineID = "unknown"
	}
	reason := alert.Reason
	if !validHubAlertReason(reason) {
		reason = "unknown"
	}
	if alert.Recovery {
		reason = "recovered_" + reason
	}
	check := alert.Check
	if check != hubAlertNoCheck && !hubCheckNamePattern.MatchString(check) {
		check = "unknown"
	}
	return "machine: " + machineID + "\nreason: " + reason + "\ncheck: " + check
}
