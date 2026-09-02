package panewire

import (
	"encoding/json"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// authorizeContext identifies the credential without trusting a caller
// supplied machine header. The operator is represented as "operator" and may
// manage every record; a machine identity can read every record and write
// records stamped with its own stable ID.
func (h *HubServer) authorizeContext(request *http.Request) (string, bool) {
	token := hubRequestBearerToken(request)
	if hubTokenMatches(h.tokens[hubOperatorMachineID], token) {
		return hubOperatorMachineID, true
	}
	for machineID, expected := range h.tokens {
		if machineID != hubOperatorMachineID && hubTokenMatches(expected, token) {
			return machineID, true
		}
	}
	return "", false
}

func contextJSON(writer http.ResponseWriter, status int, value any) {
	writer.Header().Set("Content-Type", "application/json")
	writer.WriteHeader(status)
	_ = json.NewEncoder(writer).Encode(value)
}
func contextError(writer http.ResponseWriter, status int, code string) {
	contextJSON(writer, status, map[string]string{"error": code})
}
func contextStoreError(writer http.ResponseWriter, err error) {
	if secret, found := strings.CutPrefix(err.Error(), "secret_like_content:"); found {
		contextJSON(writer, http.StatusBadRequest, map[string]string{"error": "secret_like_content", "pattern": secret})
		return
	}
	contextError(writer, http.StatusBadRequest, "invalid_context")
}
func (h *HubServer) handleContextCheckpoints(writer http.ResponseWriter, request *http.Request) {
	machineID, ok := h.authorizeContext(request)
	if !ok {
		hubUnauthorized(writer)
		return
	}
	if request.Method == http.MethodPost {
		defer request.Body.Close()
		var input struct {
			Session string            `json:"session"`
			Kind    string            `json:"kind"`
			Title   string            `json:"title"`
			Body    string            `json:"body"`
			Refs    map[string]string `json:"refs"`
		}
		decoder := json.NewDecoder(io.LimitReader(request.Body, contextMaxBytes+4096))
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(&input); err != nil {
			contextError(writer, http.StatusBadRequest, "invalid_context")
			return
		}
		item, err := h.contextStore.CreateCheckpoint(request.Context(), ContextCheckpoint{Session: input.Session, Kind: input.Kind, Title: input.Title, Body: input.Body, Refs: input.Refs, CreatedBy: machineID})
		if err != nil {
			contextStoreError(writer, err)
			return
		}
		contextJSON(writer, http.StatusCreated, item)
		return
	}
	limit := 3
	if raw := request.URL.Query().Get("limit"); raw != "" {
		parsed, err := strconv.Atoi(raw)
		if err != nil || parsed < 1 || parsed > contextCheckpointKeep {
			contextError(writer, http.StatusBadRequest, "invalid_context")
			return
		}
		limit = parsed
	}
	items, err := h.contextStore.RecentCheckpoints(request.Context(), request.URL.Query().Get("session"), request.URL.Query().Get("kind"), limit)
	if err != nil {
		contextStoreError(writer, err)
		return
	}
	contextJSON(writer, http.StatusOK, map[string]any{"checkpoints": items})
}

func (h *HubServer) handleContextSearch(writer http.ResponseWriter, request *http.Request) {
	if _, ok := h.authorizeContext(request); !ok {
		hubUnauthorized(writer)
		return
	}
	query := request.URL.Query().Get("q")
	scope := request.URL.Query().Get("scope")
	if scope == "" {
		scope = "all"
	}
	limit := 20
	if raw := request.URL.Query().Get("limit"); raw != "" {
		parsed, err := strconv.Atoi(raw)
		if err != nil || parsed < 1 || parsed > 100 {
			contextError(writer, http.StatusBadRequest, "invalid_context")
			return
		}
		limit = parsed
	}
	items, err := h.contextStore.Search(request.Context(), query, scope, request.URL.Query().Get("session"), request.URL.Query().Get("kind"), limit)
	if err != nil {
		contextStoreError(writer, err)
		return
	}
	contextJSON(writer, http.StatusOK, map[string]any{"results": items})
}

func (h *HubServer) handleContextDocuments(writer http.ResponseWriter, request *http.Request) {
	if _, ok := h.authorizeContext(request); !ok {
		hubUnauthorized(writer)
		return
	}
	limit := 100
	if raw := request.URL.Query().Get("limit"); raw != "" {
		parsed, err := strconv.Atoi(raw)
		if err != nil || parsed < 1 || parsed > 1000 {
			contextError(writer, http.StatusBadRequest, "invalid_context")
			return
		}
		limit = parsed
	}
	items, err := h.contextStore.ListDocuments(request.Context(), request.URL.Query().Get("prefix"), request.URL.Query().Get("kind"), request.URL.Query().Get("session"), limit)
	if err != nil {
		contextStoreError(writer, err)
		return
	}
	contextJSON(writer, http.StatusOK, map[string]any{"documents": items})
}

func (h *HubServer) handleContextDocumentItem(writer http.ResponseWriter, request *http.Request) {
	machineID, ok := h.authorizeContext(request)
	if !ok {
		hubUnauthorized(writer)
		return
	}
	key := request.PathValue("key")
	switch request.Method {
	case http.MethodGet:
		item, found, err := h.contextStore.GetDocument(request.Context(), key)
		if err != nil {
			contextStoreError(writer, err)
			return
		}
		if !found {
			contextError(writer, http.StatusNotFound, "not_found")
			return
		}
		contextJSON(writer, http.StatusOK, item)
	case http.MethodPut:
		defer request.Body.Close()
		var input struct {
			Kind    string `json:"kind"`
			Session string `json:"session"`
			Job     string `json:"job"`
			Body    string `json:"body"`
		}
		decoder := json.NewDecoder(io.LimitReader(request.Body, contextDocumentMaxBytes+4096))
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(&input); err != nil {
			contextError(writer, http.StatusBadRequest, "invalid_context")
			return
		}
		item, changed, err := h.contextStore.PutDocument(request.Context(), ContextDocument{Key: key, Kind: input.Kind, Session: input.Session, Job: input.Job, Body: input.Body, CreatedBy: machineID})
		if err != nil {
			contextStoreError(writer, err)
			return
		}
		contextJSON(writer, http.StatusOK, map[string]any{"document": item, "changed": changed})
	default:
		contextError(writer, http.StatusMethodNotAllowed, "method_not_allowed")
	}
}
func (h *HubServer) handleContextMemoryList(writer http.ResponseWriter, request *http.Request) {
	if _, ok := h.authorizeContext(request); !ok {
		hubUnauthorized(writer)
		return
	}
	items, err := h.contextStore.ListMemory(request.Context(), request.PathValue("agent"), false)
	if err != nil {
		contextStoreError(writer, err)
		return
	}
	// List is intentionally metadata-only. Content is fetched one item at a
	// time so an inventory cannot accidentally become a bulk data export.
	type summary struct {
		Name        string    `json:"name"`
		Description string    `json:"description"`
		MemoryType  string    `json:"type"`
		UpdatedAt   time.Time `json:"updated_at"`
	}
	out := make([]summary, 0, len(items))
	for _, item := range items {
		out = append(out, summary{Name: item.Name, Description: item.Description, MemoryType: item.MemoryType, UpdatedAt: item.UpdatedAt})
	}
	contextJSON(writer, http.StatusOK, map[string]any{"memory": out})
}
func (h *HubServer) handleContextMemoryItem(writer http.ResponseWriter, request *http.Request) {
	machineID, ok := h.authorizeContext(request)
	if !ok {
		hubUnauthorized(writer)
		return
	}
	agent, name := request.PathValue("agent"), request.PathValue("name")
	switch request.Method {
	case http.MethodGet:
		item, found, err := h.contextStore.GetMemory(request.Context(), agent, name)
		if err != nil {
			contextStoreError(writer, err)
			return
		}
		if !found {
			contextError(writer, http.StatusNotFound, "not_found")
			return
		}
		contextJSON(writer, http.StatusOK, item)
	case http.MethodPut:
		defer request.Body.Close()
		var input struct {
			Description string `json:"description"`
			Type        string `json:"type"`
			Content     string `json:"content"`
		}
		decoder := json.NewDecoder(io.LimitReader(request.Body, contextMaxBytes+4096))
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(&input); err != nil {
			contextError(writer, http.StatusBadRequest, "invalid_context")
			return
		}
		item, err := h.contextStore.PutMemory(request.Context(), ContextMemory{Agent: agent, Name: name, Description: input.Description, MemoryType: input.Type, Content: input.Content, UpdatedBy: machineID})
		if err != nil {
			contextStoreError(writer, err)
			return
		}
		contextJSON(writer, http.StatusOK, item)
	case http.MethodDelete:
		if err := h.contextStore.DeleteMemory(request.Context(), agent, name); err != nil {
			contextStoreError(writer, err)
			return
		}
		writer.WriteHeader(http.StatusNoContent)
	default:
		contextError(writer, http.StatusMethodNotAllowed, "method_not_allowed")
	}
}
