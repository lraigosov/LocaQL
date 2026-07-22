package server

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
)

func (s *Server) handleModelsCollection(w http.ResponseWriter, r *http.Request, projectID, datasetID string) {
	switch r.Method {
	case http.MethodGet:
		s.listModels(w, r, projectID, datasetID)
	case http.MethodPost:
		s.insertModel(w, r, projectID, datasetID)
	default:
		writeError(w, http.StatusMethodNotAllowed, "Method not allowed", "methodNotAllowed")
	}
}

func (s *Server) handleModelByID(w http.ResponseWriter, r *http.Request, projectID, datasetID, modelID string) {
	switch r.Method {
	case http.MethodGet:
		s.getModel(w, projectID, datasetID, modelID)
	case http.MethodPatch:
		s.patchModel(w, r, projectID, datasetID, modelID)
	case http.MethodDelete:
		s.deleteModel(w, projectID, datasetID, modelID)
	default:
		writeError(w, http.StatusMethodNotAllowed, "Method not allowed", "methodNotAllowed")
	}
}

func (s *Server) listModels(w http.ResponseWriter, r *http.Request, projectID, datasetID string) {
	start, size := parsePagination(r, 2, 1000)
	items, next, _ := s.models.list(projectID, datasetID, start, size)

	out := make([]map[string]any, 0, len(items))
	for _, m := range items {
		out = append(out, renderModelResource(m))
	}
	resp := map[string]any{
		"kind":   "bigquery#modelList",
		"models": out,
	}
	if next >= 0 {
		resp["nextPageToken"] = encodePageToken(next)
	}
	writeJSON(w, http.StatusOK, resp)
}

func (s *Server) insertModel(w http.ResponseWriter, r *http.Request, projectID, datasetID string) {
	defer func() {
		if r.Body != nil {
			_ = r.Body.Close()
		}
	}()

	var payload struct {
		ModelType     string            `json:"modelType"`
		FriendlyName  string            `json:"friendlyName"`
		Description   string            `json:"description"`
		Labels        map[string]string `json:"labels"`
		ModelReference struct {
			ModelID string `json:"modelId"`
		} `json:"modelReference"`
	}
	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
		writeError(w, http.StatusBadRequest, "Invalid request body", "invalid")
		return
	}
	modelID := strings.TrimSpace(payload.ModelReference.ModelID)
	if modelID == "" {
		writeError(w, http.StatusBadRequest, "modelReference.modelId is required", "required")
		return
	}

	rec, created := s.models.insert(modelInsert{
		ProjectID:    projectID,
		DatasetID:    datasetID,
		ModelID:      modelID,
		ModelType:    payload.ModelType,
		FriendlyName: payload.FriendlyName,
		Description:  payload.Description,
		Labels:       payload.Labels,
	})
	if !created {
		writeError(w, http.StatusConflict, fmt.Sprintf("Already Exists: Model %s:%s.%s", projectID, datasetID, modelID), "duplicate")
		return
	}
	writeJSON(w, http.StatusOK, renderModelResource(rec))
}

func (s *Server) getModel(w http.ResponseWriter, projectID, datasetID, modelID string) {
	rec, ok := s.models.get(projectID, datasetID, modelID)
	if !ok {
		writeError(w, http.StatusNotFound, fmt.Sprintf("Not found: Model %s:%s.%s", projectID, datasetID, modelID), "notFound")
		return
	}
	writeJSON(w, http.StatusOK, renderModelResource(rec))
}

func (s *Server) patchModel(w http.ResponseWriter, r *http.Request, projectID, datasetID, modelID string) {
	defer func() {
		if r.Body != nil {
			_ = r.Body.Close()
		}
	}()

	var raw map[string]any
	if err := json.NewDecoder(r.Body).Decode(&raw); err != nil {
		writeError(w, http.StatusBadRequest, "Invalid request body", "invalid")
		return
	}

	patch := modelPatch{ProjectID: projectID, DatasetID: datasetID, ModelID: modelID}
	if v, ok := raw["friendlyName"]; ok {
		str, isString := v.(string)
		if !isString {
			writeError(w, http.StatusBadRequest, "friendlyName must be a string", "invalid")
			return
		}
		patch.HasFriendlyName = true
		patch.FriendlyName = str
	}
	if v, ok := raw["description"]; ok {
		str, isString := v.(string)
		if !isString {
			writeError(w, http.StatusBadRequest, "description must be a string", "invalid")
			return
		}
		patch.HasDescription = true
		patch.Description = str
	}
	if v, ok := raw["labels"]; ok {
		labels, errMsg := parseLabelsPatchValue(v)
		if errMsg != "" {
			writeError(w, http.StatusBadRequest, errMsg, "invalid")
			return
		}
		patch.HasLabels = true
		patch.Labels = labels
	}
	if !patch.HasFriendlyName && !patch.HasDescription && !patch.HasLabels {
		writeError(w, http.StatusBadRequest, "at least one patchable field is required", "required")
		return
	}

	rec, ok := s.models.patch(patch)
	if !ok {
		writeError(w, http.StatusNotFound, fmt.Sprintf("Not found: Model %s:%s.%s", projectID, datasetID, modelID), "notFound")
		return
	}
	writeJSON(w, http.StatusOK, renderModelResource(rec))
}

func (s *Server) deleteModel(w http.ResponseWriter, projectID, datasetID, modelID string) {
	if !s.models.delete(projectID, datasetID, modelID) {
		writeError(w, http.StatusNotFound, fmt.Sprintf("Not found: Model %s:%s.%s", projectID, datasetID, modelID), "notFound")
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// renderModelResource never reports training runs or evaluation metrics:
// there is no ML training/inference backend behind this resource, only
// metadata CRUD, and inventing training data would misrepresent that.
func renderModelResource(m *modelRecord) map[string]any {
	resp := map[string]any{
		"kind": "bigquery#model",
		"modelReference": map[string]string{
			"projectId": m.ProjectID,
			"datasetId": m.DatasetID,
			"modelId":   m.ModelID,
		},
		"modelType":        m.ModelType,
		"creationTime":     formatUnixMillis(m.CreatedAt),
		"lastModifiedTime": formatUnixMillis(m.UpdatedAt),
		"etag":             fmt.Sprintf("\"v%d\"", m.Version),
	}
	if m.FriendlyName != "" {
		resp["friendlyName"] = m.FriendlyName
	}
	if m.Description != "" {
		resp["description"] = m.Description
	}
	if len(m.Labels) > 0 {
		resp["labels"] = m.Labels
	}
	return resp
}
