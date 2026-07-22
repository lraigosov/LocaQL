package server

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"
)

func formatUnixMillis(t time.Time) string {
	return fmt.Sprintf("%d", t.UnixMilli())
}

func (s *Server) handleRoutinesCollection(w http.ResponseWriter, r *http.Request, projectID, datasetID string) {
	switch r.Method {
	case http.MethodGet:
		s.listRoutines(w, r, projectID, datasetID)
	case http.MethodPost:
		s.insertRoutine(w, r, projectID, datasetID)
	default:
		writeError(w, http.StatusMethodNotAllowed, "Method not allowed", "methodNotAllowed")
	}
}

func (s *Server) handleRoutineByID(w http.ResponseWriter, r *http.Request, projectID, datasetID, routineID string) {
	switch r.Method {
	case http.MethodGet:
		s.getRoutine(w, projectID, datasetID, routineID)
	case http.MethodPatch:
		s.patchRoutine(w, r, projectID, datasetID, routineID)
	case http.MethodDelete:
		s.deleteRoutine(w, projectID, datasetID, routineID)
	default:
		writeError(w, http.StatusMethodNotAllowed, "Method not allowed", "methodNotAllowed")
	}
}

func (s *Server) listRoutines(w http.ResponseWriter, r *http.Request, projectID, datasetID string) {
	start, size := parsePagination(r, 2, 1000)
	items, next, _ := s.routines.list(projectID, datasetID, start, size)

	out := make([]map[string]any, 0, len(items))
	for _, rt := range items {
		out = append(out, renderRoutineResource(rt))
	}
	resp := map[string]any{
		"kind":     "bigquery#routineList",
		"routines": out,
	}
	if next >= 0 {
		resp["nextPageToken"] = encodePageToken(next)
	}
	writeJSON(w, http.StatusOK, resp)
}

func (s *Server) insertRoutine(w http.ResponseWriter, r *http.Request, projectID, datasetID string) {
	defer func() {
		if r.Body != nil {
			_ = r.Body.Close()
		}
	}()

	var payload struct {
		RoutineType      string `json:"routineType"`
		Language         string `json:"language"`
		DefinitionBody   string `json:"definitionBody"`
		Description      string `json:"description"`
		RoutineReference struct {
			RoutineID string `json:"routineId"`
		} `json:"routineReference"`
	}
	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
		writeError(w, http.StatusBadRequest, "Invalid request body", "invalid")
		return
	}
	routineID := strings.TrimSpace(payload.RoutineReference.RoutineID)
	if routineID == "" {
		writeError(w, http.StatusBadRequest, "routineReference.routineId is required", "required")
		return
	}
	if strings.TrimSpace(payload.DefinitionBody) == "" {
		writeError(w, http.StatusBadRequest, "definitionBody is required", "required")
		return
	}

	rec, created := s.routines.insert(routineInsert{
		ProjectID:      projectID,
		DatasetID:      datasetID,
		RoutineID:      routineID,
		RoutineType:    payload.RoutineType,
		Language:       payload.Language,
		DefinitionBody: payload.DefinitionBody,
		Description:    payload.Description,
	})
	if !created {
		writeError(w, http.StatusConflict, fmt.Sprintf("Already Exists: Routine %s:%s.%s", projectID, datasetID, routineID), "duplicate")
		return
	}
	writeJSON(w, http.StatusOK, renderRoutineResource(rec))
}

func (s *Server) getRoutine(w http.ResponseWriter, projectID, datasetID, routineID string) {
	rec, ok := s.routines.get(projectID, datasetID, routineID)
	if !ok {
		writeError(w, http.StatusNotFound, fmt.Sprintf("Not found: Routine %s:%s.%s", projectID, datasetID, routineID), "notFound")
		return
	}
	writeJSON(w, http.StatusOK, renderRoutineResource(rec))
}

func (s *Server) patchRoutine(w http.ResponseWriter, r *http.Request, projectID, datasetID, routineID string) {
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

	patch := routinePatch{ProjectID: projectID, DatasetID: datasetID, RoutineID: routineID}
	if errMsg := applyRoutinePatchFields(&patch, raw); errMsg != "" {
		writeError(w, http.StatusBadRequest, errMsg, "invalid")
		return
	}
	if !patch.HasRoutineType && !patch.HasLanguage && !patch.HasDefinitionBody && !patch.HasDescription {
		writeError(w, http.StatusBadRequest, "at least one patchable field is required", "required")
		return
	}

	rec, ok := s.routines.patch(patch)
	if !ok {
		writeError(w, http.StatusNotFound, fmt.Sprintf("Not found: Routine %s:%s.%s", projectID, datasetID, routineID), "notFound")
		return
	}
	writeJSON(w, http.StatusOK, renderRoutineResource(rec))
}

func (s *Server) deleteRoutine(w http.ResponseWriter, projectID, datasetID, routineID string) {
	if !s.routines.delete(projectID, datasetID, routineID) {
		writeError(w, http.StatusNotFound, fmt.Sprintf("Not found: Routine %s:%s.%s", projectID, datasetID, routineID), "notFound")
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func applyRoutinePatchFields(patch *routinePatch, raw map[string]any) string {
	stringFields := []struct {
		key    string
		has    *bool
		target *string
	}{
		{"routineType", &patch.HasRoutineType, &patch.RoutineType},
		{"language", &patch.HasLanguage, &patch.Language},
		{"definitionBody", &patch.HasDefinitionBody, &patch.DefinitionBody},
		{"description", &patch.HasDescription, &patch.Description},
	}
	for _, field := range stringFields {
		v, ok := raw[field.key]
		if !ok {
			continue
		}
		str, isString := v.(string)
		if !isString {
			return field.key + " must be a string"
		}
		*field.has = true
		*field.target = str
	}
	return ""
}

func renderRoutineResource(rt *routineRecord) map[string]any {
	return map[string]any{
		"kind": "bigquery#routine",
		"routineReference": map[string]string{
			"projectId": rt.ProjectID,
			"datasetId": rt.DatasetID,
			"routineId": rt.RoutineID,
		},
		"routineType":      rt.RoutineType,
		"language":         rt.Language,
		"definitionBody":   rt.DefinitionBody,
		"description":      rt.Description,
		"creationTime":     formatUnixMillis(rt.CreatedAt),
		"lastModifiedTime": formatUnixMillis(rt.UpdatedAt),
		"etag":             fmt.Sprintf("\"v%d\"", rt.Version),
	}
}
