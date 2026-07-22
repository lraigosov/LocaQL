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
		Arguments []struct {
			Name     string `json:"name"`
			DataType string `json:"dataType"`
		} `json:"arguments"`
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
		Arguments:      parseRoutineArguments(payload.Arguments),
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
	if !patch.HasRoutineType && !patch.HasLanguage && !patch.HasDefinitionBody && !patch.HasDescription && !patch.HasArguments {
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
	if v, ok := raw["arguments"]; ok {
		args, errMsg := decodeRoutineArguments(v)
		if errMsg != "" {
			return errMsg
		}
		patch.HasArguments = true
		patch.Arguments = args
	}
	return ""
}

// parseRoutineArguments converts the JSON-decoded arguments payload (already
// typed via json.Decoder into insertRoutine's anonymous struct slice) into
// routineArgument records.
func parseRoutineArguments(items []struct {
	Name     string `json:"name"`
	DataType string `json:"dataType"`
}) []routineArgument {
	if len(items) == 0 {
		return nil
	}
	out := make([]routineArgument, 0, len(items))
	for _, item := range items {
		out = append(out, routineArgument{Name: strings.TrimSpace(item.Name), DataType: strings.TrimSpace(item.DataType)})
	}
	return out
}

// decodeRoutineArguments handles the patch path, where the body is decoded
// into a generic map[string]any instead of a typed struct, so "arguments"
// arrives as []any of map[string]any entries that need manual type assertion.
func decodeRoutineArguments(v any) ([]routineArgument, string) {
	list, ok := v.([]any)
	if !ok {
		return nil, "arguments must be an array"
	}
	out := make([]routineArgument, 0, len(list))
	for _, raw := range list {
		entry, ok := raw.(map[string]any)
		if !ok {
			return nil, "arguments entries must be objects with name/dataType"
		}
		name, _ := entry["name"].(string)
		dataType, _ := entry["dataType"].(string)
		out = append(out, routineArgument{Name: strings.TrimSpace(name), DataType: strings.TrimSpace(dataType)})
	}
	return out, ""
}

func renderRoutineResource(rt *routineRecord) map[string]any {
	arguments := make([]map[string]string, 0, len(rt.Arguments))
	for _, arg := range rt.Arguments {
		arguments = append(arguments, map[string]string{"name": arg.Name, "dataType": arg.DataType})
	}
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
		"arguments":        arguments,
		"creationTime":     formatUnixMillis(rt.CreatedAt),
		"lastModifiedTime": formatUnixMillis(rt.UpdatedAt),
		"etag":             fmt.Sprintf("\"v%d\"", rt.Version),
	}
}
