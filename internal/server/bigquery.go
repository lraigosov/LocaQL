package server

import (
	"bufio"
	"bytes"
	"encoding/base64"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/linkedin/goavro/v2"
	"github.com/parquet-go/parquet-go"
)

type dataset struct {
	ID string
}

type table struct {
	ID string
}

type job struct {
	ID string
}

type tableRow struct {
	Values []string
}

func (s *Server) bigQueryV2(w http.ResponseWriter, r *http.Request) {
	path := strings.TrimPrefix(r.URL.Path, "/bigquery/v2/projects/")
	parts := strings.Split(strings.Trim(path, "/"), "/")
	if len(parts) < 2 {
		writeError(w, http.StatusNotFound, "Not found: Project identifier missing", "notFound")
		return
	}

	projectID := parts[0]
	scope := parts[1]

	switch scope {
	case "datasets":
		if s.handleDatasetsScope(w, r, projectID, parts) {
			return
		}
	case "jobs":
		if len(parts) == 2 {
			if r.Method == http.MethodGet {
				s.listJobs(w, r, projectID)
				return
			}
			if r.Method == http.MethodPost {
				s.insertJob(w, r, projectID)
				return
			}
			writeError(w, http.StatusMethodNotAllowed, "Method not allowed", "methodNotAllowed")
			return
		}
		if len(parts) == 3 && r.Method == http.MethodGet {
			s.getJob(w, r, projectID, parts[2])
			return
		}
		if len(parts) == 4 && parts[3] == "queryResults" && r.Method == http.MethodGet {
			s.getQueryResults(w, r, projectID, parts[2])
			return
		}
		if len(parts) == 4 && parts[3] == "cancel" && r.Method == http.MethodPost {
			s.cancelJob(w, r, projectID, parts[2])
			return
		}
	case "queries":
		if len(parts) == 2 && r.Method == http.MethodPost {
			s.handleJobsQuery(w, r, projectID)
			return
		}
		if len(parts) == 3 && r.Method == http.MethodGet {
			// GET /queries/{jobId} is an alias for jobs.getQueryResults
			s.getQueryResults(w, r, projectID, parts[2])
			return
		}
	case "tabledata":
		if r.Method != http.MethodGet {
			writeError(w, http.StatusMethodNotAllowed, "Method not allowed", "methodNotAllowed")
			return
		}
		if len(parts) == 5 && parts[4] == "data" {
			s.listTableData(w, r, projectID, parts[2], parts[3])
			return
		}
	}

	writeError(w, http.StatusNotFound, "Not found", "notFound")
}

// requireDatasetExists writes a 404 and returns false if the dataset is
// missing, so callers can early-return in one line.
func (s *Server) requireDatasetExists(w http.ResponseWriter, projectID, datasetID string) bool {
	if s.datasets.exists(projectID, datasetID) {
		return true
	}
	writeError(w, http.StatusNotFound, fmt.Sprintf("Not found: Dataset %s:%s", projectID, datasetID), "notFound")
	return false
}

// handleDatasetsScope dispatches every /datasets/... path shape (the dataset
// collection/resource itself, plus its tables/routines/models sub-resources).
// It reports whether it handled the request so bigQueryV2 can fall through to
// its shared 404 for unmatched shapes.
func (s *Server) handleDatasetsScope(w http.ResponseWriter, r *http.Request, projectID string, parts []string) bool {
	if len(parts) == 2 {
		s.handleDatasetsCollection(w, r, projectID)
		return true
	}
	if len(parts) == 3 {
		s.handleDatasetByID(w, r, projectID, parts[2])
		return true
	}
	if len(parts) < 4 {
		return false
	}

	datasetID := parts[2]
	switch parts[3] {
	case "tables":
		return s.dispatchDatasetSubResource(w, r, projectID, datasetID, parts, s.handleTablesCollection, s.handleTableByID)
	case "routines":
		return s.dispatchDatasetSubResource(w, r, projectID, datasetID, parts, s.handleRoutinesCollection, s.handleRoutineByID)
	case "models":
		return s.dispatchDatasetSubResource(w, r, projectID, datasetID, parts, s.handleModelsCollection, s.handleModelByID)
	default:
		return false
	}
}

// dispatchDatasetSubResource handles the common /datasets/{id}/{subResource}
// and /datasets/{id}/{subResource}/{itemID} shapes shared by tables, routines
// and models, after checking the parent dataset exists.
func (s *Server) dispatchDatasetSubResource(
	w http.ResponseWriter,
	r *http.Request,
	projectID, datasetID string,
	parts []string,
	handleCollection func(http.ResponseWriter, *http.Request, string, string),
	handleByID func(http.ResponseWriter, *http.Request, string, string, string),
) bool {
	if !s.requireDatasetExists(w, projectID, datasetID) {
		return true
	}
	if len(parts) == 4 {
		handleCollection(w, r, projectID, datasetID)
		return true
	}
	if len(parts) == 5 {
		handleByID(w, r, projectID, datasetID, parts[4])
		return true
	}
	return false
}

func (s *Server) handleDatasetsCollection(w http.ResponseWriter, r *http.Request, projectID string) {
	switch r.Method {
	case http.MethodGet:
		s.listDatasets(w, r, projectID)
	case http.MethodPost:
		s.insertDataset(w, r, projectID)
	default:
		writeError(w, http.StatusMethodNotAllowed, "Method not allowed", "methodNotAllowed")
	}
}

func (s *Server) handleDatasetByID(w http.ResponseWriter, r *http.Request, projectID, datasetID string) {
	switch r.Method {
	case http.MethodGet:
		s.getDataset(w, projectID, datasetID)
	case http.MethodPatch:
		s.patchDataset(w, r, projectID, datasetID)
	case http.MethodDelete:
		s.deleteDataset(w, r, projectID, datasetID)
	default:
		writeError(w, http.StatusMethodNotAllowed, "Method not allowed", "methodNotAllowed")
	}
}

func (s *Server) listDatasets(w http.ResponseWriter, r *http.Request, projectID string) {
	start, size := parsePagination(r, 2, 1000)
	items, next, version := s.datasets.list(projectID, start, size)

	if s.checkETag(w, r, version) {
		return
	}

	out := make([]map[string]any, 0, len(items))
	for _, ds := range items {
		out = append(out, renderDatasetResource(ds))
	}

	resp := map[string]any{
		"kind":     "bigquery#datasetList",
		"datasets": out,
		"etag":     fmt.Sprintf("\"v%d\"", version),
	}
	if next >= 0 {
		resp["nextPageToken"] = encodePageToken(next)
	}
	writeJSON(w, http.StatusOK, resp)
}

func (s *Server) insertDataset(w http.ResponseWriter, r *http.Request, projectID string) {
	defer func() {
		if r.Body != nil {
			_ = r.Body.Close()
		}
	}()

	var payload struct {
		FriendlyName             string            `json:"friendlyName"`
		Location                 string            `json:"location"`
		Labels                   map[string]string `json:"labels"`
		DefaultTableExpirationMs any               `json:"defaultTableExpirationMs"`
		DatasetReference         struct {
			DatasetID string `json:"datasetId"`
		} `json:"datasetReference"`
	}
	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
		writeError(w, http.StatusBadRequest, "Invalid request body", "invalid")
		return
	}
	datasetID := strings.TrimSpace(payload.DatasetReference.DatasetID)
	if datasetID == "" {
		writeError(w, http.StatusBadRequest, "datasetReference.datasetId is required", "required")
		return
	}
	expirationMs, ok := parseFlexibleInt64FromAny(payload.DefaultTableExpirationMs)
	if payload.DefaultTableExpirationMs != nil && !ok {
		writeError(w, http.StatusBadRequest, "defaultTableExpirationMs must be a numeric string or number", "invalid")
		return
	}

	rec, created := s.datasets.insert(datasetInsert{
		ProjectID:                projectID,
		DatasetID:                datasetID,
		FriendlyName:             payload.FriendlyName,
		Location:                 payload.Location,
		Labels:                   payload.Labels,
		DefaultTableExpirationMs: expirationMs,
	})
	if !created {
		writeError(w, http.StatusConflict, fmt.Sprintf("Already Exists: Dataset %s:%s", projectID, datasetID), "duplicate")
		return
	}
	writeJSON(w, http.StatusOK, renderDatasetResource(rec))
}

// parseFlexibleInt64FromAny accepts either a JSON number or a JSON string
// containing digits, matching how official BigQuery clients encode int64
// fields as strings while manual/test payloads often send plain numbers.
func parseFlexibleInt64FromAny(v any) (int64, bool) {
	switch val := v.(type) {
	case nil:
		return 0, true
	case string:
		n, err := strconv.ParseInt(strings.TrimSpace(val), 10, 64)
		return n, err == nil
	case float64:
		return int64(val), true
	default:
		return 0, false
	}
}

func (s *Server) getDataset(w http.ResponseWriter, projectID, datasetID string) {
	rec, ok := s.datasets.get(projectID, datasetID)
	if !ok {
		writeError(w, http.StatusNotFound, fmt.Sprintf("Not found: Dataset %s:%s", projectID, datasetID), "notFound")
		return
	}
	writeJSON(w, http.StatusOK, renderDatasetResource(rec))
}

func (s *Server) deleteDataset(w http.ResponseWriter, r *http.Request, projectID, datasetID string) {
	deleteContents := r.URL.Query().Get("deleteContents") == "true"
	tableCount := s.tables.datasetTableCount(projectID, datasetID)
	if tableCount > 0 && !deleteContents {
		writeError(w, http.StatusBadRequest, fmt.Sprintf("Dataset %s:%s still contains %d table(s); pass deleteContents=true to delete them along with the dataset", projectID, datasetID, tableCount), "invalid")
		return
	}
	if !s.datasets.delete(projectID, datasetID) {
		writeError(w, http.StatusNotFound, fmt.Sprintf("Not found: Dataset %s:%s", projectID, datasetID), "notFound")
		return
	}
	if tableCount > 0 {
		s.tables.deleteAllForDataset(projectID, datasetID)
	}
	w.WriteHeader(http.StatusNoContent)
}

// undeleteDataset is a LocaQL-only convenience endpoint, deliberately kept
// outside the /bigquery/v2/ namespace: BigQuery's REST API has no public
// dataset undelete contract, so exposing this under bigquery/v2 would invent
// a BigQuery endpoint that doesn't exist. It restores dataset metadata
// (friendlyName, location, labels) from the tombstone left by the most
// recent delete; table contents are never restored.
func (s *Server) undeleteDataset(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "Method not allowed", "methodNotAllowed")
		return
	}
	defer func() {
		if r.Body != nil {
			_ = r.Body.Close()
		}
	}()

	var payload struct {
		ProjectID string `json:"projectId"`
		DatasetID string `json:"datasetId"`
	}
	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
		writeError(w, http.StatusBadRequest, "Invalid request body", "invalid")
		return
	}
	projectID := strings.TrimSpace(payload.ProjectID)
	datasetID := strings.TrimSpace(payload.DatasetID)
	if projectID == "" || datasetID == "" {
		writeError(w, http.StatusBadRequest, "projectId and datasetId are required", "required")
		return
	}

	if s.datasets.exists(projectID, datasetID) {
		writeError(w, http.StatusConflict, fmt.Sprintf("Already Exists: Dataset %s:%s", projectID, datasetID), "duplicate")
		return
	}
	rec, ok := s.datasets.undelete(projectID, datasetID)
	if !ok {
		writeError(w, http.StatusNotFound, fmt.Sprintf("No deleted dataset tombstone found for %s:%s", projectID, datasetID), "notFound")
		return
	}
	writeJSON(w, http.StatusOK, renderDatasetResource(rec))
}

func (s *Server) patchDataset(w http.ResponseWriter, r *http.Request, projectID, datasetID string) {
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

	patch := datasetPatch{ProjectID: projectID, DatasetID: datasetID}

	if err := applyDatasetPatchFields(&patch, raw); err != "" {
		writeError(w, http.StatusBadRequest, err, "invalid")
		return
	}

	if !patch.HasFriendlyName && !patch.HasLocation && !patch.HasLabels && !patch.HasDefaultTableExpirationMs {
		writeError(w, http.StatusBadRequest, "at least one patchable field is required", "required")
		return
	}

	rec, ok := s.datasets.patch(patch)
	if !ok {
		writeError(w, http.StatusNotFound, fmt.Sprintf("Not found: Dataset %s:%s", projectID, datasetID), "notFound")
		return
	}

	writeJSON(w, http.StatusOK, renderDatasetResource(rec))
}

// applyDatasetPatchFields applies each patchable field present in raw onto
// patch, returning a non-empty error message on the first invalid field.
// Splitting per-field parsing out of patchDataset keeps each check flat
// instead of nesting them all in one handler body.
func applyDatasetPatchFields(patch *datasetPatch, raw map[string]any) string {
	if v, ok := raw["friendlyName"]; ok {
		str, isString := v.(string)
		if !isString {
			return "friendlyName must be a string"
		}
		patch.HasFriendlyName = true
		patch.FriendlyName = str
	}

	if v, ok := raw["location"]; ok {
		str, isString := v.(string)
		if !isString {
			return "location must be a string"
		}
		patch.HasLocation = true
		patch.Location = str
	}

	if v, ok := raw["labels"]; ok {
		labels, errMsg := parseLabelsPatchValue(v)
		if errMsg != "" {
			return errMsg
		}
		patch.HasLabels = true
		patch.Labels = labels
	}

	if v, ok := raw["defaultTableExpirationMs"]; ok {
		n, parsed := parseFlexibleInt64FromAny(v)
		if !parsed {
			return "defaultTableExpirationMs must be a numeric string or number"
		}
		patch.HasDefaultTableExpirationMs = true
		patch.DefaultTableExpirationMs = n
	}

	return ""
}

// parseLabelsPatchValue parses a patch "labels" value, which may be JSON
// null (clear all labels) or an object of string keys to string values.
func parseLabelsPatchValue(v any) (map[string]string, string) {
	if v == nil {
		return nil, ""
	}
	obj, ok := v.(map[string]any)
	if !ok {
		return nil, "labels must be an object"
	}
	labels := make(map[string]string, len(obj))
	for k, rv := range obj {
		str, ok := rv.(string)
		if !ok {
			return nil, "labels values must be strings"
		}
		labels[k] = str
	}
	return labels, ""
}

func renderDatasetResource(ds *datasetRecord) map[string]any {
	resp := map[string]any{
		"kind": "bigquery#dataset",
		"id":   fmt.Sprintf("%s:%s", ds.ProjectID, ds.DatasetID),
		"datasetReference": map[string]string{
			"projectId": ds.ProjectID,
			"datasetId": ds.DatasetID,
		},
	}
	if ds.FriendlyName != "" {
		resp["friendlyName"] = ds.FriendlyName
	}
	if ds.Location != "" {
		resp["location"] = ds.Location
	}
	if len(ds.Labels) > 0 {
		resp["labels"] = ds.Labels
	}
	if ds.DefaultTableExpirationMs > 0 {
		// Rendered as a string to match the real BigQuery Discovery contract for
		// int64 fields (avoids precision loss for large millisecond values).
		resp["defaultTableExpirationMs"] = strconv.FormatInt(ds.DefaultTableExpirationMs, 10)
	}
	return resp
}

func (s *Server) handleTablesCollection(w http.ResponseWriter, r *http.Request, projectID, datasetID string) {
	switch r.Method {
	case http.MethodGet:
		s.listTables(w, r, projectID, datasetID)
	case http.MethodPost:
		s.insertTable(w, r, projectID, datasetID)
	default:
		writeError(w, http.StatusMethodNotAllowed, "Method not allowed", "methodNotAllowed")
	}
}

func (s *Server) handleTableByID(w http.ResponseWriter, r *http.Request, projectID, datasetID, tableID string) {
	switch r.Method {
	case http.MethodGet:
		s.getTable(w, r, projectID, datasetID, tableID)
	case http.MethodPatch:
		s.patchTable(w, r, projectID, datasetID, tableID)
	case http.MethodPut:
		s.updateTable(w, r, projectID, datasetID, tableID)
	case http.MethodDelete:
		s.deleteTable(w, projectID, datasetID, tableID)
	default:
		writeError(w, http.StatusMethodNotAllowed, "Method not allowed", "methodNotAllowed")
	}
}

func (s *Server) listTables(w http.ResponseWriter, r *http.Request, projectID, datasetID string) {
	start, size := parsePagination(r, 2, 1000)
	items, next, version := s.tables.list(projectID, datasetID, start, size)

	if s.checkETag(w, r, version) {
		return
	}

	out := make([]map[string]any, 0, len(items))
	for _, t := range items {
		out = append(out, renderTableResource(t))
	}

	resp := map[string]any{
		"kind":   "bigquery#tableList",
		"tables": out,
		"etag":   fmt.Sprintf("\"v%d\"", version),
	}
	if next >= 0 {
		resp["nextPageToken"] = encodePageToken(next)
	}
	writeJSON(w, http.StatusOK, resp)
}

func (s *Server) insertTable(w http.ResponseWriter, r *http.Request, projectID, datasetID string) {
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

	refRaw, ok := raw["tableReference"].(map[string]any)
	if !ok {
		writeError(w, http.StatusBadRequest, "tableReference.tableId is required", "required")
		return
	}
	tableID, _ := refRaw["tableId"].(string)
	tableID = strings.TrimSpace(tableID)
	if tableID == "" {
		writeError(w, http.StatusBadRequest, "tableReference.tableId is required", "required")
		return
	}

	friendlyName := ""
	if v, ok := raw["friendlyName"]; ok {
		str, ok := v.(string)
		if !ok {
			writeError(w, http.StatusBadRequest, "friendlyName must be a string", "invalid")
			return
		}
		friendlyName = str
	}
	description := ""
	if v, ok := raw["description"]; ok {
		str, ok := v.(string)
		if !ok {
			writeError(w, http.StatusBadRequest, "description must be a string", "invalid")
			return
		}
		description = str
	}
	labels := map[string]string(nil)
	if v, ok := raw["labels"]; ok {
		if v == nil {
			labels = nil
		} else {
			obj, ok := v.(map[string]any)
			if !ok {
				writeError(w, http.StatusBadRequest, "labels must be an object", "invalid")
				return
			}
			labels = make(map[string]string, len(obj))
			for k, rv := range obj {
				str, ok := rv.(string)
				if !ok {
					writeError(w, http.StatusBadRequest, "labels values must be strings", "invalid")
					return
				}
				labels[k] = str
			}
		}
	}

	item, created := s.tables.insert(tableInsert{
		ProjectID:    projectID,
		DatasetID:    datasetID,
		TableID:      tableID,
		FriendlyName: friendlyName,
		Description:  description,
		Labels:       labels,
	})
	if !created {
		writeError(w, http.StatusConflict, fmt.Sprintf("Already Exists: Table %s:%s.%s", projectID, datasetID, tableID), "duplicate")
		return
	}

	writeJSON(w, http.StatusOK, renderTableResource(item))
}

func (s *Server) getTable(w http.ResponseWriter, r *http.Request, projectID, datasetID, tableID string) {
	item, ok, version := s.tables.get(projectID, datasetID, tableID)
	if !ok {
		writeError(w, http.StatusNotFound, fmt.Sprintf("Not found: Table %s:%s.%s", projectID, datasetID, tableID), "notFound")
		return
	}

	if s.checkETag(w, r, version) {
		return
	}
	writeJSON(w, http.StatusOK, renderTableResource(item))
}

func (s *Server) patchTable(w http.ResponseWriter, r *http.Request, projectID, datasetID, tableID string) {
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

	patch := tablePatch{ProjectID: projectID, DatasetID: datasetID, TableID: tableID}
	if v, ok := raw["friendlyName"]; ok {
		str, ok := v.(string)
		if !ok {
			writeError(w, http.StatusBadRequest, "friendlyName must be a string", "invalid")
			return
		}
		patch.HasFriendlyName = true
		patch.FriendlyName = str
	}
	if v, ok := raw["description"]; ok {
		str, ok := v.(string)
		if !ok {
			writeError(w, http.StatusBadRequest, "description must be a string", "invalid")
			return
		}
		patch.HasDescription = true
		patch.Description = str
	}
	if v, ok := raw["labels"]; ok {
		patch.HasLabels = true
		if v == nil {
			patch.Labels = nil
		} else {
			obj, ok := v.(map[string]any)
			if !ok {
				writeError(w, http.StatusBadRequest, "labels must be an object", "invalid")
				return
			}
			labels := make(map[string]string, len(obj))
			for k, rv := range obj {
				str, ok := rv.(string)
				if !ok {
					writeError(w, http.StatusBadRequest, "labels values must be strings", "invalid")
					return
				}
				labels[k] = str
			}
			patch.Labels = labels
		}
	}

	if !patch.HasFriendlyName && !patch.HasDescription && !patch.HasLabels {
		writeError(w, http.StatusBadRequest, "at least one patchable field is required", "required")
		return
	}

	item, ok := s.tables.patch(patch)
	if !ok {
		writeError(w, http.StatusNotFound, fmt.Sprintf("Not found: Table %s:%s.%s", projectID, datasetID, tableID), "notFound")
		return
	}

	writeJSON(w, http.StatusOK, renderTableResource(item))
}

func (s *Server) updateTable(w http.ResponseWriter, r *http.Request, projectID, datasetID, tableID string) {
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

	if refRaw, ok := raw["tableReference"]; ok {
		ref, ok := refRaw.(map[string]any)
		if !ok {
			writeError(w, http.StatusBadRequest, "tableReference must be an object", "invalid")
			return
		}
		if tableVal, ok := ref["tableId"].(string); ok && strings.TrimSpace(tableVal) != "" && strings.TrimSpace(tableVal) != tableID {
			writeError(w, http.StatusBadRequest, "tableReference.tableId does not match path", "invalid")
			return
		}
	}

	friendlyName := ""
	if v, ok := raw["friendlyName"]; ok {
		str, ok := v.(string)
		if !ok {
			writeError(w, http.StatusBadRequest, "friendlyName must be a string", "invalid")
			return
		}
		friendlyName = str
	}
	description := ""
	if v, ok := raw["description"]; ok {
		str, ok := v.(string)
		if !ok {
			writeError(w, http.StatusBadRequest, "description must be a string", "invalid")
			return
		}
		description = str
	}
	labels := map[string]string(nil)
	if v, ok := raw["labels"]; ok {
		if v == nil {
			labels = nil
		} else {
			obj, ok := v.(map[string]any)
			if !ok {
				writeError(w, http.StatusBadRequest, "labels must be an object", "invalid")
				return
			}
			labels = make(map[string]string, len(obj))
			for k, rv := range obj {
				str, ok := rv.(string)
				if !ok {
					writeError(w, http.StatusBadRequest, "labels values must be strings", "invalid")
					return
				}
				labels[k] = str
			}
		}
	}

	item, ok := s.tables.update(tableUpdate{
		ProjectID:    projectID,
		DatasetID:    datasetID,
		TableID:      tableID,
		FriendlyName: friendlyName,
		Description:  description,
		Labels:       labels,
	})
	if !ok {
		writeError(w, http.StatusNotFound, fmt.Sprintf("Not found: Table %s:%s.%s", projectID, datasetID, tableID), "notFound")
		return
	}

	writeJSON(w, http.StatusOK, renderTableResource(item))
}

func (s *Server) deleteTable(w http.ResponseWriter, projectID, datasetID, tableID string) {
	if !s.tables.delete(projectID, datasetID, tableID) {
		writeError(w, http.StatusNotFound, fmt.Sprintf("Not found: Table %s:%s.%s", projectID, datasetID, tableID), "notFound")
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func renderTableResource(t *tableRecord) map[string]any {
	resp := map[string]any{
		"kind": "bigquery#table",
		"id":   fmt.Sprintf("%s:%s.%s", t.ProjectID, t.DatasetID, t.TableID),
		"tableReference": map[string]string{
			"projectId": t.ProjectID,
			"datasetId": t.DatasetID,
			"tableId":   t.TableID,
		},
		"etag":             fmt.Sprintf("\"v%d\"", t.Version),
		"creationTime":     fmt.Sprintf("%d", t.CreatedAt.UnixMilli()),
		"lastModifiedTime": fmt.Sprintf("%d", t.UpdatedAt.UnixMilli()),
		"schema": map[string]any{
			"fields": renderTableSchemaFields(t.Schema),
		},
	}
	if t.FriendlyName != "" {
		resp["friendlyName"] = t.FriendlyName
	}
	if t.Description != "" {
		resp["description"] = t.Description
	}
	if len(t.Labels) > 0 {
		resp["labels"] = t.Labels
	}
	return resp
}

func (s *Server) listJobs(w http.ResponseWriter, r *http.Request, projectID string) {
	start, size := parsePagination(r, 2, 1000)
	filters := jobListFilters{
		StateFilter: r.URL.Query().Get("stateFilter"),
		UserEmail:   r.URL.Query().Get("userEmail"),
		AllUsers:    r.URL.Query().Get("allUsers") == "true",
		ParentJobID: r.URL.Query().Get("parentJobId"),
	}
	if filters.UserEmail == "" {
		filters.UserEmail = r.Header.Get("X-User-Email")
	}
	if raw := r.URL.Query().Get("minCreationTime"); raw != "" {
		if ms, err := strconv.ParseInt(raw, 10, 64); err == nil {
			filters.MinCreated = time.UnixMilli(ms).UTC()
		}
	}
	if raw := r.URL.Query().Get("maxCreationTime"); raw != "" {
		if ms, err := strconv.ParseInt(raw, 10, 64); err == nil {
			filters.MaxCreated = time.UnixMilli(ms).UTC()
		}
	}
	items, next, version := s.jobs.list(projectID, filters, start, size)

	if s.checkETag(w, r, version) {
		return
	}

	out := make([]map[string]any, 0, len(items))
	for _, j := range items {
		out = append(out, renderJobResource(j))
	}

	resp := map[string]any{
		"kind": "bigquery#jobList",
		"jobs": out,
		"etag": fmt.Sprintf("\"v%d\"", version),
	}
	if next != "" {
		if n, err := strconv.Atoi(next); err == nil {
			resp["nextPageToken"] = encodePageToken(n)
		} else {
			resp["nextPageToken"] = next
		}
	}
	writeJSON(w, http.StatusOK, resp)
}

func (s *Server) insertJob(w http.ResponseWriter, r *http.Request, projectID string) {
	requestID := r.URL.Query().Get("requestId")
	userEmail := r.URL.Query().Get("userEmail")
	if userEmail == "" {
		userEmail = r.Header.Get("X-User-Email")
	}
	queryText := ""
	isScript := false
	jobType := "query"
	targetDataset := ""
	targetTable := ""
	sourceTables := []tableReference(nil)
	loadSchema := []tableField(nil)
	loadSourceURIs := []string(nil)
	loadSourceFormat := ""
	loadFieldDelimiter := ""
	loadSkipLeadingRows := 0
	extractSourceTable := tableReference{}
	extractDestinationURIs := []string(nil)
	extractDestinationFormat := ""
	extractFieldDelimiter := ""
	extractPrintHeader := true
	createDisposition := ""
	writeDisposition := ""
	priority := "INTERACTIVE"
	if r.Body != nil {
		body, _ := io.ReadAll(r.Body)
		if len(body) > 0 {
			var raw map[string]any
			if err := json.Unmarshal(body, &raw); err == nil {
				if conf, ok := raw["configuration"].(map[string]any); ok {
					if qCfg, ok := conf["query"].(map[string]any); ok {
						if p, ok := qCfg["priority"].(string); ok {
							priority = p
						}
					}
					if loadRaw, ok := conf["load"]; ok {
						jobType = "load"
						if loadCfg, ok := loadRaw.(map[string]any); ok {
							parsed := parseLoadConfig(loadCfg, projectID)
							targetDataset, targetTable = parsed.TargetDataset, parsed.TargetTable
							loadSchema = parsed.Schema
							loadSourceURIs = parsed.SourceURIs
							loadSourceFormat = parsed.SourceFormat
							loadFieldDelimiter = parsed.FieldDelimiter
							loadSkipLeadingRows = parsed.SkipLeadingRows
							createDisposition = parsed.CreateDisposition
							writeDisposition = parsed.WriteDisposition
						}
					}
					if extractRaw, ok := conf["extract"]; ok {
						jobType = "extract"
						if extractCfg, ok := extractRaw.(map[string]any); ok {
							parsed := parseExtractConfig(extractCfg, projectID)
							extractSourceTable = parsed.SourceTable
							extractDestinationURIs = parsed.DestinationURIs
							extractDestinationFormat = parsed.DestinationFormat
							extractFieldDelimiter = parsed.FieldDelimiter
							extractPrintHeader = parsed.PrintHeader
						}
					}
					if copyRaw, ok := conf["copy"]; ok {
						jobType = "copy"
						if copyCfg, ok := copyRaw.(map[string]any); ok {
							dest := extractTableRef(copyCfg["destinationTable"], projectID)
							targetDataset, targetTable = dest.DatasetID, dest.TableID
							sourceTables = append(sourceTables, extractTableRefs(copyCfg["sourceTables"], projectID)...)
							if singleSource := extractTableRef(copyCfg["sourceTable"], projectID); singleSource.DatasetID != "" && singleSource.TableID != "" {
								sourceTables = append(sourceTables, singleSource)
							}
							if value, ok := copyCfg["createDisposition"].(string); ok {
								createDisposition = value
							}
							if value, ok := copyCfg["writeDisposition"].(string); ok {
								writeDisposition = value
							}
						}
					}
				}
			}

			var payload struct {
				Configuration struct {
					Query struct {
						Query string `json:"query"`
					} `json:"query"`
				} `json:"configuration"`
			}
			if err := json.Unmarshal(body, &payload); err == nil {
				queryText = payload.Configuration.Query.Query
				if queryText != "" {
					jobType = "query"
				}
			}
		}
		_ = r.Body.Close()
	}
	if strings.Count(queryText, ";") > 0 {
		isScript = true
	}

	insertOpts := jobInsertOptions{
		ProjectID:     projectID,
		RequestID:     requestID,
		UserEmail:     userEmail,
		QueryText:     queryText,
		JobType:       jobType,
		Priority:      priority,
		SourceTables:  sourceTables,
		LoadSchema:    loadSchema,
		LoadSourceURIs: loadSourceURIs,
		LoadSourceFormat: loadSourceFormat,
		LoadFieldDelimiter: loadFieldDelimiter,
		LoadSkipLeadingRows: loadSkipLeadingRows,
		ExtractSourceTable: extractSourceTable,
		ExtractDestinationURIs: extractDestinationURIs,
		ExtractDestinationFormat: extractDestinationFormat,
		ExtractFieldDelimiter: extractFieldDelimiter,
		ExtractPrintHeader: extractPrintHeader,
		CreateDisposition: createDisposition,
		WriteDisposition:  writeDisposition,
		TargetDataset: targetDataset,
		TargetTable:   targetTable,
		IsScript:      isScript,
	}

	if isScript {
		jr, childJobs, created := s.jobs.insertScriptWithChildren(insertOpts)
		status := http.StatusOK
		if created {
			status = http.StatusCreated
		}
		children := make([]map[string]any, 0, len(childJobs))
		for _, c := range childJobs {
			children = append(children, renderJobResource(c))
		}
		writeJSON(w, status, map[string]any{
			"job":      renderJobResource(jr),
			"children": children,
		})
		return
	}

	jr, created := s.jobs.insert(insertOpts)
	status := http.StatusOK
	if created {
		status = http.StatusCreated
	}
	writeJSON(w, status, renderJobResource(jr))
}

func (s *Server) getJob(w http.ResponseWriter, _ *http.Request, projectID, jobID string) {
	jr, ok := s.jobs.get(projectID, jobID)
	if !ok {
		writeError(w, http.StatusNotFound, fmt.Sprintf("Not found: Job %s:%s", projectID, jobID), "notFound")
		return
	}
	writeJSON(w, http.StatusOK, renderJobResource(jr))
}

func (s *Server) cancelJob(w http.ResponseWriter, _ *http.Request, projectID, jobID string) {
	jr, ok := s.jobs.cancel(projectID, jobID)
	if !ok {
		writeError(w, http.StatusNotFound, fmt.Sprintf("Not found: Job %s:%s", projectID, jobID), "notFound")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"kind": "bigquery#jobCancelResponse",
		"job":  renderJobResource(jr),
	})
}

func (s *Server) handleJobsQuery(w http.ResponseWriter, r *http.Request, projectID string) {
	defer func() {
		if r.Body != nil {
			_ = r.Body.Close()
		}
	}()

	var payload struct {
		Query      string `json:"query"`
		MaxResults int    `json:"maxResults"`
		TimeoutMs  int    `json:"timeoutMs"`
		DryRun     bool   `json:"dryRun"`
		RequestId  string `json:"requestId"`
		Priority   string `json:"priority"`
	}
	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
		writeError(w, http.StatusBadRequest, "Invalid request body", "invalid")
		return
	}

	if payload.DryRun {
		// Basic dry run simulation
		writeJSON(w, http.StatusOK, map[string]any{
			"kind":                "bigquery#queryResponse",
			"jobComplete":         true,
			"totalBytesProcessed": "1024", // simulated
			"schema": map[string]any{
				"fields": []map[string]string{
					{"name": "dry_run", "type": "BOOLEAN"},
				},
			},
		})
		return
	}

	// For now, we reuse jobs.insert logic by creating a job and immediately waiting/polling for results
	// In a real implementation, we would wait up to TimeoutMs
	requestID := strings.TrimSpace(r.URL.Query().Get("requestId"))
	if requestID == "" {
		requestID = strings.TrimSpace(payload.RequestId)
	}
	userEmail := strings.TrimSpace(r.URL.Query().Get("userEmail"))
	if userEmail == "" {
		userEmail = strings.TrimSpace(r.Header.Get("X-User-Email"))
	}

	insertOpts := jobInsertOptions{
		ProjectID: projectID,
		RequestID: requestID,
		UserEmail: userEmail,
		QueryText: payload.Query,
		JobType:   "query",
		Priority:  payload.Priority,
	}

	jr, created := s.jobs.insert(insertOpts)
	_ = created // jobId is what matters

	// Wait loop (simulated)
	start := time.Now()
	timeout := 10 * time.Second
	if payload.TimeoutMs > 0 {
		timeout = time.Duration(payload.TimeoutMs) * time.Millisecond
	}

	for {
		job, ok := s.jobs.get(projectID, jr.JobID)
		if !ok {
			writeJSON(w, http.StatusInternalServerError, map[string]any{"error": "job lost after creation"})
			return
		}

		if job.State == jobStateDone {
			// Job finished, fetch results
			s.writeQueryResults(w, r, projectID, jr.JobID, "bigquery#queryResponse")
			return
		}

		if time.Since(start) > timeout {
			// Timeout reached, return jobReference with jobComplete=false
			writeJSON(w, http.StatusOK, map[string]any{
				"kind": "bigquery#queryResponse",
				"jobReference": map[string]string{
					"projectId": projectID,
					"jobId":     jr.JobID,
				},
				"jobComplete": false,
			})
			return
		}

		time.Sleep(100 * time.Millisecond)
	}
}

func (s *Server) getQueryResults(w http.ResponseWriter, r *http.Request, projectID, jobID string) {
	s.writeQueryResults(w, r, projectID, jobID, "bigquery#getQueryResultsResponse")
}

func (s *Server) writeQueryResults(w http.ResponseWriter, r *http.Request, projectID, jobID, kind string) {
	j, ok := s.jobs.get(projectID, jobID)
	if !ok {
		writeError(w, http.StatusNotFound, fmt.Sprintf("Not found: Job %s:%s", projectID, jobID), "notFound")
		return
	}
	if j.JobType != "query" && j.JobType != "script" {
		writeError(w, http.StatusBadRequest, "Query results only available for query jobs", "invalid")
		return
	}

	start, size := parsePagination(r, 20, 1000)
	schema, values := s.simulateQueryResultTable(projectID, j.QueryText, j.UserEmail)
	end := clampEnd(start, size, len(values))

	rows := make([]map[string]any, 0, end-start)
	for _, raw := range values[start:end] {
		cells := make([]map[string]string, 0, len(raw))
		for _, value := range raw {
			cells = append(cells, map[string]string{"v": value})
		}
		rows = append(rows, map[string]any{"f": cells})
	}

	resp := map[string]any{
		"kind": kind,
		"jobReference": map[string]string{
			"projectId": projectID,
			"jobId":     jobID,
		},
		"schema": map[string]any{
			"fields": schema,
		},
		"rows":           rows,
		"totalRows":      strconv.Itoa(len(values)),
		"jobComplete":    j.State == jobStateDone,
		"pageToken":      strconv.Itoa(start),
		"maxResults":     size,
		"startIndexUsed": start,
	}
	if end < len(values) {
		resp["pageToken"] = encodePageToken(end)
	}

	writeJSON(w, http.StatusOK, resp)
}

func (s *Server) simulateQueryResultTable(projectID, queryText, callingUserEmail string) ([]map[string]string, [][]string) {
	trimmed := strings.TrimSpace(queryText)
	if trimmed == "" {
		return []map[string]string{{"name": "result", "type": "STRING"}}, [][]string{{"query job executed"}}
	}

	lower := strings.ToLower(trimmed)
	if schema, rows, ok := s.simulateInformationSchemaQuery(projectID, trimmed, lower, callingUserEmail); ok {
		return schema, rows
	}
	if schema, rows, ok := s.simulateTableSelectQuery(projectID, trimmed); ok {
		return schema, rows
	}
	if strings.HasPrefix(lower, "select") && !strings.Contains(lower, " from ") {
		expr := strings.TrimSpace(trimmed[len("select"):])
		if expr == "" {
			return []map[string]string{{"name": "result", "type": "STRING"}}, [][]string{{"empty select"}}
		}
		parts := strings.Split(expr, ",")
		schema := make([]map[string]string, 0, len(parts))
		row := make([]string, 0, len(parts))
		for idx, p := range parts {
			part := strings.TrimSpace(p)
			name := fmt.Sprintf("col_%d", idx+1)
			value := part
			if asIdx := strings.LastIndex(strings.ToLower(part), " as "); asIdx >= 0 {
				value = strings.TrimSpace(part[:asIdx])
				alias := strings.TrimSpace(part[asIdx+4:])
				if alias != "" {
					name = strings.Trim(alias, "`")
				}
			}
			value = strings.Trim(value, "'\"")
			schema = append(schema, map[string]string{"name": name, "type": "STRING"})
			row = append(row, value)
		}
		return schema, [][]string{row}
	}

	return []map[string]string{
			{"name": "row_num", "type": "INT64"},
			{"name": "preview", "type": "STRING"},
		}, [][]string{
			{"1", "Simulated query result row"},
			{"2", "Add SQL engine integration for full fidelity"},
			{"3", "Current mode returns deterministic preview"},
		}
}

func (s *Server) listTableData(w http.ResponseWriter, r *http.Request, projectID, datasetID, tableID string) {
	_, rawRows, ok := s.tables.getData(projectID, datasetID, tableID)
	if !ok {
		writeError(w, http.StatusNotFound, fmt.Sprintf("Not found: Table %s:%s.%s", projectID, datasetID, tableID), "notFound")
		return
	}
	rows := make([]tableRow, 0, len(rawRows))
	for _, raw := range rawRows {
		rows = append(rows, tableRow{Values: append([]string(nil), raw...)})
	}

	start, size := parsePagination(r, 2, 100000)
	if startIndex := r.URL.Query().Get("startIndex"); startIndex != "" {
		if n, err := strconv.Atoi(startIndex); err == nil && n >= 0 {
			start = n
		}
	}
	end := clampEnd(start, size, len(rows))

	out := make([]map[string]any, 0, end-start)
	for _, row := range rows[start:end] {
		cells := make([]map[string]string, 0, len(row.Values))
		for _, v := range row.Values {
			cells = append(cells, map[string]string{"v": v})
		}
		out = append(out, map[string]any{"f": cells})
	}

	resp := map[string]any{
		"kind":           "bigquery#tableDataList",
		"etag":           "locaql",
		"totalRows":      strconv.Itoa(len(rows)),
		"rows":           out,
		"pageToken":      strconv.Itoa(start),
		"datasetId":      datasetID,
		"tableId":        tableID,
		"projectId":      projectID,
		"maxResults":     size,
		"startIndexUsed": start,
	}
	if end < len(rows) {
		resp["nextPageToken"] = encodePageToken(end)
	}
	writeJSON(w, http.StatusOK, resp)
}

func renderTableSchemaFields(fields []tableField) []map[string]string {
	out := make([]map[string]string, 0, len(fields))
	for _, field := range fields {
		out = append(out, map[string]string{"name": field.Name, "type": field.Type})
	}
	return out
}

func extractTableRef(v any, defaultProjectID string) tableReference {
	m, ok := v.(map[string]any)
	if !ok {
		return tableReference{}
	}
	projectID, _ := m["projectId"].(string)
	datasetID, _ := m["datasetId"].(string)
	tableID, _ := m["tableId"].(string)
	projectID = strings.TrimSpace(projectID)
	if projectID == "" {
		projectID = strings.TrimSpace(defaultProjectID)
	}
	return tableReference{ProjectID: projectID, DatasetID: strings.TrimSpace(datasetID), TableID: strings.TrimSpace(tableID)}
}

func extractTableRefs(v any, defaultProjectID string) []tableReference {
	list, ok := v.([]any)
	if !ok {
		return nil
	}
	out := make([]tableReference, 0, len(list))
	for _, raw := range list {
		ref := extractTableRef(raw, defaultProjectID)
		if ref.DatasetID != "" && ref.TableID != "" {
			out = append(out, ref)
		}
	}
	return out
}

type loadConfigParsed struct {
	TargetDataset     string
	TargetTable       string
	Schema            []tableField
	SourceURIs        []string
	SourceFormat      string
	FieldDelimiter    string
	SkipLeadingRows   int
	CreateDisposition string
	WriteDisposition  string
}

func parseLoadConfig(loadCfg map[string]any, projectID string) loadConfigParsed {
	dest := extractTableRef(loadCfg["destinationTable"], projectID)
	out := loadConfigParsed{
		TargetDataset: dest.DatasetID,
		TargetTable:   dest.TableID,
		Schema:        parseTableSchemaFields(loadCfg["schema"]),
		SourceURIs:    extractStringList(loadCfg["sourceUris"]),
	}
	if value, ok := loadCfg["sourceFormat"].(string); ok {
		out.SourceFormat = value
	}
	if value, ok := loadCfg["fieldDelimiter"].(string); ok {
		out.FieldDelimiter = value
	}
	if value, ok := loadCfg["skipLeadingRows"].(float64); ok {
		out.SkipLeadingRows = int(value)
	}
	if value, ok := loadCfg["createDisposition"].(string); ok {
		out.CreateDisposition = value
	}
	if value, ok := loadCfg["writeDisposition"].(string); ok {
		out.WriteDisposition = value
	}
	return out
}

type extractConfigParsed struct {
	SourceTable       tableReference
	DestinationURIs   []string
	DestinationFormat string
	FieldDelimiter    string
	PrintHeader       bool
}

func parseExtractConfig(extractCfg map[string]any, projectID string) extractConfigParsed {
	out := extractConfigParsed{
		SourceTable:     extractTableRef(extractCfg["sourceTable"], projectID),
		DestinationURIs: extractStringList(extractCfg["destinationUris"]),
		PrintHeader:     true,
	}
	if value, ok := extractCfg["destinationFormat"].(string); ok {
		out.DestinationFormat = value
	}
	if value, ok := extractCfg["fieldDelimiter"].(string); ok {
		out.FieldDelimiter = value
	}
	if value, ok := extractCfg["printHeader"].(bool); ok {
		out.PrintHeader = value
	}
	return out
}

func extractStringList(v any) []string {
	list, ok := v.([]any)
	if !ok {
		return nil
	}
	out := make([]string, 0, len(list))
	for _, raw := range list {
		s, ok := raw.(string)
		if !ok {
			continue
		}
		s = strings.TrimSpace(s)
		if s != "" {
			out = append(out, s)
		}
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func parseTableSchemaFields(v any) []tableField {
	obj, ok := v.(map[string]any)
	if !ok {
		return nil
	}
	rawFields, ok := obj["fields"].([]any)
	if !ok {
		return nil
	}
	out := make([]tableField, 0, len(rawFields))
	for _, raw := range rawFields {
		fieldObj, ok := raw.(map[string]any)
		if !ok {
			continue
		}
		name, _ := fieldObj["name"].(string)
		name = strings.TrimSpace(name)
		if name == "" {
			continue
		}
		typ, _ := fieldObj["type"].(string)
		typ = strings.ToUpper(strings.TrimSpace(typ))
		if typ == "" {
			typ = "STRING"
		}
		out = append(out, tableField{Name: name, Type: typ})
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

var fromTablePattern = regexp.MustCompile("(?is)\\bfrom\\s+`?([a-zA-Z0-9_\\-\\.]+)`?")
var informationSchemaPattern = regexp.MustCompile("(?is)(?:`?([a-zA-Z0-9_\\-]+)`?\\.)?(?:`?([a-zA-Z0-9_\\-]+)`?\\.)?information_schema\\.(schemata_options|schemata|table_options|tables|columns|jobs_by_project|jobs_by_user|jobs|partitions|routines|models|views)")

func (s *Server) simulateTableSelectQuery(projectID, queryText string) ([]map[string]string, [][]string, bool) {
	matches := fromTablePattern.FindStringSubmatch(queryText)
	if len(matches) < 2 {
		return nil, nil, false
	}
	parts := strings.Split(strings.TrimSpace(matches[1]), ".")
	ref := tableReference{ProjectID: projectID}
	switch len(parts) {
	case 3:
		ref.ProjectID, ref.DatasetID, ref.TableID = parts[0], parts[1], parts[2]
	case 2:
		ref.DatasetID, ref.TableID = parts[0], parts[1]
	default:
		return nil, nil, false
	}
	fields, rows, ok := s.tables.getData(ref.ProjectID, ref.DatasetID, ref.TableID)
	if !ok {
		return nil, nil, false
	}
	return renderTableSchemaFields(fields), rows, true
}

func (s *Server) simulateInformationSchemaQuery(projectID, queryText, lower, callingUserEmail string) ([]map[string]string, [][]string, bool) {
	if !strings.Contains(lower, "information_schema.") {
		return nil, nil, false
	}
	matches := informationSchemaPattern.FindStringSubmatch(queryText)
	if len(matches) < 4 {
		return nil, nil, false
	}
	targetProjectID, targetDatasetID := resolveInformationSchemaTarget(s, projectID, matches[1], matches[2])
	objectType := strings.ToLower(strings.TrimSpace(matches[3]))

	datasets, _, _ := s.datasets.list(targetProjectID, 0, 1000)
	scope := informationSchemaScope{
		server:           s,
		targetProjectID:  targetProjectID,
		datasets:         datasets,
		callingUserEmail: callingUserEmail,
		filterDataset: func(datasetID string) bool {
			return targetDatasetID == "" || datasetID == targetDatasetID
		},
	}

	build, ok := informationSchemaBuilders[objectType]
	if !ok {
		return nil, nil, false
	}
	schema, rows := build(scope)
	return schema, rows, true
}

func resolveInformationSchemaTarget(s *Server, projectID, rawProject, rawDataset string) (targetProjectID, targetDatasetID string) {
	targetProjectID = projectID
	if strings.TrimSpace(rawDataset) != "" {
		if strings.TrimSpace(rawProject) != "" {
			targetProjectID = strings.TrimSpace(rawProject)
		}
		targetDatasetID = strings.TrimSpace(rawDataset)
		return
	}
	if candidate := strings.TrimSpace(rawProject); candidate != "" {
		if candidate == projectID || !s.datasets.exists(projectID, candidate) {
			targetProjectID = candidate
		} else {
			targetDatasetID = candidate
		}
	}
	return
}

// informationSchemaScope carries the resolved project/dataset filter shared
// by every INFORMATION_SCHEMA builder below.
type informationSchemaScope struct {
	server           *Server
	targetProjectID  string
	datasets         []*datasetRecord
	callingUserEmail string
	filterDataset    func(datasetID string) bool
}

// forEachTable iterates every table in every dataset the scope allows,
// factoring out the nested dataset/table loop that every table-scoped
// INFORMATION_SCHEMA view needs.
func (sc informationSchemaScope) forEachTable(fn func(datasetID string, table *tableRecord)) {
	for _, ds := range sc.datasets {
		if !sc.filterDataset(ds.DatasetID) {
			continue
		}
		tables, _, _ := sc.server.tables.list(sc.targetProjectID, ds.DatasetID, 0, 1000)
		for _, table := range tables {
			fn(ds.DatasetID, table)
		}
	}
}

type informationSchemaBuilder func(scope informationSchemaScope) ([]map[string]string, [][]string)

var informationSchemaBuilders = map[string]informationSchemaBuilder{
	"schemata":         buildInformationSchemaSchemata,
	"schemata_options": buildInformationSchemaSchemataOptions,
	"tables":           buildInformationSchemaTables,
	"columns":          buildInformationSchemaColumns,
	"jobs":             buildInformationSchemaJobs,
	"jobs_by_project":  buildInformationSchemaJobs,
	"jobs_by_user":     buildInformationSchemaJobsByUser,
	"partitions":       buildInformationSchemaPartitions,
	"routines":         buildInformationSchemaRoutines,
	"models":           buildInformationSchemaModels,
	"table_options":    buildInformationSchemaTableOptions,
	"views":            buildInformationSchemaViews,
}

func buildInformationSchemaSchemata(scope informationSchemaScope) ([]map[string]string, [][]string) {
	rows := make([][]string, 0, len(scope.datasets))
	for _, ds := range scope.datasets {
		if !scope.filterDataset(ds.DatasetID) {
			continue
		}
		rows = append(rows, []string{scope.targetProjectID, ds.DatasetID})
	}
	return []map[string]string{{"name": "catalog_name", "type": "STRING"}, {"name": "schema_name", "type": "STRING"}}, rows
}

func buildInformationSchemaSchemataOptions(scope informationSchemaScope) ([]map[string]string, [][]string) {
	rows := make([][]string, 0, len(scope.datasets)*2)
	for _, ds := range scope.datasets {
		if !scope.filterDataset(ds.DatasetID) {
			continue
		}
		if strings.TrimSpace(ds.Location) != "" {
			rows = append(rows, []string{scope.targetProjectID, ds.DatasetID, "location", "STRING", ds.Location})
		}
		if strings.TrimSpace(ds.FriendlyName) != "" {
			rows = append(rows, []string{scope.targetProjectID, ds.DatasetID, "friendly_name", "STRING", ds.FriendlyName})
		}
		if ds.DefaultTableExpirationMs > 0 {
			rows = append(rows, []string{scope.targetProjectID, ds.DatasetID, "default_table_expiration_ms", "INT64", strconv.FormatInt(ds.DefaultTableExpirationMs, 10)})
		}
	}
	return []map[string]string{{"name": "catalog_name", "type": "STRING"}, {"name": "schema_name", "type": "STRING"}, {"name": "option_name", "type": "STRING"}, {"name": "option_type", "type": "STRING"}, {"name": "option_value", "type": "STRING"}}, rows
}

func buildInformationSchemaTables(scope informationSchemaScope) ([]map[string]string, [][]string) {
	rows := [][]string{}
	scope.forEachTable(func(datasetID string, table *tableRecord) {
		rows = append(rows, []string{scope.targetProjectID, datasetID, table.TableID, "BASE TABLE"})
	})
	return []map[string]string{{"name": "table_catalog", "type": "STRING"}, {"name": "table_schema", "type": "STRING"}, {"name": "table_name", "type": "STRING"}, {"name": "table_type", "type": "STRING"}}, rows
}

func buildInformationSchemaColumns(scope informationSchemaScope) ([]map[string]string, [][]string) {
	rows := [][]string{}
	scope.forEachTable(func(datasetID string, table *tableRecord) {
		fields, _, ok := scope.server.tables.getData(scope.targetProjectID, datasetID, table.TableID)
		if !ok {
			return
		}
		for i, field := range fields {
			rows = append(rows, []string{scope.targetProjectID, datasetID, table.TableID, field.Name, strconv.Itoa(i + 1), field.Type})
		}
	})
	return []map[string]string{{"name": "table_catalog", "type": "STRING"}, {"name": "table_schema", "type": "STRING"}, {"name": "table_name", "type": "STRING"}, {"name": "column_name", "type": "STRING"}, {"name": "ordinal_position", "type": "INT64"}, {"name": "data_type", "type": "STRING"}}, rows
}

func buildInformationSchemaJobs(scope informationSchemaScope) ([]map[string]string, [][]string) {
	items, _, _ := scope.server.jobs.list(scope.targetProjectID, jobListFilters{AllUsers: true}, 0, 1000)
	return jobsInformationSchemaColumns(), jobRecordsToInformationSchemaRows(items)
}

// buildInformationSchemaJobsByUser scopes results to jobs whose UserEmail
// matches the user that submitted THIS query (the job running the
// INFORMATION_SCHEMA.JOBS_BY_USER statement itself). The emulator has no
// broader caller-identity/auth model (anonymous by default), so that
// submitting job's UserEmail is the only real "calling user" available. If
// it has none (not supplied via the userEmail param or X-User-Email header),
// this returns zero rows rather than silently falling back to all jobs,
// which would defeat the point of a per-user view.
func buildInformationSchemaJobsByUser(scope informationSchemaScope) ([]map[string]string, [][]string) {
	columns := jobsInformationSchemaColumns()
	if strings.TrimSpace(scope.callingUserEmail) == "" {
		return columns, [][]string{}
	}
	items, _, _ := scope.server.jobs.list(scope.targetProjectID, jobListFilters{UserEmail: scope.callingUserEmail}, 0, 1000)
	return columns, jobRecordsToInformationSchemaRows(items)
}

func jobsInformationSchemaColumns() []map[string]string {
	return []map[string]string{{"name": "project_id", "type": "STRING"}, {"name": "job_id", "type": "STRING"}, {"name": "job_type", "type": "STRING"}, {"name": "state", "type": "STRING"}, {"name": "user_email", "type": "STRING"}, {"name": "creation_time", "type": "INT64"}, {"name": "end_time", "type": "INT64"}}
}

func jobRecordsToInformationSchemaRows(items []*jobRecord) [][]string {
	rows := make([][]string, 0, len(items))
	for _, job := range items {
		rows = append(rows, []string{
			job.ProjectID,
			job.JobID,
			job.JobType,
			string(job.State),
			job.UserEmail,
			strconv.FormatInt(job.CreatedAt.UnixMilli(), 10),
			strconv.FormatInt(job.EndedAt.UnixMilli(), 10),
		})
	}
	return rows
}

func buildInformationSchemaPartitions(scope informationSchemaScope) ([]map[string]string, [][]string) {
	rows := [][]string{}
	scope.forEachTable(func(datasetID string, table *tableRecord) {
		_, tableRows, ok := scope.server.tables.getData(scope.targetProjectID, datasetID, table.TableID)
		if !ok {
			return
		}
		rows = append(rows, []string{scope.targetProjectID, datasetID, table.TableID, "__UNPARTITIONED__", strconv.Itoa(len(tableRows))})
	})
	return []map[string]string{{"name": "table_catalog", "type": "STRING"}, {"name": "table_schema", "type": "STRING"}, {"name": "table_name", "type": "STRING"}, {"name": "partition_id", "type": "STRING"}, {"name": "total_rows", "type": "INT64"}}, rows
}

func buildInformationSchemaRoutines(scope informationSchemaScope) ([]map[string]string, [][]string) {
	rows := [][]string{}
	for _, ds := range scope.datasets {
		if !scope.filterDataset(ds.DatasetID) {
			continue
		}
		items, _, _ := scope.server.routines.list(scope.targetProjectID, ds.DatasetID, 0, 1000)
		for _, rt := range items {
			rows = append(rows, []string{scope.targetProjectID, ds.DatasetID, rt.RoutineID, rt.RoutineType})
		}
	}
	return []map[string]string{{"name": "routine_catalog", "type": "STRING"}, {"name": "routine_schema", "type": "STRING"}, {"name": "routine_name", "type": "STRING"}, {"name": "routine_type", "type": "STRING"}}, rows
}

func buildInformationSchemaModels(scope informationSchemaScope) ([]map[string]string, [][]string) {
	rows := [][]string{}
	for _, ds := range scope.datasets {
		if !scope.filterDataset(ds.DatasetID) {
			continue
		}
		items, _, _ := scope.server.models.list(scope.targetProjectID, ds.DatasetID, 0, 1000)
		for _, m := range items {
			rows = append(rows, []string{scope.targetProjectID, ds.DatasetID, m.ModelID, m.ModelType})
		}
	}
	return []map[string]string{{"name": "model_catalog", "type": "STRING"}, {"name": "model_schema", "type": "STRING"}, {"name": "model_name", "type": "STRING"}, {"name": "model_type", "type": "STRING"}}, rows
}

func buildInformationSchemaTableOptions(scope informationSchemaScope) ([]map[string]string, [][]string) {
	rows := [][]string{}
	scope.forEachTable(func(datasetID string, table *tableRecord) {
		if strings.TrimSpace(table.FriendlyName) != "" {
			rows = append(rows, []string{scope.targetProjectID, datasetID, table.TableID, "friendly_name", "STRING", table.FriendlyName})
		}
		if strings.TrimSpace(table.Description) != "" {
			rows = append(rows, []string{scope.targetProjectID, datasetID, table.TableID, "description", "STRING", table.Description})
		}
	})
	return []map[string]string{{"name": "table_catalog", "type": "STRING"}, {"name": "table_schema", "type": "STRING"}, {"name": "table_name", "type": "STRING"}, {"name": "option_name", "type": "STRING"}, {"name": "option_type", "type": "STRING"}, {"name": "option_value", "type": "STRING"}}, rows
}

// buildInformationSchemaViews always returns zero rows: views are not a real
// resource in the local catalog yet (only tables exist), so this exposes a
// structurally correct empty result rather than fabricating view rows that
// don't correspond to anything real.
func buildInformationSchemaViews(informationSchemaScope) ([]map[string]string, [][]string) {
	return []map[string]string{{"name": "table_catalog", "type": "STRING"}, {"name": "table_schema", "type": "STRING"}, {"name": "table_name", "type": "STRING"}, {"name": "view_definition", "type": "STRING"}, {"name": "use_standard_sql", "type": "STRING"}}, [][]string{}
}

// executeQueryJob resolves the query against the live in-memory catalog (the
// same resolution getQueryResults uses) so OutputRows and ProcessedBytes
// reflect the actual result set instead of a fixed placeholder. TotalSlotMs
// stays a synthetic constant: real slot/reservation timing is a declared
// non-goal (there is no distributed execution engine to measure).
func (s *Server) executeQueryJob(job *jobRecord) (jobStatistics, error) {
	if strings.Contains(strings.ToUpper(job.QueryText), "FORCE_ERROR") {
		return jobStatistics{Executor: "query", Simulated: false}, fmt.Errorf("simulated forced error from query text")
	}
	_, rows := s.simulateQueryResultTable(job.ProjectID, job.QueryText, job.UserEmail)
	return jobStatistics{Executor: "query", Simulated: false, TotalSlotMs: 60, ProcessedBytes: estimateRowsByteSize(rows), OutputRows: int64(len(rows))}, nil
}

func estimateRowsByteSize(rows [][]string) int64 {
	var total int64
	for _, row := range rows {
		for _, cell := range row {
			total += int64(len(cell))
		}
	}
	return total
}

func (s *Server) executeCopyJob(job *jobRecord) (jobStatistics, error) {
	if strings.TrimSpace(job.TargetDataset) == "" || strings.TrimSpace(job.TargetTable) == "" {
		return jobStatistics{Executor: "copy", Simulated: false}, fmt.Errorf("destinationTable is required")
	}
	if len(job.SourceTables) == 0 {
		return jobStatistics{Executor: "copy", Simulated: false}, fmt.Errorf("copy job requires at least one source table")
	}
	if !s.datasets.exists(job.ProjectID, job.TargetDataset) {
		return jobStatistics{Executor: "copy", Simulated: false}, fmt.Errorf("destination dataset not found")
	}

	var schema []tableField
	rows := make([][]string, 0)
	for idx, source := range job.SourceTables {
		if source.ProjectID == "" {
			source.ProjectID = job.ProjectID
		}
		if !s.datasets.exists(source.ProjectID, source.DatasetID) {
			return jobStatistics{Executor: "copy", Simulated: false}, fmt.Errorf("source dataset not found")
		}
		sourceSchema, sourceRows, ok := s.tables.getData(source.ProjectID, source.DatasetID, source.TableID)
		if !ok {
			return jobStatistics{Executor: "copy", Simulated: false}, fmt.Errorf("source table not found")
		}
		if idx == 0 {
			schema = sourceSchema
		} else if !sameSchema(schema, sourceSchema) {
			return jobStatistics{Executor: "copy", Simulated: false}, fmt.Errorf("source tables must share the same schema")
		}
		rows = append(rows, sourceRows...)
	}

	outputRows, err := s.tables.upsertCopyDestination(tableReference{ProjectID: job.ProjectID, DatasetID: job.TargetDataset, TableID: job.TargetTable}, schema, rows, job.CreateDisposition, job.WriteDisposition)
	if err != nil {
		return jobStatistics{Executor: "copy", Simulated: false}, err
	}
	return jobStatistics{Executor: "copy", Simulated: false, TotalSlotMs: 30, ProcessedBytes: int64(outputRows * 128), OutputRows: int64(outputRows)}, nil
}

func (s *Server) executeLoadJob(job *jobRecord) (jobStatistics, error) {
	if strings.TrimSpace(job.TargetDataset) == "" || strings.TrimSpace(job.TargetTable) == "" {
		return jobStatistics{Executor: "load", Simulated: false}, fmt.Errorf("destinationTable is required")
	}
	if !s.datasets.exists(job.ProjectID, job.TargetDataset) {
		return jobStatistics{Executor: "load", Simulated: false}, fmt.Errorf("destination dataset not found")
	}

	schema := cloneTableFields(job.LoadSchema)

	if len(job.LoadSourceURIs) == 0 {
		if len(schema) == 0 {
			schema = []tableField{{Name: "col_1", Type: "STRING"}}
		}
		outputRows, err := s.tables.upsertCopyDestination(
			tableReference{ProjectID: job.ProjectID, DatasetID: job.TargetDataset, TableID: job.TargetTable},
			schema,
			[][]string{},
			job.CreateDisposition,
			job.WriteDisposition,
		)
		if err != nil {
			return jobStatistics{Executor: "load", Simulated: false}, err
		}
		return jobStatistics{Executor: "load", Simulated: false, TotalSlotMs: 55, ProcessedBytes: 1024, OutputRows: int64(outputRows)}, nil
	}

	if len(schema) == 0 {
		return jobStatistics{Executor: "load", Simulated: false}, fmt.Errorf("schema.fields is required to ingest rows from sourceUris")
	}

	rows, totalBytes, err := loadRowsFromSourceURIs(job, schema)
	if err != nil {
		return jobStatistics{Executor: "load", Simulated: false}, err
	}

	outputRows, err := s.tables.upsertCopyDestination(
		tableReference{ProjectID: job.ProjectID, DatasetID: job.TargetDataset, TableID: job.TargetTable},
		schema,
		rows,
		job.CreateDisposition,
		job.WriteDisposition,
	)
	if err != nil {
		return jobStatistics{Executor: "load", Simulated: false}, err
	}

	return jobStatistics{Executor: "load", Simulated: false, TotalSlotMs: 55, ProcessedBytes: totalBytes, OutputRows: int64(outputRows)}, nil
}

// executeExtractJob reads a source table from the local catalog and writes it
// to local destinationUris in CSV or NEWLINE_DELIMITED_JSON. gs:// URIs and
// wildcard sharding are rejected explicitly: there is no fake GCS backend and
// no multi-shard writer yet.
func (s *Server) executeExtractJob(job *jobRecord) (jobStatistics, error) {
	source := job.ExtractSourceTable
	if strings.TrimSpace(source.DatasetID) == "" || strings.TrimSpace(source.TableID) == "" {
		return jobStatistics{Executor: "extract", Simulated: false}, fmt.Errorf("sourceTable is required")
	}
	if strings.TrimSpace(source.ProjectID) == "" {
		source.ProjectID = job.ProjectID
	}
	if len(job.ExtractDestinationURIs) == 0 {
		return jobStatistics{Executor: "extract", Simulated: false}, fmt.Errorf("destinationUris is required")
	}

	schema, rows, ok := s.tables.getData(source.ProjectID, source.DatasetID, source.TableID)
	if !ok {
		return jobStatistics{Executor: "extract", Simulated: false}, fmt.Errorf("source table not found")
	}

	format := job.ExtractDestinationFormat
	if format == "" {
		format = "CSV"
	}

	totalBytes, err := writeExtractDestinations(job.ExtractDestinationURIs, format, schema, rows, job.ExtractFieldDelimiter, job.ExtractPrintHeader)
	if err != nil {
		return jobStatistics{Executor: "extract", Simulated: false}, err
	}

	return jobStatistics{Executor: "extract", Simulated: false, TotalSlotMs: 45, ProcessedBytes: totalBytes, OutputRows: int64(len(rows))}, nil
}

func writeExtractDestinations(uris []string, format string, schema []tableField, rows [][]string, fieldDelimiter string, printHeader bool) (int64, error) {
	payload, err := encodeExtractPayload(format, schema, rows, fieldDelimiter, printHeader)
	if err != nil {
		return 0, err
	}

	var totalBytes int64
	for _, uri := range uris {
		path, err := resolveExtractShardPath(uri)
		if err != nil {
			return 0, err
		}
		// GCS (real or fake-local) has no real directory concept: object
		// paths like "out/events.csv" don't require a pre-existing "out"
		// folder, so the parent directory is created on write rather than
		// requiring callers to pre-create it themselves.
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			return 0, fmt.Errorf("failed to create parent directory for destinationUri %q: %w", uri, err)
		}
		if err := os.WriteFile(path, payload, 0o644); err != nil {
			return 0, fmt.Errorf("failed to write destinationUri %q: %w", uri, err)
		}
		totalBytes += int64(len(payload))
	}
	return totalBytes, nil
}

func encodeExtractPayload(format string, schema []tableField, rows [][]string, fieldDelimiter string, printHeader bool) ([]byte, error) {
	switch format {
	case "NEWLINE_DELIMITED_JSON":
		return encodeNDJSON(schema, rows)
	case "CSV":
		return encodeCSV(schema, rows, fieldDelimiter, printHeader)
	case "AVRO":
		return encodeAvro(schema, rows)
	case "PARQUET":
		return encodeParquet(schema, rows)
	default:
		return nil, fmt.Errorf("destinationFormat %q is not supported; local extract currently supports NEWLINE_DELIMITED_JSON, CSV, AVRO and PARQUET", format)
	}
}

func encodeNDJSON(schema []tableField, rows [][]string) ([]byte, error) {
	var buf bytes.Buffer
	for _, row := range rows {
		record := make(map[string]any, len(schema))
		for i, field := range schema {
			if i >= len(row) {
				record[field.Name] = nil
				continue
			}
			record[field.Name] = stringToJSONValue(row[i], field.Type)
		}
		encoded, err := json.Marshal(record)
		if err != nil {
			return nil, fmt.Errorf("failed to encode row as NDJSON: %w", err)
		}
		buf.Write(encoded)
		buf.WriteByte('\n')
	}
	return buf.Bytes(), nil
}

func encodeCSV(schema []tableField, rows [][]string, fieldDelimiter string, printHeader bool) ([]byte, error) {
	var buf bytes.Buffer
	writer := csv.NewWriter(&buf)
	if delim := []rune(fieldDelimiter); len(delim) > 0 {
		writer.Comma = delim[0]
	}
	if printHeader {
		header := make([]string, len(schema))
		for i, field := range schema {
			header[i] = field.Name
		}
		if err := writer.Write(header); err != nil {
			return nil, fmt.Errorf("failed to write CSV header: %w", err)
		}
	}
	for _, row := range rows {
		if err := writer.Write(row); err != nil {
			return nil, fmt.Errorf("failed to write CSV row: %w", err)
		}
	}
	writer.Flush()
	if err := writer.Error(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// stringToJSONValue converts an internal string cell back to a typed JSON
// value using the destination field type, so NDJSON extract round-trips
// numbers and booleans instead of re-encoding everything as strings.
func stringToJSONValue(v string, fieldType string) any {
	switch strings.ToUpper(fieldType) {
	case "INT64", "INTEGER":
		if n, err := strconv.ParseInt(v, 10, 64); err == nil {
			return n
		}
	case "FLOAT64", "FLOAT":
		if f, err := strconv.ParseFloat(v, 64); err == nil {
			return f
		}
	case "BOOL", "BOOLEAN":
		if b, err := strconv.ParseBool(v); err == nil {
			return b
		}
	}
	return v
}

// parseAvroRows reads an Avro Object Container File and projects each record
// onto schema field order by name, mirroring the NDJSON path. The emulator
// does not autodetect a BigQuery schema from the file's embedded Avro
// schema; schema.fields is required just like NDJSON and CSV.
func parseAvroRows(uri string, data []byte, schema []tableField) ([][]string, error) {
	reader, err := goavro.NewOCFReader(bytes.NewReader(data))
	if err != nil {
		return nil, fmt.Errorf("invalid Avro OCF in sourceUri %q: %w", uri, err)
	}

	var rows [][]string
	for reader.Scan() {
		datum, err := reader.Read()
		if err != nil {
			return nil, fmt.Errorf("invalid Avro record in sourceUri %q: %w", uri, err)
		}
		record, ok := datum.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("expected an Avro record in sourceUri %q, got %T", uri, datum)
		}
		row := make([]string, len(schema))
		for i, field := range schema {
			row[i] = scalarValueToString(record[field.Name])
		}
		rows = append(rows, row)
	}
	if err := reader.Err(); err != nil {
		return nil, fmt.Errorf("failed to read Avro sourceUri %q: %w", uri, err)
	}
	return rows, nil
}

// encodeAvro writes rows as an Avro Object Container File using a record
// schema derived from schema field names/types. Fields are encoded as
// non-nullable scalars (no union/null branch): this codebase has no NULLABLE
// vs REQUIRED mode tracking for any format yet, so Avro follows the same
// bound as NDJSON/CSV rather than inventing null support only here. A row
// value that fails to parse as its declared type falls back to that type's
// zero value instead of failing the whole encode.
func encodeAvro(schema []tableField, rows [][]string) ([]byte, error) {
	schemaJSON, err := buildAvroSchemaJSON(schema)
	if err != nil {
		return nil, fmt.Errorf("failed to build Avro schema: %w", err)
	}

	var buf bytes.Buffer
	writer, err := goavro.NewOCFWriter(goavro.OCFConfig{W: &buf, Schema: schemaJSON})
	if err != nil {
		return nil, fmt.Errorf("failed to create Avro writer: %w", err)
	}

	records := make([]any, 0, len(rows))
	for _, row := range rows {
		record := make(map[string]any, len(schema))
		for i, field := range schema {
			if i >= len(row) {
				record[field.Name] = avroZeroValue(field.Type)
				continue
			}
			record[field.Name] = stringToAvroValue(row[i], field.Type)
		}
		records = append(records, record)
	}
	if len(records) > 0 {
		if err := writer.Append(records); err != nil {
			return nil, fmt.Errorf("failed to encode Avro rows: %w", err)
		}
	}
	return buf.Bytes(), nil
}

type avroFieldSchema struct {
	Name string `json:"name"`
	Type string `json:"type"`
}

type avroRecordSchema struct {
	Type   string            `json:"type"`
	Name   string            `json:"name"`
	Fields []avroFieldSchema `json:"fields"`
}

func buildAvroSchemaJSON(schema []tableField) (string, error) {
	fields := make([]avroFieldSchema, 0, len(schema))
	for _, field := range schema {
		fields = append(fields, avroFieldSchema{Name: field.Name, Type: avroTypeFor(field.Type)})
	}
	encoded, err := json.Marshal(avroRecordSchema{Type: "record", Name: "LocaQLRow", Fields: fields})
	if err != nil {
		return "", err
	}
	return string(encoded), nil
}

func avroTypeFor(bqType string) string {
	switch strings.ToUpper(bqType) {
	case "INT64", "INTEGER":
		return "long"
	case "FLOAT64", "FLOAT":
		return "double"
	case "BOOL", "BOOLEAN":
		return "boolean"
	default:
		return "string"
	}
}

func stringToAvroValue(v, fieldType string) any {
	switch strings.ToUpper(fieldType) {
	case "INT64", "INTEGER":
		if n, err := strconv.ParseInt(v, 10, 64); err == nil {
			return n
		}
		return int64(0)
	case "FLOAT64", "FLOAT":
		if f, err := strconv.ParseFloat(v, 64); err == nil {
			return f
		}
		return float64(0)
	case "BOOL", "BOOLEAN":
		if b, err := strconv.ParseBool(v); err == nil {
			return b
		}
		return false
	default:
		return v
	}
}

func avroZeroValue(fieldType string) any {
	return stringToAvroValue("", fieldType)
}

// buildParquetSchema derives a parquet.Schema from schema field names/types.
// Fields are Required (non-nullable), matching the same NULLABLE/REQUIRED
// scope bound already documented for Avro: this codebase has no mode
// tracking for any format yet.
func buildParquetSchema(schema []tableField) *parquet.Schema {
	group := make(parquet.Group, len(schema))
	for _, field := range schema {
		group[field.Name] = parquet.Required(parquetNodeFor(field.Type))
	}
	return parquet.NewSchema("LocaQLRow", group)
}

func parquetNodeFor(bqType string) parquet.Node {
	switch strings.ToUpper(bqType) {
	case "INT64", "INTEGER":
		return parquet.Leaf(parquet.Int64Type)
	case "FLOAT64", "FLOAT":
		return parquet.Leaf(parquet.DoubleType)
	case "BOOL", "BOOLEAN":
		return parquet.Leaf(parquet.BooleanType)
	default:
		return parquet.String()
	}
}

// stringToParquetValue mirrors stringToAvroValue: a row value that fails to
// parse as its declared type falls back to that type's zero value rather
// than failing the whole encode, same bound as the other formats' "no
// per-row error tolerance yet" limitation.
func stringToParquetValue(v, fieldType string) any {
	switch strings.ToUpper(fieldType) {
	case "INT64", "INTEGER":
		if n, err := strconv.ParseInt(v, 10, 64); err == nil {
			return n
		}
		return int64(0)
	case "FLOAT64", "FLOAT":
		if f, err := strconv.ParseFloat(v, 64); err == nil {
			return f
		}
		return float64(0)
	case "BOOL", "BOOLEAN":
		if b, err := strconv.ParseBool(v); err == nil {
			return b
		}
		return false
	default:
		return v
	}
}

// parseParquetRows reads a Parquet file and projects each row onto schema
// field order by name, mirroring the Avro/NDJSON path. The emulator does not
// autodetect a BigQuery schema from the Parquet file's embedded schema;
// schema.fields is required just like the other formats.
func parseParquetRows(uri string, data []byte, schema []tableField) ([][]string, error) {
	parquetSchema := buildParquetSchema(schema)
	reader := parquet.NewReader(bytes.NewReader(data), parquetSchema)
	defer reader.Close()

	var rows [][]string
	for {
		record := map[string]any{}
		err := reader.Read(&record)
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("invalid Parquet record in sourceUri %q: %w", uri, err)
		}
		row := make([]string, len(schema))
		for i, field := range schema {
			row[i] = scalarValueToString(record[field.Name])
		}
		rows = append(rows, row)
	}
	return rows, nil
}

// encodeParquet writes rows as a Parquet file using a schema derived from
// schema field names/types.
func encodeParquet(schema []tableField, rows [][]string) ([]byte, error) {
	parquetSchema := buildParquetSchema(schema)

	var buf bytes.Buffer
	writer := parquet.NewWriter(&buf, parquetSchema)
	for _, row := range rows {
		record := make(map[string]any, len(schema))
		for i, field := range schema {
			if i >= len(row) {
				record[field.Name] = stringToParquetValue("", field.Type)
				continue
			}
			record[field.Name] = stringToParquetValue(row[i], field.Type)
		}
		if err := writer.Write(record); err != nil {
			return nil, fmt.Errorf("failed to encode Parquet row: %w", err)
		}
	}
	if err := writer.Close(); err != nil {
		return nil, fmt.Errorf("failed to finalize Parquet file: %w", err)
	}
	return buf.Bytes(), nil
}

// resolveLocalFilePath validates that uri is a local file reference (an
// optional file:// prefix, or a bare path) and rejects gs:// explicitly,
// because the emulator has no fake GCS backend yet. It is shared by load
// (reading) and extract (writing) so both directions fail the same way.
func resolveLocalFilePath(uri string) (string, error) {
	path := strings.TrimSpace(uri)
	if strings.HasPrefix(path, "gs://") {
		return resolveFakeGCSPath(path)
	}
	return strings.TrimPrefix(path, "file://"), nil
}

// resolveFakeGCSPath maps a gs:// URI onto a local directory when
// LOCAQL_FAKE_GCS_ROOT is configured, so load/extract can exercise the same
// sourceUris/destinationUris shape official clients use without a real GCS
// backend. This is a LocaQL-only local-disk mapping, not a GCS-compatible
// HTTP API: it never talks to Google Cloud Storage or emulates its API
// surface. Without the env var, gs:// stays rejected explicitly (previous
// behavior) rather than silently defaulting to some location.
func resolveFakeGCSPath(uri string) (string, error) {
	root := strings.TrimSpace(os.Getenv("LOCAQL_FAKE_GCS_ROOT"))
	if root == "" {
		return "", fmt.Errorf("URI %q uses gs:// which is not supported by the local emulator; use a local file path, or set LOCAQL_FAKE_GCS_ROOT to map gs:// URIs to a local directory", uri)
	}
	rest := strings.TrimPrefix(uri, "gs://")
	bucket, key, _ := strings.Cut(rest, "/")
	if bucket == "" {
		return "", fmt.Errorf("URI %q is missing a bucket name", uri)
	}

	cleanRoot := filepath.Clean(root)
	joined := filepath.Join(cleanRoot, bucket, key)
	if joined != cleanRoot && !strings.HasPrefix(joined, cleanRoot+string(filepath.Separator)) {
		return "", fmt.Errorf("URI %q resolves outside LOCAQL_FAKE_GCS_ROOT", uri)
	}
	return joined, nil
}

// resolveExtractShardPath resolves a destinationUri that may contain a single
// '*' wildcard to the BigQuery convention for a single-shard result: the
// wildcard becomes the first shard index, zero-padded to 12 digits
// ("000000000000"). The emulator does not split large results into multiple
// physical shards yet, so every row always lands in that one file.
func resolveExtractShardPath(uri string) (string, error) {
	path, err := resolveLocalFilePath(uri)
	if err != nil {
		return "", err
	}
	switch strings.Count(path, "*") {
	case 0:
		return path, nil
	case 1:
		return strings.Replace(path, "*", "000000000000", 1), nil
	default:
		return "", fmt.Errorf("destinationUri %q must contain at most one '*' wildcard", uri)
	}
}

// loadRowsFromSourceURIs reads rows for a load job from local sourceUris,
// dispatching on job.LoadSourceFormat. Unsupported formats fail explicitly
// rather than silently falling back to schema-only materialization.
func loadRowsFromSourceURIs(job *jobRecord, schema []tableField) ([][]string, int64, error) {
	switch job.LoadSourceFormat {
	case "NEWLINE_DELIMITED_JSON":
		return loadRowsAcrossURIs(job.LoadSourceURIs, func(uri string, data []byte) ([][]string, error) {
			return parseNDJSONLines(uri, data, schema)
		})
	case "CSV":
		return loadRowsAcrossURIs(job.LoadSourceURIs, func(uri string, data []byte) ([][]string, error) {
			return parseCSVRows(uri, data, schema, job.LoadFieldDelimiter, job.LoadSkipLeadingRows)
		})
	case "AVRO":
		return loadRowsAcrossURIs(job.LoadSourceURIs, func(uri string, data []byte) ([][]string, error) {
			return parseAvroRows(uri, data, schema)
		})
	case "PARQUET":
		return loadRowsAcrossURIs(job.LoadSourceURIs, func(uri string, data []byte) ([][]string, error) {
			return parseParquetRows(uri, data, schema)
		})
	default:
		return nil, 0, fmt.Errorf("sourceFormat %q is not supported; local sourceUris ingestion currently supports NEWLINE_DELIMITED_JSON, CSV, AVRO and PARQUET", job.LoadSourceFormat)
	}
}

// loadRowsAcrossURIs reads and concatenates rows from each local sourceUri,
// delegating the format-specific parsing to parse.
func loadRowsAcrossURIs(uris []string, parse func(uri string, data []byte) ([][]string, error)) ([][]string, int64, error) {
	var rows [][]string
	var totalBytes int64
	for _, uri := range uris {
		path, err := resolveLocalFilePath(uri)
		if err != nil {
			return nil, 0, err
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return nil, 0, fmt.Errorf("failed to read sourceUri %q: %w", uri, err)
		}
		fileRows, err := parse(uri, data)
		if err != nil {
			return nil, 0, err
		}
		rows = append(rows, fileRows...)
		totalBytes += int64(len(data))
	}
	return rows, totalBytes, nil
}

// parseCSVRows parses CSV rows positionally onto schema field order. Row
// width must match len(schema) exactly; jagged rows fail closed rather than
// being silently padded or truncated.
func parseCSVRows(uri string, data []byte, schema []tableField, fieldDelimiter string, skipLeadingRows int) ([][]string, error) {
	reader := csv.NewReader(bytes.NewReader(data))
	reader.FieldsPerRecord = -1
	if delim := []rune(fieldDelimiter); len(delim) > 0 {
		reader.Comma = delim[0]
	}
	records, err := reader.ReadAll()
	if err != nil {
		return nil, fmt.Errorf("invalid CSV in sourceUri %q: %w", uri, err)
	}
	if skipLeadingRows > len(records) {
		skipLeadingRows = len(records)
	}
	records = records[skipLeadingRows:]

	rows := make([][]string, 0, len(records))
	for i, record := range records {
		if len(record) != len(schema) {
			return nil, fmt.Errorf("CSV row %d in sourceUri %q has %d fields, expected %d matching schema", skipLeadingRows+i+1, uri, len(record), len(schema))
		}
		row := make([]string, len(record))
		copy(row, record)
		rows = append(rows, row)
	}
	return rows, nil
}

func parseNDJSONLines(uri string, data []byte, schema []tableField) ([][]string, error) {
	var rows [][]string
	scanner := bufio.NewScanner(bytes.NewReader(data))
	scanner.Buffer(make([]byte, 0, 64*1024), 10*1024*1024)
	lineNum := 0
	for scanner.Scan() {
		lineNum++
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		row, err := parseNDJSONRow(line, schema)
		if err != nil {
			return nil, fmt.Errorf("invalid NDJSON row at %s:%d: %w", uri, lineNum, err)
		}
		rows = append(rows, row)
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("failed to scan sourceUri %q: %w", uri, err)
	}
	return rows, nil
}

func parseNDJSONRow(line string, schema []tableField) ([]string, error) {
	var record map[string]any
	if err := json.Unmarshal([]byte(line), &record); err != nil {
		return nil, err
	}
	row := make([]string, len(schema))
	for i, field := range schema {
		row[i] = scalarValueToString(record[field.Name])
	}
	return row, nil
}

// scalarValueToString stringifies a decoded NDJSON or Avro scalar value.
// encoding/json only ever produces float64 for numbers, while Avro's decoder
// (goavro) produces the Go type matching the Avro schema (int64 for "long",
// float32 for "float", []byte for "bytes"), so both are handled here.
func scalarValueToString(v any) string {
	switch val := v.(type) {
	case nil:
		return ""
	case string:
		return val
	case bool:
		if val {
			return "true"
		}
		return "false"
	case int32:
		return strconv.FormatInt(int64(val), 10)
	case int64:
		return strconv.FormatInt(val, 10)
	case float32:
		return strconv.FormatFloat(float64(val), 'f', -1, 32)
	case float64:
		if !math.IsInf(val, 0) && val == math.Trunc(val) {
			return strconv.FormatInt(int64(val), 10)
		}
		return strconv.FormatFloat(val, 'f', -1, 64)
	case []byte:
		return string(val)
	default:
		encoded, err := json.Marshal(val)
		if err != nil {
			return fmt.Sprintf("%v", val)
		}
		return string(encoded)
	}
}

func parsePagination(r *http.Request, defaultSize, maxSize int) (start, size int) {
	size = defaultSize
	if raw := r.URL.Query().Get("maxResults"); raw != "" {
		if n, err := strconv.Atoi(raw); err == nil && n > 0 {
			size = n
		}
	}
	if size > maxSize {
		size = maxSize
	}

	if token := r.URL.Query().Get("pageToken"); token != "" {
		if n, ok := decodePageToken(token); ok {
			start = n
		}
	}
	return start, size
}

func encodePageToken(start int) string {
	if start < 0 {
		start = 0
	}
	raw := "idx:" + strconv.Itoa(start)
	return base64.RawURLEncoding.EncodeToString([]byte(raw))
}

func decodePageToken(token string) (int, bool) {
	if n, err := strconv.Atoi(token); err == nil && n >= 0 {
		return n, true
	}
	decoded, err := base64.RawURLEncoding.DecodeString(token)
	if err != nil {
		return 0, false
	}
	text := string(decoded)
	if !strings.HasPrefix(text, "idx:") {
		return 0, false
	}
	n, err := strconv.Atoi(strings.TrimPrefix(text, "idx:"))
	if err != nil || n < 0 {
		return 0, false
	}
	return n, true
}

func clampEnd(start, size, total int) int {
	if start > total {
		start = total
	}
	end := start + size
	if end > total {
		end = total
	}
	return end
}
