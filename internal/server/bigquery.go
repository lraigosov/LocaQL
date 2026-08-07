package server

import (
	"bufio"
	"bytes"
	"compress/gzip"
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
	pqcompress "github.com/parquet-go/parquet-go/compress"
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
		// Real BigQuery's tabledata.list lives at .../tables/{tableId}/data (6
		// path segments), not under a separate "tabledata" top-level scope —
		// that shape only exists here as a pre-existing internal alias (see
		// the "tabledata" case in bigQueryV2) that the official client
		// libraries never actually request. Handle the real shape explicitly
		// before falling through to the generic collection/by-ID dispatch,
		// which stops at 5 segments and would otherwise 404 this.
		if len(parts) == 6 && parts[5] == "data" {
			if r.Method != http.MethodGet {
				writeError(w, http.StatusMethodNotAllowed, "Method not allowed", "methodNotAllowed")
				return true
			}
			s.listTableData(w, r, projectID, datasetID, parts[4])
			return true
		}
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

// parseExpirationTimeField parses a table's expirationTime field (accepts a
// numeric string or number, same flexible int64 convention already used for
// defaultTableExpirationMs) as an ABSOLUTE Unix-millis timestamp, matching
// real BigQuery's table-level expirationTime contract. This is a different
// unit of meaning than dataset.defaultTableExpirationMs, which is a DURATION
// relative to table creation time, not an absolute instant. A nil/non-positive
// value clears any expiration (zero time.Time).
func parseExpirationTimeField(v any) (time.Time, bool) {
	ms, ok := parseFlexibleInt64FromAny(v)
	if !ok {
		return time.Time{}, false
	}
	if ms <= 0 {
		return time.Time{}, true
	}
	return time.UnixMilli(ms).UTC(), true
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

	var schema []tableField
	var external *externalTableConfig
	if rawExternal, ok := raw["externalDataConfiguration"].(map[string]any); ok {
		parsed := parseExternalDataConfig(rawExternal)
		if len(parsed.SourceURIs) == 0 {
			writeError(w, http.StatusBadRequest, "externalDataConfiguration.sourceUris is required", "required")
			return
		}
		if parsed.SourceFormat == "" {
			writeError(w, http.StatusBadRequest, "externalDataConfiguration.sourceFormat is required", "required")
			return
		}
		schema = parseTableSchemaFields(raw["schema"])
		if len(schema) == 0 {
			writeError(w, http.StatusBadRequest, "schema.fields is required for external tables; autodetect is not supported", "required")
			return
		}
		external = &externalTableConfig{
			SourceURIs:      parsed.SourceURIs,
			SourceFormat:    parsed.SourceFormat,
			FieldDelimiter:  parsed.FieldDelimiter,
			SkipLeadingRows: parsed.SkipLeadingRows,
		}
	}

	var view *viewConfig
	rawView, hasView := raw["view"].(map[string]any)
	rawMatView, hasMatView := raw["materializedView"].(map[string]any)
	if hasView || hasMatView {
		if hasView && hasMatView {
			writeError(w, http.StatusBadRequest, "a table cannot have both view and materializedView", "invalid")
			return
		}
		if external != nil {
			writeError(w, http.StatusBadRequest, "a table cannot have both externalDataConfiguration and view/materializedView", "invalid")
			return
		}
		target := rawView
		materialized := false
		if hasMatView {
			target = rawMatView
			materialized = true
		}
		query, _ := target["query"].(string)
		query = strings.TrimSpace(query)
		if query == "" {
			writeError(w, http.StatusBadRequest, "view.query (or materializedView.query) is required", "required")
			return
		}
		// Derive the schema by actually running the query once, the same way
		// real BigQuery validates a view's SQL and infers its schema at
		// creation time rather than accepting an unverifiable definition.
		derivedSchema, _, err := s.executeRealSQLQuery(projectID, query, nil)
		if err != nil {
			writeError(w, http.StatusBadRequest, fmt.Sprintf("invalid view query: %s", err.Error()), "invalid")
			return
		}
		schema = derivedSchema
		view = &viewConfig{Query: query, Materialized: materialized}
	}

	if external == nil && view == nil {
		schema = parseTableSchemaFields(raw["schema"])
	}

	var timePartitioning *timePartitioningConfig
	if value, exists := raw["timePartitioning"]; exists {
		var err error
		timePartitioning, err = parseTimePartitioning(value, schema)
		if err != nil {
			writeError(w, http.StatusBadRequest, err.Error(), "invalid")
			return
		}
	}
	var rangePartitioning *rangePartitioningConfig
	if value, exists := raw["rangePartitioning"]; exists {
		var err error
		rangePartitioning, err = parseRangePartitioning(value, schema)
		if err != nil {
			writeError(w, http.StatusBadRequest, err.Error(), "invalid")
			return
		}
	}
	var clustering []string
	if value, exists := raw["clustering"]; exists {
		var err error
		clustering, err = parseClustering(value, schema)
		if err != nil {
			writeError(w, http.StatusBadRequest, err.Error(), "invalid")
			return
		}
	}
	requirePartitionFilter := false
	if value, exists := raw["requirePartitionFilter"]; exists {
		parsed, ok := value.(bool)
		if !ok {
			writeError(w, http.StatusBadRequest, "requirePartitionFilter must be a boolean", "invalid")
			return
		}
		requirePartitionFilter = parsed
	}
	if (external != nil || view != nil) && (timePartitioning != nil || rangePartitioning != nil || len(clustering) > 0 || requirePartitionFilter) {
		writeError(w, http.StatusBadRequest, "standard partitioning and clustering are supported only for managed tables", "invalid")
		return
	}
	if err := validatePartitioningCombination(timePartitioning, rangePartitioning, requirePartitionFilter); err != nil {
		writeError(w, http.StatusBadRequest, err.Error(), "invalid")
		return
	}

	// An explicit expirationTime overrides the dataset's defaultTableExpirationMs
	// (a duration relative to creation), matching real BigQuery precedence.
	var expirationTime time.Time
	if v, ok := raw["expirationTime"]; ok {
		et, ok := parseExpirationTimeField(v)
		if !ok {
			writeError(w, http.StatusBadRequest, "expirationTime must be a numeric string or number", "invalid")
			return
		}
		expirationTime = et
	} else if ds, ok := s.datasets.get(projectID, datasetID); ok && ds.DefaultTableExpirationMs > 0 {
		expirationTime = s.tables.now().UTC().Add(time.Duration(ds.DefaultTableExpirationMs) * time.Millisecond)
	}

	item, created := s.tables.insert(tableInsert{
		ProjectID:              projectID,
		DatasetID:              datasetID,
		TableID:                tableID,
		FriendlyName:           friendlyName,
		Description:            description,
		Labels:                 labels,
		Schema:                 schema,
		External:               external,
		View:                   view,
		TimePartitioning:       timePartitioning,
		RangePartitioning:      rangePartitioning,
		Clustering:             clustering,
		RequirePartitionFilter: requirePartitionFilter,
		ExpirationTime:         expirationTime,
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
	current, exists, _ := s.tables.get(projectID, datasetID, tableID)
	if !exists {
		writeError(w, http.StatusNotFound, fmt.Sprintf("Not found: Table %s:%s.%s", projectID, datasetID, tableID), "notFound")
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
	if v, ok := raw["expirationTime"]; ok {
		et, ok := parseExpirationTimeField(v)
		if !ok {
			writeError(w, http.StatusBadRequest, "expirationTime must be a numeric string or number", "invalid")
			return
		}
		patch.HasExpirationTime = true
		patch.ExpirationTime = et
	}
	if _, ok := raw["schema"]; ok {
		newSchema := parseTableSchemaFields(raw["schema"])
		if err := validateSchemaEvolution(current.Schema, newSchema); err != nil {
			writeError(w, http.StatusBadRequest, err.Error(), "invalid")
			return
		}
		patch.HasSchema = true
		patch.Schema = newSchema
	}

	effectiveSchema := current.Schema
	if patch.HasSchema {
		effectiveSchema = patch.Schema
	}
	effectiveTime := cloneTimePartitioning(current.TimePartitioning)
	effectiveRange := cloneRangePartitioning(current.RangePartitioning)
	effectiveRequire := current.RequirePartitionFilter
	if value, ok := raw["timePartitioning"]; ok {
		parsed, err := parseTimePartitioning(value, effectiveSchema)
		if err != nil {
			writeError(w, http.StatusBadRequest, err.Error(), "invalid")
			return
		}
		if current.TimePartitioning == nil || !sameTimePartitioningDefinition(current.TimePartitioning, parsed) {
			writeError(w, http.StatusBadRequest, "time partitioning type and field are immutable after table creation", "invalid")
			return
		}
		patch.HasTimePartitioning = true
		patch.TimePartitioning = parsed
		effectiveTime = parsed
	}
	if value, ok := raw["rangePartitioning"]; ok {
		parsed, err := parseRangePartitioning(value, effectiveSchema)
		if err != nil {
			writeError(w, http.StatusBadRequest, err.Error(), "invalid")
			return
		}
		if current.RangePartitioning == nil || !sameRangePartitioning(current.RangePartitioning, parsed) {
			writeError(w, http.StatusBadRequest, "range partitioning is immutable after table creation", "invalid")
			return
		}
		patch.HasRangePartitioning = true
		patch.RangePartitioning = parsed
		effectiveRange = parsed
	}
	if value, ok := raw["clustering"]; ok {
		parsed, err := parseClustering(value, effectiveSchema)
		if err != nil {
			writeError(w, http.StatusBadRequest, err.Error(), "invalid")
			return
		}
		patch.HasClustering = true
		patch.Clustering = parsed
	}
	if value, ok := raw["requirePartitionFilter"]; ok {
		parsed, ok := value.(bool)
		if !ok {
			writeError(w, http.StatusBadRequest, "requirePartitionFilter must be a boolean", "invalid")
			return
		}
		patch.HasRequirePartitionFilter = true
		patch.RequirePartitionFilter = parsed
		effectiveRequire = parsed
	}
	if (current.External != nil || current.View != nil) && (patch.HasTimePartitioning || patch.HasRangePartitioning || patch.HasClustering || patch.HasRequirePartitionFilter) {
		writeError(w, http.StatusBadRequest, "standard partitioning and clustering are supported only for managed tables", "invalid")
		return
	}
	if err := validatePartitioningCombination(effectiveTime, effectiveRange, effectiveRequire); err != nil {
		writeError(w, http.StatusBadRequest, err.Error(), "invalid")
		return
	}
	if err := validateRowsForPartitioning(effectiveSchema, current.Rows, effectiveTime, effectiveRange); err != nil {
		writeError(w, http.StatusBadRequest, err.Error(), "invalid")
		return
	}

	if !patch.HasFriendlyName && !patch.HasDescription && !patch.HasLabels && !patch.HasExpirationTime && !patch.HasSchema && !patch.HasTimePartitioning && !patch.HasRangePartitioning && !patch.HasClustering && !patch.HasRequirePartitionFilter {
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
	current, exists, _ := s.tables.get(projectID, datasetID, tableID)
	if !exists {
		writeError(w, http.StatusNotFound, fmt.Sprintf("Not found: Table %s:%s.%s", projectID, datasetID, tableID), "notFound")
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

	var expirationTime time.Time
	if v, ok := raw["expirationTime"]; ok {
		et, ok := parseExpirationTimeField(v)
		if !ok {
			writeError(w, http.StatusBadRequest, "expirationTime must be a numeric string or number", "invalid")
			return
		}
		expirationTime = et
	}

	timePartitioning := cloneTimePartitioning(current.TimePartitioning)
	if value, ok := raw["timePartitioning"]; ok {
		parsed, err := parseTimePartitioning(value, current.Schema)
		if err != nil {
			writeError(w, http.StatusBadRequest, err.Error(), "invalid")
			return
		}
		if current.TimePartitioning == nil || !sameTimePartitioningDefinition(current.TimePartitioning, parsed) {
			writeError(w, http.StatusBadRequest, "time partitioning type and field are immutable after table creation", "invalid")
			return
		}
		timePartitioning = parsed
	}
	rangePartitioning := cloneRangePartitioning(current.RangePartitioning)
	if value, ok := raw["rangePartitioning"]; ok {
		parsed, err := parseRangePartitioning(value, current.Schema)
		if err != nil {
			writeError(w, http.StatusBadRequest, err.Error(), "invalid")
			return
		}
		if current.RangePartitioning == nil || !sameRangePartitioning(current.RangePartitioning, parsed) {
			writeError(w, http.StatusBadRequest, "range partitioning is immutable after table creation", "invalid")
			return
		}
		rangePartitioning = parsed
	}
	var clustering []string
	if value, ok := raw["clustering"]; ok {
		parsed, err := parseClustering(value, current.Schema)
		if err != nil {
			writeError(w, http.StatusBadRequest, err.Error(), "invalid")
			return
		}
		clustering = parsed
	}
	requirePartitionFilter := false
	if value, ok := raw["requirePartitionFilter"]; ok {
		parsed, ok := value.(bool)
		if !ok {
			writeError(w, http.StatusBadRequest, "requirePartitionFilter must be a boolean", "invalid")
			return
		}
		requirePartitionFilter = parsed
	}
	if (current.External != nil || current.View != nil) && (raw["timePartitioning"] != nil || raw["rangePartitioning"] != nil || raw["clustering"] != nil || requirePartitionFilter) {
		writeError(w, http.StatusBadRequest, "standard partitioning and clustering are supported only for managed tables", "invalid")
		return
	}
	if err := validatePartitioningCombination(timePartitioning, rangePartitioning, requirePartitionFilter); err != nil {
		writeError(w, http.StatusBadRequest, err.Error(), "invalid")
		return
	}

	item, ok := s.tables.update(tableUpdate{
		ProjectID:              projectID,
		DatasetID:              datasetID,
		TableID:                tableID,
		FriendlyName:           friendlyName,
		Description:            description,
		Labels:                 labels,
		ExpirationTime:         expirationTime,
		TimePartitioning:       timePartitioning,
		RangePartitioning:      rangePartitioning,
		Clustering:             clustering,
		RequirePartitionFilter: requirePartitionFilter,
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
	// numRows/numBytes were never set here at all, so official clients
	// (Table.num_rows / Table.num_bytes) always read None regardless of
	// whether a load job had actually populated the table. Reported only for
	// managed tables — external tables and views don't materialize rows into
	// t.Rows the way a plain TABLE does, so real BigQuery doesn't report a
	// meaningful row/byte count for them either.
	if t.External == nil && t.View == nil {
		resp["numRows"] = strconv.FormatInt(int64(len(t.Rows)), 10)
		resp["numBytes"] = strconv.FormatInt(estimateRowsByteSize(t.Rows), 10)
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
	if !t.ExpirationTime.IsZero() {
		resp["expirationTime"] = strconv.FormatInt(t.ExpirationTime.UnixMilli(), 10)
	}
	if t.TimePartitioning != nil {
		partitioning := map[string]any{"type": t.TimePartitioning.Type}
		if t.TimePartitioning.Field != "" {
			partitioning["field"] = t.TimePartitioning.Field
		}
		if t.TimePartitioning.ExpirationMs > 0 {
			partitioning["expirationMs"] = strconv.FormatInt(t.TimePartitioning.ExpirationMs, 10)
		}
		resp["timePartitioning"] = partitioning
	}
	if t.RangePartitioning != nil {
		resp["rangePartitioning"] = map[string]any{
			"field": t.RangePartitioning.Field,
			"range": map[string]string{
				"start":    strconv.FormatInt(t.RangePartitioning.Start, 10),
				"end":      strconv.FormatInt(t.RangePartitioning.End, 10),
				"interval": strconv.FormatInt(t.RangePartitioning.Interval, 10),
			},
		}
	}
	if len(t.Clustering) > 0 {
		resp["clustering"] = map[string]any{"fields": cloneStrings(t.Clustering)}
	}
	if t.RequirePartitionFilter {
		resp["requirePartitionFilter"] = true
	}
	if t.External != nil {
		resp["type"] = "EXTERNAL"
		external := map[string]any{
			"sourceUris":   t.External.SourceURIs,
			"sourceFormat": t.External.SourceFormat,
		}
		if t.External.FieldDelimiter != "" {
			external["fieldDelimiter"] = t.External.FieldDelimiter
		}
		if t.External.SkipLeadingRows > 0 {
			external["skipLeadingRows"] = t.External.SkipLeadingRows
		}
		resp["externalDataConfiguration"] = external
	} else if t.View != nil && t.View.Materialized {
		resp["type"] = "MATERIALIZED_VIEW"
		resp["materializedView"] = map[string]any{"query": t.View.Query}
	} else if t.View != nil {
		resp["type"] = "VIEW"
		resp["view"] = map[string]any{"query": t.View.Query}
	} else {
		resp["type"] = "TABLE"
	}
	return resp
}

type externalDataConfigParsed struct {
	SourceURIs      []string
	SourceFormat    string
	FieldDelimiter  string
	SkipLeadingRows int
}

// parseExternalDataConfig mirrors parseLoadConfig's flat field layout
// (sourceUris/sourceFormat/fieldDelimiter/skipLeadingRows) instead of real
// BigQuery's nested csvOptions, for consistency with how load jobs already
// parse the same concepts in this codebase.
func parseExternalDataConfig(raw map[string]any) externalDataConfigParsed {
	out := externalDataConfigParsed{
		SourceURIs: extractStringList(raw["sourceUris"]),
	}
	if value, ok := raw["sourceFormat"].(string); ok {
		out.SourceFormat = strings.TrimSpace(value)
	}
	if value, ok := raw["fieldDelimiter"].(string); ok {
		out.FieldDelimiter = value
	}
	if value, ok := parseFlexibleInt64FromAny(raw["skipLeadingRows"]); ok && value >= 0 {
		out.SkipLeadingRows = int(value)
	}
	return out
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
	uploadedMedia, hasUploadedMedia := uploadedLoadMediaFromRequest(r)
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
	loadInlineData := []byte(nil)
	loadInlineName := ""
	loadInline := false
	loadSourceFormat := ""
	loadFieldDelimiter := ""
	loadSkipLeadingRows := 0
	loadCompression := ""
	extractSourceTable := tableReference{}
	extractDestinationURIs := []string(nil)
	extractDestinationFormat := ""
	extractFieldDelimiter := ""
	extractPrintHeader := true
	extractCompression := ""
	createDisposition := ""
	writeDisposition := ""
	priority := "INTERACTIVE"
	createSession := false
	var connectionProperties []connectionProperty
	var queryParameters []storedQueryParameter
	parameterMode := ""
	var queryParametersErr error
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
						if cs, ok := qCfg["createSession"].(bool); ok {
							createSession = cs
						}
						if propsRaw, ok := qCfg["connectionProperties"].([]any); ok {
							for _, pRaw := range propsRaw {
								pMap, ok := pRaw.(map[string]any)
								if !ok {
									continue
								}
								key, _ := pMap["key"].(string)
								value, _ := pMap["value"].(string)
								connectionProperties = append(connectionProperties, connectionProperty{Key: key, Value: value})
							}
						}
						rawParameterMode, _ := qCfg["parameterMode"].(string)
						rawQueryParameters, _ := qCfg["queryParameters"].([]any)
						queryParameters, parameterMode, queryParametersErr = parseQueryParametersFromRaw(rawParameterMode, rawQueryParameters)
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
							loadCompression = parsed.Compression
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
							extractCompression = parsed.Compression
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
	if queryParametersErr != nil {
		writeError(w, http.StatusBadRequest, queryParametersErr.Error(), "invalid")
		return
	}
	if hasUploadedMedia {
		if jobType != "load" {
			writeError(w, http.StatusBadRequest, "uploaded media requires configuration.load", "invalid")
			return
		}
		loadInlineData = uploadedMedia.Data
		loadInlineName = uploadedMedia.Name
		loadInline = true
	}
	if len(splitScriptStatements(queryText)) > 1 {
		isScript = true
	}

	sessionID, err := s.resolveSessionID(projectID, userEmail, createSession, connectionProperties)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error(), "invalid")
		return
	}

	insertOpts := jobInsertOptions{
		ProjectID:                projectID,
		RequestID:                requestID,
		UserEmail:                userEmail,
		QueryText:                queryText,
		JobType:                  jobType,
		Priority:                 priority,
		SessionID:                sessionID,
		SourceTables:             sourceTables,
		LoadSchema:               loadSchema,
		LoadSourceURIs:           loadSourceURIs,
		LoadInlineData:           loadInlineData,
		LoadInlineName:           loadInlineName,
		LoadInline:               loadInline,
		LoadSourceFormat:         loadSourceFormat,
		LoadFieldDelimiter:       loadFieldDelimiter,
		LoadSkipLeadingRows:      loadSkipLeadingRows,
		LoadCompression:          loadCompression,
		ExtractSourceTable:       extractSourceTable,
		ExtractDestinationURIs:   extractDestinationURIs,
		ExtractDestinationFormat: extractDestinationFormat,
		ExtractFieldDelimiter:    extractFieldDelimiter,
		ExtractPrintHeader:       extractPrintHeader,
		ExtractCompression:       extractCompression,
		CreateDisposition:        createDisposition,
		WriteDisposition:         writeDisposition,
		TargetDataset:            targetDataset,
		TargetTable:              targetTable,
		IsScript:                 isScript,
		ParameterMode:            parameterMode,
		QueryParameters:          queryParameters,
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
		Query                string               `json:"query"`
		MaxResults           int                  `json:"maxResults"`
		TimeoutMs            int                  `json:"timeoutMs"`
		DryRun               bool                 `json:"dryRun"`
		RequestId            string               `json:"requestId"`
		Priority             string               `json:"priority"`
		CreateSession        bool                 `json:"createSession"`
		ConnectionProperties []connectionProperty `json:"connectionProperties"`
		ParameterMode        string               `json:"parameterMode"`
		QueryParameters      []any                `json:"queryParameters"`
	}
	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
		writeError(w, http.StatusBadRequest, "Invalid request body", "invalid")
		return
	}

	queryParams, parameterMode, err := parseQueryParametersFromRaw(payload.ParameterMode, payload.QueryParameters)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error(), "invalid")
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

	sessionID, err := s.resolveSessionID(projectID, userEmail, payload.CreateSession, payload.ConnectionProperties)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error(), "invalid")
		return
	}

	insertOpts := jobInsertOptions{
		ProjectID:       projectID,
		RequestID:       requestID,
		UserEmail:       userEmail,
		QueryText:       payload.Query,
		JobType:         "query",
		Priority:        payload.Priority,
		SessionID:       sessionID,
		ParameterMode:   parameterMode,
		QueryParameters: queryParams,
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
	if j.State == jobStateDone && j.ErrorReason != "" {
		writeError(w, http.StatusBadRequest, j.ErrorMessage, j.ErrorReason)
		return
	}

	start, size := parsePagination(r, 20, 1000)
	// A DONE query job's result set was already computed once by
	// executeQueryJob and cached on Statistics.ResultSchema/ResultRows —
	// reuse it instead of re-running queryText a second time. This isn't
	// just an optimization: some query text has real, non-idempotent side
	// effects (session control statements, see jobStatistics.ResultSchema),
	// so re-running it here to serve a second/paginated fetch would
	// re-apply those side effects instead of returning the already-computed
	// result.
	var schema []tableField
	var values [][]string
	if j.State == jobStateDone && j.Statistics.ResultSchema != nil {
		schema, values = j.Statistics.ResultSchema, j.Statistics.ResultRows
	} else {
		// Preserve the existing early-result behavior for side-effect-free
		// SELECTs, but never execute DDL/DML or session control merely because a
		// PENDING/RUNNING job was polled. A mutation must run exactly once after
		// its job acquires the target-table lock.
		_, mutating, _ := parsePersistentSQLStatement(projectID, j.QueryText)
		trimmedQuery := strings.TrimSpace(strings.TrimSuffix(strings.TrimSpace(j.QueryText), ";"))
		sessionControl := sessionBeginPattern.MatchString(trimmedQuery) || sessionCommitPattern.MatchString(trimmedQuery) || sessionRollbackPattern.MatchString(trimmedQuery) || sessionCreateTempTablePattern.MatchString(trimmedQuery)
		if mutating || sessionControl {
			schema, values = []tableField{}, [][]string{}
		} else {
			var err error
			schema, values, err = s.simulateQueryResultTable(projectID, j.SessionID, j.QueryText, j.UserEmail, j.ParameterMode, j.QueryParameters)
			if err != nil {
				writeError(w, http.StatusBadRequest, err.Error(), "invalid")
				return
			}
		}
	}
	end := clampEnd(start, size, len(values))
	rows := renderRESTRows(schema, values[start:end])

	resp := map[string]any{
		"kind": kind,
		"jobReference": map[string]string{
			"projectId": projectID,
			"jobId":     jobID,
		},
		"schema": map[string]any{
			"fields": renderTableSchemaFields(schema),
		},
		"rows":           rows,
		"totalRows":      strconv.Itoa(len(values)),
		"jobComplete":    j.State == jobStateDone,
		"maxResults":     size,
		"startIndexUsed": start,
	}
	if end < len(values) {
		resp["pageToken"] = encodePageToken(end)
	}
	if j.SessionID != "" {
		resp["sessionInfo"] = map[string]string{"sessionId": j.SessionID}
	}
	if isDMLStatementType(j.Statistics.StatementType) {
		resp["numDmlAffectedRows"] = strconv.FormatInt(j.Statistics.DMLAffectedRows, 10)
	}

	writeJSON(w, http.StatusOK, resp)
}

func (s *Server) simulateQueryResultTable(projectID, sessionID, queryText, callingUserEmail string, paramMode string, params []storedQueryParameter) ([]tableField, [][]string, error) {
	result, err := s.executeQueryStatement(projectID, sessionID, queryText, callingUserEmail, paramMode, params)
	return result.schema, result.rows, err
}

// executeQueryStatement is the single execution dispatcher for SELECT,
// INFORMATION_SCHEMA, session control, and persistent DDL/DML. Keeping the
// mutation path here ensures jobs.query and jobs.insert share exactly the same
// semantics and that executeQueryJob can retain statement-specific statistics.
func (s *Server) executeQueryStatement(projectID, sessionID, queryText, callingUserEmail string, paramMode string, params []storedQueryParameter) (persistentSQLResult, error) {
	trimmed := strings.TrimSpace(queryText)
	if trimmed == "" {
		return persistentSQLResult{schema: []tableField{{Name: "result", Type: "STRING"}}, rows: [][]string{{"query job executed"}}, statementType: "SELECT"}, nil
	}
	trimmed = strings.TrimSpace(strings.TrimSuffix(trimmed, ";"))

	var sess *sessionRecord
	if sessionID != "" {
		var handled bool
		var schema []tableField
		var rows [][]string
		var err error
		sess, handled, schema, rows, err = s.executeSessionControlStatement(projectID, sessionID, trimmed)
		if handled {
			return persistentSQLResult{schema: schema, rows: rows, statementType: "SCRIPT"}, err
		}
	}

	lower := strings.ToLower(trimmed)
	if schema, rows, ok := s.simulateInformationSchemaQuery(projectID, trimmed, lower, callingUserEmail); ok {
		return persistentSQLResult{schema: flatSchemaToTableFields(schema), rows: rows, statementType: "SELECT"}, nil
	}
	if result, handled, err := s.executePersistentSQLStatement(projectID, trimmed, sess, paramMode, params); handled {
		return result, err
	}
	schema, rows, err := s.executeRealSQLQueryWithParams(projectID, trimmed, sess, paramMode, params)
	return persistentSQLResult{schema: schema, rows: rows, statementType: "SELECT"}, err
}

// flatSchemaToTableFields converts an INFORMATION_SCHEMA builder's flat
// {name,type} schema (LocaQL's own synthetic metadata tables — never
// nested) into tableField, the single schema type simulateQueryResultTable
// now returns regardless of which path produced it.
func flatSchemaToTableFields(schema []map[string]string) []tableField {
	fields := make([]tableField, len(schema))
	for i, f := range schema {
		fields[i] = tableField{Name: f["name"], Type: f["type"]}
	}
	return fields
}

func (s *Server) listTableData(w http.ResponseWriter, r *http.Request, projectID, datasetID, tableID string) {
	schema, rawRows, ok, err := s.resolveTableRows(projectID, datasetID, tableID)
	if !ok {
		writeError(w, http.StatusNotFound, fmt.Sprintf("Not found: Table %s:%s.%s", projectID, datasetID, tableID), "notFound")
		return
	}
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error(), "invalid")
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

	values := make([][]string, 0, end-start)
	for _, row := range rows[start:end] {
		values = append(values, row.Values)
	}
	out := renderRESTRows(schema, values)

	resp := map[string]any{
		"kind":           "bigquery#tableDataList",
		"etag":           "locaql",
		"totalRows":      strconv.Itoa(len(rows)),
		"rows":           out,
		"datasetId":      datasetID,
		"tableId":        tableID,
		"projectId":      projectID,
		"maxResults":     size,
		"startIndexUsed": start,
	}
	// "pageToken" here (echoing the request's own start, unconditionally) was
	// a real, previously-undiscovered bug, not just a redundant field: the
	// official client's RowIterator for tabledata.list is constructed with
	// next_token="pageToken" (google-cloud-bigquery's table.py), i.e. it
	// checks for the mere *presence* of a "pageToken" key in the response —
	// not "nextPageToken" — to decide whether to keep paging. Always
	// including it, even on the final page, made list_rows() loop forever
	// re-fetching the same page. "nextPageToken" is kept alongside it only
	// for whatever already depends on that name (e.g. this endpoint's other
	// existing test, TestTableDataListPagination) — both are set to the same
	// value and both are omitted once there's nothing left to page through.
	if end < len(rows) {
		token := encodePageToken(end)
		resp["pageToken"] = token
		resp["nextPageToken"] = token
	}
	writeJSON(w, http.StatusOK, resp)
}

// renderTableSchemaFields renders a schema for the REST API, recursively:
// a RECORD/STRUCT field's own nested Fields become a "fields" array, and
// mode defaults to "NULLABLE" (BigQuery's own default) when unset, matching
// real BigQuery's schema JSON shape rather than the flat {name,type} pairs
// this emulator used before nested schemas existed.
func renderTableSchemaFields(fields []tableField) []map[string]any {
	out := make([]map[string]any, 0, len(fields))
	for _, field := range fields {
		mode := field.Mode
		if mode == "" {
			mode = "NULLABLE"
		}
		entry := map[string]any{"name": field.Name, "type": field.Type, "mode": mode}
		if isRecordType(field.Type) && len(field.Fields) > 0 {
			entry["fields"] = renderTableSchemaFields(field.Fields)
		}
		out = append(out, entry)
	}
	return out
}

// renderRESTRows converts stored rows into BigQuery's REST row shape
// ({"f": [{"v": ...}, ...]}), one call site shared by tabledata.list and
// query results.
func renderRESTRows(schema []tableField, rows [][]string) []map[string]any {
	out := make([]map[string]any, 0, len(rows))
	for _, row := range rows {
		cells := make([]map[string]any, len(schema))
		for i, field := range schema {
			raw := storedNullCell
			if i < len(row) {
				raw = row[i]
			}
			cells[i] = map[string]any{"v": renderCellForREST(field, raw)}
		}
		out = append(out, map[string]any{"f": cells})
	}
	return out
}

// renderCellForREST converts one stored cell (BigQuery REST convention: a
// plain string for scalars; a canonical JSON object/array for RECORD/
// REPEATED, per the storage convention established in sql_engine.go) into
// BigQuery's actual REST value shape: unchanged for scalars, a nested
// {"f": [{"v": ...}, ...]} row for RECORD (ordered by field.Fields), or
// [{"v": ...}, ...] of single rendered values for REPEATED. A stored value
// that fails to parse as the expected JSON shape falls back to the raw
// string rather than failing the whole response.
func renderCellForREST(field tableField, raw string) any {
	var isNull bool
	raw, isNull = loadStoredCell(raw)
	if isNull {
		return nil
	}
	if raw == "" {
		return raw
	}
	if field.Mode == "REPEATED" {
		var decoded []any
		if err := json.Unmarshal([]byte(raw), &decoded); err != nil {
			return raw
		}
		base := field
		base.Mode = ""
		out := make([]map[string]any, len(decoded))
		for i, elem := range decoded {
			out[i] = map[string]any{"v": renderDecodedCellForREST(base, elem)}
		}
		return out
	}
	if isRecordType(field.Type) {
		var decoded map[string]any
		if err := json.Unmarshal([]byte(raw), &decoded); err != nil {
			return raw
		}
		return renderDecodedCellForREST(field, decoded)
	}
	return renderScalarCellForREST(field, raw)
}

// renderScalarCellForREST applies BigQuery's wire representation for scalar
// values whose REST encoding differs from the human-readable catalog format.
// In particular, TIMESTAMP cells are integer microseconds since Unix epoch;
// official clients use that representation to construct timezone-aware
// datetime values. Malformed legacy values remain visible unchanged instead
// of making an entire page unreadable.
func renderScalarCellForREST(field tableField, raw string) any {
	if strings.EqualFold(field.Type, "TIMESTAMP") {
		if parsed, err := parsePartitionTimestamp(strings.Trim(raw, `"`)); err == nil {
			return strconv.FormatInt(parsed.UTC().UnixMicro(), 10)
		}
	}
	return raw
}

// renderDecodedCellForREST is renderCellForREST's counterpart for a value
// that has already been json.Unmarshal-decoded (used for elements inside a
// REPEATED array, which have no separate raw string of their own).
func renderDecodedCellForREST(field tableField, decoded any) any {
	if decoded == nil {
		return nil
	}
	if isRecordType(field.Type) {
		obj, _ := decoded.(map[string]any)
		cells := make([]map[string]any, len(field.Fields))
		for i, sub := range field.Fields {
			cells[i] = map[string]any{"v": renderDecodedCellForREST(sub, obj[sub.Name])}
		}
		return map[string]any{"f": cells}
	}
	return renderScalarCellForREST(field, scalarValueToPlainString(decoded))
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
	Compression       string
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
	if value, ok := parseFlexibleInt64FromAny(loadCfg["skipLeadingRows"]); ok && value >= 0 {
		out.SkipLeadingRows = int(value)
	}
	if value, ok := loadCfg["compression"].(string); ok {
		out.Compression = value
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
	Compression       string
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
	if value, ok := extractCfg["compression"].(string); ok {
		out.Compression = value
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

// validateSchemaEvolution allows exactly the schema changes real BigQuery
// allows for an existing table via tables.patch: adding new columns
// (which must be NULLABLE, since existing rows have no value for them) and
// relaxing an existing column's mode from REQUIRED to NULLABLE. Removing a
// column, renaming or reordering one, changing its type, or tightening its
// mode are all rejected explicitly rather than silently applied or ignored.
func validateSchemaEvolution(oldSchema, newSchema []tableField) error {
	if len(newSchema) < len(oldSchema) {
		return fmt.Errorf("schema evolution cannot remove existing columns")
	}
	for i, oldField := range oldSchema {
		newField := newSchema[i]
		if !strings.EqualFold(oldField.Name, newField.Name) {
			return fmt.Errorf("column %d: existing columns cannot be renamed or reordered (expected %q, got %q)", i, oldField.Name, newField.Name)
		}
		if !strings.EqualFold(oldField.Type, newField.Type) {
			return fmt.Errorf("column %q: changing type from %q to %q is not supported", oldField.Name, oldField.Type, newField.Type)
		}
		oldMode := normalizeMode(oldField.Mode)
		newMode := normalizeMode(newField.Mode)
		if oldMode != newMode && !(oldMode == "REQUIRED" && newMode == "NULLABLE") {
			return fmt.Errorf("column %q: only relaxing REQUIRED to NULLABLE is supported (got %s -> %s)", oldField.Name, oldMode, newMode)
		}
	}
	for i := len(oldSchema); i < len(newSchema); i++ {
		newField := newSchema[i]
		if normalizeMode(newField.Mode) != "NULLABLE" {
			return fmt.Errorf("column %q: new columns must be NULLABLE", newField.Name)
		}
	}
	return nil
}

func normalizeMode(mode string) string {
	if mode == "" {
		return "NULLABLE"
	}
	return mode
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
	return parseTableSchemaFieldList(rawFields)
}

// parseTableSchemaFieldList parses a JSON fields array recursively: a
// RECORD/STRUCT field's own nested "fields" array is parsed the same way,
// and "mode" (NULLABLE/REQUIRED/REPEATED) is read alongside name/type,
// defaulting to NULLABLE (BigQuery's own default) when absent.
func parseTableSchemaFieldList(rawFields []any) []tableField {
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
		mode, _ := fieldObj["mode"].(string)
		mode = strings.ToUpper(strings.TrimSpace(mode))
		field := tableField{Name: name, Type: typ, Mode: mode}
		if isRecordType(typ) {
			if nestedRaw, ok := fieldObj["fields"].([]any); ok {
				field.Fields = parseTableSchemaFieldList(nestedRaw)
			}
		}
		out = append(out, field)
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

var informationSchemaPattern = regexp.MustCompile("(?is)(?:`?([a-zA-Z0-9_\\-]+)`?\\.)?(?:`?([a-zA-Z0-9_\\-]+)`?\\.)?information_schema\\.(schemata_options|schemata|table_options|tables|columns|jobs_by_project|jobs_by_user|jobs|partitions|routines|parameters|models|materialized_views|views|sessions_by_user)")

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
	"schemata":           buildInformationSchemaSchemata,
	"schemata_options":   buildInformationSchemaSchemataOptions,
	"tables":             buildInformationSchemaTables,
	"columns":            buildInformationSchemaColumns,
	"jobs":               buildInformationSchemaJobs,
	"jobs_by_project":    buildInformationSchemaJobs,
	"jobs_by_user":       buildInformationSchemaJobsByUser,
	"partitions":         buildInformationSchemaPartitions,
	"routines":           buildInformationSchemaRoutines,
	"parameters":         buildInformationSchemaParameters,
	"models":             buildInformationSchemaModels,
	"table_options":      buildInformationSchemaTableOptions,
	"views":              buildInformationSchemaViews,
	"materialized_views": buildInformationSchemaMaterializedViews,
	"sessions_by_user":   buildInformationSchemaSessionsByUser,
}

// buildInformationSchemaSessionsByUser mirrors real BigQuery's
// INFORMATION_SCHEMA.SESSIONS_BY_USER: scoped strictly to the calling user's
// own sessions (zero rows if callingUserEmail is empty, same convention as
// JOBS_BY_USER), listing only sessions that are not idle-expired (sessions
// service purges expired ones lazily on list/get, so anything returned here
// is real and current). is_active is always "true" for a listed row since
// there is no concept of a currently-executing query to distinguish it from
// an idle-but-not-yet-expired session.
func buildInformationSchemaSessionsByUser(scope informationSchemaScope) ([]map[string]string, [][]string) {
	columns := []map[string]string{
		{"name": "creation_time", "type": "INT64"},
		{"name": "session_id", "type": "STRING"},
		{"name": "last_modified_time", "type": "INT64"},
		{"name": "is_active", "type": "BOOL"},
	}
	if strings.TrimSpace(scope.callingUserEmail) == "" {
		return columns, [][]string{}
	}
	sessions := scope.server.sessions.list(scope.targetProjectID)
	rows := make([][]string, 0, len(sessions))
	for _, rec := range sessions {
		if rec.UserEmail != scope.callingUserEmail {
			continue
		}
		rows = append(rows, []string{
			strconv.FormatInt(rec.CreatedAt.UnixMilli(), 10),
			rec.SessionID,
			strconv.FormatInt(rec.LastUsedAt.UnixMilli(), 10),
			"true",
		})
	}
	return columns, rows
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
		tableType := "BASE TABLE"
		switch {
		case table.External != nil:
			tableType = "EXTERNAL"
		case table.View != nil && table.View.Materialized:
			tableType = "MATERIALIZED VIEW"
		case table.View != nil:
			tableType = "VIEW"
		}
		rows = append(rows, []string{scope.targetProjectID, datasetID, table.TableID, tableType})
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
			isPartitioning := "NO"
			if table.TimePartitioning != nil && strings.EqualFold(table.TimePartitioning.Field, field.Name) {
				isPartitioning = "YES"
			}
			if table.RangePartitioning != nil && strings.EqualFold(table.RangePartitioning.Field, field.Name) {
				isPartitioning = "YES"
			}
			clusteringOrdinal := storedNullCell
			for ordinal, clusteringField := range table.Clustering {
				if strings.EqualFold(clusteringField, field.Name) {
					clusteringOrdinal = strconv.Itoa(ordinal + 1)
					break
				}
			}
			rows = append(rows, []string{scope.targetProjectID, datasetID, table.TableID, field.Name, strconv.Itoa(i + 1), field.Type, isPartitioning, clusteringOrdinal})
		}
	})
	return []map[string]string{{"name": "table_catalog", "type": "STRING"}, {"name": "table_schema", "type": "STRING"}, {"name": "table_name", "type": "STRING"}, {"name": "column_name", "type": "STRING"}, {"name": "ordinal_position", "type": "INT64"}, {"name": "data_type", "type": "STRING"}, {"name": "is_partitioning_column", "type": "STRING"}, {"name": "clustering_ordinal_position", "type": "INT64"}}, rows
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

// buildInformationSchemaPartitions skips a table (rather than failing the
// whole INFORMATION_SCHEMA.PARTITIONS scan) whenever its row count can't be
// resolved, which for an external table means its sourceUris could not be
// read right now. A broad metadata scan across a whole dataset returning
// partial results for one broken external table beats hard-failing every
// other table in it.
func buildInformationSchemaPartitions(scope informationSchemaScope) ([]map[string]string, [][]string) {
	rows := [][]string{}
	scope.forEachTable(func(datasetID string, table *tableRecord) {
		if table.External != nil || table.View != nil {
			return
		}
		counts := partitionCounts(table)
		for _, partitionID := range sortedPartitionIDs(counts) {
			rows = append(rows, []string{scope.targetProjectID, datasetID, table.TableID, partitionID, strconv.Itoa(counts[partitionID])})
		}
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

// buildInformationSchemaParameters documents each routine argument's
// name/type/position. parameter_mode is always "IN": there is no execution
// engine to observe or enforce OUT/INOUT semantics for procedures, so a
// distinct mode per argument would be fabricated rather than real.
func buildInformationSchemaParameters(scope informationSchemaScope) ([]map[string]string, [][]string) {
	rows := [][]string{}
	for _, ds := range scope.datasets {
		if !scope.filterDataset(ds.DatasetID) {
			continue
		}
		items, _, _ := scope.server.routines.list(scope.targetProjectID, ds.DatasetID, 0, 1000)
		for _, rt := range items {
			for i, arg := range rt.Arguments {
				rows = append(rows, []string{scope.targetProjectID, ds.DatasetID, rt.RoutineID, strconv.Itoa(i + 1), "IN", arg.Name, arg.DataType})
			}
		}
	}
	return []map[string]string{{"name": "specific_catalog", "type": "STRING"}, {"name": "specific_schema", "type": "STRING"}, {"name": "specific_name", "type": "STRING"}, {"name": "ordinal_position", "type": "INT64"}, {"name": "parameter_mode", "type": "STRING"}, {"name": "parameter_name", "type": "STRING"}, {"name": "data_type", "type": "STRING"}}, rows
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
		if table.RequirePartitionFilter {
			rows = append(rows, []string{scope.targetProjectID, datasetID, table.TableID, "require_partition_filter", "BOOL", "true"})
		}
		if table.TimePartitioning != nil && table.TimePartitioning.ExpirationMs > 0 {
			days := float64(table.TimePartitioning.ExpirationMs) / float64(24*time.Hour/time.Millisecond)
			rows = append(rows, []string{scope.targetProjectID, datasetID, table.TableID, "partition_expiration_days", "FLOAT64", strconv.FormatFloat(days, 'f', -1, 64)})
		}
	})
	return []map[string]string{{"name": "table_catalog", "type": "STRING"}, {"name": "table_schema", "type": "STRING"}, {"name": "table_name", "type": "STRING"}, {"name": "option_name", "type": "STRING"}, {"name": "option_type", "type": "STRING"}, {"name": "option_value", "type": "STRING"}}, rows
}

func buildInformationSchemaViews(scope informationSchemaScope) ([]map[string]string, [][]string) {
	rows := [][]string{}
	scope.forEachTable(func(datasetID string, table *tableRecord) {
		if table.View == nil || table.View.Materialized {
			return
		}
		rows = append(rows, []string{scope.targetProjectID, datasetID, table.TableID, table.View.Query, "true"})
	})
	return []map[string]string{{"name": "table_catalog", "type": "STRING"}, {"name": "table_schema", "type": "STRING"}, {"name": "table_name", "type": "STRING"}, {"name": "view_definition", "type": "STRING"}, {"name": "use_standard_sql", "type": "STRING"}}, rows
}

func buildInformationSchemaMaterializedViews(scope informationSchemaScope) ([]map[string]string, [][]string) {
	rows := [][]string{}
	scope.forEachTable(func(datasetID string, table *tableRecord) {
		if table.View == nil || !table.View.Materialized {
			return
		}
		rows = append(rows, []string{scope.targetProjectID, datasetID, table.TableID, table.View.Query})
	})
	return []map[string]string{{"name": "table_catalog", "type": "STRING"}, {"name": "table_schema", "type": "STRING"}, {"name": "table_name", "type": "STRING"}, {"name": "view_definition", "type": "STRING"}}, rows
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
	result, err := s.executeQueryStatement(job.ProjectID, job.SessionID, job.QueryText, job.UserEmail, job.ParameterMode, job.QueryParameters)
	if err != nil {
		return jobStatistics{Executor: "query", Simulated: false}, err
	}
	return jobStatistics{Executor: "query", Simulated: false, TotalSlotMs: 60, ProcessedBytes: estimateRowsByteSize(result.rows), OutputRows: int64(len(result.rows)), StatementType: result.statementType, DMLAffectedRows: result.dmlAffectedRows, ResultSchema: result.schema, ResultRows: result.rows}, nil
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
		sourceSchema, sourceRows, ok, err := s.resolveTableRows(source.ProjectID, source.DatasetID, source.TableID)
		if !ok {
			return jobStatistics{Executor: "copy", Simulated: false}, fmt.Errorf("source table not found")
		}
		if err != nil {
			return jobStatistics{Executor: "copy", Simulated: false}, err
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

	if len(job.LoadSourceURIs) == 0 && !job.LoadInline {
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
		return jobStatistics{Executor: "load", Simulated: false}, fmt.Errorf("schema.fields is required to ingest uploaded media or rows from sourceUris")
	}

	var rows [][]string
	var totalBytes int64
	var err error
	if job.LoadInline {
		rows, totalBytes, err = readRowsFromData(job.LoadSourceFormat, job.LoadInlineName, job.LoadInlineData, job.LoadFieldDelimiter, job.LoadSkipLeadingRows, schema, job.LoadCompression)
	} else {
		rows, totalBytes, err = loadRowsFromSourceURIs(job, schema)
	}
	if err != nil {
		return jobStatistics{Executor: "load", Simulated: false}, err
	}
	if err := validateStoredRows(schema, rows); err != nil {
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

	schema, rows, ok, err := s.resolveTableRows(source.ProjectID, source.DatasetID, source.TableID)
	if !ok {
		return jobStatistics{Executor: "extract", Simulated: false}, fmt.Errorf("source table not found")
	}
	if err != nil {
		return jobStatistics{Executor: "extract", Simulated: false}, err
	}

	format := job.ExtractDestinationFormat
	if format == "" {
		format = "CSV"
	}

	compression, err := normalizeExtractCompression(format, job.ExtractCompression)
	if err != nil {
		return jobStatistics{Executor: "extract", Simulated: false}, err
	}

	totalBytes, err := writeExtractDestinations(job.ExtractDestinationURIs, format, schema, rows, job.ExtractFieldDelimiter, job.ExtractPrintHeader, compression)
	if err != nil {
		return jobStatistics{Executor: "extract", Simulated: false}, err
	}

	return jobStatistics{Executor: "extract", Simulated: false, TotalSlotMs: 45, ProcessedBytes: totalBytes, OutputRows: int64(len(rows))}, nil
}

// normalizeExtractCompression validates configuration.extract.compression
// against the destination format's actually-supported codec set, defaulting
// to "NONE" when omitted. An unsupported combination (e.g. GZIP for AVRO)
// fails explicitly rather than silently falling back to uncompressed output.
func normalizeExtractCompression(format, compression string) (string, error) {
	c := strings.ToUpper(strings.TrimSpace(compression))
	if c == "" {
		c = "NONE"
	}
	var supported []string
	switch format {
	case "NEWLINE_DELIMITED_JSON", "CSV":
		supported = []string{"NONE", "GZIP"}
	case "AVRO":
		supported = []string{"NONE", "SNAPPY", "DEFLATE"}
	case "PARQUET":
		supported = []string{"NONE", "SNAPPY", "GZIP"}
	default:
		return c, nil // unsupported destinationFormat itself surfaces elsewhere
	}
	for _, s := range supported {
		if c == s {
			return c, nil
		}
	}
	return "", fmt.Errorf("compression %q is not supported for destinationFormat %q; supported: %s", compression, format, strings.Join(supported, ", "))
}

// maybeGzip wraps payload in gzip when compression is "GZIP", matching real
// BigQuery's CSV/NEWLINE_DELIMITED_JSON extract compression contract; any
// other normalized value (only "NONE" reaches here after
// normalizeExtractCompression) returns payload unchanged.
func maybeGzip(payload []byte, compression string) ([]byte, error) {
	if compression != "GZIP" {
		return payload, nil
	}
	var buf bytes.Buffer
	gw := gzip.NewWriter(&buf)
	if _, err := gw.Write(payload); err != nil {
		return nil, fmt.Errorf("failed to gzip extract payload: %w", err)
	}
	if err := gw.Close(); err != nil {
		return nil, fmt.Errorf("failed to finalize gzip extract payload: %w", err)
	}
	return buf.Bytes(), nil
}

// avroCodecNameFor maps a normalized compression value to the OCF codec name
// goavro understands; GZIP never reaches here since normalizeExtractCompression
// only allows NONE/SNAPPY/DEFLATE for AVRO.
func avroCodecNameFor(compression string) string {
	switch compression {
	case "SNAPPY":
		return goavro.CompressionSnappyLabel
	case "DEFLATE":
		return goavro.CompressionDeflateLabel
	default:
		return goavro.CompressionNullLabel
	}
}

// parquetCodecFor maps a normalized compression value to a parquet-go codec;
// DEFLATE never reaches here since normalizeExtractCompression only allows
// NONE/SNAPPY/GZIP for PARQUET.
func parquetCodecFor(compression string) pqcompress.Codec {
	switch compression {
	case "SNAPPY":
		return &parquet.Snappy
	case "GZIP":
		return &parquet.Gzip
	default:
		return &parquet.Uncompressed
	}
}

// writeExtractDestinations encodes the result once to measure its size, and
// only splits into multiple shard files when LOCAQL_EXTRACT_SHARD_MAX_BYTES
// is configured and exceeded — by default (env var unset) this always writes
// a single shard, unchanged from before multi-shard support existed.
func writeExtractDestinations(uris []string, format string, schema []tableField, rows [][]string, fieldDelimiter string, printHeader bool, compression string) (int64, error) {
	fullPayload, err := encodeExtractPayload(format, schema, rows, fieldDelimiter, printHeader, compression)
	if err != nil {
		return 0, err
	}

	maxShardBytes := readExtractShardMaxBytes()
	if maxShardBytes <= 0 || int64(len(fullPayload)) <= maxShardBytes || len(rows) <= 1 {
		return writeExtractShards(uris, []extractShard{{index: 0, payload: fullPayload}})
	}

	// Splitting is required: mirror real BigQuery's contract that a
	// multi-shard result requires exactly one destinationUri carrying a
	// single '*' wildcard, rather than silently picking one of several
	// literal URIs or duplicating shards across all of them.
	if len(uris) != 1 || strings.Count(uris[0], "*") != 1 {
		return 0, fmt.Errorf("result is %d bytes, over the %d byte LOCAQL_EXTRACT_SHARD_MAX_BYTES threshold, so it must be split across multiple shard files; provide exactly one destinationUri containing a single '*' wildcard instead of %d", len(fullPayload), maxShardBytes, len(uris))
	}

	shardCount := int(math.Ceil(float64(len(fullPayload)) / float64(maxShardBytes)))
	if shardCount < 1 {
		shardCount = 1
	}
	if shardCount > len(rows) {
		shardCount = len(rows)
	}
	rowsPerShard := int(math.Ceil(float64(len(rows)) / float64(shardCount)))

	shards := make([]extractShard, 0, shardCount)
	for start, shardIndex := 0, 0; start < len(rows); start, shardIndex = start+rowsPerShard, shardIndex+1 {
		end := start + rowsPerShard
		if end > len(rows) {
			end = len(rows)
		}
		shardPayload, err := encodeExtractPayload(format, schema, rows[start:end], fieldDelimiter, printHeader, compression)
		if err != nil {
			return 0, err
		}
		shards = append(shards, extractShard{index: shardIndex, payload: shardPayload})
	}
	return writeExtractShards(uris, shards)
}

func readExtractShardMaxBytes() int64 {
	raw := strings.TrimSpace(os.Getenv("LOCAQL_EXTRACT_SHARD_MAX_BYTES"))
	if raw == "" {
		return 0
	}
	n, err := strconv.ParseInt(raw, 10, 64)
	if err != nil || n <= 0 {
		return 0
	}
	return n
}

type extractShard struct {
	index   int
	payload []byte
}

// writeExtractShards writes each shard to every destinationUri, substituting
// the shard's index for that URI's '*' wildcard (or writing as-is when there
// is none). With a single shard (the common, pre-existing case) and multiple
// literal URIs, every URI receives the same shard 0 content — unchanged from
// before multi-shard support existed. A real multi-shard result always has
// exactly one URI (validated by the caller), so each shard lands in its own
// numbered file next to the others.
func writeExtractShards(uris []string, shards []extractShard) (int64, error) {
	var totalBytes int64
	for _, shard := range shards {
		for _, uri := range uris {
			path, err := resolveNumberedShardPath(uri, shard.index)
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
			if err := os.WriteFile(path, shard.payload, 0o644); err != nil {
				return 0, fmt.Errorf("failed to write destinationUri %q: %w", uri, err)
			}
			totalBytes += int64(len(shard.payload))
		}
	}
	return totalBytes, nil
}

func encodeExtractPayload(format string, schema []tableField, rows [][]string, fieldDelimiter string, printHeader bool, compression string) ([]byte, error) {
	switch format {
	case "NEWLINE_DELIMITED_JSON":
		payload, err := encodeNDJSON(schema, rows)
		if err != nil {
			return nil, err
		}
		return maybeGzip(payload, compression)
	case "CSV":
		payload, err := encodeCSV(schema, rows, fieldDelimiter, printHeader)
		if err != nil {
			return nil, err
		}
		return maybeGzip(payload, compression)
	case "AVRO":
		return encodeAvro(schema, rows, avroCodecNameFor(compression))
	case "PARQUET":
		return encodeParquet(schema, rows, parquetCodecFor(compression))
	default:
		return nil, fmt.Errorf("destinationFormat %q is not supported; local extract currently supports NEWLINE_DELIMITED_JSON, CSV, AVRO and PARQUET", format)
	}
}

func encodeNDJSON(schema []tableField, rows [][]string) ([]byte, error) {
	var buf bytes.Buffer
	for _, row := range rows {
		record := make(map[string]any, len(schema))
		for i, field := range schema {
			if i >= len(row) || storedCellIsNull(row[i]) {
				record[field.Name] = nil
				continue
			}
			v, err := stringToJSONValue(row[i], field)
			if err != nil {
				return nil, fmt.Errorf("column %s: %w", field.Name, err)
			}
			record[field.Name] = v
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

// rejectNestedFields fails explicitly when schema declares a RECORD or
// REPEATED field, for formats that don't support real nested/repeated
// encode-decode yet (CSV never will, matching real BigQuery; Avro/Parquet
// could in principle but that is deferred, declared here rather than
// silently flattening or corrupting the data).
func rejectNestedFields(format string, schema []tableField) error {
	for _, field := range schema {
		if field.Mode == "REPEATED" || isRecordType(field.Type) {
			return fmt.Errorf("%s does not support RECORD/REPEATED schema fields yet (column %q); use NEWLINE_DELIMITED_JSON for nested data", format, field.Name)
		}
	}
	return nil
}

func encodeCSV(schema []tableField, rows [][]string, fieldDelimiter string, printHeader bool) ([]byte, error) {
	if err := rejectNestedFields("CSV", schema); err != nil {
		return nil, err
	}
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
		encodedRow := make([]string, len(schema))
		for i := range encodedRow {
			if i >= len(row) {
				continue
			}
			value, isNull := loadStoredCell(row[i])
			if !isNull {
				encodedRow[i] = value
			}
		}
		if err := writer.Write(encodedRow); err != nil {
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
// value using the destination field, so NDJSON extract round-trips numbers
// and booleans instead of re-encoding everything as strings. RECORD/
// REPEATED cells are already stored as a canonical JSON object/array (see
// sql_engine.go), so they are parsed and embedded as real nested JSON
// rather than double-encoded as a JSON string.
func stringToJSONValue(v string, field tableField) (any, error) {
	var isNull bool
	v, isNull = loadStoredCell(v)
	if isNull {
		return nil, nil
	}
	if field.Mode == "REPEATED" || isRecordType(field.Type) {
		var decoded any
		if err := json.Unmarshal([]byte(v), &decoded); err != nil {
			return nil, fmt.Errorf("invalid stored JSON for RECORD/REPEATED cell: %w", err)
		}
		return decoded, nil
	}
	switch strings.ToUpper(field.Type) {
	case "INT64", "INTEGER":
		if n, err := strconv.ParseInt(v, 10, 64); err == nil {
			return n, nil
		}
	case "FLOAT64", "FLOAT":
		if f, err := strconv.ParseFloat(v, 64); err == nil {
			return f, nil
		}
	case "BOOL", "BOOLEAN":
		if b, err := strconv.ParseBool(v); err == nil {
			return b, nil
		}
	}
	return v, nil
}

// parseAvroRows reads an Avro Object Container File and projects each record
// onto schema field order by name, mirroring the NDJSON path. The emulator
// does not autodetect a BigQuery schema from the file's embedded Avro
// schema; schema.fields is required just like NDJSON and CSV.
func parseAvroRows(uri string, data []byte, schema []tableField) ([][]string, error) {
	if err := rejectNestedFields("AVRO", schema); err != nil {
		return nil, err
	}
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
			row[i] = scalarValueToString(unwrapAvroUnion(record[field.Name]))
		}
		rows = append(rows, row)
	}
	if err := reader.Err(); err != nil {
		return nil, fmt.Errorf("failed to read Avro sourceUri %q: %w", uri, err)
	}
	return rows, nil
}

// encodeAvro writes rows as an Avro Object Container File using a record
// schema derived from schema field names/types. NULLABLE fields use a real
// ["null", base] union while REQUIRED fields use the base scalar directly.
// RECORD/REPEATED fields are rejected explicitly (see
// rejectNestedFields) rather than attempted — real nested Avro schema/codec
// support is deferred. A row value that fails to parse as its declared type
// falls back to that type's zero value instead of failing the whole encode.
func encodeAvro(schema []tableField, rows [][]string, codecName string) ([]byte, error) {
	if err := rejectNestedFields("AVRO", schema); err != nil {
		return nil, err
	}
	schemaJSON, err := buildAvroSchemaJSON(schema)
	if err != nil {
		return nil, fmt.Errorf("failed to build Avro schema: %w", err)
	}

	var buf bytes.Buffer
	writer, err := goavro.NewOCFWriter(goavro.OCFConfig{W: &buf, Schema: schemaJSON, CompressionName: codecName})
	if err != nil {
		return nil, fmt.Errorf("failed to create Avro writer: %w", err)
	}

	records := make([]any, 0, len(rows))
	for _, row := range rows {
		record := make(map[string]any, len(schema))
		for i, field := range schema {
			if i >= len(row) {
				record[field.Name] = nil
				continue
			}
			record[field.Name] = stringToAvroValue(row[i], field)
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
	Type any    `json:"type"`
}

type avroRecordSchema struct {
	Type   string            `json:"type"`
	Name   string            `json:"name"`
	Fields []avroFieldSchema `json:"fields"`
}

func buildAvroSchemaJSON(schema []tableField) (string, error) {
	fields := make([]avroFieldSchema, 0, len(schema))
	for _, field := range schema {
		avroType := any(avroTypeFor(field.Type))
		if normalizeMode(field.Mode) != "REQUIRED" {
			avroType = []any{"null", avroType}
		}
		fields = append(fields, avroFieldSchema{Name: field.Name, Type: avroType})
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

func stringToAvroValue(v string, field tableField) any {
	raw, isNull := loadStoredCell(v)
	if isNull {
		return nil
	}
	value := plainStringToAvroValue(raw, field.Type)
	if normalizeMode(field.Mode) != "REQUIRED" {
		return goavro.Union(avroTypeFor(field.Type), value)
	}
	return value
}

func plainStringToAvroValue(v, fieldType string) any {
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
	return plainStringToAvroValue("", fieldType)
}

func unwrapAvroUnion(v any) any {
	union, ok := v.(map[string]any)
	if !ok || len(union) != 1 {
		return v
	}
	for _, value := range union {
		return value
	}
	return v
}

// buildParquetSchema derives a parquet.Schema from schema field names/types;
// NULLABLE fields are optional Parquet nodes and REQUIRED fields are required.
func buildParquetSchema(schema []tableField) *parquet.Schema {
	group := make(parquet.Group, len(schema))
	for _, field := range schema {
		node := parquetNodeFor(field.Type)
		if normalizeMode(field.Mode) == "REQUIRED" {
			group[field.Name] = parquet.Required(node)
		} else {
			group[field.Name] = parquet.Optional(node)
		}
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
func stringToParquetValue(v string, field tableField) any {
	v, isNull := loadStoredCell(v)
	if isNull {
		return nil
	}
	switch strings.ToUpper(field.Type) {
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
	if err := rejectNestedFields("PARQUET", schema); err != nil {
		return nil, err
	}
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
// schema field names/types. RECORD/REPEATED fields are rejected explicitly
// (see rejectNestedFields) — real nested Parquet group/list support is
// deferred rather than attempted.
func encodeParquet(schema []tableField, rows [][]string, codec pqcompress.Codec) ([]byte, error) {
	if err := rejectNestedFields("PARQUET", schema); err != nil {
		return nil, err
	}
	parquetSchema := buildParquetSchema(schema)

	var buf bytes.Buffer
	writer := parquet.NewWriter(&buf, parquetSchema, parquet.Compression(codec))
	for _, row := range rows {
		record := make(map[string]any, len(schema))
		for i, field := range schema {
			if i >= len(row) {
				record[field.Name] = nil
				continue
			}
			record[field.Name] = stringToParquetValue(row[i], field)
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

// resolveNumberedShardPath resolves a destinationUri that may contain a
// single '*' wildcard to the BigQuery shard-file convention: the wildcard
// becomes shardIndex, zero-padded to 12 digits (e.g. "000000000001"). A URI
// without a wildcard is returned unchanged regardless of shardIndex, matching
// real BigQuery's behavior for a literal (non-wildcarded) destinationUri.
func resolveNumberedShardPath(uri string, shardIndex int) (string, error) {
	path, err := resolveLocalFilePath(uri)
	if err != nil {
		return "", err
	}
	switch strings.Count(path, "*") {
	case 0:
		return path, nil
	case 1:
		return strings.Replace(path, "*", fmt.Sprintf("%012d", shardIndex), 1), nil
	default:
		return "", fmt.Errorf("destinationUri %q must contain at most one '*' wildcard", uri)
	}
}

// loadRowsFromSourceURIs reads rows for a load job from local sourceUris,
// dispatching on job.LoadSourceFormat. Unsupported formats fail explicitly
// rather than silently falling back to schema-only materialization.
func loadRowsFromSourceURIs(job *jobRecord, schema []tableField) ([][]string, int64, error) {
	return readRowsFromURIs(job.LoadSourceFormat, job.LoadSourceURIs, job.LoadFieldDelimiter, job.LoadSkipLeadingRows, schema, job.LoadCompression)
}

// normalizeLoadCompression validates configuration.load.compression against
// what's actually meaningful for the given sourceFormat: GZIP is a real
// pre-processing step for CSV/NEWLINE_DELIMITED_JSON, but AVRO/PARQUET files
// carry their own codec in the file itself (decoded automatically by
// goavro/parquet-go regardless of what value would be passed here), so a
// non-NONE value for those formats is rejected explicitly instead of being
// silently accepted as a no-op the caller might mistake for having an effect.
func normalizeLoadCompression(sourceFormat, compression string) (string, error) {
	c := strings.ToUpper(strings.TrimSpace(compression))
	if c == "" {
		c = "NONE"
	}
	switch sourceFormat {
	case "NEWLINE_DELIMITED_JSON", "CSV":
		if c == "NONE" || c == "GZIP" {
			return c, nil
		}
		return "", fmt.Errorf("compression %q is not supported for sourceFormat %q; supported: NONE, GZIP", compression, sourceFormat)
	case "AVRO", "PARQUET":
		if c == "NONE" {
			return c, nil
		}
		return "", fmt.Errorf("compression %q is not applicable to sourceFormat %q: its codec is embedded in the file and decoded automatically, so configuration.load.compression must be left unset or NONE", compression, sourceFormat)
	default:
		return c, nil // unsupported sourceFormat itself surfaces elsewhere
	}
}

// readRowsFromURIs is the shared format-dispatch reader behind both load jobs
// (loadRowsFromSourceURIs) and external tables (resolveTableRows): it reads
// and parses NEWLINE_DELIMITED_JSON/CSV/AVRO/PARQUET rows from local sourceUris
// (or fake-GCS via LOCAQL_FAKE_GCS_ROOT). Kept as a single source of truth for
// which sourceFormat values are actually supported.
func readRowsFromURIs(sourceFormat string, sourceURIs []string, fieldDelimiter string, skipLeadingRows int, schema []tableField, compression string) ([][]string, int64, error) {
	parser, err := loadRowsParser(sourceFormat, fieldDelimiter, skipLeadingRows, schema, compression)
	if err != nil {
		return nil, 0, err
	}
	return loadRowsAcrossURIs(sourceURIs, parser)
}

// readRowsFromData parses bytes supplied directly by the BigQuery upload API.
// It deliberately shares the exact format dispatcher used by sourceUris so
// load_table_from_file and load_table_from_uri cannot drift semantically.
func readRowsFromData(sourceFormat, sourceName string, data []byte, fieldDelimiter string, skipLeadingRows int, schema []tableField, compression string) ([][]string, int64, error) {
	parser, err := loadRowsParser(sourceFormat, fieldDelimiter, skipLeadingRows, schema, compression)
	if err != nil {
		return nil, 0, err
	}
	if strings.TrimSpace(sourceName) == "" {
		sourceName = "uploaded-file"
	}
	rows, err := parser(sourceName, data)
	if err != nil {
		return nil, 0, err
	}
	return rows, int64(len(data)), nil
}

func loadRowsParser(sourceFormat, fieldDelimiter string, skipLeadingRows int, schema []tableField, compression string) (func(string, []byte) ([][]string, error), error) {
	sourceFormat = strings.ToUpper(strings.TrimSpace(sourceFormat))
	if sourceFormat == "" {
		sourceFormat = "CSV"
	}
	normalizedCompression, err := normalizeLoadCompression(sourceFormat, compression)
	if err != nil {
		return nil, err
	}
	switch sourceFormat {
	case "NEWLINE_DELIMITED_JSON":
		return func(uri string, data []byte) ([][]string, error) {
			data, err := maybeGunzip(data, normalizedCompression)
			if err != nil {
				return nil, fmt.Errorf("sourceUri %q: %w", uri, err)
			}
			return parseNDJSONLines(uri, data, schema)
		}, nil
	case "CSV":
		return func(uri string, data []byte) ([][]string, error) {
			data, err := maybeGunzip(data, normalizedCompression)
			if err != nil {
				return nil, fmt.Errorf("sourceUri %q: %w", uri, err)
			}
			return parseCSVRows(uri, data, schema, fieldDelimiter, skipLeadingRows)
		}, nil
	case "AVRO":
		return func(uri string, data []byte) ([][]string, error) {
			return parseAvroRows(uri, data, schema)
		}, nil
	case "PARQUET":
		return func(uri string, data []byte) ([][]string, error) {
			return parseParquetRows(uri, data, schema)
		}, nil
	default:
		return nil, fmt.Errorf("sourceFormat %q is not supported; local ingestion currently supports NEWLINE_DELIMITED_JSON, CSV, AVRO and PARQUET", sourceFormat)
	}
}

// maybeGunzip reverses maybeGzip: when compression is "GZIP" it decompresses
// data before format-specific parsing; any other normalized value ("NONE")
// returns data unchanged.
func maybeGunzip(data []byte, compression string) ([]byte, error) {
	if compression != "GZIP" {
		return data, nil
	}
	reader, err := gzip.NewReader(bytes.NewReader(data))
	if err != nil {
		return nil, fmt.Errorf("failed to open gzip reader: %w", err)
	}
	defer reader.Close()
	decompressed, err := io.ReadAll(reader)
	if err != nil {
		return nil, fmt.Errorf("failed to decompress gzip data: %w", err)
	}
	return decompressed, nil
}

// resolveTableRows is the single choke point every consumer of real row data
// (query execution, tabledata.list, copy/extract job sources, partition row
// counts) goes through. For a normal table it is a thin pass-through to
// tableService.getData. For an external table it re-reads sourceUris fresh on
// every call — nothing is cached in tableRecord.Rows — so external tables
// always reflect the current file contents, matching real BigQuery external
// table semantics. ok mirrors getData's meaning (table exists); err is only
// ever set when ok is true and the table is external but its files could not
// be read.
func (s *Server) resolveTableRows(projectID, datasetID, tableID string) ([]tableField, [][]string, bool, error) {
	return s.resolveTableRowsVisiting(projectID, datasetID, tableID, map[string]bool{})
}

// resolveTableRowsVisiting is resolveTableRows plus a visiting set threaded
// through view resolution (a view's query is re-executed for real, which may
// itself reference other views) so a view that directly or transitively
// references itself fails with a clear error instead of recursing forever.
func (s *Server) resolveTableRowsVisiting(projectID, datasetID, tableID string, visiting map[string]bool) ([]tableField, [][]string, bool, error) {
	record, ok, _ := s.tables.get(projectID, datasetID, tableID)
	if !ok {
		return nil, nil, false, nil
	}
	if record.View != nil {
		key := projectID + ":" + datasetID + "." + tableID
		if visiting[key] {
			return nil, nil, true, fmt.Errorf("circular view reference detected at %s.%s", datasetID, tableID)
		}
		visiting[key] = true
		defer delete(visiting, key)
		schema, rows, err := s.executeRealSQLQueryVisiting(projectID, record.View.Query, visiting, nil)
		if err != nil {
			return nil, nil, true, fmt.Errorf("resolve view %s.%s: %w", datasetID, tableID, err)
		}
		return schema, rows, true, nil
	}
	if record.External == nil {
		fields, rows, ok := s.tables.getData(projectID, datasetID, tableID)
		return fields, rows, ok, nil
	}
	ext := record.External
	rows, _, err := readRowsFromURIs(ext.SourceFormat, ext.SourceURIs, ext.FieldDelimiter, ext.SkipLeadingRows, record.Schema, "")
	if err != nil {
		return nil, nil, true, fmt.Errorf("failed to read external table %s.%s from sourceUris: %w", datasetID, tableID, err)
	}
	return cloneTableFields(record.Schema), rows, true, nil
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
	if err := rejectNestedFields("CSV", schema); err != nil {
		return nil, err
	}
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
	if v == nil {
		return storedNullCell
	}
	return storeStringCell(scalarValueToPlainString(v))
}

// scalarValueToPlainString is used for already-decoded nested values whose
// nullability is represented structurally by JSON nil, not by the top-level
// stored-cell tag.
func scalarValueToPlainString(v any) string {
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
