package server

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"
)

// streamingInsertDedupTTL bounds how long an insertId is remembered for
// best-effort deduplication. Real BigQuery's own streaming-insert dedup
// window is an undocumented, effectively-unbounded best effort; this
// project makes the same trade-off explicit and finite instead, matching
// the existing requestId idempotency convention (jobService.requestIDTTL).
const streamingInsertDedupTTL = 15 * time.Minute

// streamingInsertDedupStore remembers which (table, insertId) pairs were
// already accepted so a client's at-least-once retry of the same insertAll
// row does not double-insert it. Entries are purged lazily on access, the
// same convention already used for table/session expiration.
type streamingInsertDedupStore struct {
	mu   sync.Mutex
	seen map[string]time.Time
}

func newStreamingInsertDedupStore() *streamingInsertDedupStore {
	return &streamingInsertDedupStore{seen: make(map[string]time.Time)}
}

// seenRecently reports whether insertID was already accepted for
// resourceKey within the dedup TTL, and records it as seen otherwise.
func (d *streamingInsertDedupStore) seenRecently(resourceKey, insertID string, now time.Time) bool {
	if insertID == "" {
		return false
	}
	key := resourceKey + "\x00" + insertID
	d.mu.Lock()
	defer d.mu.Unlock()
	for k, t := range d.seen {
		if now.Sub(t) > streamingInsertDedupTTL {
			delete(d.seen, k)
		}
	}
	if t, ok := d.seen[key]; ok && now.Sub(t) <= streamingInsertDedupTTL {
		return true
	}
	d.seen[key] = now
	return false
}

// tableDataInsertAllRequest is BigQuery's tabledata.insertAll request body.
// templateSuffix is accepted syntactically and rejected explicitly (see
// insertAllTableData) rather than silently ignored.
type tableDataInsertAllRequest struct {
	SkipInvalidRows     bool                         `json:"skipInvalidRows"`
	IgnoreUnknownValues bool                         `json:"ignoreUnknownValues"`
	TemplateSuffix      string                       `json:"templateSuffix"`
	Rows                []tableDataInsertAllRowInput `json:"rows"`
}

type tableDataInsertAllRowInput struct {
	InsertID string         `json:"insertId"`
	JSON     map[string]any `json:"json"`
}

type tableDataInsertAllErrorDetail struct {
	Reason   string `json:"reason"`
	Location string `json:"location,omitempty"`
	Message  string `json:"message"`
}

type tableDataInsertAllRowError struct {
	Index  int                             `json:"index"`
	Errors []tableDataInsertAllErrorDetail `json:"errors"`
}

// insertAllTableData implements BigQuery's streaming inserts
// (tabledata.insertAll): POST
// .../datasets/{datasetId}/tables/{tableId}/insertAll. Unlike load/DML/copy,
// this is a direct, job-free REST write — the same category as BigQuery
// Storage Write's `_default` stream — so it commits straight to the catalog
// via tableService.upsertCopyDestination, which already provides the
// atomic WRITE_APPEND/CREATE_NEVER semantics and REQUIRED/partition
// validation that path is built on (see storage_write_service.go for the
// sibling implementation this one deliberately mirrors).
func (s *Server) insertAllTableData(w http.ResponseWriter, r *http.Request, projectID, datasetID, tableID string) {
	rec, ok, _ := s.tables.get(projectID, datasetID, tableID)
	if !ok {
		writeError(w, http.StatusNotFound, fmt.Sprintf("Not found: Table %s:%s.%s", projectID, datasetID, tableID), "notFound")
		return
	}
	if rec.View != nil {
		writeError(w, http.StatusBadRequest, "streaming inserts are not supported for views", "invalid")
		return
	}
	if rec.External != nil {
		writeError(w, http.StatusBadRequest, "streaming inserts are not supported for external tables", "invalid")
		return
	}

	var req tableDataInsertAllRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body: "+err.Error(), "invalid")
		return
	}
	if strings.TrimSpace(req.TemplateSuffix) != "" {
		writeError(w, http.StatusBadRequest, "templateSuffix is not supported", "invalid")
		return
	}
	if len(req.Rows) == 0 {
		writeJSON(w, http.StatusOK, map[string]any{"kind": "bigquery#tableDataInsertAllResponse"})
		return
	}

	schema := rec.Schema
	resourceKey := projectID + ":" + datasetID + "." + tableID
	now := time.Now().UTC()

	var validRows [][]string
	var rowErrors []tableDataInsertAllRowError
	seenThisRequest := make(map[string]bool, len(req.Rows))
	for i, row := range req.Rows {
		insertID := strings.TrimSpace(row.InsertID)
		if insertID != "" {
			if seenThisRequest[insertID] || s.streamingInserts.seenRecently(resourceKey, insertID, now) {
				continue
			}
			seenThisRequest[insertID] = true
		}

		built, errs := buildStreamingInsertRow(row.JSON, schema, req.IgnoreUnknownValues)
		if len(errs) > 0 {
			details := make([]tableDataInsertAllErrorDetail, len(errs))
			for j, e := range errs {
				details[j] = tableDataInsertAllErrorDetail{Reason: "invalid", Message: e}
			}
			rowErrors = append(rowErrors, tableDataInsertAllRowError{Index: i, Errors: details})
			continue
		}
		validRows = append(validRows, built)
	}

	if len(rowErrors) > 0 && !req.SkipInvalidRows {
		writeJSON(w, http.StatusOK, map[string]any{
			"kind":         "bigquery#tableDataInsertAllResponse",
			"insertErrors": rowErrors,
		})
		return
	}

	if len(validRows) > 0 {
		dest := tableReference{ProjectID: projectID, DatasetID: datasetID, TableID: tableID}
		if _, err := s.tables.upsertCopyDestination(dest, nil, validRows, "CREATE_NEVER", "WRITE_APPEND"); err != nil {
			writeError(w, http.StatusBadRequest, err.Error(), "invalid")
			return
		}
	}

	resp := map[string]any{"kind": "bigquery#tableDataInsertAllResponse"}
	if len(rowErrors) > 0 {
		resp["insertErrors"] = rowErrors
	}
	writeJSON(w, http.StatusOK, resp)
}

// buildStreamingInsertRow converts one insertAll row's decoded JSON object
// into this project's stored row shape, reusing the exact same encoding
// (parseNDJSONRow) and REQUIRED validation (validateStoredField) that
// NEWLINE_DELIMITED_JSON load jobs already use, so nested RECORD/REPEATED
// fields behave identically end-to-end instead of a second, divergent
// implementation. It returns the built row, or a non-empty list of
// human-readable error messages if the row is invalid.
func buildStreamingInsertRow(payload map[string]any, schema []tableField, ignoreUnknownValues bool) ([]string, []string) {
	var errs []string
	if !ignoreUnknownValues {
		known := make(map[string]bool, len(schema))
		for _, f := range schema {
			known[f.Name] = true
		}
		for key := range payload {
			if !known[key] {
				errs = append(errs, fmt.Sprintf("no such field: %s", key))
			}
		}
	}

	encoded, err := json.Marshal(payload)
	if err != nil {
		return nil, append(errs, err.Error())
	}
	row, err := parseNDJSONRow(string(encoded), schema)
	if err != nil {
		return nil, append(errs, err.Error())
	}
	for i, field := range schema {
		if i >= len(row) {
			continue
		}
		if verr := validateStoredField(field, row[i]); verr != nil {
			errs = append(errs, fmt.Sprintf("%s: %v", field.Name, verr))
		}
	}
	if len(errs) > 0 {
		return nil, errs
	}
	return row, nil
}
