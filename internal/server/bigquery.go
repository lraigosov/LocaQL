package server

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"
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
		if len(parts) == 2 {
			s.handleDatasetsCollection(w, r, projectID)
			return
		}
		if len(parts) == 3 {
			s.handleDatasetByID(w, r, projectID, parts[2])
			return
		}
		if len(parts) == 4 && parts[3] == "tables" {
			if !s.datasets.exists(projectID, parts[2]) {
				writeError(w, http.StatusNotFound, fmt.Sprintf("Not found: Dataset %s:%s", projectID, parts[2]), "notFound")
				return
			}
			s.listTables(w, r, projectID, parts[2])
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
	case http.MethodDelete:
		s.deleteDataset(w, projectID, datasetID)
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
		FriendlyName     string            `json:"friendlyName"`
		Location         string            `json:"location"`
		Labels           map[string]string `json:"labels"`
		DatasetReference struct {
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

	rec, created := s.datasets.insert(datasetInsert{
		ProjectID:    projectID,
		DatasetID:    datasetID,
		FriendlyName: payload.FriendlyName,
		Location:     payload.Location,
		Labels:       payload.Labels,
	})
	if !created {
		writeError(w, http.StatusConflict, fmt.Sprintf("Already Exists: Dataset %s:%s", projectID, datasetID), "duplicate")
		return
	}
	writeJSON(w, http.StatusOK, renderDatasetResource(rec))
}

func (s *Server) getDataset(w http.ResponseWriter, projectID, datasetID string) {
	rec, ok := s.datasets.get(projectID, datasetID)
	if !ok {
		writeError(w, http.StatusNotFound, fmt.Sprintf("Not found: Dataset %s:%s", projectID, datasetID), "notFound")
		return
	}
	writeJSON(w, http.StatusOK, renderDatasetResource(rec))
}

func (s *Server) deleteDataset(w http.ResponseWriter, projectID, datasetID string) {
	if !s.datasets.delete(projectID, datasetID) {
		writeError(w, http.StatusNotFound, fmt.Sprintf("Not found: Dataset %s:%s", projectID, datasetID), "notFound")
		return
	}
	w.WriteHeader(http.StatusNoContent)
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
	return resp
}

func (s *Server) listTables(w http.ResponseWriter, r *http.Request, projectID, datasetID string) {
	items := []table{{ID: "events"}, {ID: "daily_metrics"}, {ID: "users"}, {ID: "raw_import"}}
	start, size := parsePagination(r, 2, 1000)
	end := clampEnd(start, size, len(items))

	out := make([]map[string]any, 0, end-start)
	for _, t := range items[start:end] {
		out = append(out, map[string]any{
			"kind": "bigquery#table",
			"id":   fmt.Sprintf("%s:%s.%s", projectID, datasetID, t.ID),
			"tableReference": map[string]string{
				"projectId": projectID,
				"datasetId": datasetID,
				"tableId":   t.ID,
			},
		})
	}

	resp := map[string]any{
		"kind":   "bigquery#tableList",
		"tables": out,
	}
	if end < len(items) {
		resp["nextPageToken"] = encodePageToken(end)
	}
	writeJSON(w, http.StatusOK, resp)
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
	priority := "INTERACTIVE"
	if r.Body != nil {
		body, _ := io.ReadAll(r.Body)
		if len(body) > 0 {
			extractTableRef := func(v any) (string, string) {
				m, ok := v.(map[string]any)
				if !ok {
					return "", ""
				}
				datasetID, _ := m["datasetId"].(string)
				tableID, _ := m["tableId"].(string)
				return strings.TrimSpace(datasetID), strings.TrimSpace(tableID)
			}

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
							targetDataset, targetTable = extractTableRef(loadCfg["destinationTable"])
						}
					}
					if _, ok := conf["extract"]; ok {
						jobType = "extract"
					}
					if copyRaw, ok := conf["copy"]; ok {
						jobType = "copy"
						if copyCfg, ok := copyRaw.(map[string]any); ok {
							targetDataset, targetTable = extractTableRef(copyCfg["destinationTable"])
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
	insertOpts := jobInsertOptions{
		ProjectID: projectID,
		RequestID: payload.RequestId,
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
	schema, values := simulateQueryResultTable(j.QueryText)
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

func simulateQueryResultTable(queryText string) ([]map[string]string, [][]string) {
	trimmed := strings.TrimSpace(queryText)
	if trimmed == "" {
		return []map[string]string{{"name": "result", "type": "STRING"}}, [][]string{{"query job executed"}}
	}

	lower := strings.ToLower(trimmed)
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
	rows := []tableRow{
		{Values: []string{"1", "alice"}},
		{Values: []string{"2", "bob"}},
		{Values: []string{"3", "carol"}},
		{Values: []string{"4", "dave"}},
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
