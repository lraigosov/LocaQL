package server

import (
	"fmt"
	"net/http"
	"strconv"
	"strings"
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
	if r.Method != http.MethodGet {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]any{"error": "method not allowed"})
		return
	}

	path := strings.TrimPrefix(r.URL.Path, "/bigquery/v2/projects/")
	parts := strings.Split(strings.Trim(path, "/"), "/")
	if len(parts) < 2 {
		writeJSON(w, http.StatusNotFound, map[string]any{"error": "not found"})
		return
	}

	projectID := parts[0]
	scope := parts[1]

	switch scope {
	case "datasets":
		if len(parts) == 2 {
			s.listDatasets(w, r, projectID)
			return
		}
		if len(parts) == 4 && parts[3] == "tables" {
			s.listTables(w, r, projectID, parts[2])
			return
		}
	case "jobs":
		if len(parts) == 2 {
			s.listJobs(w, r, projectID)
			return
		}
	case "tabledata":
		if len(parts) == 5 && parts[4] == "data" {
			s.listTableData(w, r, projectID, parts[2], parts[3])
			return
		}
	}

	writeJSON(w, http.StatusNotFound, map[string]any{"error": "not found"})
}

func (s *Server) listDatasets(w http.ResponseWriter, r *http.Request, projectID string) {
	items := []dataset{{ID: "analytics"}, {ID: "finance"}, {ID: "ops"}, {ID: "sandbox"}}
	start, size := parsePagination(r, 2, 1000)
	end := clampEnd(start, size, len(items))

	out := make([]map[string]any, 0, end-start)
	for _, ds := range items[start:end] {
		out = append(out, map[string]any{
			"kind": "bigquery#dataset",
			"id":   fmt.Sprintf("%s:%s", projectID, ds.ID),
			"datasetReference": map[string]string{
				"projectId": projectID,
				"datasetId": ds.ID,
			},
		})
	}

	resp := map[string]any{
		"kind":     "bigquery#datasetList",
		"datasets": out,
	}
	if end < len(items) {
		resp["nextPageToken"] = strconv.Itoa(end)
	}
	writeJSON(w, http.StatusOK, resp)
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
		resp["nextPageToken"] = strconv.Itoa(end)
	}
	writeJSON(w, http.StatusOK, resp)
}

func (s *Server) listJobs(w http.ResponseWriter, r *http.Request, projectID string) {
	items := []job{{ID: "job_1"}, {ID: "job_2"}, {ID: "job_3"}, {ID: "job_4"}, {ID: "job_5"}}
	start, size := parsePagination(r, 2, 1000)
	end := clampEnd(start, size, len(items))

	out := make([]map[string]any, 0, end-start)
	for _, j := range items[start:end] {
		out = append(out, map[string]any{
			"kind": "bigquery#job",
			"id":   fmt.Sprintf("%s:%s", projectID, j.ID),
			"jobReference": map[string]string{
				"projectId": projectID,
				"jobId":     j.ID,
			},
		})
	}

	resp := map[string]any{
		"kind": "bigquery#jobList",
		"jobs": out,
	}
	if end < len(items) {
		resp["nextPageToken"] = strconv.Itoa(end)
	}
	writeJSON(w, http.StatusOK, resp)
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
		resp["nextPageToken"] = strconv.Itoa(end)
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
		if n, err := strconv.Atoi(token); err == nil && n >= 0 {
			start = n
		}
	}
	return start, size
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
