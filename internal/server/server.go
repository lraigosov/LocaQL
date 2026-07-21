package server

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	"github.com/lraigosov/LocaQL/internal/capabilities"
	"github.com/lraigosov/LocaQL/internal/version"
)

type Server struct {
	mux      *http.ServeMux
	registry capabilities.Registry
	jobs     *jobService
	datasets *datasetService
	tables   *tableService
}

func New(reg capabilities.Registry) *Server {
	s := &Server{
		mux:      http.NewServeMux(),
		registry: reg,
		jobs:     newJobService(),
		datasets: newDatasetService(),
		tables:   newTableService(),
	}
	s.jobs.copyExecutor = s.executeCopyJob
	s.jobs.loadExecutor = s.executeLoadJob
	s.routes()
	return s
}

func (s *Server) Handler() http.Handler {
	return s.mux
}

func (s *Server) routes() {
	s.mux.HandleFunc("/_emulator/health", s.health)
	s.mux.HandleFunc("/_emulator/readiness", s.readiness)
	s.mux.HandleFunc("/_emulator/version", s.version)
	s.mux.HandleFunc("/_emulator/capabilities", s.capabilities)
	s.mux.HandleFunc("/bigquery/v2/projects/", s.bigQueryV2)
}

func (s *Server) health(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{
		"status":  "ok",
		"service": version.Name,
	})
}

func (s *Server) readiness(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{
		"status":  "ready",
		"service": version.Name,
	})
}

func (s *Server) version(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{
		"name":    version.Name,
		"version": version.Version,
	})
}

func (s *Server) capabilities(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, s.registry)
}

func writeJSON(w http.ResponseWriter, status int, data any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(data)
}

func writeError(w http.ResponseWriter, status int, message string, reason string) {
	// Simple mapping for Status string: "Not Found" -> "NOT_FOUND"
	statusStr := strings.ReplaceAll(strings.ToUpper(http.StatusText(status)), " ", "_")
	if statusStr == "" {
		statusStr = "UNKNOWN"
	}

	resp := map[string]any{
		"error": map[string]any{
			"code":    status,
			"message": message,
			"errors": []map[string]any{
				{
					"message": message,
					"domain":  "global",
					"reason":  reason,
				},
			},
			"status": statusStr,
		},
	}
	writeJSON(w, status, resp)
}

func (s *Server) checkETag(w http.ResponseWriter, r *http.Request, version int) bool {
	etag := fmt.Sprintf("\"v%d\"", version)
	w.Header().Set("ETag", etag)
	if r.Header.Get("If-None-Match") == etag {
		w.WriteHeader(http.StatusNotModified)
		return true
	}
	return false
}
