package server

import (
	"encoding/json"
	"net/http"

	"github.com/lraigosov/LocaQL/internal/capabilities"
	"github.com/lraigosov/LocaQL/internal/version"
)

type Server struct {
	mux      *http.ServeMux
	registry capabilities.Registry
	jobs     *jobService
}

func New(reg capabilities.Registry) *Server {
	s := &Server{
		mux:      http.NewServeMux(),
		registry: reg,
		jobs:     newJobService(),
	}
	s.routes()
	return s
}

func (s *Server) Handler() http.Handler {
	return s.mux
}

func (s *Server) routes() {
	s.mux.HandleFunc("/_emulator/health", s.health)
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
