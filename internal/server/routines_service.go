package server

import (
	"sort"
	"strings"
	"sync"
	"time"
)

// routineRecord is metadata-only: there is no SQL execution engine behind it,
// so insert/get/list/patch/delete round-trip the routine definition without
// ever calling it. This matches the master plan's own scoping for routines
// (CRUD lifecycle first; execution is a separate, unimplemented concern).
type routineRecord struct {
	ProjectID      string
	DatasetID      string
	RoutineID      string
	RoutineType    string
	Language       string
	DefinitionBody string
	Description    string
	CreatedAt      time.Time
	UpdatedAt      time.Time
	Version        int
}

type routineInsert struct {
	ProjectID      string
	DatasetID      string
	RoutineID      string
	RoutineType    string
	Language       string
	DefinitionBody string
	Description    string
}

type routinePatch struct {
	ProjectID         string
	DatasetID         string
	RoutineID         string
	RoutineType       string
	Language          string
	DefinitionBody    string
	Description       string
	HasRoutineType    bool
	HasLanguage       bool
	HasDefinitionBody bool
	HasDescription    bool
}

type routineService struct {
	mu       sync.RWMutex
	projects map[string]map[string]map[string]*routineRecord
}

func newRoutineService() *routineService {
	return &routineService{
		projects: make(map[string]map[string]map[string]*routineRecord),
	}
}

func (s *routineService) list(projectID, datasetID string, start, size int) ([]*routineRecord, int, int) {
	s.mu.Lock()
	defer s.mu.Unlock()

	routines := s.ensureDatasetLocked(projectID, datasetID)
	ids := make([]string, 0, len(routines))
	for id := range routines {
		ids = append(ids, id)
	}
	sort.Strings(ids)

	if start > len(ids) {
		start = len(ids)
	}
	end := start + size
	if end > len(ids) {
		end = len(ids)
	}

	out := make([]*routineRecord, 0, end-start)
	for _, id := range ids[start:end] {
		cp := *routines[id]
		out = append(out, &cp)
	}

	next := -1
	if end < len(ids) {
		next = end
	}
	return out, next, len(ids)
}

func (s *routineService) get(projectID, datasetID, routineID string) (*routineRecord, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	routines := s.ensureDatasetLocked(projectID, datasetID)
	r := routines[routineID]
	if r == nil {
		return nil, false
	}
	cp := *r
	return &cp, true
}

func (s *routineService) insert(input routineInsert) (*routineRecord, bool) {
	projectID := strings.TrimSpace(input.ProjectID)
	datasetID := strings.TrimSpace(input.DatasetID)
	routineID := strings.TrimSpace(input.RoutineID)
	if projectID == "" || datasetID == "" || routineID == "" {
		return nil, false
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	routines := s.ensureDatasetLocked(projectID, datasetID)
	if _, exists := routines[routineID]; exists {
		return nil, false
	}

	now := time.Now().UTC()
	rec := &routineRecord{
		ProjectID:      projectID,
		DatasetID:      datasetID,
		RoutineID:      routineID,
		RoutineType:    normalizeRoutineType(input.RoutineType),
		Language:       normalizeRoutineLanguage(input.Language),
		DefinitionBody: input.DefinitionBody,
		Description:    strings.TrimSpace(input.Description),
		CreatedAt:      now,
		UpdatedAt:      now,
		Version:        1,
	}
	routines[routineID] = rec
	cp := *rec
	return &cp, true
}

func (s *routineService) patch(input routinePatch) (*routineRecord, bool) {
	projectID := strings.TrimSpace(input.ProjectID)
	datasetID := strings.TrimSpace(input.DatasetID)
	routineID := strings.TrimSpace(input.RoutineID)
	if projectID == "" || datasetID == "" || routineID == "" {
		return nil, false
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	routines := s.ensureDatasetLocked(projectID, datasetID)
	rec := routines[routineID]
	if rec == nil {
		return nil, false
	}

	if input.HasRoutineType {
		rec.RoutineType = normalizeRoutineType(input.RoutineType)
	}
	if input.HasLanguage {
		rec.Language = normalizeRoutineLanguage(input.Language)
	}
	if input.HasDefinitionBody {
		rec.DefinitionBody = input.DefinitionBody
	}
	if input.HasDescription {
		rec.Description = strings.TrimSpace(input.Description)
	}
	rec.UpdatedAt = time.Now().UTC()
	rec.Version++

	cp := *rec
	return &cp, true
}

func (s *routineService) delete(projectID, datasetID, routineID string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	routines := s.ensureDatasetLocked(projectID, datasetID)
	if _, exists := routines[routineID]; !exists {
		return false
	}
	delete(routines, routineID)
	return true
}

func (s *routineService) ensureDatasetLocked(projectID, datasetID string) map[string]*routineRecord {
	proj := s.projects[projectID]
	if proj == nil {
		proj = make(map[string]map[string]*routineRecord)
		s.projects[projectID] = proj
	}
	routines := proj[datasetID]
	if routines == nil {
		routines = make(map[string]*routineRecord)
		proj[datasetID] = routines
	}
	return routines
}

func normalizeRoutineType(v string) string {
	switch strings.ToUpper(strings.TrimSpace(v)) {
	case "PROCEDURE":
		return "PROCEDURE"
	case "TABLE_VALUED_FUNCTION":
		return "TABLE_VALUED_FUNCTION"
	default:
		return "SCALAR_FUNCTION"
	}
}

func normalizeRoutineLanguage(v string) string {
	if strings.EqualFold(strings.TrimSpace(v), "JAVASCRIPT") {
		return "JAVASCRIPT"
	}
	return "SQL"
}
