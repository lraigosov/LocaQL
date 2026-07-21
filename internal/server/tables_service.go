package server

import (
	"sort"
	"strings"
	"sync"
)

type tableRecord struct {
	ProjectID string
	DatasetID string
	TableID   string
}

type tableInsert struct {
	ProjectID string
	DatasetID string
	TableID   string
}

type tableService struct {
	mu       sync.RWMutex
	defaults []string
	projects map[string]map[string]map[string]*tableRecord
}

func newTableService() *tableService {
	return &tableService{
		defaults: []string{"events", "daily_metrics", "users", "raw_import"},
		projects: make(map[string]map[string]map[string]*tableRecord),
	}
}

func (s *tableService) list(projectID, datasetID string, start, size int) ([]*tableRecord, int) {
	s.mu.Lock()
	defer s.mu.Unlock()

	tables := s.ensureDatasetLocked(projectID, datasetID)
	ids := make([]string, 0, len(tables))
	for id := range tables {
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

	out := make([]*tableRecord, 0, end-start)
	for _, id := range ids[start:end] {
		cp := *tables[id]
		out = append(out, &cp)
	}

	next := -1
	if end < len(ids) {
		next = end
	}

	return out, next
}

func (s *tableService) get(projectID, datasetID, tableID string) (*tableRecord, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	tables := s.ensureDatasetLocked(projectID, datasetID)
	t := tables[tableID]
	if t == nil {
		return nil, false
	}
	cp := *t
	return &cp, true
}

func (s *tableService) insert(input tableInsert) (*tableRecord, bool) {
	projectID := strings.TrimSpace(input.ProjectID)
	datasetID := strings.TrimSpace(input.DatasetID)
	tableID := strings.TrimSpace(input.TableID)
	if projectID == "" || datasetID == "" || tableID == "" {
		return nil, false
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	tables := s.ensureDatasetLocked(projectID, datasetID)
	if _, exists := tables[tableID]; exists {
		return nil, false
	}
	t := &tableRecord{ProjectID: projectID, DatasetID: datasetID, TableID: tableID}
	tables[tableID] = t
	cp := *t
	return &cp, true
}

func (s *tableService) delete(projectID, datasetID, tableID string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	tables := s.ensureDatasetLocked(projectID, datasetID)
	if _, exists := tables[tableID]; !exists {
		return false
	}
	delete(tables, tableID)
	return true
}

func (s *tableService) ensureDatasetLocked(projectID, datasetID string) map[string]*tableRecord {
	proj := s.projects[projectID]
	if proj == nil {
		proj = make(map[string]map[string]*tableRecord)
		s.projects[projectID] = proj
	}

	tables := proj[datasetID]
	if tables != nil {
		return tables
	}

	tables = make(map[string]*tableRecord)
	for _, id := range s.defaults {
		tables[id] = &tableRecord{ProjectID: projectID, DatasetID: datasetID, TableID: id}
	}
	proj[datasetID] = tables
	return tables
}
