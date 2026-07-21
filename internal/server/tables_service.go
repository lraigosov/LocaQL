package server

import (
	"sort"
	"strings"
	"sync"
	"time"
)

type tableRecord struct {
	ProjectID    string
	DatasetID    string
	TableID      string
	FriendlyName string
	Description  string
	Labels       map[string]string
	CreatedAt    time.Time
	UpdatedAt    time.Time
	Version      int
}

type tableInsert struct {
	ProjectID    string
	DatasetID    string
	TableID      string
	FriendlyName string
	Description  string
	Labels       map[string]string
}

type tablePatch struct {
	ProjectID       string
	DatasetID       string
	TableID         string
	FriendlyName    string
	Description     string
	Labels          map[string]string
	HasFriendlyName bool
	HasDescription  bool
	HasLabels       bool
}

type tableUpdate struct {
	ProjectID    string
	DatasetID    string
	TableID      string
	FriendlyName string
	Description  string
	Labels       map[string]string
}

type tableService struct {
	mu              sync.RWMutex
	defaults        []string
	projects        map[string]map[string]map[string]*tableRecord
	datasetVersions map[string]int
}

func newTableService() *tableService {
	return &tableService{
		defaults:        []string{"events", "daily_metrics", "users", "raw_import"},
		projects:        make(map[string]map[string]map[string]*tableRecord),
		datasetVersions: make(map[string]int),
	}
}

func (s *tableService) list(projectID, datasetID string, start, size int) ([]*tableRecord, int, int) {
	s.mu.Lock()
	defer s.mu.Unlock()

	tables := s.ensureDatasetLocked(projectID, datasetID)
	version := s.datasetVersions[s.datasetKey(projectID, datasetID)]
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

	return out, next, version
}

func (s *tableService) get(projectID, datasetID, tableID string) (*tableRecord, bool, int) {
	s.mu.Lock()
	defer s.mu.Unlock()

	tables := s.ensureDatasetLocked(projectID, datasetID)
	t := tables[tableID]
	if t == nil {
		return nil, false, 0
	}
	cp := *t
	cp.Labels = cloneLabels(cp.Labels)
	return &cp, true, t.Version
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
	now := time.Now().UTC()
	t := &tableRecord{
		ProjectID:    projectID,
		DatasetID:    datasetID,
		TableID:      tableID,
		FriendlyName: strings.TrimSpace(input.FriendlyName),
		Description:  strings.TrimSpace(input.Description),
		Labels:       cloneLabels(input.Labels),
		CreatedAt:    now,
		UpdatedAt:    now,
		Version:      1,
	}
	tables[tableID] = t
	s.datasetVersions[s.datasetKey(projectID, datasetID)]++
	cp := *t
	cp.Labels = cloneLabels(cp.Labels)
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
	s.datasetVersions[s.datasetKey(projectID, datasetID)]++
	return true
}

func (s *tableService) patch(input tablePatch) (*tableRecord, bool) {
	projectID := strings.TrimSpace(input.ProjectID)
	datasetID := strings.TrimSpace(input.DatasetID)
	tableID := strings.TrimSpace(input.TableID)
	if projectID == "" || datasetID == "" || tableID == "" {
		return nil, false
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	tables := s.ensureDatasetLocked(projectID, datasetID)
	t := tables[tableID]
	if t == nil {
		return nil, false
	}

	if input.HasFriendlyName {
		t.FriendlyName = strings.TrimSpace(input.FriendlyName)
	}
	if input.HasDescription {
		t.Description = strings.TrimSpace(input.Description)
	}
	if input.HasLabels {
		t.Labels = cloneLabels(input.Labels)
	}

	t.UpdatedAt = time.Now().UTC()
	t.Version++
	s.datasetVersions[s.datasetKey(projectID, datasetID)]++

	cp := *t
	cp.Labels = cloneLabels(cp.Labels)
	return &cp, true
}

func (s *tableService) update(input tableUpdate) (*tableRecord, bool) {
	projectID := strings.TrimSpace(input.ProjectID)
	datasetID := strings.TrimSpace(input.DatasetID)
	tableID := strings.TrimSpace(input.TableID)
	if projectID == "" || datasetID == "" || tableID == "" {
		return nil, false
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	tables := s.ensureDatasetLocked(projectID, datasetID)
	t := tables[tableID]
	if t == nil {
		return nil, false
	}

	t.FriendlyName = strings.TrimSpace(input.FriendlyName)
	t.Description = strings.TrimSpace(input.Description)
	t.Labels = cloneLabels(input.Labels)
	t.UpdatedAt = time.Now().UTC()
	t.Version++
	s.datasetVersions[s.datasetKey(projectID, datasetID)]++

	cp := *t
	cp.Labels = cloneLabels(cp.Labels)
	return &cp, true
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
	now := time.Now().UTC()
	for _, id := range s.defaults {
		tables[id] = &tableRecord{ProjectID: projectID, DatasetID: datasetID, TableID: id, CreatedAt: now, UpdatedAt: now, Version: 1}
	}
	proj[datasetID] = tables
	key := s.datasetKey(projectID, datasetID)
	if _, ok := s.datasetVersions[key]; !ok {
		s.datasetVersions[key] = 1
	}
	return tables
}

func (s *tableService) datasetKey(projectID, datasetID string) string {
	return projectID + ":" + datasetID
}
