package server

import (
	"sort"
	"strings"
	"sync"
)

type datasetRecord struct {
	ProjectID    string
	DatasetID    string
	FriendlyName string
	Location     string
	Labels       map[string]string
}

type datasetInsert struct {
	ProjectID    string
	DatasetID    string
	FriendlyName string
	Location     string
	Labels       map[string]string
}

type datasetService struct {
	mu       sync.RWMutex
	defaults []string
	projects map[string]map[string]*datasetRecord
}

func newDatasetService() *datasetService {
	return &datasetService{
		defaults: []string{"analytics", "finance", "ops", "sandbox"},
		projects: make(map[string]map[string]*datasetRecord),
	}
}

func (s *datasetService) list(projectID string, start, size int) ([]*datasetRecord, int) {
	s.mu.Lock()
	defer s.mu.Unlock()

	proj := s.ensureProjectLocked(projectID)
	ids := make([]string, 0, len(proj))
	for id := range proj {
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

	out := make([]*datasetRecord, 0, end-start)
	for _, id := range ids[start:end] {
		cp := *proj[id]
		cp.Labels = cloneLabels(cp.Labels)
		out = append(out, &cp)
	}

	next := -1
	if end < len(ids) {
		next = end
	}

	return out, next
}

func (s *datasetService) get(projectID, datasetID string) (*datasetRecord, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	proj := s.ensureProjectLocked(projectID)
	item := proj[datasetID]
	if item == nil {
		return nil, false
	}
	cp := *item
	cp.Labels = cloneLabels(cp.Labels)
	return &cp, true
}

func (s *datasetService) insert(input datasetInsert) (*datasetRecord, bool) {
	projectID := strings.TrimSpace(input.ProjectID)
	datasetID := strings.TrimSpace(input.DatasetID)
	if projectID == "" || datasetID == "" {
		return nil, false
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	proj := s.ensureProjectLocked(projectID)
	if _, exists := proj[datasetID]; exists {
		return nil, false
	}

	rec := &datasetRecord{
		ProjectID:    projectID,
		DatasetID:    datasetID,
		FriendlyName: strings.TrimSpace(input.FriendlyName),
		Location:     strings.TrimSpace(input.Location),
		Labels:       cloneLabels(input.Labels),
	}
	proj[datasetID] = rec

	cp := *rec
	cp.Labels = cloneLabels(cp.Labels)
	return &cp, true
}

func (s *datasetService) delete(projectID, datasetID string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	proj := s.ensureProjectLocked(projectID)
	if _, exists := proj[datasetID]; !exists {
		return false
	}
	delete(proj, datasetID)
	return true
}

func (s *datasetService) exists(projectID, datasetID string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	proj := s.ensureProjectLocked(projectID)
	_, exists := proj[datasetID]
	return exists
}

func (s *datasetService) ensureProjectLocked(projectID string) map[string]*datasetRecord {
	proj := s.projects[projectID]
	if proj != nil {
		return proj
	}

	proj = make(map[string]*datasetRecord)
	for _, id := range s.defaults {
		proj[id] = &datasetRecord{
			ProjectID: projectID,
			DatasetID: id,
		}
	}
	s.projects[projectID] = proj
	return proj
}

func cloneLabels(labels map[string]string) map[string]string {
	if len(labels) == 0 {
		return nil
	}
	out := make(map[string]string, len(labels))
	for k, v := range labels {
		out[k] = v
	}
	return out
}
