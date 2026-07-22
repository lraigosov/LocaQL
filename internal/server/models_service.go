package server

import (
	"sort"
	"strings"
	"sync"
	"time"
)

// modelRecord is metadata-only: there is no ML training or inference backend,
// so insert/get/list/patch/delete round-trip model metadata without ever
// training or scoring anything. Exposing modelType here must never imply a
// working BigQuery ML backend exists.
type modelRecord struct {
	ProjectID    string
	DatasetID    string
	ModelID      string
	ModelType    string
	FriendlyName string
	Description  string
	Labels       map[string]string
	CreatedAt    time.Time
	UpdatedAt    time.Time
	Version      int
}

type modelInsert struct {
	ProjectID    string
	DatasetID    string
	ModelID      string
	ModelType    string
	FriendlyName string
	Description  string
	Labels       map[string]string
}

type modelPatch struct {
	ProjectID       string
	DatasetID       string
	ModelID         string
	FriendlyName    string
	Description     string
	Labels          map[string]string
	HasFriendlyName bool
	HasDescription  bool
	HasLabels       bool
}

type modelService struct {
	mu       sync.RWMutex
	projects map[string]map[string]map[string]*modelRecord
}

func newModelService() *modelService {
	return &modelService{
		projects: make(map[string]map[string]map[string]*modelRecord),
	}
}

func (s *modelService) list(projectID, datasetID string, start, size int) ([]*modelRecord, int, int) {
	s.mu.Lock()
	defer s.mu.Unlock()

	models := s.ensureDatasetLocked(projectID, datasetID)
	ids := make([]string, 0, len(models))
	for id := range models {
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

	out := make([]*modelRecord, 0, end-start)
	for _, id := range ids[start:end] {
		cp := *models[id]
		cp.Labels = cloneLabels(cp.Labels)
		out = append(out, &cp)
	}

	next := -1
	if end < len(ids) {
		next = end
	}
	return out, next, len(ids)
}

func (s *modelService) get(projectID, datasetID, modelID string) (*modelRecord, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	models := s.ensureDatasetLocked(projectID, datasetID)
	m := models[modelID]
	if m == nil {
		return nil, false
	}
	cp := *m
	cp.Labels = cloneLabels(cp.Labels)
	return &cp, true
}

func (s *modelService) insert(input modelInsert) (*modelRecord, bool) {
	projectID := strings.TrimSpace(input.ProjectID)
	datasetID := strings.TrimSpace(input.DatasetID)
	modelID := strings.TrimSpace(input.ModelID)
	if projectID == "" || datasetID == "" || modelID == "" {
		return nil, false
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	models := s.ensureDatasetLocked(projectID, datasetID)
	if _, exists := models[modelID]; exists {
		return nil, false
	}

	now := time.Now().UTC()
	rec := &modelRecord{
		ProjectID:    projectID,
		DatasetID:    datasetID,
		ModelID:      modelID,
		ModelType:    strings.ToUpper(strings.TrimSpace(input.ModelType)),
		FriendlyName: strings.TrimSpace(input.FriendlyName),
		Description:  strings.TrimSpace(input.Description),
		Labels:       cloneLabels(input.Labels),
		CreatedAt:    now,
		UpdatedAt:    now,
		Version:      1,
	}
	models[modelID] = rec
	cp := *rec
	cp.Labels = cloneLabels(cp.Labels)
	return &cp, true
}

func (s *modelService) patch(input modelPatch) (*modelRecord, bool) {
	projectID := strings.TrimSpace(input.ProjectID)
	datasetID := strings.TrimSpace(input.DatasetID)
	modelID := strings.TrimSpace(input.ModelID)
	if projectID == "" || datasetID == "" || modelID == "" {
		return nil, false
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	models := s.ensureDatasetLocked(projectID, datasetID)
	rec := models[modelID]
	if rec == nil {
		return nil, false
	}

	if input.HasFriendlyName {
		rec.FriendlyName = strings.TrimSpace(input.FriendlyName)
	}
	if input.HasDescription {
		rec.Description = strings.TrimSpace(input.Description)
	}
	if input.HasLabels {
		rec.Labels = cloneLabels(input.Labels)
	}
	rec.UpdatedAt = time.Now().UTC()
	rec.Version++

	cp := *rec
	cp.Labels = cloneLabels(cp.Labels)
	return &cp, true
}

func (s *modelService) delete(projectID, datasetID, modelID string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	models := s.ensureDatasetLocked(projectID, datasetID)
	if _, exists := models[modelID]; !exists {
		return false
	}
	delete(models, modelID)
	return true
}

func (s *modelService) ensureDatasetLocked(projectID, datasetID string) map[string]*modelRecord {
	proj := s.projects[projectID]
	if proj == nil {
		proj = make(map[string]map[string]*modelRecord)
		s.projects[projectID] = proj
	}
	models := proj[datasetID]
	if models == nil {
		models = make(map[string]*modelRecord)
		proj[datasetID] = models
	}
	return models
}
