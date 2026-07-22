package server

import (
	"sort"
	"strings"
	"sync"
)

type datasetRecord struct {
	ProjectID                string
	DatasetID                string
	FriendlyName             string
	Location                 string
	Labels                   map[string]string
	DefaultTableExpirationMs int64
}

type datasetInsert struct {
	ProjectID                string
	DatasetID                string
	FriendlyName             string
	Location                 string
	Labels                   map[string]string
	DefaultTableExpirationMs int64
}

type datasetPatch struct {
	ProjectID                   string
	DatasetID                   string
	FriendlyName                string
	Location                    string
	Labels                      map[string]string
	DefaultTableExpirationMs    int64
	HasFriendlyName             bool
	HasLocation                 bool
	HasLabels                   bool
	HasDefaultTableExpirationMs bool
}

type datasetService struct {
	mu         sync.RWMutex
	defaults   []string
	projects   map[string]map[string]*datasetRecord
	versions   map[string]int
	tombstones map[string]*datasetRecord
}

func newDatasetService() *datasetService {
	return &datasetService{
		defaults:   []string{"analytics", "finance", "ops", "sandbox"},
		projects:   make(map[string]map[string]*datasetRecord),
		versions:   make(map[string]int),
		tombstones: make(map[string]*datasetRecord),
	}
}

func (s *datasetService) list(projectID string, start, size int) ([]*datasetRecord, int, int) {
	s.mu.Lock()
	defer s.mu.Unlock()

	version := s.versions[projectID]
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

	return out, next, version
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
		ProjectID:                projectID,
		DatasetID:                datasetID,
		FriendlyName:             strings.TrimSpace(input.FriendlyName),
		Location:                 strings.TrimSpace(input.Location),
		Labels:                   cloneLabels(input.Labels),
		DefaultTableExpirationMs: input.DefaultTableExpirationMs,
	}
	proj[datasetID] = rec
	s.versions[projectID]++

	cp := *rec
	cp.Labels = cloneLabels(cp.Labels)
	return &cp, true
}

func (s *datasetService) delete(projectID, datasetID string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	proj := s.ensureProjectLocked(projectID)
	rec, exists := proj[datasetID]
	if !exists {
		return false
	}
	cp := *rec
	cp.Labels = cloneLabels(cp.Labels)
	s.tombstones[s.tombstoneKey(projectID, datasetID)] = &cp
	delete(proj, datasetID)
	s.versions[projectID]++
	return true
}

// undelete restores a dataset's metadata (friendlyName, location, labels)
// from the tombstone left by the most recent delete. It never restores table
// contents: those are removed independently by deleteAllForDataset and are
// not tracked by the tombstone. It fails if no tombstone exists or if a
// dataset with the same ID already exists (never silently overwrites).
func (s *datasetService) undelete(projectID, datasetID string) (*datasetRecord, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	proj := s.ensureProjectLocked(projectID)
	if _, exists := proj[datasetID]; exists {
		return nil, false
	}
	tombstoned, ok := s.tombstones[s.tombstoneKey(projectID, datasetID)]
	if !ok {
		return nil, false
	}
	cp := *tombstoned
	cp.Labels = cloneLabels(cp.Labels)
	proj[datasetID] = &cp
	s.versions[projectID]++
	delete(s.tombstones, s.tombstoneKey(projectID, datasetID))

	restored := cp
	restored.Labels = cloneLabels(cp.Labels)
	return &restored, true
}

func (s *datasetService) tombstoneKey(projectID, datasetID string) string {
	return projectID + ":" + datasetID
}

func (s *datasetService) patch(input datasetPatch) (*datasetRecord, bool) {
	projectID := strings.TrimSpace(input.ProjectID)
	datasetID := strings.TrimSpace(input.DatasetID)
	if projectID == "" || datasetID == "" {
		return nil, false
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	proj := s.ensureProjectLocked(projectID)
	item := proj[datasetID]
	if item == nil {
		return nil, false
	}

	if input.HasFriendlyName {
		item.FriendlyName = strings.TrimSpace(input.FriendlyName)
	}
	if input.HasLocation {
		item.Location = strings.TrimSpace(input.Location)
	}
	if input.HasLabels {
		item.Labels = cloneLabels(input.Labels)
	}
	if input.HasDefaultTableExpirationMs {
		item.DefaultTableExpirationMs = input.DefaultTableExpirationMs
	}

	s.versions[projectID]++

	cp := *item
	cp.Labels = cloneLabels(cp.Labels)
	return &cp, true
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
