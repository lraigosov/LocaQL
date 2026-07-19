package server

import (
	"fmt"
	"sort"
	"strconv"
	"sync"
	"time"
)

type jobState string

const (
	jobStatePending jobState = "PENDING"
	jobStateRunning jobState = "RUNNING"
	jobStateDone    jobState = "DONE"
)

type jobRecord struct {
	ProjectID       string
	JobID           string
	State           jobState
	RequestID       string
	CreatedAt       time.Time
	StartedAt       time.Time
	EndedAt         time.Time
	CancelRequested bool
	ErrorReason     string
	ErrorMessage    string
}

type jobService struct {
	mu             sync.Mutex
	jobsByProject  map[string]map[string]*jobRecord
	requestIDIndex map[string]map[string]string
	counter        int64
}

func newJobService() *jobService {
	return &jobService{
		jobsByProject:  make(map[string]map[string]*jobRecord),
		requestIDIndex: make(map[string]map[string]string),
	}
}

func (s *jobService) insert(projectID, requestID string) (*jobRecord, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if requestID != "" {
		if _, ok := s.requestIDIndex[projectID]; ok {
			if existingJobID, exists := s.requestIDIndex[projectID][requestID]; exists {
				if existing := s.jobsByProject[projectID][existingJobID]; existing != nil {
					cp := *existing
					return &cp, false
				}
			}
		}
	}

	s.counter++
	jobID := "job_" + strconv.FormatInt(s.counter, 10)
	now := time.Now().UTC()
	jr := &jobRecord{
		ProjectID: projectID,
		JobID:     jobID,
		State:     jobStatePending,
		RequestID: requestID,
		CreatedAt: now,
	}

	if _, ok := s.jobsByProject[projectID]; !ok {
		s.jobsByProject[projectID] = make(map[string]*jobRecord)
	}
	s.jobsByProject[projectID][jobID] = jr

	if requestID != "" {
		if _, ok := s.requestIDIndex[projectID]; !ok {
			s.requestIDIndex[projectID] = make(map[string]string)
		}
		s.requestIDIndex[projectID][requestID] = jobID
	}

	go s.run(jobID, projectID)
	cp := *jr
	return &cp, true
}

func (s *jobService) run(jobID, projectID string) {
	s.mu.Lock()
	jr := s.jobsByProject[projectID][jobID]
	if jr == nil {
		s.mu.Unlock()
		return
	}
	if jr.CancelRequested {
		jr.State = jobStateDone
		jr.ErrorReason = "stopped"
		jr.ErrorMessage = "job cancelled before execution"
		jr.EndedAt = time.Now().UTC()
		s.mu.Unlock()
		return
	}
	jr.State = jobStateRunning
	jr.StartedAt = time.Now().UTC()
	s.mu.Unlock()

	time.Sleep(120 * time.Millisecond)

	s.mu.Lock()
	defer s.mu.Unlock()
	jr = s.jobsByProject[projectID][jobID]
	if jr == nil {
		return
	}
	if jr.CancelRequested {
		jr.State = jobStateDone
		jr.ErrorReason = "stopped"
		jr.ErrorMessage = "job cancelled"
		jr.EndedAt = time.Now().UTC()
		return
	}
	jr.State = jobStateDone
	jr.EndedAt = time.Now().UTC()
}

func (s *jobService) get(projectID, jobID string) (*jobRecord, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	proj := s.jobsByProject[projectID]
	if proj == nil {
		return nil, false
	}
	jr := proj[jobID]
	if jr == nil {
		return nil, false
	}
	cp := *jr
	return &cp, true
}

func (s *jobService) cancel(projectID, jobID string) (*jobRecord, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	proj := s.jobsByProject[projectID]
	if proj == nil {
		return nil, false
	}
	jr := proj[jobID]
	if jr == nil {
		return nil, false
	}
	jr.CancelRequested = true
	if jr.State == jobStatePending {
		jr.State = jobStateDone
		jr.ErrorReason = "stopped"
		jr.ErrorMessage = "job cancelled before execution"
		jr.EndedAt = time.Now().UTC()
	}
	cp := *jr
	return &cp, true
}

func (s *jobService) list(projectID, stateFilter string, start, size int) ([]*jobRecord, string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	proj := s.jobsByProject[projectID]
	if proj == nil {
		return []*jobRecord{}, ""
	}

	ids := make([]string, 0, len(proj))
	for id := range proj {
		ids = append(ids, id)
	}
	sort.Strings(ids)

	filtered := make([]*jobRecord, 0, len(ids))
	for _, id := range ids {
		jr := proj[id]
		if stateFilter != "" && string(jr.State) != stateFilter {
			continue
		}
		cp := *jr
		filtered = append(filtered, &cp)
	}

	if start > len(filtered) {
		start = len(filtered)
	}
	end := start + size
	if end > len(filtered) {
		end = len(filtered)
	}

	next := ""
	if end < len(filtered) {
		next = fmt.Sprintf("%d", end)
	}

	return filtered[start:end], next
}

func renderJobResource(j *jobRecord) map[string]any {
	status := map[string]any{"state": string(j.State)}
	if j.ErrorReason != "" {
		status["errorResult"] = map[string]string{
			"reason":  j.ErrorReason,
			"message": j.ErrorMessage,
		}
	}

	return map[string]any{
		"kind": "bigquery#job",
		"id":   fmt.Sprintf("%s:%s", j.ProjectID, j.JobID),
		"jobReference": map[string]string{
			"projectId": j.ProjectID,
			"jobId":     j.JobID,
		},
		"status": status,
	}
}
