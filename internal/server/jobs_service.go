package server

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
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
	ParentJobID     string
	JobType         string
	ResourceKey     string
	State           jobState
	RequestID       string
	UserEmail       string
	QueryText       string
	IsScript        bool
	Statistics      jobStatistics
	CreatedAt       time.Time
	StartedAt       time.Time
	EndedAt         time.Time
	CancelRequested bool
	ErrorReason     string
	ErrorMessage    string
}

type jobStatistics struct {
	Executor       string
	Simulated      bool
	TotalSlotMs    int64
	ProcessedBytes int64
	OutputRows     int64
}

type requestIDRecord struct {
	JobID     string
	CreatedAt time.Time
}

type jobInsertOptions struct {
	ProjectID     string
	RequestID     string
	ParentJobID   string
	UserEmail     string
	QueryText     string
	JobType       string
	TargetDataset string
	TargetTable   string
	IsScript      bool
}

type jobListFilters struct {
	StateFilter string
	UserEmail   string
	ParentJobID string
	MinCreated  time.Time
	MaxCreated  time.Time
}

type jobService struct {
	mu              sync.RWMutex
	jobsByProject   map[string]map[string]*jobRecord
	requestIDIndex  map[string]map[string]requestIDRecord
	requestIDTTL    time.Duration
	maxConcurrent   int
	runSlots        chan struct{}
	resourceSlots   map[string]chan struct{}
	persistencePath string
	counter         int64
}

type jobServiceSnapshot struct {
	Counter        int64                                 `json:"counter"`
	JobsByProject  map[string]map[string]*jobRecord      `json:"jobs_by_project"`
	RequestIDIndex map[string]map[string]requestIDRecord `json:"request_id_index"`
}

func newJobService() *jobService {
	return newJobServiceWithWorkerLimit(readDefaultJobWorkerLimit())
}

func newJobServiceWithWorkerLimit(limit int) *jobService {
	s := &jobService{
		jobsByProject:  make(map[string]map[string]*jobRecord),
		requestIDIndex: make(map[string]map[string]requestIDRecord),
		resourceSlots:  make(map[string]chan struct{}),
		requestIDTTL:   15 * time.Minute,
	}
	if limit > 0 {
		s.maxConcurrent = limit
		s.runSlots = make(chan struct{}, limit)
	}
	return s
}

func newJobServiceWithTTL(ttl time.Duration) *jobService {
	s := newJobService()
	s.requestIDTTL = ttl
	return s
}

func newJobServiceWithPersistence(path string) *jobService {
	s := newJobService()
	s.persistencePath = path
	s.loadPersistence()
	return s
}

func readDefaultJobWorkerLimit() int {
	raw := strings.TrimSpace(os.Getenv("LOCAQL_JOB_WORKERS"))
	if raw == "" {
		return 0
	}
	n, err := strconv.Atoi(raw)
	if err != nil || n <= 0 {
		return 0
	}
	return n
}

func (s *jobService) insert(opts jobInsertOptions) (*jobRecord, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	projectID := opts.ProjectID
	requestID := opts.RequestID
	now := time.Now().UTC()
	s.cleanupExpiredRequestIDsLocked(now)

	if requestID != "" {
		if _, ok := s.requestIDIndex[projectID]; ok {
			if existingRef, exists := s.requestIDIndex[projectID][requestID]; exists {
				if existing := s.jobsByProject[projectID][existingRef.JobID]; existing != nil {
					cp := *existing
					return &cp, false
				}
			}
		}
	}

	s.counter++
	jobID := "job_" + strconv.FormatInt(s.counter, 10)
	jr := &jobRecord{
		ProjectID:   projectID,
		JobID:       jobID,
		ParentJobID: opts.ParentJobID,
		JobType:     normalizeJobType(opts),
		ResourceKey: buildResourceKey(opts),
		State:       jobStatePending,
		RequestID:   requestID,
		UserEmail:   opts.UserEmail,
		QueryText:   opts.QueryText,
		IsScript:    opts.IsScript,
		Statistics:  newSimulatedStatistics(normalizeJobType(opts)),
		CreatedAt:   now,
	}

	if _, ok := s.jobsByProject[projectID]; !ok {
		s.jobsByProject[projectID] = make(map[string]*jobRecord)
	}
	s.jobsByProject[projectID][jobID] = jr

	if requestID != "" {
		if _, ok := s.requestIDIndex[projectID]; !ok {
			s.requestIDIndex[projectID] = make(map[string]requestIDRecord)
		}
		s.requestIDIndex[projectID][requestID] = requestIDRecord{JobID: jobID, CreatedAt: now}
	}

	s.persistLocked()

	go s.run(jobID, projectID)
	cp := *jr
	return &cp, true
}

func (s *jobService) insertScriptWithChildren(opts jobInsertOptions) (*jobRecord, []*jobRecord, bool) {
	parent, created := s.insert(opts)
	if !created {
		return parent, nil, false
	}

	childJobs := make([]*jobRecord, 0)
	parts := splitScriptStatements(opts.QueryText)
	for range parts {
		child, _ := s.insert(jobInsertOptions{
			ProjectID:   opts.ProjectID,
			ParentJobID: parent.JobID,
			UserEmail:   opts.UserEmail,
			QueryText:   opts.QueryText,
			JobType:     "query",
			IsScript:    false,
		})
		childJobs = append(childJobs, child)
	}

	return parent, childJobs, true
}

func (s *jobService) run(jobID, projectID string) {
	releaseSlot := func() {}
	if s.runSlots != nil {
		s.runSlots <- struct{}{}
		releaseSlot = func() {
			<-s.runSlots
		}
	}
	defer releaseSlot()

	s.mu.Lock()
	jr := s.jobsByProject[projectID][jobID]
	if jr == nil {
		s.mu.Unlock()
		return
	}
	resourceKey := jr.ResourceKey
	if jr.CancelRequested {
		jr.State = jobStateDone
		jr.ErrorReason = "stopped"
		jr.ErrorMessage = "job cancelled before execution"
		jr.EndedAt = time.Now().UTC()
		s.persistLocked()
		s.mu.Unlock()
		return
	}
	s.mu.Unlock()

	releaseResource := s.acquireResourceSlot(resourceKey)
	defer releaseResource()

	s.mu.Lock()
	jr = s.jobsByProject[projectID][jobID]
	if jr == nil {
		s.mu.Unlock()
		return
	}
	if jr.CancelRequested {
		jr.State = jobStateDone
		jr.ErrorReason = "stopped"
		jr.ErrorMessage = "job cancelled before execution"
		jr.EndedAt = time.Now().UTC()
		s.persistLocked()
		s.mu.Unlock()
		return
	}
	jr.State = jobStateRunning
	jr.StartedAt = time.Now().UTC()
	s.persistLocked()
	s.mu.Unlock()

	d := executorDuration(jr.JobType)
	time.Sleep(d)

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
		s.persistLocked()
		return
	}
	applyExecutorResult(jr)
	jr.State = jobStateDone
	jr.EndedAt = time.Now().UTC()
	s.persistLocked()
}

func (s *jobService) get(projectID, jobID string) (*jobRecord, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
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
	s.persistLocked()
	cp := *jr
	return &cp, true
}

func (s *jobService) list(projectID string, filters jobListFilters, start, size int) ([]*jobRecord, string) {
	s.mu.RLock()
	defer s.mu.RUnlock()

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
		if filters.StateFilter != "" && string(jr.State) != filters.StateFilter {
			continue
		}
		if filters.UserEmail != "" && jr.UserEmail != filters.UserEmail {
			continue
		}
		if filters.ParentJobID != "" && jr.ParentJobID != filters.ParentJobID {
			continue
		}
		if !filters.MinCreated.IsZero() && jr.CreatedAt.Before(filters.MinCreated) {
			continue
		}
		if !filters.MaxCreated.IsZero() && jr.CreatedAt.After(filters.MaxCreated) {
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

func (s *jobService) cleanupExpiredRequestIDsLocked(now time.Time) {
	if s.requestIDTTL <= 0 {
		return
	}
	for projectID, idx := range s.requestIDIndex {
		for reqID, ref := range idx {
			if ref.CreatedAt.Add(s.requestIDTTL).Before(now) {
				delete(idx, reqID)
			}
		}
		if len(idx) == 0 {
			delete(s.requestIDIndex, projectID)
		}
	}
}

func splitScriptStatements(query string) []string {
	parts := strings.Split(query, ";")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if strings.TrimSpace(p) == "" {
			continue
		}
		out = append(out, strings.TrimSpace(p))
	}
	if len(out) == 0 {
		return []string{"noop"}
	}
	return out
}

func buildResourceKey(opts jobInsertOptions) string {
	if opts.TargetDataset == "" || opts.TargetTable == "" {
		return ""
	}
	return fmt.Sprintf("%s:%s.%s", opts.ProjectID, opts.TargetDataset, opts.TargetTable)
}

func (s *jobService) acquireResourceSlot(resourceKey string) func() {
	if strings.TrimSpace(resourceKey) == "" {
		return func() {}
	}

	s.mu.Lock()
	slot, ok := s.resourceSlots[resourceKey]
	if !ok {
		slot = make(chan struct{}, 1)
		s.resourceSlots[resourceKey] = slot
	}
	s.mu.Unlock()

	slot <- struct{}{}
	return func() {
		<-slot
	}
}

func renderJobResource(j *jobRecord) map[string]any {
	status := map[string]any{"state": string(j.State)}
	if j.ErrorReason != "" {
		status["errorResult"] = map[string]string{
			"reason":  j.ErrorReason,
			"message": j.ErrorMessage,
		}
	}

	stats := map[string]any{
		"totalSlotMs":    j.Statistics.TotalSlotMs,
		"processedBytes": j.Statistics.ProcessedBytes,
		"outputRows":     j.Statistics.OutputRows,
		"simulation": map[string]any{
			"enabled":  j.Statistics.Simulated,
			"executor": j.Statistics.Executor,
		},
	}

	return map[string]any{
		"kind": "bigquery#job",
		"id":   fmt.Sprintf("%s:%s", j.ProjectID, j.JobID),
		"jobReference": map[string]string{
			"projectId": j.ProjectID,
			"jobId":     j.JobID,
		},
		"user_email":  j.UserEmail,
		"parentJobId": j.ParentJobID,
		"jobType":     j.JobType,
		"statistics":  stats,
		"status":      status,
	}
}

func normalizeJobType(opts jobInsertOptions) string {
	if opts.IsScript {
		return "script"
	}
	t := strings.ToLower(strings.TrimSpace(opts.JobType))
	switch t {
	case "query", "load", "extract", "copy":
		return t
	default:
		return "query"
	}
}

func newSimulatedStatistics(jobType string) jobStatistics {
	return jobStatistics{Executor: jobType, Simulated: true}
}

func executorDuration(jobType string) time.Duration {
	switch jobType {
	case "load":
		return 160 * time.Millisecond
	case "extract":
		return 140 * time.Millisecond
	case "copy":
		return 100 * time.Millisecond
	case "script":
		return 180 * time.Millisecond
	default:
		return 120 * time.Millisecond
	}
}

func applyExecutorResult(j *jobRecord) {
	switch j.JobType {
	case "load":
		j.Statistics.TotalSlotMs = 75
		j.Statistics.ProcessedBytes = 2048
		j.Statistics.OutputRows = 20
	case "extract":
		j.Statistics.TotalSlotMs = 40
		j.Statistics.ProcessedBytes = 1024
		j.Statistics.OutputRows = 10
	case "copy":
		j.Statistics.TotalSlotMs = 30
		j.Statistics.ProcessedBytes = 768
		j.Statistics.OutputRows = 8
	case "script":
		j.Statistics.TotalSlotMs = 90
		j.Statistics.ProcessedBytes = 1536
		j.Statistics.OutputRows = 12
	default:
		j.Statistics.TotalSlotMs = 60
		j.Statistics.ProcessedBytes = 512
		j.Statistics.OutputRows = 5
	}
}

func (s *jobService) loadPersistence() {
	if strings.TrimSpace(s.persistencePath) == "" {
		return
	}
	content, err := os.ReadFile(s.persistencePath)
	if err != nil {
		return
	}
	var snap jobServiceSnapshot
	if err := json.Unmarshal(content, &snap); err != nil {
		return
	}
	if snap.JobsByProject != nil {
		s.jobsByProject = snap.JobsByProject
	}
	if snap.RequestIDIndex != nil {
		s.requestIDIndex = snap.RequestIDIndex
	}
	s.counter = snap.Counter
}

func (s *jobService) persistLocked() {
	if strings.TrimSpace(s.persistencePath) == "" {
		return
	}
	snap := jobServiceSnapshot{
		Counter:        s.counter,
		JobsByProject:  s.jobsByProject,
		RequestIDIndex: s.requestIDIndex,
	}
	content, err := json.MarshalIndent(snap, "", "  ")
	if err != nil {
		return
	}
	dir := filepath.Dir(s.persistencePath)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return
	}
	_ = os.WriteFile(s.persistencePath, content, 0o644)
}
