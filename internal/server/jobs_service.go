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
	Priority        string // INTERACTIVE or BATCH
	ResourceKey     string
	SourceTables    []tableReference
	LoadSchema      []tableField
	LoadSourceURIs  []string
	LoadSourceFormat string
	LoadFieldDelimiter string
	LoadSkipLeadingRows int
	ExtractSourceTable      tableReference
	ExtractDestinationURIs  []string
	ExtractDestinationFormat string
	ExtractFieldDelimiter   string
	ExtractPrintHeader      bool
	TargetDataset   string
	TargetTable     string
	CreateDisposition string
	WriteDisposition  string
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
	Errors          []jobError // Secondary errors
}

type jobError struct {
	Reason   string `json:"reason"`
	Message  string `json:"message"`
	Location string `json:"location,omitempty"`
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
	Priority      string
	SourceTables   []tableReference
	LoadSchema      []tableField
	LoadSourceURIs  []string
	LoadSourceFormat string
	LoadFieldDelimiter string
	LoadSkipLeadingRows int
	ExtractSourceTable      tableReference
	ExtractDestinationURIs  []string
	ExtractDestinationFormat string
	ExtractFieldDelimiter   string
	ExtractPrintHeader      bool
	CreateDisposition string
	WriteDisposition  string
	TargetDataset string
	TargetTable   string
	IsScript      bool
}

type jobListFilters struct {
	StateFilter string
	UserEmail   string
	AllUsers    bool
	ParentJobID string
	MinCreated  time.Time
	MaxCreated  time.Time
}

type jobService struct {
	mu                sync.RWMutex
	jobsByProject     map[string]map[string]*jobRecord
	requestIDIndex    map[string]map[string]requestIDRecord
	projectVersions   map[string]int
	requestIDTTL      time.Duration
	maxConcurrent     int
	runSlots          chan struct{}
	maxStorageWrite   int
	storageWriteSlots chan struct{}
	resourceSlots     map[string]chan struct{}
	persistencePath   string
	copyExecutor      func(*jobRecord) (jobStatistics, error)
	loadExecutor      func(*jobRecord) (jobStatistics, error)
	extractExecutor   func(*jobRecord) (jobStatistics, error)
	queryExecutor     func(*jobRecord) (jobStatistics, error)
	counter           int64
}

type jobServiceSnapshot struct {
	Counter         int64                                 `json:"counter"`
	JobsByProject   map[string]map[string]*jobRecord      `json:"jobs_by_project"`
	RequestIDIndex  map[string]map[string]requestIDRecord `json:"request_id_index"`
	ProjectVersions map[string]int                        `json:"project_versions"`
}

func newJobService() *jobService {
	return newJobServiceWithWorkerLimit(readDefaultJobWorkerLimit())
}

func newJobServiceWithWorkerLimit(limit int) *jobService {
	s := &jobService{
		jobsByProject:   make(map[string]map[string]*jobRecord),
		requestIDIndex:  make(map[string]map[string]requestIDRecord),
		projectVersions: make(map[string]int),
		resourceSlots:   make(map[string]chan struct{}),
		requestIDTTL:    15 * time.Minute,
	}
	storageLimit := readDefaultStorageWriteWorkerLimit()
	if storageLimit > 0 {
		s.maxStorageWrite = storageLimit
		s.storageWriteSlots = make(chan struct{}, storageLimit)
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

func readDefaultStorageWriteWorkerLimit() int {
	raw := strings.TrimSpace(os.Getenv("LOCAQL_STORAGE_WRITE_WORKERS"))
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
		Priority:    normalizePriority(opts.Priority),
		ResourceKey: buildResourceKey(opts),
		SourceTables: cloneTableReferences(opts.SourceTables),
		LoadSchema:   cloneTableFields(opts.LoadSchema),
		LoadSourceURIs: cloneStringSlice(opts.LoadSourceURIs),
		LoadSourceFormat: strings.TrimSpace(opts.LoadSourceFormat),
		LoadFieldDelimiter: opts.LoadFieldDelimiter,
		LoadSkipLeadingRows: opts.LoadSkipLeadingRows,
		ExtractSourceTable: opts.ExtractSourceTable,
		ExtractDestinationURIs: cloneStringSlice(opts.ExtractDestinationURIs),
		ExtractDestinationFormat: strings.TrimSpace(opts.ExtractDestinationFormat),
		ExtractFieldDelimiter: opts.ExtractFieldDelimiter,
		ExtractPrintHeader: opts.ExtractPrintHeader,
		TargetDataset: strings.TrimSpace(opts.TargetDataset),
		TargetTable: strings.TrimSpace(opts.TargetTable),
		CreateDisposition: normalizeCreateDisposition(opts.CreateDisposition),
		WriteDisposition: normalizeWriteDisposition(opts.WriteDisposition),
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
	s.mu.RLock()
	jrForPriority := s.jobsByProject[projectID][jobID]
	priority := ""
	if jrForPriority != nil {
		priority = jrForPriority.Priority
	}
	s.mu.RUnlock()

	// Batch jobs wait a bit longer to simulate lower priority
	if priority == "BATCH" {
		time.Sleep(200 * time.Millisecond)
	}

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
	jobType := jr.JobType
	if jr.CancelRequested {
		jr.State = jobStateDone
		jr.ErrorReason = "stopped"
		jr.ErrorMessage = "job cancelled before execution"
		jr.EndedAt = time.Now().UTC()
		_ = s.persistLocked()
		s.mu.Unlock()
		return
	}
	s.mu.Unlock()

	releaseStorageWrite := s.acquireStorageWriteSlot(jobType)
	defer releaseStorageWrite()

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
		_ = s.persistLocked()
		s.mu.Unlock()
		return
	}
	jr.State = jobStateRunning
	jr.StartedAt = time.Now().UTC()
	s.projectVersions[projectID]++
	_ = s.persistLocked()
	s.mu.Unlock()

	d := executorDuration(jr.JobType)
	time.Sleep(d)

	outcome, missing := s.runJobExecutors(projectID, jobID, jobType)
	if missing {
		return
	}

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
		s.projectVersions[projectID]++
		_ = s.persistLocked()
		return
	}
	switch {
	case outcome.err != nil:
		jr.ErrorReason = "invalid"
		jr.ErrorMessage = outcome.err.Error()
		jr.Errors = []jobError{{Reason: "invalid", Message: outcome.err.Error(), Location: outcome.location}}
	case outcome.executed:
		jr.Statistics = outcome.stats
	default:
		applyExecutorResult(jr)
	}
	jr.State = jobStateDone
	jr.EndedAt = time.Now().UTC()
	s.projectVersions[projectID]++
	_ = s.persistLocked()
}

type jobExecutorOutcome struct {
	stats    jobStatistics
	err      error
	location string
	executed bool
}

type jobExecutorSpec struct {
	jobType  string
	location string
	execute  func(*jobRecord) (jobStatistics, error)
	prepare  func(*jobRecord)
}

// runJobExecutors runs whichever real executor (copy/load/extract) matches
// jobType, if one is wired. missing is true if the job record disappeared
// mid-flight (e.g. deleted concurrently), in which case the caller must abort.
func (s *jobService) runJobExecutors(projectID, jobID, jobType string) (jobExecutorOutcome, bool) {
	specs := []jobExecutorSpec{
		{jobType: "copy", location: "configuration.copy", execute: s.copyExecutor, prepare: func(snap *jobRecord) {
			snap.SourceTables = cloneTableReferences(snap.SourceTables)
		}},
		{jobType: "load", location: "configuration.load", execute: s.loadExecutor, prepare: func(snap *jobRecord) {
			snap.LoadSchema = cloneTableFields(snap.LoadSchema)
			snap.LoadSourceURIs = cloneStringSlice(snap.LoadSourceURIs)
		}},
		{jobType: "extract", location: "configuration.extract", execute: s.extractExecutor, prepare: func(snap *jobRecord) {
			snap.ExtractDestinationURIs = cloneStringSlice(snap.ExtractDestinationURIs)
		}},
		{jobType: "query", location: "configuration.query", execute: s.queryExecutor, prepare: nil},
	}

	var outcome jobExecutorOutcome
	for _, spec := range specs {
		stats, err, missing := s.runExecutor(projectID, jobID, jobType, spec.jobType, spec.execute, spec.prepare)
		if missing {
			return jobExecutorOutcome{}, true
		}
		if jobType == spec.jobType && spec.execute != nil {
			outcome = jobExecutorOutcome{stats: stats, err: err, location: spec.location, executed: true}
		}
	}
	return outcome, false
}

// runExecutor runs executor against a fresh snapshot of the job record when
// jobType matches wantType. missing is true if the job record disappeared
// mid-flight, signalling the caller should abort immediately.
func (s *jobService) runExecutor(projectID, jobID, jobType, wantType string, executor func(*jobRecord) (jobStatistics, error), prepare func(*jobRecord)) (jobStatistics, error, bool) {
	if jobType != wantType || executor == nil {
		return jobStatistics{}, nil, false
	}
	s.mu.Lock()
	jr := s.jobsByProject[projectID][jobID]
	if jr == nil {
		s.mu.Unlock()
		return jobStatistics{}, nil, true
	}
	snapshot := *jr
	if prepare != nil {
		prepare(&snapshot)
	}
	s.mu.Unlock()
	stats, err := executor(&snapshot)
	return stats, err, false
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
	s.projectVersions[projectID]++
	_ = s.persistLocked()
	cp := *jr
	return &cp, true
}

func (s *jobService) list(projectID string, filters jobListFilters, start, size int) ([]*jobRecord, string, int) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	version := s.projectVersions[projectID]
	proj := s.jobsByProject[projectID]
	if proj == nil {
		return []*jobRecord{}, "", version
	}

	all := make([]*jobRecord, 0, len(proj))
	for _, jr := range proj {
		all = append(all, jr)
	}

	// Sort by CreatedAt DESC (newest first)
	sort.Slice(all, func(i, j int) bool {
		if all[i].CreatedAt.Equal(all[j].CreatedAt) {
			return all[i].JobID > all[j].JobID
		}
		return all[i].CreatedAt.After(all[j].CreatedAt)
	})

	filtered := make([]*jobRecord, 0, len(all))
	for _, jr := range all {
		if filters.StateFilter != "" && string(jr.State) != filters.StateFilter {
			continue
		}
		if !filters.AllUsers && filters.UserEmail != "" && jr.UserEmail != filters.UserEmail {
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

	return filtered[start:end], next, version
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

func (s *jobService) acquireStorageWriteSlot(jobType string) func() {
	if s.storageWriteSlots == nil {
		return func() {}
	}
	if !requiresStorageWriteBackpressure(jobType) {
		return func() {}
	}
	s.storageWriteSlots <- struct{}{}
	return func() {
		<-s.storageWriteSlots
	}
}

func requiresStorageWriteBackpressure(jobType string) bool {
	switch strings.ToLower(strings.TrimSpace(jobType)) {
	case "load", "copy":
		return true
	default:
		return false
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
	if len(j.Errors) > 0 {
		status["errors"] = j.Errors
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

	res := map[string]any{
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

	// For query jobs, include priority if set
	if j.JobType == "query" || j.JobType == "script" {
		res["configuration"] = map[string]any{
			"query": map[string]any{
				"priority": j.Priority,
			},
		}
	}

	return res
}

func normalizePriority(p string) string {
	p = strings.ToUpper(strings.TrimSpace(p))
	if p == "BATCH" {
		return "BATCH"
	}
	return "INTERACTIVE"
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

func cloneTableReferences(refs []tableReference) []tableReference {
	if len(refs) == 0 {
		return nil
	}
	out := make([]tableReference, len(refs))
	copy(out, refs)
	return out
}

func cloneStringSlice(values []string) []string {
	if len(values) == 0 {
		return nil
	}
	out := make([]string, len(values))
	copy(out, values)
	return out
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
	if strings.Contains(strings.ToUpper(j.QueryText), "FORCE_ERROR") {
		j.ErrorReason = "invalid"
		j.ErrorMessage = "Simulated forced error from query text"
		j.Errors = []jobError{
			{Reason: "invalid", Message: "Simulated forced error from query text", Location: "query"},
			{Reason: "secondary", Message: "Additional error detail", Location: "execution"},
		}
		return
	}
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

func (s *jobService) persistLocked() error {
	if strings.TrimSpace(s.persistencePath) == "" {
		return nil
	}
	snap := jobServiceSnapshot{
		Counter:        s.counter,
		JobsByProject:  s.jobsByProject,
		RequestIDIndex: s.requestIDIndex,
	}
	content, err := json.MarshalIndent(snap, "", "  ")
	if err != nil {
		return err
	}
	dir := filepath.Dir(s.persistencePath)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	tmpPath := s.persistencePath + ".tmp"
	if err := os.WriteFile(tmpPath, content, 0o644); err != nil {
		return err
	}
	if err := os.Rename(tmpPath, s.persistencePath); err == nil {
		return nil
	}

	// Windows does not always allow replacing an existing file with Rename.
	if removeErr := os.Remove(s.persistencePath); removeErr != nil && !os.IsNotExist(removeErr) {
		_ = os.Remove(tmpPath)
		return removeErr
	}
	if err := os.Rename(tmpPath, s.persistencePath); err != nil {
		_ = os.Remove(tmpPath)
		return err
	}
	return nil
}
