package server

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"runtime/debug"
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
	ProjectID                string
	JobID                    string
	ParentJobID              string
	JobType                  string
	Priority                 string // INTERACTIVE or BATCH
	ResourceKey              string
	SourceTables             []tableReference
	LoadSchema               []tableField
	LoadSourceURIs           []string
	LoadInlineData           []byte `json:"-"`
	LoadInlineName           string `json:"-"`
	LoadInline               bool   `json:"-"`
	LoadSourceFormat         string
	LoadFieldDelimiter       string
	LoadSkipLeadingRows      int
	LoadCompression          string
	LoadAutodetect           bool
	ExtractSourceTable       tableReference
	ExtractDestinationURIs   []string
	ExtractDestinationFormat string
	ExtractFieldDelimiter    string
	ExtractPrintHeader       bool
	ExtractCompression       string
	TargetDataset            string
	TargetTable              string
	CreateDisposition        string
	WriteDisposition         string
	State                    jobState
	RequestID                string
	UserEmail                string
	QueryText                string
	ParameterMode            string
	QueryParameters          []storedQueryParameter
	IsScript                 bool
	SessionID                string
	Statistics               jobStatistics
	CreatedAt                time.Time
	StartedAt                time.Time
	EndedAt                  time.Time
	CancelRequested          bool
	ErrorReason              string
	ErrorMessage             string
	Errors                   []jobError // Secondary errors
}

type jobError struct {
	Reason   string `json:"reason"`
	Message  string `json:"message"`
	Location string `json:"location,omitempty"`
}

type jobStatistics struct {
	Executor        string
	Simulated       bool
	TotalSlotMs     int64
	ProcessedBytes  int64
	OutputRows      int64
	StatementType   string
	DMLAffectedRows int64
	// ResultSchema/ResultRows cache a query job's actual result set, computed
	// once when the job runs (see executeQueryJob) rather than recomputed on
	// every subsequent getQueryResults/jobs.query fetch. This matters beyond
	// performance: some query text now has real, non-idempotent side effects
	// (BEGIN/COMMIT/ROLLBACK TRANSACTION, CREATE TEMP TABLE — see
	// session_service.go), so re-running it a second time to serve a second
	// fetch would re-apply those side effects (a second BEGIN would fail
	// with "already active", a second COMMIT would fail with "no active
	// transaction", etc.) instead of just returning the already-computed
	// result, unlike a plain SELECT which happened to be safe to re-run
	// only because it has no side effects at all.
	ResultSchema []tableField
	ResultRows   [][]string
}

type requestIDRecord struct {
	JobID     string
	CreatedAt time.Time
}

type jobInsertOptions struct {
	ProjectID                string
	RequestID                string
	ParentJobID              string
	UserEmail                string
	QueryText                string
	ParameterMode            string
	QueryParameters          []storedQueryParameter
	JobType                  string
	Priority                 string
	SourceTables             []tableReference
	LoadSchema               []tableField
	LoadSourceURIs           []string
	LoadInlineData           []byte
	LoadInlineName           string
	LoadInline               bool
	LoadSourceFormat         string
	LoadFieldDelimiter       string
	LoadSkipLeadingRows      int
	LoadCompression          string
	LoadAutodetect           bool
	ExtractSourceTable       tableReference
	ExtractDestinationURIs   []string
	ExtractDestinationFormat string
	ExtractFieldDelimiter    string
	ExtractPrintHeader       bool
	ExtractCompression       string
	CreateDisposition        string
	WriteDisposition         string
	TargetDataset            string
	TargetTable              string
	IsScript                 bool
	SessionID                string
	RequestedJobID           string
}

// validJobID matches real BigQuery's own jobId character rules: letters,
// numbers, underscores and hyphens. The 1024-character length cap is
// checked separately (isValidJobID) rather than in the pattern itself —
// Go's RE2-based regexp engine rejects a bounded repeat above 1000
// (`invalid repeat count`), so {1,1024} cannot be expressed directly here.
var validJobID = regexp.MustCompile(`^[A-Za-z0-9_-]+$`)

const maxJobIDLength = 1024

func isValidJobID(id string) bool {
	return len(id) <= maxJobIDLength && validJobID.MatchString(id)
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
	mu                 sync.RWMutex
	jobsByProject      map[string]map[string]*jobRecord
	requestIDIndex     map[string]map[string]requestIDRecord
	projectVersions    map[string]int
	requestIDTTL       time.Duration
	maxConcurrent      int
	runSlots           chan struct{}
	maxStorageWrite    int
	storageWriteSlots  chan struct{}
	resourceSlots      map[string]chan struct{}
	persistencePath    string
	copyExecutor       func(*jobRecord) (jobStatistics, error)
	loadExecutor       func(*jobRecord) (jobStatistics, error)
	extractExecutor    func(*jobRecord) (jobStatistics, error)
	queryExecutor      func(*jobRecord) (jobStatistics, error)
	counter            int64
	submittedTotal     int64
	completedTotal     int64
	failedTotal        int64
	lastPersistError   string
	lastPersistErrorAt time.Time
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

// jobIDConflictError reports that a client-supplied jobReference.jobId
// collides with a job that already exists, or fails BigQuery's own jobId
// character rules — distinct from requestId-based idempotent retry below,
// which returns the existing job successfully rather than an error.
// HTTPStatus/Reason let the REST handler render the same status/reason
// convention already used for every other "duplicate"/"invalid" error in
// this codebase, without the handler needing to parse the message text.
type jobIDConflictError struct {
	msg        string
	HTTPStatus int
	Reason     string
}

func (e *jobIDConflictError) Error() string { return e.msg }

// resolveJobIDLocked decides the new job's ID: a client-supplied
// jobReference.jobId is honored exactly if valid and unused (matching real
// BigQuery's own contract — official client libraries, the Node.js one
// among them, generate their own jobId client-side and poll
// getQueryResults/jobs.get using that exact value afterward, assuming the
// server used it; an emulator that silently substitutes its own id breaks
// that poll with a real "job not found"), otherwise one is auto-generated as
// before. Callers must already hold s.mu.
func (s *jobService) resolveJobIDLocked(projectID, requestedJobID string) (string, error) {
	requestedJobID = strings.TrimSpace(requestedJobID)
	if requestedJobID == "" {
		s.counter++
		return "job_" + strconv.FormatInt(s.counter, 10), nil
	}
	if !isValidJobID(requestedJobID) {
		return "", &jobIDConflictError{
			msg:        fmt.Sprintf("Invalid job ID %q: must be 1-%d characters matching %s", requestedJobID, maxJobIDLength, validJobID.String()),
			HTTPStatus: http.StatusBadRequest, Reason: "invalid",
		}
	}
	if _, exists := s.jobsByProject[projectID][requestedJobID]; exists {
		return "", &jobIDConflictError{
			msg:        fmt.Sprintf("Already Exists: Job %s:%s", projectID, requestedJobID),
			HTTPStatus: http.StatusConflict, Reason: "duplicate",
		}
	}
	return requestedJobID, nil
}

func (s *jobService) insert(opts jobInsertOptions) (*jobRecord, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if strings.TrimSpace(opts.TargetDataset) == "" && strings.TrimSpace(opts.TargetTable) == "" && !opts.IsScript {
		if stmt, handled, err := parsePersistentSQLStatement(opts.ProjectID, opts.QueryText); handled && err == nil {
			opts.TargetDataset = stmt.target.DatasetID
			opts.TargetTable = stmt.target.TableID
		}
	}

	projectID := opts.ProjectID
	requestID := opts.RequestID
	now := time.Now().UTC()
	s.cleanupExpiredRequestIDsLocked(now)

	if requestID != "" {
		if _, ok := s.requestIDIndex[projectID]; ok {
			if existingRef, exists := s.requestIDIndex[projectID][requestID]; exists {
				if existing := s.jobsByProject[projectID][existingRef.JobID]; existing != nil {
					cp := *existing
					return &cp, false, nil
				}
			}
		}
	}

	jobID, err := s.resolveJobIDLocked(projectID, opts.RequestedJobID)
	if err != nil {
		return nil, false, err
	}
	jr := &jobRecord{
		ProjectID:                projectID,
		JobID:                    jobID,
		ParentJobID:              opts.ParentJobID,
		JobType:                  normalizeJobType(opts),
		Priority:                 normalizePriority(opts.Priority),
		ResourceKey:              buildResourceKey(opts),
		SourceTables:             cloneTableReferences(opts.SourceTables),
		LoadSchema:               cloneTableFields(opts.LoadSchema),
		LoadSourceURIs:           cloneStringSlice(opts.LoadSourceURIs),
		LoadInlineData:           cloneBytes(opts.LoadInlineData),
		LoadInlineName:           strings.TrimSpace(opts.LoadInlineName),
		LoadInline:               opts.LoadInline,
		LoadSourceFormat:         strings.TrimSpace(opts.LoadSourceFormat),
		LoadFieldDelimiter:       opts.LoadFieldDelimiter,
		LoadSkipLeadingRows:      opts.LoadSkipLeadingRows,
		LoadCompression:          strings.TrimSpace(opts.LoadCompression),
		LoadAutodetect:           opts.LoadAutodetect,
		ExtractSourceTable:       opts.ExtractSourceTable,
		ExtractDestinationURIs:   cloneStringSlice(opts.ExtractDestinationURIs),
		ExtractDestinationFormat: strings.TrimSpace(opts.ExtractDestinationFormat),
		ExtractFieldDelimiter:    opts.ExtractFieldDelimiter,
		ExtractPrintHeader:       opts.ExtractPrintHeader,
		ExtractCompression:       strings.TrimSpace(opts.ExtractCompression),
		TargetDataset:            strings.TrimSpace(opts.TargetDataset),
		TargetTable:              strings.TrimSpace(opts.TargetTable),
		CreateDisposition:        normalizeCreateDisposition(opts.CreateDisposition),
		WriteDisposition:         normalizeWriteDisposition(opts.WriteDisposition),
		State:                    jobStatePending,
		RequestID:                requestID,
		UserEmail:                opts.UserEmail,
		QueryText:                opts.QueryText,
		ParameterMode:            opts.ParameterMode,
		QueryParameters:          cloneQueryParameters(opts.QueryParameters),
		IsScript:                 opts.IsScript,
		SessionID:                opts.SessionID,
		Statistics:               newSimulatedStatistics(normalizeJobType(opts)),
		CreatedAt:                now,
	}

	if _, ok := s.jobsByProject[projectID]; !ok {
		s.jobsByProject[projectID] = make(map[string]*jobRecord)
	}
	s.jobsByProject[projectID][jobID] = jr
	s.submittedTotal++

	if requestID != "" {
		if _, ok := s.requestIDIndex[projectID]; !ok {
			s.requestIDIndex[projectID] = make(map[string]requestIDRecord)
		}
		s.requestIDIndex[projectID][requestID] = requestIDRecord{JobID: jobID, CreatedAt: now}
	}

	s.persistLocked()

	go s.run(jobID, projectID)
	cp := *jr
	return &cp, true, nil
}

func (s *jobService) insertScriptWithChildren(opts jobInsertOptions) (*jobRecord, []*jobRecord, bool, error) {
	parent, created, err := s.insert(opts)
	if err != nil {
		return nil, nil, false, err
	}
	if !created {
		return parent, nil, false, nil
	}

	childJobs := make([]*jobRecord, 0)
	parts := splitScriptStatements(opts.QueryText)
	for range parts {
		// Child jobs never have their own client-supplied jobId — real
		// BigQuery script children are always server-assigned regardless of
		// the parent's jobReference.
		child, _, _ := s.insert(jobInsertOptions{
			ProjectID:   opts.ProjectID,
			ParentJobID: parent.JobID,
			UserEmail:   opts.UserEmail,
			QueryText:   opts.QueryText,
			JobType:     "query",
			IsScript:    false,
		})
		childJobs = append(childJobs, child)
	}

	return parent, childJobs, true, nil
}

// recordJobOutcomeLocked increments the completed/failed counters exposed by
// metricsSnapshot; callers must already hold s.mu (all four jobStateDone
// transition points in run() do).
func (s *jobService) recordJobOutcomeLocked(jr *jobRecord) {
	// Uploaded files are transient executor input. Keeping a copy on every
	// completed job would retain potentially large payloads for the lifetime of
	// the server even though job rendering and polling never need the bytes.
	jr.LoadInlineData = nil
	if jr.ErrorReason == "" {
		s.completedTotal++
	} else {
		s.failedTotal++
	}
}

// recoverJobPanic converts a panic anywhere in this job's synchronous
// execution chain into a normal failed-job outcome instead of letting it
// propagate and crash the entire server process — every other in-flight
// request would otherwise go down with it, since a goroutine panic with no
// recover is always fatal to the whole Go process, not just that goroutine.
// Discovered as a real, reproducible failure while building this project's
// own benchmark suite (see docs/benchmarks.md): the embedded engine's WASM
// bridge (goccy/googlesqlite -> goccy/go-googlesql) can panic with a
// memory-corruption-shaped "slice bounds out of range" error from
// database/sql.Open itself, after enough sequential/concurrent
// materializations over a long-running process's lifetime — and this
// reproduced on Linux/WSL, the officially supported platform, not just the
// already-documented native Windows/macOS WASM trap (KNOWN-DIVERGENCES.md
// Blocking #2, a distinct issue). This does not fix that root cause, which
// lives in the third-party WASM bridge, not this project's own code; it
// only contains the damage to the one job that triggered it. The failed job
// surfaces through the exact same failedTotal/recentFailures path any other
// job failure does (recordJobOutcomeLocked), so no new diagnostics plumbing
// was needed for it to show up in GET /_emulator/diagnostics.
func (s *jobService) recoverJobPanic(jobID, projectID string) {
	r := recover()
	if r == nil {
		return
	}
	log.Printf("job %s (project %s) panicked and was recovered at the job-executor boundary: %v\n%s", jobID, projectID, r, debug.Stack())

	s.mu.Lock()
	defer s.mu.Unlock()
	jr := s.jobsByProject[projectID][jobID]
	if jr == nil {
		return
	}
	jr.State = jobStateDone
	jr.ErrorReason = "internalError"
	jr.ErrorMessage = fmt.Sprintf("job execution panicked and was recovered at the process boundary: %v", r)
	jr.EndedAt = time.Now().UTC()
	s.recordJobOutcomeLocked(jr)
	_ = s.persistLocked()
}

func (s *jobService) run(jobID, projectID string) {
	defer s.recoverJobPanic(jobID, projectID)
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
		s.recordJobOutcomeLocked(jr)
		_ = s.persistLocked()
		s.mu.Unlock()
		return
	}
	s.mu.Unlock()

	releaseStorageWrite := s.acquireStorageWriteSlot(jobType, resourceKey)
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
		s.recordJobOutcomeLocked(jr)
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
		s.recordJobOutcomeLocked(jr)
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
	s.recordJobOutcomeLocked(jr)
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
			snap.LoadInlineData = cloneBytes(snap.LoadInlineData)
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
		jr.LoadInlineData = nil
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

func (s *jobService) acquireStorageWriteSlot(jobType, resourceKey string) func() {
	if s.storageWriteSlots == nil {
		return func() {}
	}
	if !requiresStorageWriteBackpressure(jobType, resourceKey) {
		return func() {}
	}
	s.storageWriteSlots <- struct{}{}
	return func() {
		<-s.storageWriteSlots
	}
}

func requiresStorageWriteBackpressure(jobType, resourceKey string) bool {
	switch strings.ToLower(strings.TrimSpace(jobType)) {
	case "load", "copy":
		return true
	case "query":
		return strings.TrimSpace(resourceKey) != ""
	default:
		return false
	}
}

func isDMLStatementType(statementType string) bool {
	switch statementType {
	case "INSERT", "UPDATE", "DELETE", "MERGE", "TRUNCATE_TABLE":
		return true
	default:
		return false
	}
}

// metricsSnapshot backs GET /_emulator/metrics: submitted/completed/failed
// job counters, plus a live occupancy/capacity gauge for each concurrency
// limiter already in the run() path (runSlots, storageWriteSlots,
// resourceSlots) — no new bookkeeping needed, since a buffered channel's
// len/cap already is that gauge. There is no retry mechanism anywhere in
// this codebase (a failed job is never automatically re-run), so no retry
// counter is reported here rather than fabricating one that would always
// read zero.
func (s *jobService) metricsSnapshot() map[string]any {
	s.mu.RLock()
	defer s.mu.RUnlock()

	resourceLocksHeld := 0
	for _, slot := range s.resourceSlots {
		resourceLocksHeld += len(slot)
	}

	return map[string]any{
		"submittedTotal":     s.submittedTotal,
		"completedTotal":     s.completedTotal,
		"failedTotal":        s.failedTotal,
		"runQueue":           channelGauge(s.runSlots),
		"storageWriteQueue":  channelGauge(s.storageWriteSlots),
		"resourceLocksHeld":  resourceLocksHeld,
		"resourceLocksTotal": len(s.resourceSlots),
	}
}

// channelGauge reports a buffered-channel semaphore's live occupancy: nil
// (the limiter is disabled, e.g. LOCAQL_JOB_WORKERS unset) reports
// unbounded rather than a misleading capacity of 0.
func channelGauge(ch chan struct{}) map[string]any {
	if ch == nil {
		return map[string]any{"depth": 0, "capacity": 0, "unbounded": true}
	}
	return map[string]any{"depth": len(ch), "capacity": cap(ch), "unbounded": false}
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

	// Real BigQuery renders every int64 statistics field as a JSON string,
	// not a bare number — a long-standing, deliberate convention of Google's
	// REST APIs to avoid precision loss in JavaScript's Number type, which
	// cannot safely represent the full int64 range. Official client
	// libraries built on generated REST stubs enforce this on the wire:
	// Python/Node's JSON parsing is lenient enough to accept a bare number
	// too, but the official Go client's generated structs use a `,string`
	// json tag that rejects an unquoted number outright ("invalid use of
	// ,string struct tag") — caught by test/clients/go/persistent_ddl_dml.go
	// the first time it ran against a real server, the same way the
	// Node.js client caught the jobReference.jobId gap above.
	stats := map[string]any{
		"totalSlotMs":    strconv.FormatInt(j.Statistics.TotalSlotMs, 10),
		"processedBytes": strconv.FormatInt(j.Statistics.ProcessedBytes, 10),
		"outputRows":     strconv.FormatInt(j.Statistics.OutputRows, 10),
		"simulation": map[string]any{
			"enabled":  j.Statistics.Simulated,
			"executor": j.Statistics.Executor,
		},
	}
	if j.SessionID != "" {
		stats["sessionInfo"] = map[string]string{"sessionId": j.SessionID}
	}
	// Real BigQuery nests job-type-specific stats (e.g. statistics.load.outputRows,
	// statistics.load.outputBytes) rather than exposing them flat under
	// statistics.*: the official client libraries read from that nested shape
	// (see google-cloud-bigquery's LoadJob.output_rows /
	// _job_statistics()->statistics[job_type]), so a load job's row/byte counts
	// were previously invisible to real clients despite being computed
	// correctly server-side. Added additively — the flat keys above stay for
	// whatever already depends on them. Only "load" is covered here since it's
	// the verified case; query/copy/extract likely have the same gap but
	// weren't part of this bug's repro.
	if j.JobType == "load" {
		stats["load"] = map[string]any{
			"outputRows":  strconv.FormatInt(j.Statistics.OutputRows, 10),
			"outputBytes": strconv.FormatInt(j.Statistics.ProcessedBytes, 10),
		}
	}
	if j.JobType == "query" && j.Statistics.StatementType != "" {
		queryStats := map[string]any{
			"statementType":       j.Statistics.StatementType,
			"totalBytesProcessed": strconv.FormatInt(j.Statistics.ProcessedBytes, 10),
		}
		if isDMLStatementType(j.Statistics.StatementType) {
			queryStats["numDmlAffectedRows"] = strconv.FormatInt(j.Statistics.DMLAffectedRows, 10)
			switch j.Statistics.StatementType {
			case "INSERT":
				queryStats["dmlStats"] = map[string]string{"insertedRowCount": strconv.FormatInt(j.Statistics.DMLAffectedRows, 10), "updatedRowCount": "0", "deletedRowCount": "0"}
			case "UPDATE":
				queryStats["dmlStats"] = map[string]string{"insertedRowCount": "0", "updatedRowCount": strconv.FormatInt(j.Statistics.DMLAffectedRows, 10), "deletedRowCount": "0"}
			case "DELETE":
				queryStats["dmlStats"] = map[string]string{"insertedRowCount": "0", "updatedRowCount": "0", "deletedRowCount": strconv.FormatInt(j.Statistics.DMLAffectedRows, 10)}
			}
		}
		stats["query"] = queryStats
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

	// For query jobs, echo the submitted query text back alongside priority.
	// Real BigQuery always includes configuration.query.query in a job
	// resource — omitting it isn't just incomplete, it broke the official
	// Java client outright: QueryJobConfiguration.fromPb (used to
	// reconstruct a Job from the REST response returned by jobs.insert)
	// requires it non-null and throws a bare NullPointerException via
	// Preconditions.checkNotNull otherwise, found the first time
	// test/clients/java/PersistentDdlDml.java ran against a real server.
	if j.JobType == "query" || j.JobType == "script" {
		res["configuration"] = map[string]any{
			"query": map[string]any{
				"query":    j.QueryText,
				"priority": j.Priority,
			},
		}
	}
	if j.JobType == "load" {
		loadConfig := map[string]any{
			"destinationTable": map[string]string{
				"projectId": j.ProjectID,
				"datasetId": j.TargetDataset,
				"tableId":   j.TargetTable,
			},
			"schema":            map[string]any{"fields": renderTableSchemaFields(j.LoadSchema)},
			"sourceFormat":      j.LoadSourceFormat,
			"createDisposition": j.CreateDisposition,
			"writeDisposition":  j.WriteDisposition,
			"fieldDelimiter":    j.LoadFieldDelimiter,
			"skipLeadingRows":   j.LoadSkipLeadingRows,
			"compression":       j.LoadCompression,
		}
		if len(j.LoadSourceURIs) > 0 {
			loadConfig["sourceUris"] = cloneStringSlice(j.LoadSourceURIs)
		}
		res["configuration"] = map[string]any{"load": loadConfig}
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

func cloneQueryParameters(params []storedQueryParameter) []storedQueryParameter {
	if len(params) == 0 {
		return nil
	}
	out := make([]storedQueryParameter, len(params))
	copy(out, params)
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

func cloneBytes(value []byte) []byte {
	if value == nil {
		return nil
	}
	out := make([]byte, len(value))
	copy(out, value)
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

// persistLocked snapshots job state to disk when persistence is configured,
// tracking the outcome in lastPersistError/lastPersistErrorAt for
// GET /_emulator/diagnostics — every existing call site already discards
// this function's returned error (persistence failures were, until Sesión
// 83, completely invisible: a full disk or a permissions problem would fail
// silently forever). This does not change that call-site behavior; it just
// makes the last failure observable rather than swallowed entirely.
func (s *jobService) persistLocked() error {
	if strings.TrimSpace(s.persistencePath) == "" {
		return nil
	}
	err := s.persistLockedInner()
	if err != nil {
		s.lastPersistError = err.Error()
		s.lastPersistErrorAt = time.Now().UTC()
	} else {
		s.lastPersistError = ""
		s.lastPersistErrorAt = time.Time{}
	}
	return err
}

func (s *jobService) persistLockedInner() error {
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

// jobFailureSummary is one entry in GET /_emulator/diagnostics'
// recentJobFailures — real failed jobs from the live catalog, not synthetic.
type jobFailureSummary struct {
	ProjectID    string    `json:"projectId"`
	JobID        string    `json:"jobId"`
	JobType      string    `json:"jobType"`
	ErrorReason  string    `json:"errorReason"`
	ErrorMessage string    `json:"errorMessage"`
	EndedAt      time.Time `json:"endedAt"`
}

// recentFailures returns up to limit failed jobs across every project,
// newest first — the "why did my load/query/copy job fail" troubleshooting
// view, without needing to already know the job ID to look it up.
func (s *jobService) recentFailures(limit int) []jobFailureSummary {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var out []jobFailureSummary
	for projectID, jobs := range s.jobsByProject {
		for _, jr := range jobs {
			if jr.ErrorReason == "" {
				continue
			}
			out = append(out, jobFailureSummary{
				ProjectID:    projectID,
				JobID:        jr.JobID,
				JobType:      jr.JobType,
				ErrorReason:  jr.ErrorReason,
				ErrorMessage: jr.ErrorMessage,
				EndedAt:      jr.EndedAt,
			})
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].EndedAt.After(out[j].EndedAt) })
	if len(out) > limit {
		out = out[:limit]
	}
	return out
}

// resourceLockKeysHeld reports exactly which resource keys (project:dataset.table)
// are currently locked, not just a count — the "guided" part of
// troubleshooting a mutation that appears to hang: it tells you which
// specific table is contended, not just that something is.
func (s *jobService) resourceLockKeysHeld() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var held []string
	for key, slot := range s.resourceSlots {
		if len(slot) > 0 {
			held = append(held, key)
		}
	}
	sort.Strings(held)
	return held
}

// persistenceStatus backs the "persistence" field of GET /_emulator/diagnostics.
func (s *jobService) persistenceStatus() map[string]any {
	s.mu.RLock()
	defer s.mu.RUnlock()
	status := map[string]any{
		"enabled": s.persistencePath != "",
		"path":    s.persistencePath,
	}
	if s.lastPersistError != "" {
		status["lastError"] = s.lastPersistError
		status["lastErrorAt"] = s.lastPersistErrorAt
	}
	return status
}
