package server

import (
	"testing"
	"time"
)

// TestJobExecutorPanicIsContainedNotFatal proves that a panic anywhere in a
// job's synchronous execution chain fails only that job instead of crashing
// the whole process — the real failure mode discovered while building this
// project's benchmark suite (see docs/benchmarks.md and the doc comment on
// jobService.recoverJobPanic). Without the fix, this test process itself
// would die (a goroutine panic with no recover is fatal to the whole Go
// process), so simply reaching the assertions below is already most of the
// proof; the assertions confirm the failure surfaces the normal way too.
func TestJobExecutorPanicIsContainedNotFatal(t *testing.T) {
	s := newTestServer()
	originalExecutor := s.jobs.queryExecutor
	defer func() { s.jobs.queryExecutor = originalExecutor }()
	s.jobs.queryExecutor = func(*jobRecord) (jobStatistics, error) {
		panic("simulated engine panic, matching the real slice-bounds-out-of-range crash")
	}

	jr, _ := s.jobs.insert(jobInsertOptions{ProjectID: "p1", JobType: "query", QueryText: "SELECT 1"})

	deadline := time.Now().Add(5 * time.Second)
	var final *jobRecord
	for time.Now().Before(deadline) {
		job, ok := s.jobs.get("p1", jr.JobID)
		if ok && job.State == jobStateDone {
			final = job
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if final == nil {
		t.Fatalf("job never reached DONE after a panicking executor")
	}
	if final.ErrorReason == "" {
		t.Fatalf("expected a recorded error after a panicking executor, got none: %+v", final)
	}

	failures := s.jobs.recentFailures(10)
	found := false
	for _, f := range failures {
		if f.JobID == jr.JobID {
			found = true
			if f.ErrorMessage == "" {
				t.Fatalf("expected a non-empty error message in recentFailures for the panicked job")
			}
		}
	}
	if !found {
		t.Fatalf("expected the panicked job to appear in recentFailures (diagnostics), got %+v", failures)
	}

	// The server must still be fully usable afterward — the whole point of
	// containing the panic to one job.
	s.jobs.queryExecutor = originalExecutor
	jr2, _ := s.jobs.insert(jobInsertOptions{ProjectID: "p1", JobType: "query", QueryText: "SELECT 1 AS one"})
	deadline = time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		job, ok := s.jobs.get("p1", jr2.JobID)
		if ok && job.State == jobStateDone {
			if job.ErrorReason != "" {
				t.Fatalf("expected the next job to succeed normally, got error: %s", job.ErrorMessage)
			}
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("server did not remain usable after recovering from a panicked job")
}

// TestRecoverJobPanicIsANoOpWithoutAPanic guards against a regression where
// recoverJobPanic itself might mistakenly mark a normally-completed job as
// failed.
func TestRecoverJobPanicIsANoOpWithoutAPanic(t *testing.T) {
	s := newTestServer()
	jr := &jobRecord{ProjectID: "p1", JobID: "job_no_panic", State: jobStateRunning}
	s.jobs.jobsByProject["p1"] = map[string]*jobRecord{jr.JobID: jr}

	func() {
		defer s.jobs.recoverJobPanic(jr.JobID, "p1")
	}()

	if jr.State != jobStateRunning || jr.ErrorReason != "" {
		t.Fatalf("expected recoverJobPanic to be a no-op absent a real panic, got %+v", jr)
	}
}
