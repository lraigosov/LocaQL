package procsupervisor

import (
	"context"
	"os/exec"
	"testing"
	"time"
)

func TestPruneOlderThan(t *testing.T) {
	now := time.Now()
	in := []time.Time{now.Add(-2 * time.Hour), now.Add(-30 * time.Second), now.Add(-1 * time.Second)}
	got := PruneOlderThan(in, now.Add(-time.Minute))
	if len(got) != 2 {
		t.Fatalf("expected 2 entries newer than the cutoff, got %d: %v", len(got), got)
	}
}

// TestSuperviseGivesUpPastMaxRestarts proves the crash-loop bound is real: a
// child that exits nonzero immediately, every time, must eventually make
// Supervise give up and return an error rather than restart forever — the
// difference between "absorbing the known, occasional WASM-bridge crash"
// (KNOWN-DIVERGENCES.md Blocking #3) and silently spinning on an unrelated,
// persistent problem (bad config, corrupted capabilities file).
func TestSuperviseGivesUpPastMaxRestarts(t *testing.T) {
	binary, err := exec.LookPath("false")
	if err != nil {
		t.Skip("no 'false' binary available on this system to simulate a crashing child")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	policy := RestartPolicy{MaxRestarts: 2, Window: time.Minute, Backoff: 10 * time.Millisecond}

	runErr := Supervise(ctx, binary, nil, nil, policy)
	if runErr == nil {
		t.Fatalf("expected Supervise to give up and return an error after exceeding MaxRestarts")
	}
	if ctx.Err() != nil {
		t.Fatalf("expected Supervise to give up well before the test's own context timeout")
	}
}

// TestSuperviseStopsOnRequestedShutdown proves a canceled context is treated
// as a normal shutdown, not a crash to restart from: without this, killing
// the supervisor would look identical to the crash-loop case above.
func TestSuperviseStopsOnRequestedShutdown(t *testing.T) {
	binary, err := exec.LookPath("sleep")
	if err != nil {
		t.Skip("no 'sleep' binary available on this system to simulate a long-running child")
	}
	ctx, cancel := context.WithCancel(context.Background())
	policy := RestartPolicy{MaxRestarts: 20, Window: time.Minute, Backoff: 10 * time.Millisecond}

	done := make(chan error, 1)
	go func() { done <- Supervise(ctx, binary, []string{"5"}, nil, policy) }()

	time.Sleep(100 * time.Millisecond)
	cancel()

	select {
	case runErr := <-done:
		if runErr != nil {
			t.Fatalf("expected a requested shutdown to return nil, got %v", runErr)
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("Supervise did not return promptly after ctx was canceled")
	}
}

// TestSuperviseSetsExtraEnvOnChild proves the extraEnv mechanism actually
// reaches the child process — cmd/locaql's --self-restart mode depends on
// this to mark the re-exec'd child so it doesn't try to supervise itself
// recursively.
func TestSuperviseSetsExtraEnvOnChild(t *testing.T) {
	binary, err := exec.LookPath("sh")
	if err != nil {
		t.Skip("no 'sh' binary available on this system")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	policy := RestartPolicy{MaxRestarts: 0, Window: time.Minute, Backoff: 10 * time.Millisecond}

	runErr := Supervise(ctx, binary, []string{"-c", `test "$PROC_SUPERVISOR_TEST_MARKER" = "1"`}, []string{"PROC_SUPERVISOR_TEST_MARKER=1"}, policy)
	if runErr != nil {
		t.Fatalf("expected the child to observe the extra env var, got exit error: %v", runErr)
	}
}
