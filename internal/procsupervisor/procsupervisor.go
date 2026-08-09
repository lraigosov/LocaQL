// Package procsupervisor provides a small, dependency-free "restart a
// child process on crash, bounded by a sliding window" primitive.
//
// It exists for one reason: the embedded query engine's WASM bridge
// (goccy/go-googlesql -> goccy/googlesqlite) has a known, upstream,
// third-party failure mode where a long-running process can eventually
// crash under sustained query load (see KNOWN-DIVERGENCES.md Blocking #3
// and docs/benchmarks.md) — not fixable from this project's own code.
// Since the crash is a process-level event, not a per-request one, the
// pragmatic mitigation is the same one long-running worker processes have
// used for this exact class of problem for decades (Unicorn/Puma worker
// recycling, PHP-FPM's max_requests): treat an occasional crash as routine
// and recycle the process automatically. The bounded window still turns a
// real, unrelated, rapid crash-loop (bad config, corrupted capabilities
// file) into a fatal exit rather than spinning forever.
//
// Two callers share this package: cmd/locaql-supervisor (which supervises
// the locaql and locaql-ui binaries as two separate child processes) and
// cmd/locaql's own --self-restart flag (which supervises a re-exec'd copy
// of itself, for deployments that run the bare binary directly).
package procsupervisor

import (
	"context"
	"log"
	"os"
	"os/exec"
	"syscall"
	"time"
)

// RestartPolicy bounds automatic restarts to a sliding window.
type RestartPolicy struct {
	MaxRestarts int
	Window      time.Duration
	Backoff     time.Duration
}

// Supervise runs one child process (binaryPath with args, plus extraEnv
// appended to the current environment), restarting it automatically (with
// a short backoff) if it exits on its own while ctx is still active, up to
// policy.MaxRestarts within policy.Window. It returns nil only when ctx is
// canceled (a requested shutdown); it returns an error if the child exits
// cleanly on its own with no request to shut down (should not happen in
// practice, since the supervised binaries run until killed), or if the
// crash rate exceeds the configured bound — at that point this is treated
// as a real problem rather than the known, occasional, upstream crash this
// loop exists to absorb, and the caller should still fail loudly rather
// than spin forever.
func Supervise(ctx context.Context, binaryPath string, args []string, extraEnv []string, policy RestartPolicy) error {
	var recentCrashes []time.Time
	for {
		cmd := ChildCommand(ctx, binaryPath, args...)
		if len(extraEnv) > 0 {
			cmd.Env = append(os.Environ(), extraEnv...)
		}
		if err := cmd.Start(); err != nil {
			return err
		}
		runErr := cmd.Wait()

		if ctx.Err() != nil {
			return nil // requested shutdown; runErr is just SIGTERM's own exit status
		}
		if runErr == nil {
			return nil // clean exit with no shutdown requested — nothing to restart
		}

		now := time.Now()
		recentCrashes = PruneOlderThan(append(recentCrashes, now), now.Add(-policy.Window))
		log.Printf("%s exited unexpectedly (%v); restart %d/%d within the last %s", binaryPath, runErr, len(recentCrashes), policy.MaxRestarts, policy.Window)
		if len(recentCrashes) > policy.MaxRestarts {
			return runErr
		}

		select {
		case <-ctx.Done():
			return nil
		case <-time.After(policy.Backoff):
		}
	}
}

// PruneOlderThan drops entries at or before cutoff, keeping the slice
// sorted-ascending order of the survivors.
func PruneOlderThan(times []time.Time, cutoff time.Time) []time.Time {
	out := times[:0]
	for _, t := range times {
		if t.After(cutoff) {
			out = append(out, t)
		}
	}
	return out
}

// ChildCommand builds an *exec.Cmd wired for graceful shutdown: canceling
// ctx sends a real SIGTERM (rather than exec.CommandContext's default
// immediate SIGKILL), so a well-behaved child gets its normal shutdown
// path, with a bounded grace period before Go force-kills it anyway.
func ChildCommand(ctx context.Context, name string, args ...string) *exec.Cmd {
	cmd := exec.CommandContext(ctx, name, args...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	cmd.Cancel = func() error { return cmd.Process.Signal(syscall.SIGTERM) }
	cmd.WaitDelay = 5 * time.Second
	return cmd
}
