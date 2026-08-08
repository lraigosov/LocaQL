// locaql-supervisor is the container entrypoint for the combined image: it
// starts the emulator (locaql) and the console (locaql-ui) as two real
// subprocesses of one container, so a single `docker run` gets the full
// solution instead of the emulator alone. It deliberately doesn't use a
// shell (the final image is distroless/static, which has none) — both
// children are exec'd directly by absolute path.
//
// The emulator specifically gets automatic restart-on-crash via
// internal/procsupervisor — see that package's doc comment for why.
package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/lraigosov/LocaQL/internal/procsupervisor"
)

type config struct {
	addr           string
	storageAddr    string
	capabilities   string
	uiAddr         string
	maxRestarts    int
	restartWindow  time.Duration
	restartBackoff time.Duration
}

func parseConfig(args []string) (config, error) {
	fs := flag.NewFlagSet("locaql-supervisor", flag.ContinueOnError)
	addr := fs.String("addr", ":9050", "emulator REST/gRPC-JSON address")
	storageAddr := fs.String("storage-grpc-addr", ":9060", "Storage API gRPC address")
	capabilities := fs.String("capabilities", "/etc/locaql/capabilities/registry.yaml", "capabilities registry path")
	uiAddr := fs.String("ui-addr", ":9070", "console UI address")
	maxRestarts := fs.Int("emulator-max-restarts", 20, "give up and exit if the emulator crashes this many times within --emulator-restart-window, instead of restarting it again")
	restartWindow := fs.Duration("emulator-restart-window", 10*time.Minute, "sliding window --emulator-max-restarts is measured over")
	restartBackoff := fs.Duration("emulator-restart-backoff", 2*time.Second, "delay before restarting a crashed emulator process, so a client mid-retry has time to find the new process listening again")
	if err := fs.Parse(args); err != nil {
		return config{}, err
	}
	return config{
		addr: *addr, storageAddr: *storageAddr, capabilities: *capabilities, uiAddr: *uiAddr,
		maxRestarts: *maxRestarts, restartWindow: *restartWindow, restartBackoff: *restartBackoff,
	}, nil
}

func (c config) emulatorArgs() []string {
	return []string{"start", "--addr", c.addr, "--storage-grpc-addr", c.storageAddr, "--capabilities", c.capabilities}
}

func (c config) uiArgs() []string {
	return []string{"--addr", c.uiAddr, "--emulator", "http://localhost" + c.addr}
}

func main() {
	cfg, err := parseConfig(os.Args[1:])
	if err != nil {
		log.Fatal(err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	ui := procsupervisor.ChildCommand(ctx, "/usr/local/bin/locaql-ui", cfg.uiArgs()...)
	if err := ui.Start(); err != nil {
		log.Fatalf("starting locaql-ui: %v", err)
	}

	type exit struct {
		name string
		err  error
	}
	done := make(chan exit, 2)
	go func() { done <- exit{"locaql-ui", ui.Wait()} }()
	go func() {
		policy := procsupervisor.RestartPolicy{MaxRestarts: cfg.maxRestarts, Window: cfg.restartWindow, Backoff: cfg.restartBackoff}
		done <- exit{"locaql", procsupervisor.Supervise(ctx, "/usr/local/bin/locaql", cfg.emulatorArgs(), nil, policy)}
	}()

	// Whichever side exits first, bring the other down with it — a
	// container should never look "up" while one of its two processes is
	// silently dead. ctx.Err() distinguishes a requested shutdown (signal
	// already canceled ctx before either side exited) from a real,
	// unrecovered failure — for the emulator, that specifically means the
	// restart loop itself gave up (see procsupervisor.Supervise), not just
	// one individual crash.
	first := <-done
	requested := ctx.Err() != nil
	stop()
	second := <-done

	if requested {
		log.Printf("shutting down (%s exited first: %v)", first.name, first.err)
		return
	}
	log.Fatalf("%s exited unexpectedly (%v); stopped %s (%v)", first.name, first.err, second.name, second.err)
}
