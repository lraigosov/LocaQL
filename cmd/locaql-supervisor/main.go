// locaql-supervisor is the container entrypoint for the combined image: it
// starts the emulator (locaql) and the console (locaql-ui) as two real
// subprocesses of one container, so a single `docker run` gets the full
// solution instead of the emulator alone. It deliberately doesn't use a
// shell (the final image is distroless/static, which has none) — both
// children are exec'd directly by absolute path.
package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/exec"
	"os/signal"
	"syscall"
	"time"
)

type config struct {
	addr         string
	storageAddr  string
	capabilities string
	uiAddr       string
}

func parseConfig(args []string) (config, error) {
	fs := flag.NewFlagSet("locaql-supervisor", flag.ContinueOnError)
	addr := fs.String("addr", ":9050", "emulator REST/gRPC-JSON address")
	storageAddr := fs.String("storage-grpc-addr", ":9060", "Storage API gRPC address")
	capabilities := fs.String("capabilities", "/etc/locaql/capabilities/registry.yaml", "capabilities registry path")
	uiAddr := fs.String("ui-addr", ":9070", "console UI address")
	if err := fs.Parse(args); err != nil {
		return config{}, err
	}
	return config{addr: *addr, storageAddr: *storageAddr, capabilities: *capabilities, uiAddr: *uiAddr}, nil
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

	emulator := childCommand(ctx, "/usr/local/bin/locaql", cfg.emulatorArgs()...)
	ui := childCommand(ctx, "/usr/local/bin/locaql-ui", cfg.uiArgs()...)

	if err := emulator.Start(); err != nil {
		log.Fatalf("starting locaql: %v", err)
	}
	if err := ui.Start(); err != nil {
		log.Fatalf("starting locaql-ui: %v", err)
	}

	type exit struct {
		name string
		err  error
	}
	done := make(chan exit, 2)
	go func() { done <- exit{"locaql", emulator.Wait()} }()
	go func() { done <- exit{"locaql-ui", ui.Wait()} }()

	// Whichever process exits first, bring the other down with it — a
	// container should never look "up" while one of its two processes is
	// silently dead. ctx.Err() distinguishes a requested shutdown (signal
	// already canceled ctx before either child exited) from a real crash.
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

func childCommand(ctx context.Context, name string, args ...string) *exec.Cmd {
	cmd := exec.CommandContext(ctx, name, args...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	// Default CommandContext cancellation is an immediate SIGKILL; send a
	// real SIGTERM instead so each server gets its normal graceful shutdown,
	// with a bounded grace period before Go force-kills it anyway.
	cmd.Cancel = func() error { return cmd.Process.Signal(syscall.SIGTERM) }
	cmd.WaitDelay = 5 * time.Second
	return cmd
}
