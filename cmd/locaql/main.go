package main

import (
	"errors"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"strings"

	"github.com/lraigosov/LocaQL/internal/capabilities"
	"github.com/lraigosov/LocaQL/internal/conformance"
	"github.com/lraigosov/LocaQL/internal/server"
)

func main() {
	if len(os.Args) < 2 {
		printUsage()
		os.Exit(2)
	}

	switch os.Args[1] {
	case "start":
		if err := runStart(os.Args[2:]); err != nil {
			log.Fatal(err)
		}
	case "capabilities":
		if err := runCapabilities(os.Args[2:]); err != nil {
			log.Fatal(err)
		}
	case "conformance":
		if err := runConformance(os.Args[2:]); err != nil {
			log.Fatal(err)
		}
	default:
		printUsage()
		os.Exit(2)
	}
}

func runStart(args []string) error {
	fs := flag.NewFlagSet("start", flag.ContinueOnError)
	addr := fs.String("addr", ":9050", "http address")
	capPath := fs.String("capabilities", "capabilities/registry.yaml", "capabilities registry path")
	if err := fs.Parse(args); err != nil {
		return err
	}

	reg, err := capabilities.Load(*capPath)
	if err != nil {
		return err
	}

	srv := server.New(reg)
	log.Printf("LocaQL listening on %s", *addr)
	return http.ListenAndServe(*addr, srv.Handler())
}

func runCapabilities(args []string) error {
	fs := flag.NewFlagSet("capabilities", flag.ContinueOnError)
	capPath := fs.String("capabilities", "capabilities/registry.yaml", "capabilities registry path")
	if err := fs.Parse(args); err != nil {
		return err
	}

	reg, err := capabilities.Load(*capPath)
	if err != nil {
		return err
	}

	fmt.Println("capability,status,fidelity")
	for _, k := range reg.SortedKeys() {
		entry := reg.Capabilities[k]
		fmt.Printf("%s,%s,%s\n", k, entry.Status, entry.Fidelity)
	}
	return nil
}

func runConformance(args []string) error {
	fs := flag.NewFlagSet("conformance", flag.ContinueOnError)
	baseURL := fs.String("base-url", "http://localhost:9050", "target base URL")
	casesPath := fs.String("cases", "test/conformance/cases/foundation.yaml", "cases file")
	reportJSON := fs.String("report-json", "test/conformance/reports/foundation-report.json", "report json path")
	reportMD := fs.String("report-md", "test/conformance/reports/foundation-report.md", "report markdown path")
	if err := fs.Parse(args); err != nil {
		return err
	}

	if strings.TrimSpace(*baseURL) == "" {
		return errors.New("base-url is required")
	}

	runner := conformance.Runner{BaseURL: *baseURL}
	result, err := runner.Run(*casesPath)
	if err != nil {
		return err
	}
	if err := result.WriteJSON(*reportJSON); err != nil {
		return err
	}
	if err := result.WriteMarkdown(*reportMD); err != nil {
		return err
	}

	fmt.Printf("Conformance completed: %d passed, %d failed\n", result.Passed, result.Failed)
	fmt.Printf("JSON report: %s\n", *reportJSON)
	fmt.Printf("MD report: %s\n", *reportMD)
	if result.Failed > 0 {
		return errors.New("conformance failures detected")
	}
	return nil
}

func printUsage() {
	fmt.Println("LocaQL CLI")
	fmt.Println("Usage:")
	fmt.Println("  locaql start [--addr :9050] [--capabilities capabilities/registry.yaml]")
	fmt.Println("  locaql capabilities [--capabilities capabilities/registry.yaml]")
	fmt.Println("  locaql conformance [--base-url http://localhost:9050] [--cases test/conformance/cases/foundation.yaml]")
}
