package main

import (
	"encoding/json"
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
	"github.com/lraigosov/LocaQL/internal/workspace"
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
	case "workspace":
		if err := runWorkspace(os.Args[2:]); err != nil {
			log.Fatal(err)
		}
	default:
		printUsage()
		os.Exit(2)
	}
}

func runWorkspace(args []string) error {
	if len(args) == 0 {
		return errors.New("workspace subcommand is required (supported: validate, plan, diff, apply)")
	}

	switch args[0] {
	case "validate":
		return runWorkspaceValidate(args[1:])
	case "plan":
		return runWorkspacePlan(args[1:])
	case "diff":
		return runWorkspaceDiff(args[1:])
	case "apply":
		return runWorkspaceApply(args[1:])
	default:
		return fmt.Errorf("unsupported workspace subcommand: %s", args[0])
	}
}

func runWorkspacePlan(args []string) error {
	fs := flag.NewFlagSet("workspace plan", flag.ContinueOnError)
	path := fs.String("path", ".", "workspace root path")
	jsonOutput := fs.Bool("json", false, "print plan as json")
	if err := fs.Parse(args); err != nil {
		return err
	}

	plan, err := workspace.BuildPlan(*path)
	if err != nil {
		return err
	}

	if *jsonOutput {
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		return enc.Encode(plan)
	}

	fmt.Printf("Workspace: %s\n", plan.Validation.Root)
	fmt.Printf("Valid: %t\n", plan.Validation.IsValid())
	fmt.Printf("Files tracked: %d\n", len(plan.Files))
	fmt.Printf("Required missing: %d\n", len(plan.Validation.MissingRequired))
	if len(plan.Validation.MissingRequired) > 0 {
		fmt.Printf("Missing required: %s\n", strings.Join(plan.Validation.MissingRequired, ", "))
	}
	if len(plan.Validation.MissingRecommended) > 0 {
		fmt.Printf("Missing recommended: %s\n", strings.Join(plan.Validation.MissingRecommended, ", "))
	}
	if !plan.Validation.IsValid() {
		return errors.New("workspace planning failed: required structure is incomplete")
	}
	return nil
}

func runWorkspaceDiff(args []string) error {
	fs := flag.NewFlagSet("workspace diff", flag.ContinueOnError)
	source := fs.String("source", ".", "source workspace root")
	target := fs.String("target", "", "target workspace root")
	jsonOutput := fs.Bool("json", false, "print diff as json")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if strings.TrimSpace(*target) == "" {
		return errors.New("target is required")
	}

	diffRes, err := workspace.Diff(*source, *target)
	if err != nil {
		return err
	}

	if *jsonOutput {
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		return enc.Encode(diffRes)
	}

	fmt.Printf("Source: %s\n", diffRes.SourceRoot)
	fmt.Printf("Target: %s\n", diffRes.TargetRoot)
	fmt.Printf("Only in source: %d\n", len(diffRes.OnlyInSource))
	fmt.Printf("Only in target: %d\n", len(diffRes.OnlyInTarget))
	fmt.Printf("Changed: %d\n", len(diffRes.Changed))
	return nil
}

func runWorkspaceApply(args []string) error {
	fs := flag.NewFlagSet("workspace apply", flag.ContinueOnError)
	source := fs.String("source", ".", "source workspace root")
	target := fs.String("target", "", "target workspace root")
	dryRun := fs.Bool("dry-run", true, "show actions without mutating target")
	jsonOutput := fs.Bool("json", false, "print dry-run plan as json")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if strings.TrimSpace(*target) == "" {
		return errors.New("target is required")
	}
	if !*dryRun {
		return errors.New("non dry-run apply is not enabled yet; use --dry-run=true")
	}

	applyRes, err := workspace.BuildApplyDryRun(*source, *target)
	if err != nil {
		return err
	}

	if *jsonOutput {
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		return enc.Encode(applyRes)
	}

	fmt.Printf("Source: %s\n", applyRes.SourceRoot)
	fmt.Printf("Target: %s\n", applyRes.TargetRoot)
	fmt.Printf("Dry-run actions: %d\n", len(applyRes.Actions))
	for _, action := range applyRes.Actions {
		fmt.Printf("- %s %s\n", action.Action, action.Path)
	}
	return nil
}

func runWorkspaceValidate(args []string) error {
	fs := flag.NewFlagSet("workspace validate", flag.ContinueOnError)
	path := fs.String("path", ".", "workspace root path")
	jsonOutput := fs.Bool("json", false, "print validation report as json")
	if err := fs.Parse(args); err != nil {
		return err
	}

	res, err := workspace.Validate(*path)
	if err != nil {
		return err
	}

	if *jsonOutput {
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		if err := enc.Encode(res); err != nil {
			return err
		}
	} else {
		fmt.Printf("Workspace: %s\n", res.Root)
		fmt.Printf("Required missing: %d\n", len(res.MissingRequired))
		fmt.Printf("Recommended missing: %d\n", len(res.MissingRecommended))
		if len(res.MissingRequired) > 0 {
			fmt.Printf("Missing required: %s\n", strings.Join(res.MissingRequired, ", "))
		}
		if len(res.MissingRecommended) > 0 {
			fmt.Printf("Missing recommended: %s\n", strings.Join(res.MissingRecommended, ", "))
		}
	}

	if !res.IsValid() {
		return errors.New("workspace validation failed")
	}
	return nil
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
	fmt.Println("  locaql workspace validate [--path .] [--json]")
	fmt.Println("  locaql workspace plan [--path .] [--json]")
	fmt.Println("  locaql workspace diff [--source .] --target <path> [--json]")
	fmt.Println("  locaql workspace apply [--source .] --target <path> [--dry-run=true] [--json]")
}
