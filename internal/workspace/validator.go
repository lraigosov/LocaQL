package workspace

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

// Rule defines validation behavior for one workspace path.
type Rule struct {
	Path      string
	Kind      string
	Required  bool
	Reference string
}

// Result contains a validation report for a workspace.
type Result struct {
	Root              string
	Found             []string
	MissingRequired   []string
	MissingRecommended []string
}

// Rules are based on the portable workspace structure in the local support plan.
var Rules = []Rule{
	{Path: "manifest.yaml", Kind: "file", Required: true, Reference: "workspace-manifest"},
	{Path: "datasets", Kind: "dir", Required: true, Reference: "portable-artifacts"},
	{Path: "schemas", Kind: "dir", Required: true, Reference: "portable-artifacts"},
	{Path: "queries", Kind: "dir", Required: true, Reference: "portable-artifacts"},
	{Path: "profiles", Kind: "dir", Required: true, Reference: "execution-profiles"},
	{Path: "migrations", Kind: "dir", Required: false, Reference: "portable-artifacts"},
	{Path: "views", Kind: "dir", Required: false, Reference: "portable-artifacts"},
	{Path: "routines", Kind: "dir", Required: false, Reference: "portable-artifacts"},
	{Path: "pipelines", Kind: "dir", Required: false, Reference: "portable-artifacts"},
	{Path: "seed", Kind: "dir", Required: false, Reference: "portable-artifacts"},
	{Path: "tests", Kind: "dir", Required: false, Reference: "portable-artifacts"},
}

// Validate checks whether a workspace root follows the expected structure.
func Validate(root string) (Result, error) {
	clean := strings.TrimSpace(root)
	if clean == "" {
		return Result{}, fmt.Errorf("workspace path is required")
	}
	abs, err := filepath.Abs(clean)
	if err != nil {
		return Result{}, err
	}

	info, err := os.Stat(abs)
	if err != nil {
		return Result{}, err
	}
	if !info.IsDir() {
		return Result{}, fmt.Errorf("workspace path is not a directory: %s", abs)
	}

	res := Result{Root: abs}
	for _, rule := range Rules {
		target := filepath.Join(abs, rule.Path)
		ok, kindOk := matchKind(target, rule.Kind)
		if ok && kindOk {
			res.Found = append(res.Found, rule.Path)
			continue
		}
		if rule.Required {
			res.MissingRequired = append(res.MissingRequired, rule.Path)
		} else {
			res.MissingRecommended = append(res.MissingRecommended, rule.Path)
		}
	}

	sort.Strings(res.Found)
	sort.Strings(res.MissingRequired)
	sort.Strings(res.MissingRecommended)
	return res, nil
}

func matchKind(path, expectedKind string) (bool, bool) {
	info, err := os.Stat(path)
	if err != nil {
		return false, false
	}
	if expectedKind == "dir" {
		return true, info.IsDir()
	}
	if expectedKind == "file" {
		return true, !info.IsDir()
	}
	return true, false
}

// IsValid indicates if all required paths are present.
func (r Result) IsValid() bool {
	return len(r.MissingRequired) == 0
}
