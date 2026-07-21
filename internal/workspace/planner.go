package workspace

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

// FileEntry describes one workspace file included in planning and diff.
type FileEntry struct {
	Path   string
	Size   int64
	SHA256 string
}

// PlanResult captures what a workspace contains and whether required structure is valid.
type PlanResult struct {
	Validation Result
	Files      []FileEntry
}

// DiffResult captures differences between source and target workspace.
type DiffResult struct {
	SourceRoot   string
	TargetRoot   string
	OnlyInSource []FileEntry
	OnlyInTarget []FileEntry
	Changed      []FileChange
}

// FileChange contains old and new metadata for one changed file.
type FileChange struct {
	Path   string
	Source FileEntry
	Target FileEntry
}

// ApplyAction is one dry-run operation.
type ApplyAction struct {
	Action string
	Path   string
}

// ApplyDryRunResult returns what would be executed in apply mode.
type ApplyDryRunResult struct {
	SourceRoot string
	TargetRoot string
	Actions    []ApplyAction
}

// ApplyOptions controls mutable apply behavior.
type ApplyOptions struct {
	DeleteMissing bool
}

// ApplyResult reports actions planned and executed.
type ApplyResult struct {
	SourceRoot string
	TargetRoot string
	Applied    bool
	Actions    []ApplyAction
}

// BuildPlan validates the workspace and builds a deterministic file inventory.
func BuildPlan(root string) (PlanResult, error) {
	validation, err := Validate(root)
	if err != nil {
		return PlanResult{}, err
	}
	files, err := scanWorkspaceFiles(validation.Root)
	if err != nil {
		return PlanResult{}, err
	}
	return PlanResult{Validation: validation, Files: files}, nil
}

// Diff compares two workspace roots based on relative path and hash.
func Diff(sourceRoot, targetRoot string) (DiffResult, error) {
	sourcePlan, err := BuildPlan(sourceRoot)
	if err != nil {
		return DiffResult{}, err
	}
	targetPlan, err := BuildPlan(targetRoot)
	if err != nil {
		return DiffResult{}, err
	}

	sourceMap := make(map[string]FileEntry, len(sourcePlan.Files))
	targetMap := make(map[string]FileEntry, len(targetPlan.Files))
	for _, f := range sourcePlan.Files {
		sourceMap[f.Path] = f
	}
	for _, f := range targetPlan.Files {
		targetMap[f.Path] = f
	}

	out := DiffResult{SourceRoot: sourcePlan.Validation.Root, TargetRoot: targetPlan.Validation.Root}
	for p, s := range sourceMap {
		t, ok := targetMap[p]
		if !ok {
			out.OnlyInSource = append(out.OnlyInSource, s)
			continue
		}
		if s.SHA256 != t.SHA256 || s.Size != t.Size {
			out.Changed = append(out.Changed, FileChange{Path: p, Source: s, Target: t})
		}
	}
	for p, t := range targetMap {
		if _, ok := sourceMap[p]; !ok {
			out.OnlyInTarget = append(out.OnlyInTarget, t)
		}
	}

	sort.Slice(out.OnlyInSource, func(i, j int) bool { return out.OnlyInSource[i].Path < out.OnlyInSource[j].Path })
	sort.Slice(out.OnlyInTarget, func(i, j int) bool { return out.OnlyInTarget[i].Path < out.OnlyInTarget[j].Path })
	sort.Slice(out.Changed, func(i, j int) bool { return out.Changed[i].Path < out.Changed[j].Path })
	return out, nil
}

// BuildApplyDryRun returns deterministic actions without mutating target.
func BuildApplyDryRun(sourceRoot, targetRoot string) (ApplyDryRunResult, error) {
	applyRes, err := BuildApplyDryRunWithOptions(sourceRoot, targetRoot, ApplyOptions{})
	if err != nil {
		return ApplyDryRunResult{}, err
	}
	return applyRes, nil
}

// BuildApplyDryRunWithOptions returns deterministic actions without mutating target.
func BuildApplyDryRunWithOptions(sourceRoot, targetRoot string, opts ApplyOptions) (ApplyDryRunResult, error) {
	diffRes, err := Diff(sourceRoot, targetRoot)
	if err != nil {
		return ApplyDryRunResult{}, err
	}

	result := ApplyDryRunResult{SourceRoot: diffRes.SourceRoot, TargetRoot: diffRes.TargetRoot}

	dirSet := map[string]struct{}{}
	for _, file := range diffRes.OnlyInSource {
		dir := filepath.Dir(file.Path)
		if dir != "." && dir != "" {
			dirSet[dir] = struct{}{}
		}
	}
	dirs := make([]string, 0, len(dirSet))
	for dir := range dirSet {
		dirs = append(dirs, dir)
	}
	sort.Strings(dirs)
	for _, dir := range dirs {
		result.Actions = append(result.Actions, ApplyAction{Action: "mkdir", Path: dir})
	}

	for _, file := range diffRes.OnlyInSource {
		result.Actions = append(result.Actions, ApplyAction{Action: "copy", Path: file.Path})
	}
	for _, file := range diffRes.Changed {
		result.Actions = append(result.Actions, ApplyAction{Action: "update", Path: file.Path})
	}
	if opts.DeleteMissing {
		for _, file := range diffRes.OnlyInTarget {
			result.Actions = append(result.Actions, ApplyAction{Action: "delete", Path: file.Path})
		}
	}

	sort.Slice(result.Actions, func(i, j int) bool {
		if result.Actions[i].Action == result.Actions[j].Action {
			return result.Actions[i].Path < result.Actions[j].Path
		}
		return result.Actions[i].Action < result.Actions[j].Action
	})

	return result, nil
}

// Apply mutates target workspace to converge to source workspace.
func Apply(sourceRoot, targetRoot string, opts ApplyOptions) (ApplyResult, error) {
	plan, err := BuildApplyDryRunWithOptions(sourceRoot, targetRoot, opts)
	if err != nil {
		return ApplyResult{}, err
	}

	result := ApplyResult{
		SourceRoot: plan.SourceRoot,
		TargetRoot: plan.TargetRoot,
		Applied:    true,
		Actions:    plan.Actions,
	}

	for _, action := range plan.Actions {
		switch action.Action {
		case "mkdir":
			abs, err := resolveWithin(result.TargetRoot, action.Path)
			if err != nil {
				return ApplyResult{}, err
			}
			if err := os.MkdirAll(abs, 0o755); err != nil {
				return ApplyResult{}, err
			}
		case "copy", "update":
			src, err := resolveWithin(result.SourceRoot, action.Path)
			if err != nil {
				return ApplyResult{}, err
			}
			dst, err := resolveWithin(result.TargetRoot, action.Path)
			if err != nil {
				return ApplyResult{}, err
			}
			if err := copyFile(src, dst); err != nil {
				return ApplyResult{}, err
			}
		case "delete":
			dst, err := resolveWithin(result.TargetRoot, action.Path)
			if err != nil {
				return ApplyResult{}, err
			}
			if err := os.Remove(dst); err != nil && !os.IsNotExist(err) {
				return ApplyResult{}, err
			}
		}
	}

	return result, nil
}

func resolveWithin(root, rel string) (string, error) {
	clean := filepath.Clean(filepath.FromSlash(strings.TrimSpace(rel)))
	if clean == "." || clean == "" {
		return root, nil
	}
	if strings.HasPrefix(clean, "..") || filepath.IsAbs(clean) {
		return "", fmt.Errorf("invalid relative path: %s", rel)
	}
	return filepath.Join(root, clean), nil
}

func copyFile(src, dst string) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer func() { _ = in.Close() }()

	if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
		return err
	}
	tmp := dst + ".tmp"
	out, err := os.Create(tmp)
	if err != nil {
		return err
	}
	if _, err := io.Copy(out, in); err != nil {
		_ = out.Close()
		_ = os.Remove(tmp)
		return err
	}
	if err := out.Close(); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	if err := os.Rename(tmp, dst); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	return nil
}

func scanWorkspaceFiles(root string) ([]FileEntry, error) {
	relPaths, err := listTrackedWorkspacePaths(root)
	if err != nil {
		return nil, err
	}

	files := make([]FileEntry, 0)
	for _, rel := range relPaths {
		abs := filepath.Join(root, filepath.FromSlash(rel))
		info, err := os.Stat(abs)
		if err != nil {
			return nil, err
		}
		if info.IsDir() {
			continue
		}
		hash, err := hashFile(abs)
		if err != nil {
			return nil, err
		}
		files = append(files, FileEntry{Path: rel, Size: info.Size(), SHA256: hash})
	}
	sort.Slice(files, func(i, j int) bool { return files[i].Path < files[j].Path })
	return files, nil
}

func listTrackedWorkspacePaths(root string) ([]string, error) {
	paths := []string{"manifest.yaml"}
	for _, rule := range Rules {
		if rule.Path == "manifest.yaml" {
			continue
		}
		abs := filepath.Join(root, filepath.FromSlash(rule.Path))
		info, err := os.Stat(abs)
		if err != nil {
			if os.IsNotExist(err) {
				continue
			}
			return nil, err
		}
		if !info.IsDir() {
			continue
		}

		err = filepath.WalkDir(abs, func(path string, d os.DirEntry, walkErr error) error {
			if walkErr != nil {
				return walkErr
			}
			if d.IsDir() {
				return nil
			}
			rel, err := filepath.Rel(root, path)
			if err != nil {
				return err
			}
			paths = append(paths, filepath.ToSlash(rel))
			return nil
		})
		if err != nil {
			return nil, err
		}
	}

	seen := map[string]struct{}{}
	out := make([]string, 0, len(paths))
	for _, p := range paths {
		clean := strings.TrimSpace(filepath.ToSlash(p))
		if clean == "" {
			continue
		}
		if _, ok := seen[clean]; ok {
			continue
		}
		seen[clean] = struct{}{}
		out = append(out, clean)
	}
	sort.Strings(out)
	return out, nil
}

func hashFile(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer func() { _ = f.Close() }()

	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return "", fmt.Errorf("hash %s: %w", path, err)
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}
