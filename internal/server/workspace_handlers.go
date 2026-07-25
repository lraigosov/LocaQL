package server

import (
	"encoding/json"
	"net/http"
	"strings"

	"github.com/lraigosov/LocaQL/internal/workspace"
)

// Workspace REST endpoints are LocaQL-only convenience endpoints, deliberately
// outside /bigquery/v2/: real BigQuery has no "portable workspace" concept at
// all, this is entirely a local promotion-workflow tool built on top of the
// same internal/workspace package the locaql CLI already uses. Paths are
// resolved on the machine running the emulator process (matching the
// established convention for load/extract sourceUris/destinationUris), not
// uploaded through the request body.

func (s *Server) workspaceValidate(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "Method not allowed", "methodNotAllowed")
		return
	}
	defer func() {
		if r.Body != nil {
			_ = r.Body.Close()
		}
	}()

	var payload struct {
		Path string `json:"path"`
	}
	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
		writeError(w, http.StatusBadRequest, "Invalid request body", "invalid")
		return
	}
	res, err := workspace.Validate(payload.Path)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error(), "invalid")
		return
	}
	writeJSON(w, http.StatusOK, renderWorkspaceValidateResult(res))
}

func (s *Server) workspacePlan(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "Method not allowed", "methodNotAllowed")
		return
	}
	defer func() {
		if r.Body != nil {
			_ = r.Body.Close()
		}
	}()

	var payload struct {
		Path string `json:"path"`
	}
	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
		writeError(w, http.StatusBadRequest, "Invalid request body", "invalid")
		return
	}
	res, err := workspace.BuildPlan(payload.Path)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error(), "invalid")
		return
	}
	writeJSON(w, http.StatusOK, renderWorkspacePlanResult(res))
}

func (s *Server) workspaceDiff(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "Method not allowed", "methodNotAllowed")
		return
	}
	defer func() {
		if r.Body != nil {
			_ = r.Body.Close()
		}
	}()

	var payload struct {
		Source string `json:"source"`
		Target string `json:"target"`
	}
	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
		writeError(w, http.StatusBadRequest, "Invalid request body", "invalid")
		return
	}
	if strings.TrimSpace(payload.Target) == "" {
		writeError(w, http.StatusBadRequest, "target is required", "required")
		return
	}
	res, err := workspace.Diff(payload.Source, payload.Target)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error(), "invalid")
		return
	}
	writeJSON(w, http.StatusOK, renderWorkspaceDiffResult(res))
}

func (s *Server) workspaceApply(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "Method not allowed", "methodNotAllowed")
		return
	}
	defer func() {
		if r.Body != nil {
			_ = r.Body.Close()
		}
	}()

	var payload struct {
		Source        string `json:"source"`
		Target        string `json:"target"`
		DryRun        *bool  `json:"dryRun"`
		DeleteMissing bool   `json:"deleteMissing"`
		ConfirmDelete string `json:"confirmDelete"`
	}
	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
		writeError(w, http.StatusBadRequest, "Invalid request body", "invalid")
		return
	}
	if strings.TrimSpace(payload.Target) == "" {
		writeError(w, http.StatusBadRequest, "target is required", "required")
		return
	}
	// dryRun defaults to true, matching the CLI's own --dry-run=true default:
	// mutating the target is something a caller must opt into explicitly.
	dryRun := true
	if payload.DryRun != nil {
		dryRun = *payload.DryRun
	}
	if !dryRun && payload.DeleteMissing && strings.TrimSpace(payload.ConfirmDelete) != "DELETE" {
		writeError(w, http.StatusBadRequest, "deleteMissing requires confirmDelete=\"DELETE\" for a mutating apply", "invalid")
		return
	}

	opts := workspace.ApplyOptions{DeleteMissing: payload.DeleteMissing}
	if dryRun {
		planRes, err := workspace.BuildApplyDryRunWithOptions(payload.Source, payload.Target, opts)
		if err != nil {
			writeError(w, http.StatusBadRequest, err.Error(), "invalid")
			return
		}
		writeJSON(w, http.StatusOK, renderWorkspaceApplyResult(workspace.ApplyResult{
			SourceRoot: planRes.SourceRoot,
			TargetRoot: planRes.TargetRoot,
			Applied:    false,
			Actions:    planRes.Actions,
		}))
		return
	}

	res, err := workspace.Apply(payload.Source, payload.Target, opts)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error(), "invalid")
		return
	}
	writeJSON(w, http.StatusOK, renderWorkspaceApplyResult(res))
}

func renderWorkspaceValidateResult(res workspace.Result) map[string]any {
	return map[string]any{
		"root":               res.Root,
		"valid":              res.IsValid(),
		"found":              nonNilStrings(res.Found),
		"missingRequired":    nonNilStrings(res.MissingRequired),
		"missingRecommended": nonNilStrings(res.MissingRecommended),
	}
}

func renderWorkspaceFileEntry(f workspace.FileEntry) map[string]any {
	return map[string]any{"path": f.Path, "size": f.Size, "sha256": f.SHA256}
}

func renderWorkspaceFileEntries(entries []workspace.FileEntry) []map[string]any {
	out := make([]map[string]any, len(entries))
	for i, f := range entries {
		out[i] = renderWorkspaceFileEntry(f)
	}
	return out
}

func renderWorkspacePlanResult(res workspace.PlanResult) map[string]any {
	return map[string]any{
		"validation": renderWorkspaceValidateResult(res.Validation),
		"files":      renderWorkspaceFileEntries(res.Files),
	}
}

func renderWorkspaceDiffResult(res workspace.DiffResult) map[string]any {
	changed := make([]map[string]any, len(res.Changed))
	for i, c := range res.Changed {
		changed[i] = map[string]any{
			"path":   c.Path,
			"source": renderWorkspaceFileEntry(c.Source),
			"target": renderWorkspaceFileEntry(c.Target),
		}
	}
	return map[string]any{
		"sourceRoot":   res.SourceRoot,
		"targetRoot":   res.TargetRoot,
		"onlyInSource": renderWorkspaceFileEntries(res.OnlyInSource),
		"onlyInTarget": renderWorkspaceFileEntries(res.OnlyInTarget),
		"changed":      changed,
	}
}

func renderWorkspaceApplyResult(res workspace.ApplyResult) map[string]any {
	actions := make([]map[string]any, len(res.Actions))
	for i, a := range res.Actions {
		actions[i] = map[string]any{"action": a.Action, "path": a.Path}
	}
	return map[string]any{
		"sourceRoot": res.SourceRoot,
		"targetRoot": res.TargetRoot,
		"applied":    res.Applied,
		"actions":    actions,
	}
}

// nonNilStrings renders a nil []string as [] in JSON instead of null, so UI
// code can always safely treat these fields as arrays.
func nonNilStrings(in []string) []string {
	if in == nil {
		return []string{}
	}
	return in
}
