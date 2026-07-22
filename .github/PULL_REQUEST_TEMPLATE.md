## What does this change?

<!-- One or two sentences: what behavior changes and why. -->

## Related issue

<!-- Closes #... , or "N/A" for a small/obvious fix. -->

## Checklist

- [ ] Target branch is `dev` (or `main`, only if this is a `hotfix/*` branch — see [CONTRIBUTING.md](../CONTRIBUTING.md#branching-model-and-devops-cycle)).
- [ ] `go build ./...` passes.
- [ ] `go vet ./...` passes.
- [ ] `go test ./...` passes.
- [ ] `CGO_ENABLED=1 go test -race ./internal/server` passes (if this touches jobs/tables/datasets services or anything concurrency-sensitive).
- [ ] `capabilities/registry.yaml` and the README's [Current Scope Matrix](../README.md#current-scope-matrix) are updated if this adds, closes, or changes the status of a capability.
- [ ] No BigQuery field, error code, or behavior is invented — anything new cites the official REST/Discovery documentation or observed client behavior.
- [ ] Relevant `README.md` section(s) and Table of Contents are updated if a documented feature or endpoint was added/renamed.

## Notes for the reviewer

<!-- Anything that needs special attention: risky areas, things you're unsure about, alternatives you considered. -->
