MODULE      := github.com/lraigosov/LocaQL
VERSION     ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
COMMIT      ?= $(shell git rev-parse --short HEAD 2>/dev/null || echo none)
BUILD_DATE  ?= $(shell date -u +%Y-%m-%dT%H:%M:%SZ)
LDFLAGS     := -s -w \
	-X '$(MODULE)/internal/version.Version=$(VERSION)' \
	-X '$(MODULE)/internal/version.Commit=$(COMMIT)' \
	-X '$(MODULE)/internal/version.BuildDate=$(BUILD_DATE)'

DIST        := dist
PLATFORMS   := linux/amd64 linux/arm64 windows/amd64 darwin/amd64 darwin/arm64
BINARIES    := locaql locaql-ui

.PHONY: build build-all docker-build docker-run clean test vet sbom vuln

# build compiles both binaries for the host platform into ./dist, matching
# GOOS/GOARCH already set in the environment (cross-compiling from WSL for
# Windows, for example, still works the same way this project has already
# used elsewhere: GOOS=windows GOARCH=amd64 make build).
build:
	@mkdir -p $(DIST)
	@for bin in $(BINARIES); do \
		ext=""; \
		if [ "$$GOOS" = "windows" ]; then ext=".exe"; fi; \
		echo "building $$bin$$ext ($(VERSION), $${GOOS:-$$(go env GOOS)}/$${GOARCH:-$$(go env GOARCH)})"; \
		CGO_ENABLED=0 go build -trimpath -ldflags "$(LDFLAGS)" -o "$(DIST)/$$bin$$ext" "./cmd/$$bin" || exit 1; \
	done

# build-all cross-compiles both binaries for every platform in $(PLATFORMS)
# into dist/<os>_<arch>/ — the reproducible-release matrix: same source,
# same ldflags-injected version/commit/date, one artifact set per platform.
build-all:
	@mkdir -p $(DIST)
	@for platform in $(PLATFORMS); do \
		os=$${platform%/*}; arch=$${platform#*/}; \
		outdir="$(DIST)/$${os}_$${arch}"; \
		mkdir -p "$$outdir"; \
		for bin in $(BINARIES); do \
			ext=""; \
			if [ "$$os" = "windows" ]; then ext=".exe"; fi; \
			echo "building $$outdir/$$bin$$ext"; \
			CGO_ENABLED=0 GOOS=$$os GOARCH=$$arch go build -trimpath -ldflags "$(LDFLAGS)" -o "$$outdir/$$bin$$ext" "./cmd/$$bin" || exit 1; \
		done; \
	done

docker-build:
	docker build \
		--build-arg VERSION=$(VERSION) \
		--build-arg COMMIT=$(COMMIT) \
		--build-arg BUILD_DATE=$(BUILD_DATE) \
		-t locaql:$(VERSION) -t locaql:latest .

docker-run:
	docker run --rm -p 9050:9050 -p 9060:9060 -p 9070:9070 locaql:latest

test:
	go test ./...

vet:
	go vet ./...

clean:
	rm -rf $(DIST)

# sbom generates a CycloneDX SBOM for the locaql binary (build-constraint-aware:
# only what cmd/locaql actually imports, not every module in go.sum) into
# dist/sbom.json. Not committed to the repo — it would go stale the moment a
# dependency changes — generate it fresh per release instead (also done in CI,
# see .github/workflows/ci.yml's license-scan job).
sbom:
	@mkdir -p $(DIST)
	@command -v cyclonedx-gomod >/dev/null || go install github.com/CycloneDX/cyclonedx-gomod/cmd/cyclonedx-gomod@latest
	cyclonedx-gomod app -json -output $(DIST)/sbom.json -main cmd/locaql -licenses .

# vuln scans every imported package for known CVEs against the official Go
# vulnerability database — distinct from `sbom`/license-scan, which check
# licensing, not vulnerabilities. Uses -scan package rather than the default
# -scan symbol (which would additionally filter to only vulnerabilities this
# project's call graph actually reaches): the default panics on this
# project's transitive dependency on go-json-experiment/json, a confirmed
# upstream bug in golang.org/x/tools/go/ssa's generic-signature handling
# (golang/go#75584 and related issues), not something in this project's own
# code. See .github/workflows/ci.yml's vulnerability-scan job for the same
# check in CI.
vuln:
	@command -v govulncheck >/dev/null || go install golang.org/x/vuln/cmd/govulncheck@latest
	govulncheck -scan package ./...
