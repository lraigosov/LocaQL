# syntax=docker/dockerfile:1

# --- builder ---------------------------------------------------------------
# --platform=$BUILDPLATFORM: this stage always runs as the *build* machine's
# native architecture, even when producing a linux/arm64 image on an amd64
# runner (or vice versa) — Go's own cross-compiler targets TARGETOS/TARGETARCH
# below, which is fast and needs no QEMU emulation for the compile itself.
# Only the final distroless stage actually needs to be the target platform,
# and buildx resolves that on its own since that FROM isn't pinned.
FROM --platform=$BUILDPLATFORM golang:1.25 AS builder

ARG VERSION=dev
ARG COMMIT=none
ARG BUILD_DATE=unknown
ARG TARGETOS
ARG TARGETARCH

WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .

# CGO_ENABLED=0: every dependency this emulator actually needs at runtime
# (goccy/googlesqlite, google.golang.org/grpc, chromedp is dev/test-only and
# not part of this binary) is pure Go — confirmed when googlesqlite was
# adopted (Sesión 73) and again here by this build succeeding — so a fully
# static binary is possible, matching the minimal final image below. All
# three binaries below (emulator, console UI, and the supervisor that starts
# both) share the same build constraints.
RUN CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} go build -trimpath \
    -ldflags "-s -w \
      -X github.com/lraigosov/LocaQL/internal/version.Version=${VERSION} \
      -X github.com/lraigosov/LocaQL/internal/version.Commit=${COMMIT} \
      -X github.com/lraigosov/LocaQL/internal/version.BuildDate=${BUILD_DATE}" \
    -o /out/locaql ./cmd/locaql
RUN CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} go build -trimpath \
    -ldflags "-s -w" -o /out/locaql-ui ./cmd/locaql-ui
RUN CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} go build -trimpath \
    -ldflags "-s -w" -o /out/locaql-supervisor ./cmd/locaql-supervisor

# --- final -------------------------------------------------------------
FROM gcr.io/distroless/static-debian12:nonroot

COPY --from=builder /out/locaql /usr/local/bin/locaql
COPY --from=builder /out/locaql-ui /usr/local/bin/locaql-ui
COPY --from=builder /out/locaql-supervisor /usr/local/bin/locaql-supervisor
COPY capabilities/registry.yaml /etc/locaql/capabilities/registry.yaml

WORKDIR /etc/locaql
EXPOSE 9050 9060 9070

# locaql-supervisor starts both the emulator (:9050 REST/gRPC-JSON, :9060
# Storage API gRPC) and the console UI (:9070, proxying /api to the
# emulator) as real subprocesses of this one container — no shell involved,
# since the base image (distroless/static) doesn't have one.
ENTRYPOINT ["/usr/local/bin/locaql-supervisor"]
