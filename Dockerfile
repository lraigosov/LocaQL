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
# static binary is possible, matching the minimal final image below.
RUN CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} go build -trimpath \
    -ldflags "-s -w \
      -X github.com/lraigosov/LocaQL/internal/version.Version=${VERSION} \
      -X github.com/lraigosov/LocaQL/internal/version.Commit=${COMMIT} \
      -X github.com/lraigosov/LocaQL/internal/version.BuildDate=${BUILD_DATE}" \
    -o /out/locaql ./cmd/locaql

# --- final -------------------------------------------------------------
FROM gcr.io/distroless/static-debian12:nonroot

COPY --from=builder /out/locaql /usr/local/bin/locaql
COPY capabilities/registry.yaml /etc/locaql/capabilities/registry.yaml

WORKDIR /etc/locaql
EXPOSE 9050 9060

ENTRYPOINT ["/usr/local/bin/locaql", "start", "--addr", ":9050", "--storage-grpc-addr", ":9060", "--capabilities", "/etc/locaql/capabilities/registry.yaml"]
