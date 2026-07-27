# syntax=docker/dockerfile:1

# --- builder ---------------------------------------------------------------
FROM golang:1.25 AS builder

ARG VERSION=dev
ARG COMMIT=none
ARG BUILD_DATE=unknown

WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .

# CGO_ENABLED=0: every dependency this emulator actually needs at runtime
# (goccy/googlesqlite, google.golang.org/grpc, chromedp is dev/test-only and
# not part of this binary) is pure Go — confirmed when googlesqlite was
# adopted (Sesión 73) and again here by this build succeeding — so a fully
# static binary is possible, matching the minimal final image below.
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath \
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
