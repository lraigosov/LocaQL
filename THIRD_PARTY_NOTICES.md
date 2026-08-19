# LocaQL — Third-Party Notices

This file inventories every third-party dependency compiled into LocaQL, as
declared in `go.mod`, along with its license. It is generated from a direct
scan of each dependency's own `LICENSE`/`COPYING` file in the Go module
cache — not from assumption. Regenerate this file whenever `go.mod` changes
meaningfully (new dependency, major version bump), so it never drifts from
reality.

**No copyleft dependencies.** Every dependency below is permissive (MIT,
BSD-2/3-Clause, or Apache-2.0). None is GPL, LGPL, AGPL, or any other
copyleft license. This means none of them impose a "must release your own
source" obligation on LocaQL — but each still requires its own copyright
notice and license text to be preserved in any distribution, which this
file (plus the notices already embedded in vendored/compiled binaries) does.

## The one dependency that needs its own explanation

`github.com/goccy/go-googlesql` and `github.com/goccy/googlesqlite` are
MIT-licensed wrapper/tooling code, but the actual SQL parsing/analysis
engine they run — the real, unmodified `google/googlesql` (formerly
ZetaSQL) engine, compiled to WebAssembly and transpiled to Go — remains
**Apache License 2.0, Copyright Google LLC**. The MIT license on the
surrounding goccy/* tooling does not relicense Google's engine code inside
it. See `NOTICE` for the full explanation of this dependency chain
(`google/googlesql` → `goccy/googlesql-wasm` → `goccy/wasm2go` →
`goccy/go-googlesql` → `goccy/googlesqlite`) and why LocaQL states this
attribution explicitly rather than relying on the intermediate repositories
to carry it forward (none of them currently ship a NOTICE file covering the
transpiled engine code itself).

## Full inventory (from `go.mod`, scanned against each module's own LICENSE file)

| Module | Version | License |
|---|---|---|
| cloud.google.com/go/bigquery | v1.79.0 | Apache License 2.0 |
| github.com/apache/arrow-go/v18 | v18.7.0 | Apache License 2.0 |
| github.com/chromedp/cdproto | v0.0.0-20250403032234-65de8f5d025b | MIT |
| github.com/chromedp/chromedp | v0.13.7 | MIT |
| github.com/goccy/go-googlesql | v0.3.0 | MIT (wrapper only — see note above re: Apache-2.0 engine) |
| github.com/goccy/googlesqlite | v0.3.1 | MIT (wrapper only — see note above re: Apache-2.0 engine) |
| github.com/linkedin/goavro/v2 | v2.15.0 | Apache License 2.0 |
| github.com/parquet-go/parquet-go | v0.30.1 | Apache License 2.0 |
| google.golang.org/grpc | v1.82.1 | Apache License 2.0 |
| google.golang.org/protobuf | v1.36.12-0.20260120151049-f2248ac996af | BSD-3-Clause |
| gopkg.in/yaml.v3 | v3.0.1 | MIT and Apache-2.0 (dual, project's choice) |
| cloud.google.com/go/compute/metadata | v0.9.0 | Apache License 2.0 |
| github.com/DataDog/go-hll | v1.0.2 | MIT |
| github.com/andybalholm/brotli | v1.2.2 | MIT |
| github.com/chromedp/sysutil | v1.1.0 | MIT |
| github.com/dgryski/go-farm | v0.0.0-20240924180020-3414d57e47da | MIT |
| github.com/dlclark/regexp2 | v1.11.4 | MIT |
| github.com/dop251/goja | v0.0.0-20260311135729-065cd970411c | MIT |
| github.com/dustin/go-humanize | v1.0.1 | MIT |
| github.com/go-json-experiment/json | v0.0.0-20250211171154-1ae217ad3535 | BSD-3-Clause |
| github.com/go-sourcemap/sourcemap | v2.1.3+incompatible | BSD-2-Clause |
| github.com/gobwas/httphead | v0.1.0 | MIT |
| github.com/gobwas/pool | v0.2.1 | MIT |
| github.com/gobwas/ws | v1.4.0 | MIT |
| github.com/goccy/go-json | v0.10.6 | MIT |
| github.com/goccy/googlesqlwasm2go | v0.1.0 | MIT |
| github.com/golang/geo | v0.0.0-20260505155700-1c5af9662e82 | Apache License 2.0 |
| github.com/golang/snappy | v1.0.0 | BSD-3-Clause |
| github.com/google/flatbuffers | v25.12.19+incompatible | Apache License 2.0 |
| github.com/google/pprof | v0.0.0-20230207041349-798e818bf904 | Apache License 2.0 |
| github.com/google/uuid | v1.6.0 | BSD-3-Clause |
| github.com/klauspost/compress | v1.19.0 | BSD-3-Clause |
| github.com/klauspost/cpuid/v2 | v2.4.0 | MIT |
| github.com/mattn/go-isatty | v0.0.20 | MIT |
| github.com/ncruces/go-sqlite3 | v0.34.0 | MIT |
| github.com/ncruces/go-sqlite3-wasm/v2 | v2.1.35300 | MIT-0 (MIT No Attribution) |
| github.com/ncruces/go-strftime | v1.0.0 | MIT |
| github.com/ncruces/julianday | v1.0.0 | MIT |
| github.com/parquet-go/bitpack | v1.0.0 | Apache License 2.0 |
| github.com/parquet-go/jsonlite | v1.0.0 | MIT |
| github.com/pierrec/lz4/v4 | v4.1.27 | BSD-3-Clause |
| github.com/pkg/errors | v0.8.0 | BSD-2-Clause |
| github.com/remyoudompheng/bigfft | v0.0.0-20230129092748-24d4a6f8daec | BSD-3-Clause |
| github.com/rogpeppe/go-internal | v1.15.0 | BSD-3-Clause |
| github.com/spaolacci/murmur3 | v1.1.0 | BSD-3-Clause |
| github.com/twpayne/go-geom | v1.6.1 | BSD-2-Clause |
| github.com/zeebo/xxh3 | v1.1.0 | BSD-2-Clause |
| golang.org/x/exp | v0.0.0-20260112195511-716be5621a96 | BSD-3-Clause |
| golang.org/x/net | v0.56.0 | BSD-3-Clause |
| golang.org/x/oauth2 | v0.36.0 | BSD-3-Clause |
| golang.org/x/sys | v0.47.0 | BSD-3-Clause |
| golang.org/x/text | v0.39.0 | BSD-3-Clause |
| gonum.org/v1/gonum | v0.17.0 | BSD-3-Clause |
| google.golang.org/genproto | v0.0.0-20260319201613-d00831a3d3e7 | Apache License 2.0 |
| google.golang.org/genproto/googleapis/api | v0.0.0-20260630182238-925bb5da69e7 | Apache License 2.0 |
| google.golang.org/genproto/googleapis/rpc | v0.0.0-20260630182238-925bb5da69e7 | Apache License 2.0 |
| gopkg.in/check.v1 | v1.0.0-20201130134442-10cb98267c6c | BSD-2-Clause |
| modernc.org/libc | v1.73.4 | BSD-3-Clause |
| modernc.org/libquickjs | v0.12.8 | BSD-style (modernc's own port) + bundled original QuickJS engine license (MIT, Copyright Fabrice Bellard/Charlie Gordon) in `LICENSE-LIBQUICKJS` |
| modernc.org/mathutil | v1.7.1 | BSD-3-Clause |
| modernc.org/memory | v1.11.0 | BSD-3-Clause |
| modernc.org/quickjs | v0.18.2 | Same dual structure as `modernc.org/libquickjs` above |
