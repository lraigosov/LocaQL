# Security Policy

LocaQL is a local development tool: it is meant to run on a developer's machine or in CI, bound to `localhost`/a private network, with no authentication (anonymous-only by design — see [Known Divergences](KNOWN-DIVERGENCES.md) and the master plan's non-goals). It is **not** designed to be exposed to an untrusted network or to hold production data. That context shapes what counts as a security issue here versus a general bug.

## Supported versions

Only the latest released version (see [`CHANGELOG.md`](CHANGELOG.md) / [GitHub Releases](https://github.com/lraigosov/LocaQL/releases)) receives security fixes. This is a single-maintainer project (see [`CODEOWNERS`](.github/CODEOWNERS)); older tags are not backported.

## Reporting a vulnerability

Please **do not** open a public GitHub issue for a suspected security vulnerability.

Instead, use GitHub's private reporting:

1. Go to the [Security tab](https://github.com/lraigosov/LocaQL/security) of this repository.
2. Click **"Report a vulnerability"** to open a private advisory.

If that is not available to you, open an issue asking for a private contact channel without describing the vulnerability itself.

Please include:

- The version/commit affected.
- Steps to reproduce, or a minimal request/payload.
- What you expected versus what actually happened.
- Whether you believe it's exploitable beyond a local/trusted-network deployment (see scope below).

## Response expectations

This is a single-maintainer, part-time project — there is no SLA. As a target: an acknowledgment within a few days, and a fix or mitigation plan communicated before any public disclosure. Coordinated disclosure is appreciated; please allow a reasonable window to ship a fix before making details public.

## Scope

In scope:

- Anything that lets a request escape the emulator's intended sandbox — e.g. path traversal outside a configured data/workspace directory, arbitrary file read/write beyond what a load/extract/workspace operation should touch, or a way to execute host commands.
- Memory-safety issues surfaced by `-race` or a crash triggered by untrusted input that a real client could plausibly send.
- Dependency vulnerabilities affecting a package this project actually imports (this repository runs [`govulncheck`](https://go.dev/blog/vuln) in CI specifically to catch these — see `.github/workflows/ci.yml`'s `vulnerability-scan` job and `make vuln`; it runs in `-scan package` mode rather than the default `-scan symbol` call-graph mode due to a confirmed upstream crash unrelated to this project, see the comment in `Makefile`, so a reported finding means "an imported package has a known CVE," not necessarily "this project's code calls the vulnerable function").

Explicitly out of scope (by design, not oversight — see [Known Divergences](KNOWN-DIVERGENCES.md) and the master plan's non-goals):

- Lack of authentication/authorization (IAM, row/column-level security) — the emulator is anonymous-only, permanently, for every user.
- Missing TLS on the plaintext REST/gRPC listeners — this is a local development tool, not a service meant to sit on an untrusted network.
- Denial of service from a client that can already reach the emulator's port (e.g. large uploads, many concurrent jobs) — the deployment model assumes the caller is trusted, the same assumption real BigQuery emulator tooling makes.
