# LocaQL — License History

This file is the authoritative, unaltered record of which license governed
which released version of LocaQL. It exists so that anyone — a contributor,
a licensee, a potential acquirer, or a court — can determine exactly what
license applied to any given version without ambiguity. This history is
never rewritten, hidden, or edited after the fact; corrections are appended
as dated notes, not silent changes.

## Summary

| Period | Versions | License |
|---|---|---|
| 2026-07-27 – 2026-08-12 (09:45 UTC-5) | v0.9.0 through v0.17.0 (every publicly tagged release to date) | Apache License, Version 2.0 |
| 2026-08-12 (11:27 UTC-5) onward | Unreleased commits after `a2937f9`; first release under these terms is v0.18.0 or later, whichever ships next | Proprietary EULA (see `LICENSE`) |

## Detail

- LocaQL's actual, currently-shipping codebase has a single root commit,
  `caac0ab7` ("chore: initial commit with gitignore"), dated 2026-07-19.
  Every commit reachable from that root — which is every commit in every
  branch and tag that has ever been pushed to
  `github.com/lraigosov/LocaQL` — is authored solely by the project's
  owner (Luis Raigoso, under the identities `lraigosov`, `LuisRai`, and
  `Luis Raigoso`, all the same person and email) or by automated CI
  (`github-actions[bot]`). Verified directly against each commit's raw
  author field, independent of `.mailmap`.
- The `LICENSE` file was added in commit `ad52264` (2026-07-21) as the
  Apache License, Version 2.0, and stayed that way, unmodified, through
  every version released since — v0.9.0 (2026-07-27) up to and including
  v0.17.0 (2026-08-12 09:45:35 -05:00).
- Commit `a2937f9` ("chore: transition project to proprietary licensing
  model", 2026-08-12 11:27:26 -05:00) replaced `LICENSE` with the current
  proprietary End User License Agreement. This commit landed **after**
  v0.17.0 was already tagged and published — meaning v0.17.0 is the last
  Apache-2.0 release, and no proprietary-licensed version has shipped yet
  as of this writing.
- Anyone who obtained a copy of any tagged version through v0.17.0 — by
  cloning the repository, via `go get`/the Go module proxy, via a fork, or
  by any other means — received it under the Apache License, Version 2.0
  terms in effect for that specific version, and that grant is not
  retroactively revocable by a later license change. This is standard
  open-source licensing practice (the same rule that lets anyone keep using
  an old MIT/Apache-licensed release even after a project relicenses going
  forward) and is not something this document, or any later change to
  `LICENSE`, alters.

## A note on an unrelated dangling artifact (corrected after further verification)

During the audit that produced this file, a copy of unrelated history —
traced to `goccy/bigquery-emulator`'s own public repository, most likely
picked up through the optional read-only `upstream` reference remote this
project's own `CONTRIBUTING.md` documents (`git remote add upstream
https://github.com/goccy/bigquery-emulator.git`) — was found reachable only
via 66 stray tags (`v0.1.0` through `v0.8.1`), disconnected from every real
branch and release.

An first pass concluded these had never been pushed to the real GitHub
remote, based on an unauthenticated `git ls-remote --tags` check. That check
was run after the repository had already been made private, so it silently
returned an empty (not an error) result instead of the true answer — a
false negative, corrected here rather than left standing. Re-verified with
proper authentication: **the 66 tags do exist on the real, private
`github.com/lraigosov/LocaQL` remote** (most likely pushed unintentionally
by a broad `git push --tags` from a working copy that had the `upstream`
remote's tags in its local refs). Also verified, and still true: `main` and
`dev` — and therefore every version in the table above — are **not**
descendants of that lineage; `git merge-base --is-ancestor` confirms neither
branch has ever included a single commit from it. The tags themselves have
since been deleted from both the real remote and every local clone this
project's own maintainers control, closing the loose end rather than
leaving a set of dangling, confusing version tags on the real repository.
