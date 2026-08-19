# 5. License Hardening: Third-Party Component Split, CLA, and Separate Commercial License

## Status

Accepted. Extends [ADR 0004](0004-transition-to-proprietary-eula-license.md); does not supersede it.

## Context

Following the proprietary EULA transition (ADR 0004), a chain-of-title audit
was performed before any commercial sale or investment conversation, to
confirm the project's IP was clean and to identify any ambiguity a buyer's
or licensee's due diligence would flag. The audit found:

- The actual, published history of every LocaQL release (`v0.9.0` through
  `v0.17.0`, all still Apache-2.0 at the time of tagging) has a single root
  commit and is authored solely by the project owner — no third-party
  commit or contribution exists in the published history.
- A copy of unrelated history (traced to `goccy/bigquery-emulator`'s own
  public repository, most likely picked up through the optional read-only
  `upstream` reference remote `CONTRIBUTING.md` already documents) was found
  dangling, reachable only via 66 stray tags (`v0.1.0` through `v0.8.1`),
  disconnected from every real branch and release. These tags turned out to
  exist not just in a local clone but on the real, private
  `github.com/lraigosov/LocaQL` remote itself — an initial unauthenticated
  check wrongly suggested otherwise (a false negative caused by checking an
  already-private repo without credentials, not a real absence); confirmed
  and corrected once checked properly authenticated. `main` and `dev`
  themselves were confirmed, both times, to never descend from that
  lineage. Deleted from the real remote and every local clone, since
  leaving 66 dangling, non-canonical version tags on the real repository is
  exactly the kind of loose end this audit exists to close, not just
  document around.
- The `.mailmap` file listed dozens of `goccy/bigquery-emulator` contributor
  identities that never appear as a raw commit author anywhere in the real,
  published history — traced to having been copied wholesale as boilerplate
  from that project at some earlier point (matching the "early planning
  referenced goccy/bigquery-emulator" note already in `NOTICE`). Not
  evidence of foreign code; cleaned up to list only the project owner's own
  identity variants, so the file cannot be misread as claiming otherwise by
  a future auditor.
- The EULA's own text did not, at the time, distinguish the Licensor's own
  IP from the open-source Third-Party Components (goccy/*, the underlying
  Apache-2.0-licensed `google/googlesql` engine, and ~50 other Go module
  dependencies) the Software is built on — a real ambiguity for any future
  licensee or buyer trying to determine what they'd actually be
  buying/licensing.
- The EULA's prohibited-use language (`sublicense, sell, ... or otherwise
  commercially exploit`) was broad but not itemized — SaaS/hosting, OEM
  embedding, reselling, and sublicensing were each only implicitly covered,
  not named, making it harder to point to a specific clause when explaining
  what a prospective commercial customer would actually need to license.
- Nothing formalized contributor IP assignment as an explicit, dated,
  per-contribution acceptance — the EULA's Section 7 assignment applied by
  virtue of submitting a PR at all, with no separate acceptance record.
- No separate document existed for commercial terms (production use,
  hosting, OEM, reseller, sublicensing) distinct from the free
  Community/Evaluation tier — every commercial conversation would have
  needed bespoke drafting from scratch.

## Decision

- **`LICENSE_HISTORY.md`**: freeze and publish, in full and without editing
  after the fact, exactly which released versions were Apache-2.0 and from
  which unreleased commit (`a2937f9`) the proprietary EULA applies. Explicit
  decision **not** to rewrite or hide the Apache-2.0 period, on both
  practical grounds (copies already exist outside the project's control —
  Software Heritage's automatic archival, the Go module proxy's immutable
  cache, forks) and legal ones (altering provenance records ahead of a
  planned sale is a bad-faith red flag in due diligence, far worse than a
  documented open-source period).
- **`LICENSE` restructured**: added a new Section 2 (Licensor IP vs.
  Third-Party Components) distinguishing what the Licensor actually owns
  from the open-source dependencies (referencing the new
  `THIRD_PARTY_NOTICES.md`); renumbered subsequent sections (old Section 6
  "Contributions" is now Section 7); rewrote the old Section 5
  ("Restrictions") into an itemized Section 6 naming SaaS/hosting, OEM,
  reseller/sublicensing, sale/transfer, reverse engineering, and competing
  products as separately-named restricted uses, each pointing to a
  Commercial License as the way to obtain them; retitled the document
  "Community/Evaluation License" to name it as the free tier opposite the
  new Commercial License.
- **`CLA.md`** added as a standalone Contributor License Agreement,
  restating the same Section 7 assignment in a form requiring explicit,
  dated acceptance per contributor before a pull request merges, enforced
  via `.github/workflows/cla.yml` (`contributor-assistant/github-action`;
  requires one-time secret/branch setup documented in the workflow itself).
- **`COMMERCIAL_LICENSE.md`** added as a separate template covering
  Production Use, SaaS/Hosting, OEM/Embedded, Reseller/Distribution, and
  Sublicensing tiers, with fee/term/liability terms left as placeholders for
  per-deal drafting rather than a fixed public price list.
- **`THIRD_PARTY_NOTICES.md`** added as the persistent, versioned,
  human-readable inventory of every dependency in `go.mod` and its license
  (scanned directly against each dependency's own LICENSE file, not
  assumed) — complementing the CI-only, non-committed SBOM/license-scan
  artifacts `ci.yml`'s `license-scan` job already produced.
- `.mailmap` cleaned to list only the project owner's own identity
  variants.

## Consequences

- A future licensee, investor, or buyer can be pointed to one document
  (`LICENSE_HISTORY.md`) for exactly what was ever open-source and when,
  one document (`THIRD_PARTY_NOTICES.md`) for exactly what open-source
  material is embedded and under what terms, and a clear split within
  `LICENSE` itself for what the Licensor actually owns outright — reducing
  diligence friction instead of requiring it to be reconstructed from git
  history on request.
- Commercial conversations (SaaS, OEM, reseller) now have a named clause to
  point to in `LICENSE` and a starting template in `COMMERCIAL_LICENSE.md`,
  instead of requiring bespoke drafting from a blank page each time.
- Community contributions now require an explicit, dated CLA acceptance in
  addition to the EULA's blanket assignment clause, closing the gap between
  "assignment happens by virtue of submitting a PR" and "assignment is
  explicitly, individually acknowledged."
- None of this is legal advice, and none of the new documents (`CLA.md`,
  `COMMERCIAL_LICENSE.md`, the revised `LICENSE`) have been reviewed by a
  lawyer yet — that review remains a required gate before the first real
  commercial deal or CLA-gated external contribution, per each document's
  own draft note.
