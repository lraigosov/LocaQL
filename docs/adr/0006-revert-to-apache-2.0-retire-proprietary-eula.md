# 6. Revert to Apache License 2.0; retire the proprietary EULA

## Status

Accepted. Supersedes [ADR 0004](0004-transition-to-proprietary-eula-license.md) in full and [ADR 0005](0005-license-hardening-third-party-split-cla-commercial-license.md) in part (see that ADR's own updated Status line for exactly which decisions of it remain in effect).

## Context

ADR 0004 moved LocaQL from Apache-2.0 to a proprietary EULA to protect a
possible future commercial/sale outcome. ADR 0005 hardened that structure
(Licensor IP vs. Third-Party Components split, CLA, a separate Commercial
License template, `LICENSE_HISTORY.md`, `.mailmap` cleanup) after a
chain-of-title audit. No version of LocaQL was ever tagged or released
under the proprietary terms — v0.17.0 remains the last (and, per this ADR,
only) tagged release, all under Apache-2.0.

Before investing further in a closed-license commercial strategy, a
separate audit was performed of the actual repository (forks, stargazers,
watchers, subscribers, 14-day clone/view traffic, GHCR image pulls) to
look for any evidence of external adoption to build that strategy on. It
found none: zero forks/stars/watchers/subscribers (the only
user-attributable, permanent signals GitHub exposes), and the observed
14-day clone/view traffic is fully explained by this project's own GitHub
Actions CI rather than external visitors (clone volume tracks CI run
volume almost exactly, and drops to zero on days with no commits). This
is not proof that no copy exists anywhere (the repository was public for a
period during the Apache-2.0 era, and GitHub's traffic API cannot see
Software Heritage, the Go module proxy cache, or third-party mirrors), but
it is proof that there is no demonstrated external demand to justify
gating access behind a paid commercial license today.

Separately, a licensing model built specifically to attract sponsorship,
community adoption, and the kind of external recognition comparable
open-source Go projects receive is structurally incompatible with a
closed, access-restricted license — that kind of recognition follows
genuine, visible, freely-usable open source, not a EULA gating production/
hosting/OEM use behind a sales conversation nobody has had yet.

## Decision

- **`LICENSE`** is reverted to the exact Apache License, Version 2.0 text
  used for v0.9.0–v0.17.0 (same copyright line, same terms) — not a new or
  modified permissive license.
- **`COMMERCIAL_LICENSE.md`** is removed. Its Production/SaaS/OEM/Reseller/
  Sublicensing tiers each granted, for a fee, a right Apache-2.0 already
  grants everyone at no charge (Apache-2.0 §2's copyright grant explicitly
  includes the right to sublicense and distribute) — keeping the document
  would have directly contradicted the license actually in force.
- **`NOTICE`** is updated to drop the proprietary-EULA framing and the
  "must keep a visible credit" clause (not an Apache-2.0 requirement); the
  project-origin/non-affiliation and GoogleSQL/ZetaSQL third-party notices
  are unchanged, since those are independent of which license governs
  LocaQL's own code.
- **`CLA.md`** is kept, with only its internal reference to "the current
  proprietary `LICENSE`" corrected to reflect Apache-2.0. Requiring
  contributor IP assignment independent of the current license is a
  well-established, unremarkable pattern (used by large open-source
  projects, including Google's own) — it preserves the owner's ability to
  offer a different license for a future edition of the Software without
  needing to track down every past contributor's consent, without taking
  anything away from users of the current Apache-2.0-licensed code.
- **`LICENSE_HISTORY.md`** gets a new, dated, appended entry recording this
  reversion — consistent with that document's own rule of never rewriting
  or hiding a past period, only appending corrections.
- **ADR 0004** and the affected parts of **ADR 0005** are marked superseded
  in their own Status lines, not deleted or edited elsewhere, per this
  project's own ADR convention.
- **`README.md`** and **`CONTRIBUTING.md`**'s License sections, and the
  README's license badge, are updated to describe Apache-2.0 and the CLA,
  dropping every reference to the retired Commercial License/EULA tiers.
- The repository's visibility (currently private) is a separate, orthogonal
  decision from its license and is unaffected by this ADR — reverting to
  Apache-2.0 does not, by itself, make the repository public.

## Consequences

- LocaQL is fully open-source again, under the same license it always
  shipped its tagged releases under; there is no migration cost for any
  past user, since no proprietary-licensed version ever existed to migrate
  from.
- Future monetization, if pursued, has to come from services built on top
  of the freely-licensed code (hosting, support/SLA, consulting, genuinely
  new capabilities the maintainer chooses never to backport) rather than
  from restricting rights Apache-2.0 already grants — there is no longer a
  paid tier that unlocks SaaS/OEM/production use, because nothing in
  `LICENSE` restricts those uses in the first place.
- Community adoption, visibility work, and any resulting sponsorship or
  recognition now have a coherent license story behind them: one license,
  applied consistently since the project's first tagged release.
- This is not legal advice; the reversion itself carries little legal risk
  (Apache-2.0 is strictly more permissive than what it replaces, so no
  recipient of the interim proprietary terms loses anything), but any
  future decision to reintroduce a restricted or dual-licensed edition
  should still go through a lawyer, per the same standing gate ADR 0005
  already established.
