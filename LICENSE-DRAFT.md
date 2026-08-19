================================================================================
                 LOCAQL SOURCE-AVAILABLE LICENSE AND EULA (DRAFT)
================================================================================

STATUS: DRAFT FOR REVIEW — NOT YET IN EFFECT. This document is not the
governing license of LocaQL until the Licensor replaces the current
`LICENSE` file with this text (or a lawyer-reviewed version of it) and
publishes a corresponding `LICENSE-VERSIONS.md` (see Section 5).

This Agreement is inspired by, and uses the same general mechanics as, the
Business Source License (BSL) and Functional Source License (FSL) model
already adopted by projects such as MariaDB, CockroachDB, and Sentry. It is
NOT a verbatim reproduction of either trademarked license text — it is a
custom agreement written for LocaQL, using the same "source-available now,
permissively-licensed later" structure. Before publishing this as LocaQL's
real license, have a lawyer confirm the specific wording, especially before
using either the "Business Source License" or "Functional Source License"
name (each carries its own trademark/attribution terms from its author).

This Agreement is between you (either an individual or a single legal
entity, "You" or "User") and Luis Raigoso ("Licensor"), the sole owner and
copyright holder of the LocaQL software platform, including all associated
source code, binaries, documentation, and assets (collectively, the
"Software").

BY DOWNLOADING, INSTALLING, COPYING, OR OTHERWISE USING THE SOFTWARE, YOU
AGREE TO BE BOUND BY THE TERMS OF THIS AGREEMENT. IF YOU DO NOT AGREE TO
THESE TERMS, DO NOT DOWNLOAD, INSTALL, COPY, OR USE THE SOFTWARE.

1. OWNERSHIP AND INTELLECTUAL PROPERTY
--------------------------------------
The Software is licensed, not sold. All right, title, and interest in and to
the Software — including but not limited to any copyrights, patents,
trademarks, trade secrets, designs, and other intellectual property
rights — are and shall remain the exclusive property of the Licensor (Luis
Raigoso). This Agreement does not transfer any ownership rights to You.

2. LICENSE MODEL: SOURCE-AVAILABLE, NOT OPEN SOURCE (YET)
----------------------------------------------------------
The Software is "source-available": its source code is publicly visible and
usable under the terms below, but this Agreement is not an Open Source
Initiative-approved license while it is in effect. Each specific released
version of the Software automatically converts to the Change License defined
in Section 5 once that version's own Change Date passes — at that point,
and only for that specific version, this Agreement's restrictions no longer
apply and the Change License governs instead.

3. LICENSE GRANT AND PERMITTED USES
-------------------------------------
Subject to Your strict compliance with all terms of this Agreement,
including the Additional Use Restrictions in Section 4, the Licensor hereby
grants You a worldwide, non-exclusive, royalty-free license to:
  (a) Use, copy, and run the Software, including in production and for
      internal commercial purposes within Your own organization;
  (b) Modify the Software and create derivative works for Your own internal
      use;
  (c) Redistribute the Software or Your modifications, provided every copy
      remains subject to this Agreement (or, for a version past its Change
      Date, the applicable Change License) and retains all copyright and
      license notices.

This is a substantially broader grant than a purely proprietary EULA: unlike
a "look but don't touch" license, ordinary use — including running LocaQL
in production for Your own purposes — is permitted without a separate
commercial license, as long as You do not fall into one of the two carve-outs
in Section 4.

4. ADDITIONAL USE RESTRICTIONS (THE CARVE-OUTS)
--------------------------------------------------
Notwithstanding Section 3, You may NOT, without a separate commercial
license from the Licensor:
  (a) Hosted/Managed Service: offer the Software, or any modified or
      derivative version of it, to third parties as a hosted, managed, or
      "as-a-service" offering, where a material part of the value delivered
      to those third parties is the Software's own functionality (e.g.,
      operating a "LocaQL-as-a-Service" product, or embedding it as the core
      engine of a competing hosted BigQuery-compatible emulator/service);
  (b) Competing Product: use the Software, in whole or in part, or its
      design, architecture, or source code, as the basis to build, develop,
      market, or distribute a product or service that competes with the
      Software or with any commercial offering the Licensor provides based
      on the Software;
  (c) License Circumvention: remove, obscure, or circumvent this License,
      any Change Date mechanism, or any licensing/metering functionality in
      the Software.

For information about obtaining a commercial license covering the uses
restricted in this Section, contact the Licensor at luisraigoso79@gmail.com.

5. CHANGE DATE AND CHANGE LICENSE
------------------------------------
Each specific tagged release of the Software (e.g., "v0.16.0") is licensed
under this Agreement until that version's own Change Date, defined as:

    the third (3rd) anniversary of that version's first public release date.

On and after a given version's Change Date, that specific version — the
exact source code as originally released, not later versions — is
automatically relicensed under the MIT License (the "Change License"), and
the restrictions in Sections 3 and 4 no longer apply to that version. Later
versions remain governed by this Agreement, each with its own Change Date
calculated the same way from its own release date.

The Licensor will maintain a version-to-Change-Date mapping in
`LICENSE-VERSIONS.md` in this repository, updated with every tagged release,
so anyone can determine which versions have already converted to MIT and
which remain under this Agreement. [DRAFT NOTE: wire this into the existing
`prepare-release.yml` automation so it updates automatically per release,
instead of being maintained by hand.]

This mechanism applies separately to each version; using a newer version
does not grant you any Change License rights over that newer version before
its own Change Date arrives, and a Change Date passing for one version does
not accelerate any other version's Change Date.

6. CONTRIBUTIONS AND INTELLECTUAL PROPERTY ASSIGNMENT
--------------------------------------------------------
By submitting any code, documentation, bug reports, feature requests, or
other feedback ("Contributions") to the LocaQL repository, project, or
Licensor, You hereby assign, transfer, and convey to the Licensor (Luis
Raigoso) all right, title, and interest — including all copyrights, patents,
and other intellectual property rights — in and to such Contributions.

To the extent that any such assignment is deemed ineffective under
applicable law, You hereby grant the Licensor an unconditional, perpetual,
worldwide, non-exclusive, fully paid-up, royalty-free, transferable,
sublicensable, irrevocable, and unrestricted license to use, modify,
distribute, create derivative works of, sublicense, and re-license Your
Contributions under any terms (including under a Change License, a separate
commercial license, or any other proprietary or open terms) at the
Licensor's sole discretion, without credit or compensation to You.

7. RESERVATION OF RIGHTS AND FUTURE MONETIZATION
-----------------------------------------------------
The Licensor reserves all rights not expressly granted to You in this
Agreement. The Licensor reserves the right, at his sole and absolute
discretion, to:
  (a) Modify, update, or terminate the availability of new versions of the
      Software at any time (this does not retroactively change the Change
      Date already fixed for previously released versions);
  (b) Offer a separate commercial license covering the uses restricted under
      Section 4, on whatever commercial terms the Licensor chooses;
  (c) Sell, transfer, or assign the Software, its copyright, and all
      associated intellectual property rights — including the exclusive
      right to grant commercial licenses under Section 4 and to set future
      Change Dates for new versions — to any third party at any time. Any
      such transaction shall not affect the Change Date already fixed for
      versions already released, nor the rights already granted under this
      Agreement to existing Users.

8. TERMINATION
------------------
Your rights under this Agreement for a given version terminate automatically
if You breach Section 4 with respect to that version. Termination under this
Agreement does not affect any version that has already reached its Change
Date and converted to the Change License — those rights are permanent and
independent of this Agreement. Upon termination of Your rights in a
still-restricted version, You must immediately cease all use of that
version that would otherwise fall under Section 4, and destroy all copies
in Your possession used for that restricted purpose.

9. DISCLAIMER OF WARRANTY
-----------------------------
THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED — INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE, AND NON-INFRINGEMENT. IN NO EVENT SHALL
THE LICENSOR OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES, OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT, OR OTHERWISE, ARISING
FROM, OUT OF, OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
DEALINGS IN THE SOFTWARE.

================================================================================
