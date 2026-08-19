# 4. Transition from Apache-2.0 to Proprietary End User License Agreement (EULA)

## Status

Superseded by [ADR 0006](0006-revert-to-apache-2.0-retire-proprietary-eula.md), which reverts the project to Apache-2.0. Originally accepted (decided in Sesión 89 to protect intellectual property and future commercial monetization) and superseded the licensing part of [ADR 0001](0001-independent-project-apache-2.0-non-affiliation.md); kept here unedited as the historical record of that decision — no version of LocaQL was ever released under the proprietary terms this ADR adopted.

## Context

LocaQL was originally established under the open-source Apache License, Version 2.0 (see [ADR 0001](0001-independent-project-apache-2.0-non-affiliation.md)). While Apache-2.0 provided strong attribution requirements (Section 4(d)), it allowed unrestricted commercial redistribution, sublicensing, modification, and use by anyone, including large enterprises, without any compensation or royalty. 

As LocaQL matures and is considered a viable commercial asset or a solution whose rights/ownership may be sold to third parties in the future, retaining a fully permissive open-source license would severely devalue the project's intellectual property. However, to maintain its utility as a learning resource and developer-first platform, it remains desirable to permit non-commercial use, academic research, personal training, and limited personal local development testing.

## Decision

- **Transition to Proprietary EULA:** Supersede the Apache-2.0 license with a proprietary **End User License Agreement (EULA)**, located at `LICENSE` at the root of the repository.
- **Grant of Limited Free Use:** Permit free use strictly for academic and scientific research, personal education/training, and **Limited Local Development Testing** subject to strict operational bounds.
- **Enforcement of Local Development Limits:** To prevent abuse of the "local development" clause (such as hosting a shared local database for corporate teams or embedding it in corporate automation pipelines without paying), the EULA strictly mandates:
  - **Single-Workstation execution only:** No hosting on shared local networks, cloud instances, or shared servers.
  - **No automated corporate pipelines:** Prohibit execution inside organizational CI/CD, testing, or deployment pipelines.
  - **Data scale limit:** Limit the local catalog data to 100,000 rows per table and 1 GB of cumulative storage.
- **Contributor IP Assignment (CLA):** Per Section 7 of the EULA (renumbered by [ADR 0005](0005-license-hardening-third-party-split-cla-commercial-license.md)), any submitted code, pull requests, or suggestions automatically assign full copyright and intellectual property ownership to the Licensor (Luis Raigoso), ensuring the project can be sold or commercially licensed in the future without obtaining individual consent from community contributors.

## Consequences

- **Full IP Protection:** The project is no longer "open-source." It is proprietary software with visible source code under a restricted, revocable license.
- **Commercial and Corporate Security:** Organizations and teams that require shared instances, team environments, or automation inside corporate pipelines are legally required to purchase a commercial license, preserving future monetization avenues.
- **Asset Portability:** Selling the Software, the repository, or its intellectual property to a third party is legally straightforward, as there are no permissive redistribution rights in the wild for new versions, and all contributor code belongs exclusively to the Licensor.
- **Preserved Individual Utility:** Individual developers, students, and researchers can continue to use and learn from LocaQL on their personal machines, ensuring it remains an accessible tool for individual developers before deploying to Google Cloud.
