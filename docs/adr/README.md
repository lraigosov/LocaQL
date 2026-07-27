# Architecture Decision Records

Short records of significant, hard-to-reverse decisions and the context behind them — not a design document, not a
tutorial. Format: Status / Context / Decision / Consequences (Nygard-style). New ADRs are numbered sequentially and
never renumbered or deleted once accepted; a superseded decision gets a new ADR that says so and links back.

| # | Title |
| --- | --- |
| [0001](0001-independent-project-apache-2.0-non-affiliation.md) | Independent project, Apache-2.0, no affiliation with Google |
| [0002](0002-real-googlesql-engine-via-goccy-googlesqlite.md) | Real GoogleSQL execution via `goccy/googlesqlite`, not a hand-rolled interpreter |
| [0003](0003-sessions-and-transactions-in-locaqls-own-catalog.md) | Session temp tables and transactions live in LocaQL's own catalog, not the engine's |
