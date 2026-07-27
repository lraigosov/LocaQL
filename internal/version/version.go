package version

// Name is fixed. Version/Commit/BuildDate are vars, not consts, so a
// reproducible release build can override them via
// -ldflags "-X github.com/lraigosov/LocaQL/internal/version.Version=... "
// (see Makefile) — the standard Go release convention. A plain
// `go build`/`go run` with no ldflags (ordinary local development) reports
// exactly these defaults, unchanged from before this became overridable.
const Name = "LocaQL"

var (
	Version   = "0.1.0-dev"
	Commit    = "none"
	BuildDate = "unknown"
)
