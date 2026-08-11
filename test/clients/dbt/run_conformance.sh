#!/usr/bin/env bash
# Official dbt-bigquery adapter conformance test, mirroring
# test/clients/{python,node,go,java,ruby,bq}/persistent_ddl_dml.* so every
# official client/tool exercises the same real server behavior. This one
# exercises LocaQL through dbt's own model-materialization macros rather than
# hand-written SQL: view, table (CTAS), and incremental/MERGE, each run twice
# (a fresh create, then a real replace/merge against existing data) plus once
# more with --full-refresh, since dbt's default macros generate SQL shapes
# (per-segment-backtick-quoted `` `project`.`dataset`.`table` `` identifiers,
# `MERGE ... USING (<subquery>)`) that were never exercised by this project's
# own hand-written persistent-SQL tests and surfaced several real bugs before
# being fixed — see devlog.md and KNOWN-DIVERGENCES.md.
#
# dbt-bigquery has no documented way to point at a non-Google REST endpoint,
# but its BigQueryCredentials dataclass carries a real, if undocumented,
# api_endpoint field wired straight into
# google.cloud.bigquery.Client(client_options=ClientOptions(api_endpoint=...)),
# and its "oauth-secrets" connection method accepts a literal token string
# with no real refresh_token/client_id — a google.oauth2.credentials.Credentials
# built that way never attempts a real OAuth refresh, so no real GCP call is
# ever made. profiles.yml here uses exactly that combination.
#
# Usage: run_conformance.sh --endpoint=http://127.0.0.1:19130
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")"

endpoint="http://127.0.0.1:19130"
for arg in "$@"; do
  case "$arg" in
    --endpoint=*) endpoint="${arg#--endpoint=}" ;;
  esac
done

export LOCAQL_ENDPOINT="$endpoint"
export DBT_PROFILES_DIR="$PWD"

curl --fail --silent -X POST "$endpoint/bigquery/v2/projects/p1/datasets" \
  -H 'Content-Type: application/json' \
  -d '{"datasetReference":{"datasetId":"analytics"}}' >/dev/null 2>&1 || true

echo "=== dbt debug ==="
dbt debug

echo "=== dbt run (fresh create: view, table, incremental) ==="
dbt run

echo "=== dbt run (second pass: real CREATE OR REPLACE VIEW/TABLE, real MERGE) ==="
dbt run

echo "=== dbt run --full-refresh (forces the incremental model to rebuild) ==="
dbt run --full-refresh

echo "All dbt conformance runs completed successfully."
