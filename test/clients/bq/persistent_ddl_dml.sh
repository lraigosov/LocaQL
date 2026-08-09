#!/usr/bin/env bash
# Official `bq` CLI smoke test for persistent SQL mutations, mirroring
# test/clients/python/persistent_ddl_dml.py,
# test/clients/node/persistent_ddl_dml.js,
# test/clients/go/persistent_ddl_dml.go and
# test/clients/java/.../PersistentDdlDml.java so every official client
# exercises the same real server behavior. `bq` has no streaming-insert
# subcommand that avoids the jobs.* resource (`bq insert` uses
# tabledata.insertAll, which is deliberately out of scope for LocaQL's
# hand-written discovery document — see KNOWN-DIVERGENCES.md), so this
# covers the DDL/DML mutation path only.
#
# Usage: persistent_ddl_dml.sh --endpoint=http://127.0.0.1:19050
set -euo pipefail

endpoint="http://127.0.0.1:19050"
for arg in "$@"; do
  case "$arg" in
    --endpoint=*) endpoint="${arg#--endpoint=}" ;;
  esac
done

# `bq` refuses to run at all without an "active account" configured locally,
# even though it never actually validates a token against LocaQL (which is
# anonymous-only, by design). CLOUDSDK_AUTH_ACCESS_TOKEN supplies a literal
# bearer token bq attaches to its requests without checking it against real
# Google infrastructure — LocaQL never inspects it either.
export CLOUDSDK_AUTH_ACCESS_TOKEN="locaql-conformance-test"

bq_query() {
  bq --api "$endpoint" --headless query --project_id=p1 --nouse_legacy_sql "$1"
}

affected_rows() {
  # `bq query`'s DML output has no --format=json shape (there are no result
  # rows to format) — it always prints a human-readable
  # "Number of affected rows: N" line, so that's what gets parsed here.
  local output="$1"
  echo "$output" | grep -oE 'Number of affected rows: [0-9]+' | grep -oE '[0-9]+'
}

assert_affected_rows() {
  local label="$1" output="$2" expected="$3"
  local got
  got=$(affected_rows "$output")
  if [ "$got" != "$expected" ]; then
    echo "FAIL: expected $expected affected rows for $label, got '$got'" >&2
    echo "$output" >&2
    exit 1
  fi
}

bq_query "CREATE OR REPLACE TABLE analytics.bq_sql_mutations (id INT64, name STRING, active BOOL)" >/dev/null

inserted_out=$(bq_query "INSERT INTO analytics.bq_sql_mutations (id, name, active) VALUES (1, 'one', TRUE), (2, NULL, FALSE)")
assert_affected_rows insert "$inserted_out" 2

updated_out=$(bq_query "UPDATE analytics.bq_sql_mutations SET name = 'two', active = TRUE WHERE id = 2")
assert_affected_rows update "$updated_out" 1

bq_query "CREATE OR REPLACE TABLE analytics.bq_sql_merge_source AS SELECT 2 AS id, 'two-merged' AS name, FALSE AS active UNION ALL SELECT 3, 'three', TRUE" >/dev/null

merged_out=$(bq_query "MERGE INTO analytics.bq_sql_mutations AS T USING analytics.bq_sql_merge_source AS S ON T.id = S.id WHEN MATCHED THEN UPDATE SET name = S.name, active = S.active WHEN NOT MATCHED THEN INSERT (id, name, active) VALUES (S.id, S.name, S.active)")
assert_affected_rows merge "$merged_out" 2

deleted_out=$(bq_query "DELETE FROM analytics.bq_sql_mutations WHERE id = 1")
assert_affected_rows delete "$deleted_out" 1

rows_json=$(bq --api "$endpoint" --headless query --project_id=p1 --nouse_legacy_sql --format=json "SELECT id, name, active FROM analytics.bq_sql_mutations ORDER BY id")
expected_json='[{"active":"false","id":"2","name":"two-merged"},{"active":"true","id":"3","name":"three"}]'
if [ "$rows_json" != "$expected_json" ]; then
  echo "FAIL: persistent DML rows mismatch" >&2
  echo "  got:      $rows_json" >&2
  echo "  expected: $expected_json" >&2
  exit 1
fi

bq_query "DROP TABLE analytics.bq_sql_merge_source" >/dev/null

echo "{\"delete\":1,\"insert\":2,\"merge\":2,\"rows\":$rows_json,\"update\":1}"
