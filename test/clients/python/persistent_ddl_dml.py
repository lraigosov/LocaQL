"""Official google-cloud-bigquery smoke test for persistent SQL mutations."""

from __future__ import annotations

import argparse
import json

from google.auth.credentials import AnonymousCredentials
from google.cloud import bigquery


def run_dml(client: bigquery.Client, sql: str, expected: int) -> bigquery.QueryJob:
    job = client.query(sql)
    list(job.result(timeout=10))
    if not isinstance(job, bigquery.QueryJob):
        raise AssertionError(f"expected QueryJob, got {type(job).__name__}")
    if job.num_dml_affected_rows != expected:
        raise AssertionError(
            f"expected {expected} affected rows for {sql!r}, "
            f"got {job.num_dml_affected_rows!r}"
        )
    expected_statement_type = sql.lstrip().split(maxsplit=1)[0].upper()
    if job.statement_type != expected_statement_type:
        raise AssertionError(
            f"expected statement_type={expected_statement_type!r}, "
            f"got {job.statement_type!r}"
        )
    return job


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--endpoint", default="http://127.0.0.1:19050")
    args = parser.parse_args()

    client = bigquery.Client(
        project="p1",
        credentials=AnonymousCredentials(),
        client_options={"api_endpoint": args.endpoint},
    )

    list(
        client.query(
            "CREATE OR REPLACE TABLE analytics.python_sql_mutations "
            "(id INT64, name STRING, active BOOL)"
        ).result(timeout=10)
    )
    inserted = run_dml(
        client,
        "INSERT INTO analytics.python_sql_mutations (id, name, active) "
        "VALUES (1, 'one', TRUE), (2, NULL, FALSE)",
        2,
    )
    updated = run_dml(
        client,
        "UPDATE analytics.python_sql_mutations "
        "SET name = 'two', active = TRUE WHERE id = 2",
        1,
    )
    list(
        client.query(
            "CREATE OR REPLACE TABLE analytics.python_sql_merge_source AS "
            "SELECT 2 AS id, 'two-merged' AS name, FALSE AS active "
            "UNION ALL SELECT 3, 'three', TRUE"
        ).result(timeout=10)
    )
    merged = run_dml(
        client,
        "MERGE INTO analytics.python_sql_mutations AS T "
        "USING analytics.python_sql_merge_source AS S "
        "ON T.id = S.id "
        "WHEN MATCHED THEN UPDATE SET name = S.name, active = S.active "
        "WHEN NOT MATCHED THEN INSERT (id, name, active) "
        "VALUES (S.id, S.name, S.active)",
        2,
    )
    deleted = run_dml(
        client,
        "DELETE FROM analytics.python_sql_mutations WHERE id = 1",
        1,
    )

    list(
        client.query(
            "CREATE OR REPLACE TABLE analytics.python_sql_ctas AS "
            "SELECT id, name FROM analytics.python_sql_mutations WHERE active"
        ).result(timeout=10)
    )
    rows = [tuple(row) for row in client.query(
        "SELECT id, name, active FROM analytics.python_sql_mutations ORDER BY id"
    ).result(timeout=10)]
    if rows != [(2, "two-merged", False), (3, "three", True)]:
        raise AssertionError(f"persistent DML rows mismatch: {rows!r}")
    ctas_rows = [tuple(row) for row in client.query(
        "SELECT id, name FROM analytics.python_sql_ctas ORDER BY id"
    ).result(timeout=10)]
    if ctas_rows != [(3, "three")]:
        raise AssertionError(f"CTAS rows mismatch: {ctas_rows!r}")

    list(client.query("DROP TABLE analytics.python_sql_ctas").result(timeout=10))
    list(client.query("DROP TABLE IF EXISTS analytics.python_sql_ctas").result(timeout=10))
    list(client.query("DROP TABLE analytics.python_sql_merge_source").result(timeout=10))

    print(
        json.dumps(
            {
                "delete": deleted.num_dml_affected_rows,
                "insert": inserted.num_dml_affected_rows,
                "merge": merged.num_dml_affected_rows,
                "rows": rows,
                "update": updated.num_dml_affected_rows,
            },
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
