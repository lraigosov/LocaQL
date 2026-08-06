"""Official google-cloud-bigquery smoke test for LocaQL file uploads.

Run against a local server, for example:

    python test/clients/python/load_table_from_file.py --endpoint http://127.0.0.1:19050
"""

from __future__ import annotations

import argparse
import io
import json

from google.auth.credentials import AnonymousCredentials
from google.cloud import bigquery


def load_csv(
    client: bigquery.Client,
    table_id: str,
    payload: bytes,
    *,
    size: int | None,
) -> bigquery.LoadJob:
    config = bigquery.LoadJobConfig(
        schema=[
            bigquery.SchemaField("event_id", "INT64"),
            bigquery.SchemaField("event_name", "STRING"),
        ],
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )
    job = client.load_table_from_file(
        io.BytesIO(payload),
        table_id,
        job_config=config,
        size=size,
    )
    if not isinstance(job, bigquery.LoadJob):
        raise AssertionError(f"expected LoadJob, got {type(job).__name__}")
    job.result(timeout=10)
    if job.output_rows != 2:
        raise AssertionError(f"expected two loaded rows, got {job.output_rows}")
    return job


def validate_nullable_values(client: bigquery.Client) -> dict[str, object]:
    table_id = "p1.analytics.python_nullable_values"
    payload = (
        b'{"id":1,"null_text":null,"empty_text":"",'
        b'"zero_value":0,"false_value":false}\n'
    )
    config = bigquery.LoadJobConfig(
        schema=[
            bigquery.SchemaField("id", "INT64", mode="REQUIRED"),
            bigquery.SchemaField("null_text", "STRING"),
            bigquery.SchemaField("empty_text", "STRING"),
            bigquery.SchemaField("zero_value", "INT64"),
            bigquery.SchemaField("false_value", "BOOL"),
        ],
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )
    job = client.load_table_from_file(
        io.BytesIO(payload),
        table_id,
        job_config=config,
        size=len(payload),
    )
    job.result(timeout=10)

    table = client.get_table(table_id)
    rows = list(client.list_rows(table))
    if len(rows) != 1:
        raise AssertionError(f"expected one nullable row, got {len(rows)}")
    row = rows[0]
    actual = (
        row["null_text"],
        row["empty_text"],
        row["zero_value"],
        row["false_value"],
    )
    expected = (None, "", 0, False)
    if actual != expected:
        raise AssertionError(f"nullable row mismatch: expected {expected!r}, got {actual!r}")

    query = client.query(
        "SELECT COUNT(null_text) AS null_count, "
        "COUNT(empty_text) AS empty_count "
        "FROM analytics.python_nullable_values"
    )
    query_rows = list(query.result(timeout=10))
    if len(query_rows) != 1 or tuple(query_rows[0]) != (0, 1):
        raise AssertionError(f"nullable COUNT mismatch: {query_rows!r}")

    return {
        "empty_text": row["empty_text"],
        "false_value": row["false_value"],
        "null_text": row["null_text"],
        "zero_value": row["zero_value"],
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--endpoint", default="http://127.0.0.1:19050")
    args = parser.parse_args()

    client = bigquery.Client(
        project="p1",
        credentials=AnonymousCredentials(),
        client_options={"api_endpoint": args.endpoint},
    )
    payload = b"event_id,event_name\n1,page_view\n2,checkout\n"

    multipart_job = load_csv(
        client,
        "p1.analytics.python_multipart_upload",
        payload,
        size=len(payload),
    )
    resumable_job = load_csv(
        client,
        "p1.analytics.python_resumable_upload",
        payload,
        size=None,
    )
    nullable = validate_nullable_values(client)

    print(
        json.dumps(
            {
                "multipart": {
                    "job_id": multipart_job.job_id,
                    "output_rows": multipart_job.output_rows,
                },
                "resumable": {
                    "job_id": resumable_job.job_id,
                    "output_rows": resumable_job.output_rows,
                },
                "nullable": nullable,
            },
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
