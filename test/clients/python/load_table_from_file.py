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
            },
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
