"""Official google-cloud-bigquery smoke test for partitioned tables."""

from __future__ import annotations

import argparse
import json

from google.api_core.exceptions import BadRequest
from google.auth.credentials import AnonymousCredentials
from google.cloud import bigquery


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--endpoint", default="http://127.0.0.1:19050")
    args = parser.parse_args()

    client = bigquery.Client(
        project="p1",
        credentials=AnonymousCredentials(),
        client_options={"api_endpoint": args.endpoint},
    )

    table_id = "p1.analytics.python_partitioned"
    table = bigquery.Table(
        table_id,
        schema=[
            bigquery.SchemaField("event_date", "DATE"),
            bigquery.SchemaField("user_id", "STRING"),
            bigquery.SchemaField("score", "INT64"),
        ],
    )
    table.time_partitioning = bigquery.TimePartitioning(
        type_=bigquery.TimePartitioningType.DAY,
        field="event_date",
        expiration_ms=86_400_000,
    )
    table.clustering_fields = ["user_id", "score"]
    table.require_partition_filter = True
    created = client.create_table(table)

    if created.time_partitioning is None:
        raise AssertionError("time_partitioning was not returned")
    if created.time_partitioning.field != "event_date":
        raise AssertionError(
            f"unexpected partition field: {created.time_partitioning.field!r}"
        )
    if created.time_partitioning.expiration_ms != 86_400_000:
        raise AssertionError(
            "unexpected partition expiration: "
            f"{created.time_partitioning.expiration_ms!r}"
        )
    if created.clustering_fields != ["user_id", "score"]:
        raise AssertionError(
            f"unexpected clustering fields: {created.clustering_fields!r}"
        )
    if created.require_partition_filter is not True:
        raise AssertionError("require_partition_filter was not returned as true")

    created.time_partitioning.expiration_ms = 172_800_000
    created.clustering_fields = ["score"]
    updated = client.update_table(
        created,
        ["time_partitioning", "clustering_fields", "require_partition_filter"],
    )
    if updated.time_partitioning.expiration_ms != 172_800_000:
        raise AssertionError("partition expiration patch did not round-trip")
    if updated.clustering_fields != ["score"]:
        raise AssertionError("clustering patch did not round-trip")

    list(
        client.query(
            "INSERT INTO analytics.python_partitioned "
            "(event_date, user_id, score) VALUES "
            "(DATE '2026-08-05', 'a', 1), "
            "(DATE '2026-08-05', 'b', 2), "
            "(DATE '2026-08-06', 'c', 3)"
        ).result(timeout=10)
    )

    try:
        list(
            client.query(
                "SELECT user_id FROM analytics.python_partitioned"
            ).result(timeout=10)
        )
    except BadRequest as exc:
        if "partition" not in str(exc).lower():
            raise AssertionError(f"unexpected missing-filter error: {exc}") from exc
    else:
        raise AssertionError("query without the required partition filter succeeded")

    filtered_rows = list(
        client.query(
            "SELECT user_id FROM analytics.python_partitioned "
            "WHERE event_date = DATE '2026-08-05' ORDER BY user_id"
        ).result(timeout=10)
    )
    if [row[0] for row in filtered_rows] != ["a", "b"]:
        raise AssertionError(f"unexpected filtered rows: {filtered_rows!r}")

    range_table_id = "p1.analytics.python_range_partitioned"
    range_table = bigquery.Table(
        range_table_id,
        schema=[
            bigquery.SchemaField("bucket_id", "INT64"),
            bigquery.SchemaField("label", "STRING"),
        ],
    )
    range_table.range_partitioning = bigquery.RangePartitioning(
        field="bucket_id",
        range_=bigquery.PartitionRange(start=0, end=100, interval=10),
    )
    range_table.require_partition_filter = True
    created_range = client.create_table(range_table)
    if created_range.range_partitioning is None:
        raise AssertionError("range_partitioning was not returned")
    partition_range = created_range.range_partitioning.range_
    if (
        created_range.range_partitioning.field != "bucket_id"
        or partition_range.start != 0
        or partition_range.end != 100
        or partition_range.interval != 10
    ):
        raise AssertionError(
            f"unexpected range partitioning: {created_range.range_partitioning!r}"
        )

    partition_rows = list(
        client.query(
            "SELECT * FROM analytics.INFORMATION_SCHEMA.PARTITIONS"
        ).result(timeout=10)
    )
    time_partitions = {
        row[3]: row[4]
        for row in partition_rows
        if row[2] == "python_partitioned"
    }
    if time_partitions != {"20260805": 2, "20260806": 1}:
        raise AssertionError(f"unexpected INFORMATION_SCHEMA partitions: {time_partitions!r}")

    print(
        json.dumps(
            {
                "clustering_fields": updated.clustering_fields,
                "filtered_rows": [row[0] for row in filtered_rows],
                "partition_expiration_ms": updated.time_partitioning.expiration_ms,
                "partitions": time_partitions,
                "range": {
                    "end": partition_range.end,
                    "interval": partition_range.interval,
                    "start": partition_range.start,
                },
            },
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
