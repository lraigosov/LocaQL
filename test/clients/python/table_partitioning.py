"""Official google-cloud-bigquery smoke test for partitioned tables."""

from __future__ import annotations

import argparse
import json
from datetime import datetime, timedelta, timezone

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

    second_partition_date = datetime.now(timezone.utc).date()
    first_partition_date = second_partition_date - timedelta(days=1)

    list(
        client.query(
            "INSERT INTO analytics.python_partitioned "
            "(event_date, user_id, score) VALUES "
            f"(DATE '{first_partition_date.isoformat()}', 'a', 1), "
            f"(DATE '{first_partition_date.isoformat()}', 'b', 2), "
            f"(DATE '{second_partition_date.isoformat()}', 'c', 3)"
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
            f"WHERE event_date = DATE '{first_partition_date.isoformat()}' "
            "ORDER BY user_id"
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
    expected_partitions = {
        first_partition_date.strftime("%Y%m%d"): 2,
        second_partition_date.strftime("%Y%m%d"): 1,
    }
    if time_partitions != expected_partitions:
        raise AssertionError(f"unexpected INFORMATION_SCHEMA partitions: {time_partitions!r}")

    ingestion_table_id = "p1.analytics.python_ingestion_partitioned"
    ingestion_table = bigquery.Table(
        ingestion_table_id,
        schema=[
            bigquery.SchemaField("id", "INT64"),
            bigquery.SchemaField("label", "STRING"),
        ],
    )
    ingestion_table.time_partitioning = bigquery.TimePartitioning(
        type_=bigquery.TimePartitioningType.DAY
    )
    ingestion_table.require_partition_filter = True
    created_ingestion = client.create_table(ingestion_table)
    if created_ingestion.require_partition_filter is not True:
        raise AssertionError("ingestion require_partition_filter did not round-trip")
    if (
        created_ingestion.time_partitioning is None
        or created_ingestion.time_partitioning.field is not None
    ):
        raise AssertionError(
            f"unexpected ingestion partitioning: {created_ingestion.time_partitioning!r}"
        )

    list(
        client.query(
            "INSERT INTO analytics.python_ingestion_partitioned "
            "VALUES (1, 'first')"
        ).result(timeout=10)
    )
    try:
        list(
            client.query(
                "SELECT id FROM analytics.python_ingestion_partitioned"
            ).result(timeout=10)
        )
    except BadRequest as exc:
        if "partition" not in str(exc).lower():
            raise AssertionError(
                f"unexpected ingestion missing-filter error: {exc}"
            ) from exc
    else:
        raise AssertionError("ingestion query without required filter succeeded")

    wildcard_rows = list(
        client.query(
            "SELECT * FROM analytics.python_ingestion_partitioned "
            "WHERE _PARTITIONDATE IS NOT NULL"
        ).result(timeout=10)
    )
    if len(wildcard_rows) != 1 or list(wildcard_rows[0].keys()) != ["id", "label"]:
        raise AssertionError(
            f"ingestion pseudocolumns leaked through SELECT *: {wildcard_rows!r}"
        )
    pseudo_rows = list(
        client.query(
            "SELECT _PARTITIONDATE, _PARTITIONTIME, id, label "
            "FROM analytics.python_ingestion_partitioned "
            "WHERE _PARTITIONTIME IS NOT NULL"
        ).result(timeout=10)
    )
    if (
        len(pseudo_rows) != 1
        or pseudo_rows[0][0] is None
        or pseudo_rows[0][1] is None
        or pseudo_rows[0][2:] != (1, "first")
    ):
        raise AssertionError(f"unexpected ingestion pseudocolumn row: {pseudo_rows!r}")

    list(
        client.query(
            "UPDATE analytics.python_ingestion_partitioned SET label = 'updated' "
            "WHERE _PARTITIONDATE IS NOT NULL"
        ).result(timeout=10)
    )
    updated_ingestion_rows = list(
        client.query(
            "SELECT label FROM analytics.python_ingestion_partitioned "
            "WHERE _PARTITIONDATE IS NOT NULL"
        ).result(timeout=10)
    )
    if [row[0] for row in updated_ingestion_rows] != ["updated"]:
        raise AssertionError(
            f"ingestion DML did not persist: {updated_ingestion_rows!r}"
        )
    try:
        list(
            client.query(
                "UPDATE analytics.python_ingestion_partitioned "
                "SET _PARTITIONDATE = CURRENT_DATE() "
                "WHERE _PARTITIONDATE IS NOT NULL"
            ).result(timeout=10)
        )
    except BadRequest as exc:
        if "read-only" not in str(exc).lower():
            raise AssertionError(f"unexpected pseudocolumn write error: {exc}") from exc
    else:
        raise AssertionError("read-only ingestion pseudocolumn was writable")

    print(
        json.dumps(
            {
                "clustering_fields": updated.clustering_fields,
                "filtered_rows": [row[0] for row in filtered_rows],
                "partition_expiration_ms": updated.time_partitioning.expiration_ms,
                "partitions": time_partitions,
                "ingestion": {
                    "date": pseudo_rows[0][0].isoformat(),
                    "label": updated_ingestion_rows[0][0],
                    "timestamp": pseudo_rows[0][1].isoformat(),
                    "wildcard_fields": list(wildcard_rows[0].keys()),
                },
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
