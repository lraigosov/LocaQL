"""Official google-cloud-storage smoke test for LocaQL media downloads.

Run against a local server configured with LOCAQL_FAKE_GCS_ROOT, for example:

    python test/clients/python/storage_download.py --endpoint http://127.0.0.1:19050
"""

from __future__ import annotations

import argparse
import json

from google.auth.credentials import AnonymousCredentials
from google.cloud import storage


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--endpoint", default="http://127.0.0.1:19050")
    args = parser.parse_args()

    client = storage.Client(
        project="p1",
        credentials=AnonymousCredentials(),
        client_options={"api_endpoint": args.endpoint},
    )
    bucket = client.create_bucket("python-storage-downloads")
    blob = bucket.blob("folder/nested report.txt")
    payload = b"0123456789abcdef"

    blob.upload_from_string(payload, content_type="application/octet-stream")
    downloaded = blob.download_as_bytes(checksum="md5")
    if downloaded != payload:
        raise AssertionError(f"full download mismatch: {downloaded!r}")

    partial = blob.download_as_bytes(start=4, end=9)
    if partial != b"456789":
        raise AssertionError(f"range download mismatch: {partial!r}")

    expected_path = (
        "/download/storage/v1/b/python-storage-downloads/o/"
        "folder%2Fnested%20report.txt?alt=media"
    )
    if not blob.media_link or not blob.media_link.endswith(expected_path):
        raise AssertionError(f"unexpected mediaLink: {blob.media_link!r}")

    print(
        json.dumps(
            {
                "bucket": bucket.name,
                "full_bytes": len(downloaded),
                "media_link": blob.media_link,
                "object": blob.name,
                "range": partial.decode("ascii"),
            },
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
