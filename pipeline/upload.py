"""
Cloudflare R2 upload (SR-P-22, SR-P-24).

Uses the S3-compatible R2 API via boto3.
Upload is atomic: writes to a staging key first, then renames to the live key.
The previous live file is not removed until the new upload is confirmed.
"""

import logging
from pathlib import Path

import boto3
from botocore.config import Config

log = logging.getLogger(__name__)

_LIVE_KEY = "places.pmtiles"
_STAGING_KEY = "places.pmtiles.staging"


def upload_to_r2(pmtiles_path: Path, config: dict) -> None:
    client = boto3.client(
        "s3",
        endpoint_url=config["r2_endpoint_url"],
        aws_access_key_id=config["r2_access_key_id"],
        aws_secret_access_key=config["r2_secret_access_key"],
        config=Config(signature_version="s3v4"),
    )
    bucket = config["r2_bucket"]

    # Step 1 — upload to staging key
    log.info("Uploading to staging key: %s", _STAGING_KEY)
    client.upload_file(
        str(pmtiles_path),
        bucket,
        _STAGING_KEY,
        ExtraArgs={"ContentType": "application/octet-stream"},
    )
    log.info("Staging upload complete")

    # Step 2 — verify staging object is accessible
    head = client.head_object(Bucket=bucket, Key=_STAGING_KEY)
    expected_size = pmtiles_path.stat().st_size
    actual_size = head["ContentLength"]
    if actual_size != expected_size:
        raise RuntimeError(
            f"Upload size mismatch: expected {expected_size}, got {actual_size}"
        )
    log.info("Staging object verified (%d bytes)", actual_size)

    # Step 3 — atomically promote staging → live
    # R2 supports server-side copy; the live object is replaced only after copy succeeds
    log.info("Promoting staging → live key: %s", _LIVE_KEY)
    client.copy_object(
        Bucket=bucket,
        CopySource={"Bucket": bucket, "Key": _STAGING_KEY},
        Key=_LIVE_KEY,
    )

    # Step 4 — clean up staging key
    client.delete_object(Bucket=bucket, Key=_STAGING_KEY)
    log.info("Staging key removed — live file updated")
