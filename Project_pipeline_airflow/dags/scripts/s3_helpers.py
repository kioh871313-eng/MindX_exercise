"""
s3_helpers.py
-------------
Utility functions for interacting with Amazon S3.
All connections are made via Airflow's S3Hook so that credentials
are never hardcoded — configure the connection in the Airflow UI:
  Conn Id  : aws_default
  Conn Type: Amazon Web Services

CDC strategy
------------
Checkpoint is stored as a full ISO-8601 timestamp (not just a date)
in the Airflow Variable 'pipeline_last_run_ts', e.g.:
    "2024-11-18T08:00:00+00:00"

get_new_files_since() compares S3 LastModified against this timestamp.
Using a timestamp (vs a date) correctly handles the case where the same
S3 file is overwritten multiple times per day — each overwrite updates
the file's LastModified, and any run after that timestamp will pick it up.

The checkpoint is set to the pipeline START time (not end time) so that
files uploaded while the pipeline is running are caught on the next run.
"""

from __future__ import annotations

import io
import logging
from datetime import datetime, timezone, timedelta
from typing import List

import pandas as pd
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

logger = logging.getLogger(__name__)

# Airflow Variable key used as CDC timestamp checkpoint
CDC_TS_VARIABLE = "pipeline_last_run_ts"

# Fallback timestamp when no checkpoint exists yet (epoch = load everything)
CDC_DEFAULT_TS = "2000-01-01T00:00:00+00:00"


def get_s3_hook(aws_conn_id: str = "aws_default") -> S3Hook:
    """Return an authenticated S3Hook using an Airflow connection."""
    return S3Hook(aws_conn_id=aws_conn_id)


def list_s3_files(
    bucket: str,
    prefix: str,
    aws_conn_id: str = "aws_default",
) -> List[str]:
    """
    List all object keys in a given S3 bucket/prefix.

    Parameters
    ----------
    bucket : str
    prefix : str
    aws_conn_id : str

    Returns
    -------
    List[str]
    """
    hook = get_s3_hook(aws_conn_id)
    keys = hook.list_keys(bucket_name=bucket, prefix=prefix)
    # Filter out folder-level placeholder keys (end with /)
    keys = [k for k in (keys or []) if not k.endswith("/")]
    logger.info("Found %d files under s3://%s/%s", len(keys), bucket, prefix)
    return keys


def read_csv_from_s3(
    bucket: str,
    key: str,
    aws_conn_id: str = "aws_default",
    **pandas_kwargs,
) -> pd.DataFrame:
    """
    Read a CSV file from S3 into a pandas DataFrame.

    Parameters
    ----------
    bucket : str
    key : str
        Full object key within the bucket.
    aws_conn_id : str
    **pandas_kwargs
        Forwarded to pd.read_csv().

    Returns
    -------
    pd.DataFrame
    """
    hook = get_s3_hook(aws_conn_id)
    s3_object = hook.get_key(key=key, bucket_name=bucket)
    content = s3_object.get()["Body"].read()
    df = pd.read_csv(io.BytesIO(content), **pandas_kwargs)
    logger.info("Read %d rows from s3://%s/%s", len(df), bucket, key)
    return df


def get_new_files_since(
    bucket: str,
    prefix: str,
    last_run_ts: str,
    aws_conn_id: str = "aws_default",
) -> List[str]:
    """
    Return S3 keys whose LastModified timestamp is strictly after last_run_ts.

    Uses a full ISO-8601 timestamp (not just a date) to correctly handle
    multiple pipeline runs per day against the same S3 file.

    Parameters
    ----------
    bucket : str
    prefix : str
    last_run_ts : str
        ISO-8601 timestamp string with timezone, e.g.:
        "2024-11-18T08:00:00+00:00"
        Use CDC_DEFAULT_TS for initial load (picks up all files).
    aws_conn_id : str

    Returns
    -------
    List[str]
        Keys whose LastModified > last_run_ts.
    """
    hook = get_s3_hook(aws_conn_id)
    all_keys = hook.list_keys(bucket_name=bucket, prefix=prefix) or []

    cutoff = datetime.fromisoformat(last_run_ts)
    # Ensure timezone-aware for comparison
    if cutoff.tzinfo is None:
        cutoff = cutoff.replace(tzinfo=timezone.utc)

    new_keys = []
    for key in all_keys:
        # Skip folder-level keys (S3 "directory" placeholders end with /)
        if key.endswith("/"):
            logger.debug("Skipping folder key: %s", key)
            continue
        obj = hook.get_key(key=key, bucket_name=bucket)
        # S3 LastModified is always timezone-aware (UTC)
        if obj.last_modified > cutoff:
            new_keys.append(key)
            logger.debug(
                "New file detected: %s (LastModified=%s > cutoff=%s)",
                key, obj.last_modified, cutoff,
            )

    logger.info(
        "CDC filter: %d / %d files modified after %s",
        len(new_keys), len(all_keys), last_run_ts,
    )
    return new_keys


def get_pipeline_start_ts() -> str:
    """
    Return the current UTC time as an ISO-8601 string.
    Call this at the START of a pipeline run to capture the checkpoint
    before any S3 reads or DB writes happen.

    Storing the start time (not end time) ensures that files uploaded
    while the pipeline is running are not missed — they will have
    LastModified > this timestamp and will be picked up next run.

    Returns
    -------
    str
        e.g. "2024-11-18T08:00:00+00:00"
    """
    return datetime.now(tz=timezone.utc).isoformat()
