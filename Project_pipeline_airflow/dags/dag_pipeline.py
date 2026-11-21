"""
dag_pipeline.py
---------------
DAG: hardware_sales_pipeline
Schedule: @hourly

Single end-to-end pipeline DAG for the Computer Hardware Sales ETL.
Replaces the previous three separate DAGs (bronze_load, silver_transform,
gold_build) with one unified flow so each layer triggers immediately after
the previous one completes — no fixed schedule gaps, no ExternalTaskSensor.

Flow
----
capture_checkpoint
    └── extract_load_bronze
            └── cleanup_bronze_snapshots
                    └── validate_bronze
                            └── transform_silver
                                    └── validate_silver
                                            ├── build_dim_date     ┐
                                            ├── build_dim_geography│ parallel
                                            ├── build_dim_product  │
                                            └── build_dim_customer ┘
                                                    └── build_fact_sales
                                                            └── validate_gold
                                                                    └── update_cdc_checkpoint

CDC strategy
------------
Checkpoint variable: "pipeline_last_run_ts"  (ISO-8601 UTC timestamp)
The checkpoint is captured at the START of each run (before any S3 read).
Only S3 files with LastModified > checkpoint are processed.
Supports: multiple runs per day, same file overwritten, on-demand triggers.

Bronze idempotency
------------------
Snapshot model: every load appends a new batch (no ON CONFLICT).
cleanup_bronze_snapshots removes old batches after each load,
keeping only the latest snapshot per _source_file.

Email alerts
------------
One consolidated email per validation task (not per check).
  [FAILURE] — sent when any hard-fail check fails (pipeline stopped)
  [WARNING] — sent when only soft checks fail (pipeline continued)
  No email  — sent when all checks pass

SMTP credentials: Airflow Connection 'smtp_default' (encrypted at rest)
Recipient: ALERT_EMAIL env var in .env (not sensitive)

Credentials
-----------
All external credentials managed via Airflow UI Connections:
  aws_default   — AWS S3
  postgres_dwh  — PostgreSQL Data Warehouse
  smtp_default  — SMTP email (optional, for alerts)
"""

from __future__ import annotations

import logging
import os
from datetime import datetime, timedelta
from typing import List, Optional, Tuple

import pandas as pd
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.operators.empty import EmptyOperator

from scripts.s3_helpers import (
    read_csv_from_s3,
    get_new_files_since,
    get_pipeline_start_ts,
    CDC_DEFAULT_TS,
)
from scripts.db_helpers import get_pg_hook, write_log
from scripts.transformations import (
    run_bronze_to_silver,
    run_dim_date,
    run_dim_geography,
    run_dim_product,
    run_dim_customer,
    run_fact_sales,
)

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
DAG_ID        = "hardware_sales_pipeline"
CDC_VAR       = "pipeline_last_run_ts"
S3_BUCKET_VAR = "s3_bucket_name"
S3_PREFIX_VAR = "s3_prefix"
BRONZE_SCHEMA = "bronze"
BRONZE_TABLE  = "raw_hardware_sales"

# Recipient email for alerts — not sensitive, just an address
ALERT_EMAIL = os.environ.get("ALERT_EMAIL", "")

# ---------------------------------------------------------------------------
# Check result type: (check_name, message, value)
# ---------------------------------------------------------------------------
CheckResult = Tuple[str, str, object]


# =============================================================================
# EMAIL HELPER
# =============================================================================

def _send_validation_email(
    task_id: str,
    run_id: str,
    errors: List[CheckResult],
    warns: List[CheckResult],
    passed: List[str],
    layer: str,
) -> None:
    """
    Send one consolidated email summarising all validation check results.

    Called once at the END of each validation task.
    - [FAILURE] email when hard-fail errors exist (pipeline stopped).
    - [WARNING] email when only soft checks failed (pipeline continued).
    - No email when everything passed cleanly.

    SMTP credentials are read from Airflow Connection 'smtp_default':
      Admin → Connections → (+) Add
        Conn Id   : smtp_default
        Conn Type : Generic
        Host      : smtp.gmail.com
        Login     : sender@gmail.com
        Password  : gmail_app_password  ← encrypted at rest by Airflow
        Port      : 587
    """
    if not ALERT_EMAIL:
        logger.info("ALERT_EMAIL not configured — skipping email.")
        return
    if not errors and not warns:
        return  # all clean, nothing to report

    # Read SMTP credentials from Airflow Connection
    try:
        from airflow.hooks.base import BaseHook
        conn          = BaseHook.get_connection("smtp_default")
        smtp_host     = conn.host or ""
        smtp_user     = conn.login or ""
        smtp_password = conn.password or ""
        smtp_port     = int(conn.port) if conn.port else 587
    except Exception:
        logger.warning(
            "Airflow Connection 'smtp_default' not found — skipping email. "
            "Create it via Admin → Connections to enable alerts."
        )
        return

    if not smtp_host or not smtp_user:
        logger.warning("smtp_default: missing host or login — skipping email.")
        return

    # Subject
    if errors:
        prefix      = "[FAILURE]"
        status_line = "FAILED"
        detail      = "{} error(s)".format(len(errors))
    else:
        prefix      = "[WARNING]"
        status_line = "PASSED WITH WARNINGS"
        detail      = "{} warning(s)".format(len(warns))

    subject = "{} {} — {} ({})".format(prefix, DAG_ID, task_id, detail)

    # Body
    sep   = "─" * 60
    lines = [
        "DAG        : {}".format(DAG_ID),
        "Task       : {}".format(task_id),
        "Layer      : {}".format(layer.upper()),
        "Run ID     : {}".format(run_id),
        "Status     : {}".format(status_line),
        "",
    ]

    if errors:
        lines += [sep, "FAILURES ({})  — pipeline stopped".format(len(errors)), sep]
        for i, (name, msg, val) in enumerate(errors, 1):
            lines += [
                "[{}] {}".format(i, name),
                "    Message : {}".format(msg),
                "    Value   : {}".format(val),
                "",
            ]

    if warns:
        lines += [sep, "WARNINGS ({})  — pipeline continued".format(len(warns)), sep]
        for i, (name, msg, val) in enumerate(warns, 1):
            lines += [
                "[{}] {}".format(i, name),
                "    Message : {}".format(msg),
                "    Value   : {}".format(val),
                "",
            ]

    if passed:
        lines += [
            sep,
            "PASSED ({})".format(len(passed)),
            sep,
            ", ".join(passed),
            "",
        ]

    lines += [
        sep,
        "INVESTIGATE",
        sep,
        "Query pipeline log:",
        "  SELECT dag_id, task_id, layer, status, error_message, logged_at",
        "  FROM logs.pipeline_log",
        "  WHERE run_id = '{}'".format(run_id),
        "  ORDER BY logged_at;",
        "",
        "Check silver data quality issues:",
        "  SELECT order_id, order_date, ship_date, quantity, unit_price",
        "  FROM silver.clean_hardware_sales",
        "  WHERE ship_date < order_date  -- swapped dates",
        "     OR quantity IS NULL         -- failed numeric cast",
        "  LIMIT 50;",
    ]

    body = "\n".join(lines)

    try:
        import smtplib
        from email.mime.text import MIMEText
        mime_msg            = MIMEText("<pre style='font-family:monospace'>{}</pre>".format(body), "html")
        mime_msg["Subject"] = subject
        mime_msg["From"]    = smtp_user
        mime_msg["To"]      = ALERT_EMAIL
        with smtplib.SMTP(smtp_host, smtp_port) as server:
            server.starttls()
            server.login(smtp_user, smtp_password)
            server.sendmail(smtp_user, [ALERT_EMAIL], mime_msg.as_string())
        logger.info("Alert email sent → %s | %s", ALERT_EMAIL, subject)
    except Exception as exc:
        logger.error("Failed to send alert email: %s", exc)


# =============================================================================
# TASK FAILURE CALLBACK — fires on any task exception
# =============================================================================

def _task_failure_callback(context) -> None:
    """
    on_failure_callback fired by Airflow when any task raises an exception.
    Sends a simple error email via the same SMTP mechanism as validation emails.
    Skips silently if ALERT_EMAIL or smtp_default is not configured.
    """
    if not ALERT_EMAIL:
        return

    task_id   = context["task"].task_id
    run_id    = context["run_id"]
    exception = context.get("exception")
    error_msg = str(exception) if exception else "No exception details available."

    try:
        from airflow.hooks.base import BaseHook
        conn          = BaseHook.get_connection("smtp_default")
        smtp_host     = conn.host or ""
        smtp_user     = conn.login or ""
        smtp_password = conn.password or ""
        smtp_port     = int(conn.port) if conn.port else 587
    except Exception:
        logger.warning("smtp_default not found — skipping failure email for task '%s'.", task_id)
        return

    if not smtp_host or not smtp_user:
        return

    subject = "[ERROR] {} — task '{}' failed".format(DAG_ID, task_id)

    sep   = "─" * 60
    lines = [
        "DAG     : {}".format(DAG_ID),
        "Task    : {}".format(task_id),
        "Run ID  : {}".format(run_id),
        "Status  : FAILED",
        "",
        sep,
        "ERROR",
        sep,
        error_msg,
        "",
        sep,
        "INVESTIGATE",
        sep,
        "Query pipeline log:",
        "  SELECT dag_id, task_id, layer, status, error_message, logged_at",
        "  FROM logs.pipeline_log",
        "  WHERE run_id = '{}'".format(run_id),
        "  ORDER BY logged_at;",
    ]
    body = "\n".join(lines)

    try:
        import smtplib
        from email.mime.text import MIMEText
        mime_msg            = MIMEText("<pre style='font-family:monospace'>{}</pre>".format(body), "html")
        mime_msg["Subject"] = subject
        mime_msg["From"]    = smtp_user
        mime_msg["To"]      = ALERT_EMAIL
        with smtplib.SMTP(smtp_host, smtp_port) as server:
            server.starttls()
            server.login(smtp_user, smtp_password)
            server.sendmail(smtp_user, [ALERT_EMAIL], mime_msg.as_string())
        logger.info("Failure email sent → %s | %s", ALERT_EMAIL, subject)
    except Exception as exc:
        logger.error("Failed to send failure email for task '%s': %s", task_id, exc)


# =============================================================================
# DAG DEFAULT ARGS  (defined here — after callbacks are declared)
# =============================================================================

default_args = {
    "owner": "data_engineering",
    "retries": 1,
    "retry_delay": timedelta(seconds=30),
    "email_on_failure": False,   # _task_failure_callback handles all alerts
    "email_on_retry": False,
    "on_failure_callback": _task_failure_callback,
}


# =============================================================================
# HELPER: run a list of checks, return (hard_errors, warn_items, passed_names)
# =============================================================================

def _run_checks(hook, checks: list, layer_label: str):
    """
    Execute all (check_name, sql, assertion, message, is_hard_fail) tuples.

    Returns
    -------
    hard_errors  : List[CheckResult]  checks that failed and are hard-fail
    warn_items   : List[CheckResult]  checks that failed but are warn-only
    passed_names : List[str]          names of checks that passed
    """
    hard_errors  = []
    warn_items   = []
    passed_names = []

    for check_name, sql, assertion, message, is_hard_fail in checks:
        value  = (hook.get_first(sql) or [0])[0]
        passed = assertion(value)
        if passed:
            logger.info("%s DQ [%s]: value=%s  ✓", layer_label, check_name, value)
            passed_names.append(check_name)
        elif is_hard_fail:
            logger.error("%s DQ [%s]: value=%s  ✗ HARD FAIL", layer_label, check_name, value)
            hard_errors.append((check_name, message, value))
        else:
            logger.warning("%s DQ [%s]: value=%s  ⚠ WARN", layer_label, check_name, value)
            warn_items.append((check_name, message, value))

    return hard_errors, warn_items, passed_names


# =============================================================================
# HELPER: shared XCom key
# =============================================================================
_TS_XCOM_KEY = "run_start_ts"


# =============================================================================
# TASK 1 — Capture CDC checkpoint at run start
# =============================================================================
def capture_checkpoint(**context) -> str:
    """
    Record the pipeline start timestamp before any S3 reads.
    Stored in XCom; persisted as the new CDC checkpoint only after
    full pipeline success (by update_cdc_checkpoint task at the end).
    """
    start_ts = get_pipeline_start_ts()
    context["ti"].xcom_push(key=_TS_XCOM_KEY, value=start_ts)
    logger.info("Pipeline start timestamp captured: %s", start_ts)
    return start_ts


# =============================================================================
# TASK 2 — Extract S3 → Bronze  (ShortCircuitOperator)
# =============================================================================
def extract_load_bronze(**context) -> bool:
    """
    Read new/updated S3 files and insert into bronze.raw_hardware_sales as a snapshot.

    Returns True  → downstream tasks run normally.
    Returns False → all downstream tasks are SKIPPED (not failed).

    Behaviour by run type:
    ─────────────────────────────────────────────────────────────
    Scheduled run (hourly):
      - Checks S3 for files with LastModified > pipeline_last_run_ts
      - New/updated files found → insert all rows from those files as a
        new snapshot batch, return True (run full pipeline)
      - No new files → return False (skip all downstream, save resource)

    Manual run (Trigger DAG button in UI):
      - Bypasses CDC timestamp check entirely
      - Loads ALL files from S3 as a fresh snapshot batch
      - Always returns True (runs full pipeline)
      - Use this after changing SQL transform logic to reprocess all data
    ─────────────────────────────────────────────────────────────
    Bronze = source data verbatim (no dedup, no filtering).
    Each file's rows are identical to the S3 source.
    cleanup_bronze_snapshots removes batches older than 7 days.
    """
    dag_id  = context["dag"].dag_id
    task_id = context["task"].task_id
    run_id  = context["run_id"]

    # Distinguish scheduled vs manual trigger
    run_type = context["dag_run"].run_type  # "scheduled" | "manual" | "backfill"
    is_manual = run_type != "scheduled"

    if is_manual:
        logger.info(
            "Manual trigger detected (run_type='%s') — loading all S3 files "
            "(CDC timestamp check bypassed).",
            run_type,
        )
        s3_bucket = Variable.get(S3_BUCKET_VAR)
        s3_prefix = Variable.get(S3_PREFIX_VAR, default_var="computer_hardware_sales/")

        from scripts.s3_helpers import list_s3_files
        all_files = list_s3_files(bucket=s3_bucket, prefix=s3_prefix, aws_conn_id="aws_default")

        if not all_files:
            logger.info("Manual trigger: no files found in S3 — skipping bronze load.")
            write_log(dag_id, task_id, run_id, "bronze", "success",
                      rows_processed=0,
                      error_message="manual trigger: no S3 files found")
            return True

        hook       = get_pg_hook("postgres_dwh")
        total_rows = 0
        for s3_key in all_files:
            logger.info("Manual load file: %s", s3_key)
            df = read_csv_from_s3(bucket=s3_bucket, key=s3_key, aws_conn_id="aws_default")
            df["_source_file"] = s3_key
            df["_ingested_at"] = datetime.utcnow()
            _insert_bronze(hook, df)
            total_rows += len(df)
            logger.info("Inserted %d rows from %s", len(df), s3_key)

        write_log(dag_id, task_id, run_id, "bronze", "success", rows_processed=total_rows)
        logger.info("Manual bronze load complete. Total rows inserted: %d", total_rows)
        return True  # always proceed downstream on manual run

    # Scheduled run — check S3 for new files
    write_log(dag_id, task_id, run_id, "bronze", "started")

    s3_bucket   = Variable.get(S3_BUCKET_VAR)
    s3_prefix   = Variable.get(S3_PREFIX_VAR, default_var="computer_hardware_sales/")
    last_run_ts = Variable.get(CDC_VAR, default_var=CDC_DEFAULT_TS)

    new_files = get_new_files_since(
        bucket=s3_bucket, prefix=s3_prefix,
        last_run_ts=last_run_ts, aws_conn_id="aws_default",
    )

    if not new_files:
        logger.info(
            "Scheduled run: no new/updated files since %s — short-circuiting. "
            "All downstream tasks will be skipped.",
            last_run_ts,
        )
        return False  # ShortCircuit: skip all downstream tasks

    hook       = get_pg_hook("postgres_dwh")
    total_rows = 0

    for s3_key in new_files:
        logger.info("Loading file: %s", s3_key)
        df = read_csv_from_s3(bucket=s3_bucket, key=s3_key, aws_conn_id="aws_default")
        df["_source_file"] = s3_key
        df["_ingested_at"] = datetime.utcnow()
        _insert_bronze(hook, df)
        total_rows += len(df)
        logger.info("Inserted %d rows from %s", len(df), s3_key)

    write_log(dag_id, task_id, run_id, "bronze", "success", rows_processed=total_rows)
    logger.info("Bronze load complete. Total rows inserted: %d", total_rows)
    return True


def _insert_bronze(hook, df: pd.DataFrame) -> None:
    """
    Insert rows into bronze.raw_hardware_sales (snapshot model).
    Only schema-defined columns are inserted — extra CSV columns are dropped.
    No ON CONFLICT — every load creates a new snapshot batch identified by
    (_source_file, _ingested_at).
    """
    BRONZE_COLUMNS = [
        "order_id", "order_date", "ship_date", "ship_mode",
        "customer_id", "customer_name", "customer_segment",
        "country", "city", "state", "postal_code", "region",
        "product_id", "category", "sub_category", "product_name", "brand",
        "unit_price", "unit_cost", "quantity", "discount",
        "total_revenue", "total_cost", "profit",
        "sales_channel", "order_status",
        "_source_file", "_ingested_at",
    ]

    cols      = [c for c in BRONZE_COLUMNS if c in df.columns]
    df_insert = df[cols]

    col_names    = ", ".join(['"{}"'.format(c) for c in cols])
    placeholders = ", ".join(["%s"] * len(cols))

    sql = """
        INSERT INTO {schema}.{table} ({cols})
        VALUES ({placeholders});
    """.format(
        schema=BRONZE_SCHEMA, table=BRONZE_TABLE,
        cols=col_names, placeholders=placeholders,
    )

    rows = [tuple(row) for row in df_insert.itertuples(index=False, name=None)]
    conn = hook.get_conn()
    try:
        cursor = conn.cursor()
        cursor.executemany(sql, rows)
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


# =============================================================================
# TASK 2b — Cleanup old Bronze snapshots
# =============================================================================
def cleanup_bronze_snapshots(**context) -> int:
    """
    Remove snapshot batches from bronze.raw_hardware_sales that are older
    than 7 days relative to the pipeline run date.

    Rows with _ingested_at < NOW() - INTERVAL '7 days' are deleted.
    All batches within the 7-day window are retained — this includes
    multiple snapshots per source file if the pipeline ran more than once
    in that window.

    Returns the number of rows deleted.
    """
    dag_id  = context["dag"].dag_id
    task_id = context["task"].task_id
    run_id  = context["run_id"]

    hook = get_pg_hook("postgres_dwh")
    sql  = """
        DELETE FROM bronze.raw_hardware_sales
        WHERE _ingested_at < NOW() - INTERVAL '7 days';
    """
    conn = hook.get_conn()
    try:
        cursor = conn.cursor()
        cursor.execute(sql)
        deleted = cursor.rowcount
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

    write_log(dag_id, task_id, run_id, "bronze", "success", rows_processed=deleted)
    logger.info("Bronze snapshot cleanup: %d rows older than 7 days deleted.", deleted)
    return deleted


# =============================================================================
# TASK 3 — Validate Bronze
# =============================================================================
def validate_bronze(**context) -> None:
    """
    Bronze validation — warns on most issues, raises only on hard blockers.
    One consolidated email sent at the end (not per check).
    """
    dag_id  = context["dag"].dag_id
    task_id = context["task"].task_id
    run_id  = context["run_id"]

    hook = get_pg_hook("postgres_dwh")

    # (check_name, sql, assertion, message, is_hard_fail)
    # is_hard_fail=True  → raise (pipeline stops) | False → warn (pipeline continues)
    checks = [
        (
            "not_empty",
            "SELECT COUNT(*) FROM bronze.raw_hardware_sales;",
            lambda n: n > 0,
            "bronze.raw_hardware_sales is empty — nothing was loaded.",
            True,
        ),
        (
            "required_columns_present",
            """SELECT COUNT(*) FROM information_schema.columns
               WHERE table_schema = 'bronze' AND table_name = 'raw_hardware_sales'
                 AND column_name IN ('order_id','customer_id','product_id',
                                     'order_date','unit_price','quantity');""",
            lambda n: n >= 6,
            "bronze.raw_hardware_sales is missing one or more required source columns.",
            True,
        ),
        (
            "null_order_id_rate_critical",
            """SELECT ROUND(100.0 * SUM(CASE WHEN order_id IS NULL THEN 1 ELSE 0 END)
               / NULLIF(COUNT(*), 0), 2) FROM bronze.raw_hardware_sales;""",
            lambda pct: (pct or 0) <= 50,
            "More than 50% of order_id values are NULL — file is likely malformed.",
            True,
        ),
        (
            "null_order_id_rate_warning",
            """SELECT ROUND(100.0 * SUM(CASE WHEN order_id IS NULL THEN 1 ELSE 0 END)
               / NULLIF(COUNT(*), 0), 2) FROM bronze.raw_hardware_sales;""",
            lambda pct: (pct or 0) == 0,
            "Some order_id values are NULL (expected from dirty source data).",
            False,
        ),
        (
            "duplicate_order_id_rate",
            """SELECT ROUND(100.0 * (COUNT(*) - COUNT(DISTINCT order_id))
               / NULLIF(COUNT(*), 0), 2)
               FROM bronze.raw_hardware_sales WHERE order_id IS NOT NULL;""",
            lambda pct: (pct or 0) <= 30,
            "More than 30% duplicate order_ids in bronze — investigate source.",
            False,
        ),
        (
            "recent_rows_loaded",
            """SELECT COUNT(*) FROM bronze.raw_hardware_sales
               WHERE _ingested_at >= NOW() - INTERVAL '2 hours';""",
            lambda n: n > 0,
            "No rows ingested in the last 2 hours — pipeline may have found no new files.",
            False,
        ),
    ]

    hard_errors, warn_items, passed_names = _run_checks(hook, checks, "Bronze")

    _send_validation_email(
        task_id=task_id, run_id=run_id,
        errors=hard_errors, warns=warn_items, passed=passed_names,
        layer="bronze",
    )

    if hard_errors:
        msg = " | ".join(m for _, m, _ in hard_errors)
        write_log(dag_id, task_id, run_id, "bronze", "failed", error_message=msg)
        raise ValueError("Bronze validation failed: {}".format(msg))

    write_log(dag_id, task_id, run_id, "bronze", "success")
    logger.info("Bronze validation passed. Warnings: %d", len(warn_items))


# =============================================================================
# TASK 4 — Transform Bronze → Silver
# =============================================================================
def transform_silver(**context) -> int:
    dag_id  = context["dag"].dag_id
    task_id = context["task"].task_id
    run_id  = context["run_id"]

    write_log(dag_id, task_id, run_id, "silver", "started")

    rows_written = run_bronze_to_silver(conn_id="postgres_dwh")

    if rows_written == 0:
        logger.info("No new bronze rows to promote — silver is up to date.")
        write_log(dag_id, task_id, run_id, "silver", "success", rows_processed=0)
        return 0

    write_log(dag_id, task_id, run_id, "silver", "success", rows_processed=rows_written)
    return rows_written


# =============================================================================
# TASK 5 — Validate Silver
# =============================================================================
def validate_silver(**context) -> None:
    """
    Silver validation — strict on key checks, warn-only on source data issues.
    One consolidated email sent at the end (not per check).
    """
    dag_id  = context["dag"].dag_id
    task_id = context["task"].task_id
    run_id  = context["run_id"]

    hook = get_pg_hook("postgres_dwh")

    # (check_name, sql, assertion, message, is_hard_fail)
    checks = [
        (
            "not_empty",
            "SELECT COUNT(*) FROM silver.clean_hardware_sales;",
            lambda n: n > 0,
            "silver.clean_hardware_sales is empty.",
            True,
        ),
        (
            "no_null_order_id",
            "SELECT COUNT(*) FROM silver.clean_hardware_sales WHERE order_id IS NULL;",
            lambda n: n == 0,
            "silver: NULL order_id values found.",
            True,
        ),
        (
            "no_null_customer_id",
            "SELECT COUNT(*) FROM silver.clean_hardware_sales WHERE customer_id IS NULL;",
            lambda n: n == 0,
            "silver: NULL customer_id values found.",
            True,
        ),
        (
            "no_null_product_id",
            "SELECT COUNT(*) FROM silver.clean_hardware_sales WHERE product_id IS NULL;",
            lambda n: n == 0,
            "silver: NULL product_id values found.",
            True,
        ),
        (
            "no_duplicate_order_id",
            """SELECT COUNT(*) FROM (
                SELECT order_id FROM silver.clean_hardware_sales
                GROUP BY order_id HAVING COUNT(*) > 1
            ) t;""",
            lambda n: n == 0,
            "silver: duplicate order_id values found.",
            True,
        ),
        (
            "no_negative_quantity",
            "SELECT COUNT(*) FROM silver.clean_hardware_sales WHERE quantity < 0;",
            lambda n: n == 0,
            "silver: negative quantity values found.",
            True,
        ),
        (
            "no_negative_unit_price",
            "SELECT COUNT(*) FROM silver.clean_hardware_sales WHERE unit_price < 0;",
            lambda n: n == 0,
            "silver: negative unit_price values found.",
            True,
        ),
        (
            "no_negative_unit_cost",
            "SELECT COUNT(*) FROM silver.clean_hardware_sales WHERE unit_cost < 0;",
            lambda n: n == 0,
            "silver: negative unit_cost values found.",
            True,
        ),
        (
            "discount_range",
            "SELECT COUNT(*) FROM silver.clean_hardware_sales WHERE discount < 0 OR discount > 1;",
            lambda n: n == 0,
            "silver: discount values outside [0, 1] found.",
            True,
        ),
        (
            "profit_formula",
            """SELECT COUNT(*) FROM silver.clean_hardware_sales
               WHERE profit IS NOT NULL AND total_revenue IS NOT NULL AND total_cost IS NOT NULL
                 AND ABS(profit - (total_revenue - total_cost)) > 0.02;""",
            lambda n: n == 0,
            "silver: profit != total_revenue - total_cost (tolerance 0.02).",
            True,
        ),
        (
            "order_date_null_rate",
            """SELECT ROUND(100.0 * SUM(CASE WHEN order_date IS NULL THEN 1 ELSE 0 END)
               / NULLIF(COUNT(*), 0), 2) FROM silver.clean_hardware_sales;""",
            lambda pct: (pct or 0) < 5,
            "silver: more than 5% of order_date values could not be parsed.",
            True,
        ),
        (
            "ship_date_not_before_order_date",
            """SELECT COUNT(*) FROM silver.clean_hardware_sales
               WHERE ship_date IS NOT NULL AND order_date IS NOT NULL
                 AND ship_date < order_date;""",
            lambda n: n == 0,
            "silver: ship_date < order_date found — swapped dates in source.",
            False,   # WARN — source data issue, not a pipeline failure
        ),
        (
            "silver_vs_bronze_row_ratio",
            """SELECT ROUND(
                100.0 * (SELECT COUNT(*) FROM silver.clean_hardware_sales)
                / NULLIF((SELECT COUNT(*) FROM bronze.raw_hardware_sales
                          WHERE order_id IS NOT NULL), 0), 2);""",
            lambda pct: (pct or 0) >= 60,
            "silver: fewer than 60% of valid bronze rows made it to silver.",
            False,   # WARN — expected when source has many NULL key columns
        ),
    ]

    hard_errors, warn_items, passed_names = _run_checks(hook, checks, "Silver")

    _send_validation_email(
        task_id=task_id, run_id=run_id,
        errors=hard_errors, warns=warn_items, passed=passed_names,
        layer="silver",
    )

    if hard_errors:
        msg = " | ".join(m for _, m, _ in hard_errors)
        write_log(dag_id, task_id, run_id, "silver", "failed", error_message=msg)
        raise ValueError("Silver validation failed: {}".format(msg))

    write_log(dag_id, task_id, run_id, "silver", "success")
    logger.info("Silver validation passed. Warnings: %d", len(warn_items))


# =============================================================================
# TASKS 6a-6d — Build Gold Dimensions (run in parallel)
# =============================================================================
def _dim_task(layer_fn, dim_name: str, **context) -> int:
    dag_id  = context["dag"].dag_id
    task_id = context["task"].task_id
    run_id  = context["run_id"]
    write_log(dag_id, task_id, run_id, "gold", "started")
    rows = layer_fn(conn_id="postgres_dwh")
    write_log(dag_id, task_id, run_id, "gold", "success", rows_processed=rows)
    logger.info("%s: %d rows affected.", dim_name, rows)
    return rows


def build_dim_date(**context) -> int:
    return _dim_task(run_dim_date, "dim_date", **context)

def build_dim_geography(**context) -> int:
    return _dim_task(run_dim_geography, "dim_geography", **context)

def build_dim_product(**context) -> int:
    return _dim_task(run_dim_product, "dim_product", **context)

def build_dim_customer(**context) -> int:
    return _dim_task(run_dim_customer, "dim_customer", **context)


# =============================================================================
# TASK 7 — Build Gold Fact
# =============================================================================
def build_fact_sales(**context) -> int:
    dag_id  = context["dag"].dag_id
    task_id = context["task"].task_id
    run_id  = context["run_id"]
    write_log(dag_id, task_id, run_id, "gold", "started")
    rows = run_fact_sales(conn_id="postgres_dwh")
    write_log(dag_id, task_id, run_id, "gold", "success", rows_processed=rows)
    return rows


# =============================================================================
# TASK 8 — Validate Gold
# =============================================================================
def validate_gold(**context) -> None:
    """
    Gold validation — strict on all structural checks.
    One consolidated email sent at the end (not per check).
    """
    dag_id  = context["dag"].dag_id
    task_id = context["task"].task_id
    run_id  = context["run_id"]

    hook = get_pg_hook("postgres_dwh")

    # (check_name, sql, assertion, message, is_hard_fail)
    checks = [
        # Dimension checks
        (
            "dim_date_not_empty",
            "SELECT COUNT(*) FROM gold.dim_date;",
            lambda n: n > 0,
            "gold.dim_date is empty.",
            True,
        ),
        (
            "dim_geography_not_empty",
            "SELECT COUNT(*) FROM gold.dim_geography;",
            lambda n: n > 0,
            "gold.dim_geography is empty.",
            True,
        ),
        (
            "dim_product_has_current",
            "SELECT COUNT(*) FROM gold.dim_product WHERE is_current = TRUE;",
            lambda n: n > 0,
            "gold.dim_product has no current (is_current=TRUE) records.",
            True,
        ),
        (
            "dim_customer_has_current",
            "SELECT COUNT(*) FROM gold.dim_customer WHERE is_current = TRUE;",
            lambda n: n > 0,
            "gold.dim_customer has no current (is_current=TRUE) records.",
            True,
        ),
        (
            "dim_product_one_current_per_key",
            """SELECT COUNT(*) FROM (
                SELECT product_id FROM gold.dim_product
                WHERE is_current = TRUE GROUP BY product_id HAVING COUNT(*) > 1
            ) t;""",
            lambda n: n == 0,
            "gold.dim_product: multiple is_current=TRUE rows for the same product_id.",
            True,
        ),
        (
            "dim_customer_one_current_per_key",
            """SELECT COUNT(*) FROM (
                SELECT customer_id FROM gold.dim_customer
                WHERE is_current = TRUE GROUP BY customer_id HAVING COUNT(*) > 1
            ) t;""",
            lambda n: n == 0,
            "gold.dim_customer: multiple is_current=TRUE rows for the same customer_id.",
            True,
        ),
        (
            "dim_product_scd2_valid_range",
            """SELECT COUNT(*) FROM gold.dim_product
               WHERE valid_to IS NOT NULL AND valid_from > valid_to;""",
            lambda n: n == 0,
            "gold.dim_product: valid_from > valid_to (invalid SCD2 range).",
            True,
        ),
        (
            "dim_customer_scd2_valid_range",
            """SELECT COUNT(*) FROM gold.dim_customer
               WHERE valid_to IS NOT NULL AND valid_from > valid_to;""",
            lambda n: n == 0,
            "gold.dim_customer: valid_from > valid_to (invalid SCD2 range).",
            True,
        ),
        # Fact checks
        (
            "fact_not_empty",
            "SELECT COUNT(*) FROM gold.fact_sales;",
            lambda n: n > 0,
            "gold.fact_sales is empty.",
            True,
        ),
        (
            "fact_no_null_order_id",
            "SELECT COUNT(*) FROM gold.fact_sales WHERE order_id IS NULL;",
            lambda n: n == 0,
            "gold.fact_sales: NULL order_id found.",
            True,
        ),
        (
            "fact_product_sk_resolved",
            "SELECT COUNT(*) FROM gold.fact_sales WHERE dim_product_sk IS NULL;",
            lambda n: n == 0,
            "gold.fact_sales: unresolved dim_product_sk (NULL FK).",
            True,
        ),
        (
            "fact_customer_sk_resolved",
            "SELECT COUNT(*) FROM gold.fact_sales WHERE dim_customer_sk IS NULL;",
            lambda n: n == 0,
            "gold.fact_sales: unresolved dim_customer_sk (NULL FK).",
            True,
        ),
        (
            "fact_order_date_id_resolved",
            "SELECT COUNT(*) FROM gold.fact_sales WHERE order_date_id IS NULL;",
            lambda n: n == 0,
            "gold.fact_sales: unresolved order_date_id (NULL FK).",
            True,
        ),
        (
            "fact_profit_formula",
            """SELECT COUNT(*) FROM gold.fact_sales
               WHERE profit IS NOT NULL AND total_revenue IS NOT NULL AND total_cost IS NOT NULL
                 AND ABS(profit - (total_revenue - total_cost)) > 0.02;""",
            lambda n: n == 0,
            "gold.fact_sales: profit != total_revenue - total_cost (tolerance 0.02).",
            True,
        ),
        (
            "fact_vs_silver_row_count",
            """SELECT (SELECT COUNT(*) FROM gold.fact_sales)
                   >= (SELECT COUNT(*) FROM silver.clean_hardware_sales);""",
            lambda b: b is True,
            "gold.fact_sales has fewer rows than silver — data may have been lost.",
            False,   # WARN — can happen if silver has rows with unresolvable FKs
        ),
    ]

    hard_errors, warn_items, passed_names = _run_checks(hook, checks, "Gold")

    _send_validation_email(
        task_id=task_id, run_id=run_id,
        errors=hard_errors, warns=warn_items, passed=passed_names,
        layer="gold",
    )

    if hard_errors:
        msg = " | ".join(m for _, m, _ in hard_errors)
        write_log(dag_id, task_id, run_id, "gold", "failed", error_message=msg)
        raise ValueError("Gold validation failed: {}".format(msg))

    write_log(dag_id, task_id, run_id, "gold", "success")
    logger.info("Gold validation passed. Warnings: %d", len(warn_items))


# =============================================================================
# TASK 9 — Update CDC checkpoint (only on full pipeline success)
# =============================================================================
def update_cdc_checkpoint(**context) -> None:
    """
    Persist the run start timestamp (from XCom) as the new CDC checkpoint.
    Runs LAST — only if every upstream task succeeded.
    If any task failed, this is skipped so the checkpoint stays unchanged
    and the next run re-processes from the same point.
    """
    start_ts = context["ti"].xcom_pull(task_ids="capture_checkpoint", key=_TS_XCOM_KEY)
    if not start_ts:
        raise ValueError("Could not retrieve run start timestamp from XCom.")
    Variable.set(CDC_VAR, start_ts)
    logger.info("CDC checkpoint updated to: %s", start_ts)


# =============================================================================
# DAG definition
# =============================================================================
with DAG(
    dag_id=DAG_ID,
    default_args=default_args,
    schedule_interval="@hourly",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["etl", "hardware_sales", "medallion"],
    doc_md="""
## Hardware Sales Pipeline

End-to-end ETL: **S3 → Bronze → Silver → Gold (star schema)**.

### Schedule
Every hour.

### Trigger modes

| Run type | Behaviour |
|---|---|
| **Scheduled (hourly)** | Check CDC timestamp → load only new/updated S3 files → cleanup old snapshots → run full pipeline. Short-circuits if no new files. |
| **Manual (Trigger DAG button)** | Bypass CDC timestamp → load **all** S3 files as a fresh snapshot → cleanup old snapshots → run full Silver + Gold. |

**When to trigger manually:**
After updating SQL transform logic (e.g. fixing a cleaning rule in
`transform_bronze_to_silver.sql`), trigger manually to reload all S3 files
and reprocess through Silver and Gold with the new logic.
No JSON config needed — just click **Trigger DAG ▶**.

### CDC
`pipeline_last_run_ts` (ISO-8601 UTC). Captured at run START.
Checkpoint advances only on full success.

### Email alerts
One email per validation task (not per check).
SMTP credentials in Airflow Connection `smtp_default`.
Recipient in `ALERT_EMAIL` env var.

### Connections (Admin → Connections)
- `aws_default` — AWS S3
- `postgres_dwh` — PostgreSQL DWH
- `smtp_default` — SMTP email (optional)

### Variables (Admin → Variables)
- `s3_bucket_name`        — S3 bucket name
- `s3_prefix`             — folder prefix
- `pipeline_last_run_ts`  — CDC checkpoint (init: `2000-01-01T00:00:00+00:00`)
    """,
) as dag:

    t_checkpoint = PythonOperator(
        task_id="capture_checkpoint",
        python_callable=capture_checkpoint,
    )
    t_bronze = ShortCircuitOperator(
        task_id="extract_load_bronze",
        python_callable=extract_load_bronze,
        ignore_downstream_trigger_rules=True,  # skip ALL downstream, not just direct children
    )
    t_cleanup_bronze = PythonOperator(
        task_id="cleanup_bronze_snapshots",
        python_callable=cleanup_bronze_snapshots,
    )
    t_validate_bronze = PythonOperator(
        task_id="validate_bronze",
        python_callable=validate_bronze,
    )
    t_silver = PythonOperator(
        task_id="transform_silver",
        python_callable=transform_silver,
    )
    t_validate_silver = PythonOperator(
        task_id="validate_silver",
        python_callable=validate_silver,
    )
    t_dim_date = PythonOperator(
        task_id="build_dim_date",
        python_callable=build_dim_date,
    )
    t_dim_geo = PythonOperator(
        task_id="build_dim_geography",
        python_callable=build_dim_geography,
    )
    t_dim_product = PythonOperator(
        task_id="build_dim_product",
        python_callable=build_dim_product,
    )
    t_dim_customer = PythonOperator(
        task_id="build_dim_customer",
        python_callable=build_dim_customer,
    )
    t_fact = PythonOperator(
        task_id="build_fact_sales",
        python_callable=build_fact_sales,
    )
    t_validate_gold = PythonOperator(
        task_id="validate_gold",
        python_callable=validate_gold,
    )
    t_update_cdc = PythonOperator(
        task_id="update_cdc_checkpoint",
        python_callable=update_cdc_checkpoint,
    )

    dims = [t_dim_date, t_dim_geo, t_dim_product, t_dim_customer]

    (
        t_checkpoint
        >> t_bronze
        >> t_cleanup_bronze
        >> t_validate_bronze
        >> t_silver
        >> t_validate_silver
        >> dims
        >> t_fact
        >> t_validate_gold
        >> t_update_cdc
    )
