"""
db_helpers.py
-------------
PostgreSQL utility helpers for the pipeline.

All connections are made via Airflow's PostgresHook so that credentials
are never hardcoded — configure the connection in the Airflow UI:
  Conn Id  : postgres_dwh
  Conn Type: Postgres
  Host     : postgres-dwh   (Docker service name)
  Port     : 5432
  Schema   : hardware_sales_dwh

Functions
---------
get_pg_hook      — Return a configured PostgresHook.
get_pg_conn      — Context manager yielding a psycopg2 connection.
execute_sql_file — Run a .sql file directly against the DWH.
write_log        — Insert one row into logs.pipeline_log.
"""

from __future__ import annotations

import logging
from contextlib import contextmanager
from pathlib import Path
from typing import Optional, Union

from airflow.providers.postgres.hooks.postgres import PostgresHook

logger = logging.getLogger(__name__)

POSTGRES_CONN_ID = "postgres_dwh"


def get_pg_hook(conn_id: str = POSTGRES_CONN_ID) -> PostgresHook:
    """Return a PostgresHook using an Airflow connection."""
    return PostgresHook(postgres_conn_id=conn_id)


@contextmanager
def get_pg_conn(conn_id: str = POSTGRES_CONN_ID):
    """
    Context manager that yields a psycopg2 connection.
    Commits on clean exit, rolls back on exception.
    """
    hook = get_pg_hook(conn_id)
    conn = hook.get_conn()
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def execute_sql_file(sql_path: Union[str, Path], conn_id: str = POSTGRES_CONN_ID) -> None:
    """
    Execute a .sql file against the data warehouse.

    Used for schema bootstrap (CREATE TABLE / CREATE INDEX DDL files).
    For parameterised transformation SQL use scripts.transformations instead.

    Parameters
    ----------
    sql_path : str or Path
        Absolute path to the SQL file.
    conn_id : str
        Airflow connection ID for PostgreSQL.
    """
    sql_path = Path(sql_path)
    if not sql_path.exists():
        raise FileNotFoundError("SQL file not found: {}".format(sql_path))

    sql = sql_path.read_text(encoding="utf-8")
    hook = get_pg_hook(conn_id)
    hook.run(sql)
    logger.info("Executed SQL file: %s", sql_path)


def write_log(
    dag_id: str,
    task_id: str,
    run_id: str,
    layer: str,
    status: str,
    rows_processed: int = 0,
    error_message: Optional[str] = None,
    conn_id: str = POSTGRES_CONN_ID,
) -> None:
    """
    Write a pipeline execution log entry to logs.pipeline_log.

    Parameters
    ----------
    dag_id : str
    task_id : str
    run_id : str
    layer : str
        One of: bronze, silver, gold, general.
    status : str
        One of: started, success, failed.
    rows_processed : int
    error_message : str, optional
    conn_id : str
    """
    sql = """
        INSERT INTO logs.pipeline_log
            (dag_id, task_id, run_id, layer, status, rows_processed, error_message, logged_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s, NOW());
    """
    hook = get_pg_hook(conn_id)
    hook.run(sql, parameters=(dag_id, task_id, run_id, layer, status, rows_processed, error_message))
    logger.info("Log written — layer=%s status=%s rows=%d", layer, status, rows_processed)
