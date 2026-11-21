"""
transformations.py
------------------
SQL-driven transformation runner.

All cleaning and modelling logic lives in .sql files under sqls/.
This module provides one function per transformation step that:
  1. Reads the corresponding .sql file from disk.
  2. Substitutes runtime parameters (e.g. CDC checkpoint).
  3. Executes it against the data warehouse via PostgresHook.

No pandas data-manipulation logic lives here.
pandas is used only where SQL cannot help: reading row counts back
from Postgres after an execute (cursor.rowcount).
"""

from __future__ import annotations

import logging
import os
from pathlib import Path

from airflow.providers.postgres.hooks.postgres import PostgresHook

logger = logging.getLogger(__name__)

# Base path for SQL files — matches the Docker volume mount
# ./sqls  →  /opt/airflow/sqls  inside the Airflow container
_SQL_BASE = Path(os.environ.get("SQL_BASE_PATH", "/opt/airflow/sqls"))


def _load_sql(relative_path: str) -> str:
    """Read a .sql file and return its content as a string."""
    full_path = _SQL_BASE / relative_path
    if not full_path.exists():
        raise FileNotFoundError(f"SQL file not found: {full_path}")
    return full_path.read_text(encoding="utf-8")


def run_bronze_to_silver(
    conn_id: str = "postgres_dwh",
) -> int:
    """
    Execute the Bronze → Silver transformation SQL.

    Reads sqls/silver/transform_bronze_to_silver.sql and executes it.
    Returns the number of rows upserted.

    The SQL filters bronze rows using the snapshot model — only the latest
    _ingested_at batch per _source_file is processed. No runtime parameters
    are required.

    Parameters
    ----------
    conn_id : str
        Airflow Postgres connection ID.

    Returns
    -------
    int
        Number of rows inserted/updated in silver.clean_hardware_sales.
    """
    sql = _load_sql("silver/transform_bronze_to_silver.sql")

    hook = PostgresHook(postgres_conn_id=conn_id)
    conn = hook.get_conn()
    try:
        cursor = conn.cursor()
        cursor.execute(sql)
        rows_affected = cursor.rowcount
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

    logger.info("Bronze → Silver: %d rows upserted (snapshot model).", rows_affected)
    return rows_affected


def run_dim_date(conn_id: str = "postgres_dwh") -> int:
    """
    Execute the Silver → gold.dim_date transformation SQL.

    Returns the number of rows inserted/updated.
    """
    sql = _load_sql("gold/transform_silver_to_dim_date.sql")
    hook = PostgresHook(postgres_conn_id=conn_id)
    conn = hook.get_conn()
    try:
        cursor = conn.cursor()
        cursor.execute(sql)
        rows_affected = cursor.rowcount
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

    logger.info("dim_date: %d rows upserted.", rows_affected)
    return rows_affected


def run_dim_geography(conn_id: str = "postgres_dwh") -> int:
    """Execute the Silver → gold.dim_geography SQL."""
    sql = _load_sql("gold/transform_silver_to_dim_geography.sql")
    hook = PostgresHook(postgres_conn_id=conn_id)
    conn = hook.get_conn()
    try:
        cursor = conn.cursor()
        cursor.execute(sql)
        rows_affected = cursor.rowcount
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

    logger.info("dim_geography: %d rows upserted.", rows_affected)
    return rows_affected


def run_dim_product(conn_id: str = "postgres_dwh") -> int:
    """
    Execute the Silver → gold.dim_product SCD2 SQL.

    The SQL file runs two statements in sequence:
      1. UPDATE — close stale active records.
      2. INSERT — add new active versions for new/changed products.
    Returns total rows affected across both statements.
    """
    sql = _load_sql("gold/transform_silver_to_dim_product.sql")
    hook = PostgresHook(postgres_conn_id=conn_id)
    conn = hook.get_conn()
    total = 0
    try:
        cursor = conn.cursor()
        # Execute both UPDATE and INSERT statements in the SQL file
        cursor.execute(sql)
        total += cursor.rowcount
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

    logger.info("dim_product (SCD2): %d rows affected.", total)
    return total


def run_dim_customer(conn_id: str = "postgres_dwh") -> int:
    """
    Execute the Silver → gold.dim_customer SCD2 SQL.

    Same two-statement pattern as dim_product.
    """
    sql = _load_sql("gold/transform_silver_to_dim_customer.sql")
    hook = PostgresHook(postgres_conn_id=conn_id)
    conn = hook.get_conn()
    total = 0
    try:
        cursor = conn.cursor()
        cursor.execute(sql)
        total += cursor.rowcount
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

    logger.info("dim_customer (SCD2): %d rows affected.", total)
    return total


def run_fact_sales(conn_id: str = "postgres_dwh") -> int:
    """
    Execute the Silver → gold.fact_sales SQL.

    Resolves all surrogate keys (dim_product_sk, dim_customer_sk,
    geo_id, order_date_id, ship_date_id) entirely in SQL via JOINs.
    Returns the number of rows inserted/updated.
    """
    sql = _load_sql("gold/transform_silver_to_fact_sales.sql")
    hook = PostgresHook(postgres_conn_id=conn_id)
    conn = hook.get_conn()
    try:
        cursor = conn.cursor()
        cursor.execute(sql)
        rows_affected = cursor.rowcount
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

    logger.info("fact_sales: %d rows upserted.", rows_affected)
    return rows_affected
