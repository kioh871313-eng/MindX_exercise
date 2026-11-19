-- =============================================================
-- Logs Schema
-- Purpose : Centralised pipeline execution logging.
--           Every DAG task writes a row here on start, success,
--           or failure — enabling full audit trail across all layers.
-- =============================================================

CREATE SCHEMA IF NOT EXISTS logs;

-- -------------------------------------------------------------
-- logs.pipeline_log
-- One row per task execution event.
-- -------------------------------------------------------------
CREATE TABLE IF NOT EXISTS logs.pipeline_log (
    id              BIGSERIAL       PRIMARY KEY,
    dag_id          TEXT            NOT NULL,
    task_id         TEXT            NOT NULL,
    run_id          TEXT            NOT NULL,
    layer           TEXT            NOT NULL
                        CHECK (layer IN ('bronze', 'silver', 'gold', 'general')),
    status          TEXT            NOT NULL
                        CHECK (status IN ('started', 'success', 'failed')),
    rows_processed  INTEGER         NOT NULL DEFAULT 0,
    error_message   TEXT,
    logged_at       TIMESTAMPTZ     NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_pipeline_log_dag
    ON logs.pipeline_log (dag_id, logged_at DESC);

CREATE INDEX IF NOT EXISTS idx_pipeline_log_layer
    ON logs.pipeline_log (layer, status, logged_at DESC);

COMMENT ON TABLE logs.pipeline_log IS
    'Pipeline execution log. One row per task event (started / success / failed). '
    'Used for audit, monitoring, and debugging.';


-- -------------------------------------------------------------
-- logs.data_quality_log
-- Stores per-column validation results for deeper inspection.
-- -------------------------------------------------------------
CREATE TABLE IF NOT EXISTS logs.data_quality_log (
    id              BIGSERIAL       PRIMARY KEY,
    dag_id          TEXT            NOT NULL,
    run_id          TEXT            NOT NULL,
    layer           TEXT            NOT NULL,
    table_name      TEXT            NOT NULL,
    check_name      TEXT            NOT NULL,
    passed          BOOLEAN         NOT NULL,
    details         TEXT,
    checked_at      TIMESTAMPTZ     NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_dq_log_run
    ON logs.data_quality_log (dag_id, run_id, layer);

COMMENT ON TABLE logs.data_quality_log IS
    'Data quality check results per column/check per run.';
