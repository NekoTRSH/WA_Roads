-- Operational audit / run-tracking tables.

CREATE TABLE IF NOT EXISTS audit.pipeline_runs (
    run_id uuid PRIMARY KEY,
    pipeline_name text NOT NULL,
    source_name text NOT NULL,
    layer_id integer,
    started_at timestamptz NOT NULL DEFAULT now(),
    ended_at timestamptz NOT NULL DEFAULT now(),
    status text NOT NULL,
    rows_fetched integer NOT NULL DEFAULT 0,
    files_written integer NOT NULL DEFAULT 0,
    error_message text,
    metadata jsonb NOT NULL DEFAULT '{}'::jsonb
);
