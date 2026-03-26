from __future__ import annotations

from src.orchestration import run_pipeline


def test_ddl_dir_contains_sql_files() -> None:
    ddl_files = sorted(p.name for p in run_pipeline.DDL_DIR.glob("*.sql"))
    assert ddl_files == [
        "001_create_schema.sql",
        "002_raw_tables.sql",
        "003_core_tables.sql",
        "004_marts.sql",
    ]


def test_transform_sql_exists() -> None:
    assert run_pipeline.TRANSFORM_SQL.exists()
