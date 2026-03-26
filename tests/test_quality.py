from __future__ import annotations

from src.orchestration import run_pipeline


def test_quality_sql_exists() -> None:
    assert run_pipeline.QUALITY_SQL.exists()


def test_quality_sql_contains_expected_checks() -> None:
    sql_text = run_pipeline.QUALITY_SQL.read_text(encoding="utf-8")
    for expected in (
        "row_count",
        "invalid_geom_4283",
        "empty_geom_4283",
        "non_line_geometry",
        "non_positive_length_m",
    ):
        assert expected in sql_text
