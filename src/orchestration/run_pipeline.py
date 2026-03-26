from __future__ import annotations

import argparse
import os
import subprocess
import sys
import time
from pathlib import Path

from sqlalchemy import create_engine, text

ROOT = Path(__file__).resolve().parents[2]

DDL_DIR = ROOT / "sql" / "ddl"
TRANSFORM_SQL = ROOT / "sql" / "transforms" / "lga_road_stats.sql"
QUALITY_SQL = ROOT / "sql" / "checks" / "road_segment_quality.sql"

INGEST_SCRIPT = ROOT / "src" / "ingest" / "mainroads_api.py"
LOAD_SCRIPT = ROOT / "src" / "load" / "postgres.py"


def default_database_url() -> str:
    user = os.getenv("POSTGRES_USER", "wa_admin")
    password = os.getenv("POSTGRES_PASSWORD", "change_me")
    db = os.getenv("POSTGRES_DB", "wa_roads")
    host = os.getenv("POSTGRES_HOST", "localhost")
    port = os.getenv("POSTGRES_PORT", "5432")
    return f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db}"


def database_url_from_env() -> str:
    return os.getenv("DATABASE_URL", default_database_url())


def run_python_script(script_path: Path) -> None:
    if not script_path.exists():
        raise FileNotFoundError(f"Missing script: {script_path}")

    print(f"\n>>> Running Python script: {script_path}")
    subprocess.run(
        [sys.executable, str(script_path)],
        cwd=ROOT,
        check=True,
    )


def run_sql_file(engine, sql_path: Path) -> None:
    if not sql_path.exists():
        raise FileNotFoundError(f"Missing SQL file: {sql_path}")

    print(f"\n>>> Running SQL file: {sql_path}")
    sql_text = sql_path.read_text(encoding="utf-8")

    raw_conn = engine.raw_connection()
    try:
        cursor = raw_conn.cursor()
        cursor.execute(sql_text)
        raw_conn.commit()
        cursor.close()
    finally:
        raw_conn.close()


def run_ddl(engine) -> None:
    if not DDL_DIR.exists():
        raise FileNotFoundError(f"Missing DDL dir: {DDL_DIR}")

    ddl_files = sorted(p for p in DDL_DIR.glob("*.sql") if p.is_file())
    if not ddl_files:
        raise FileNotFoundError(f"No DDL files found in {DDL_DIR}")

    for path in ddl_files:
        run_sql_file(engine, path)


def wait_for_db(engine, timeout_s: int = 60) -> None:
    start = time.time()
    while True:
        try:
            with engine.connect() as conn:
                conn.execute(text("SELECT 1;"))
            return
        except Exception:
            if time.time() - start > timeout_s:
                raise
            time.sleep(1)


def validate_tables(engine) -> None:
    print("\n>>> Validating tables")

    checks = {
        "core.road_segment": "SELECT COUNT(*) FROM core.road_segment;",
        "mart.lga_road_stats": "SELECT COUNT(*) FROM mart.lga_road_stats;",
    }

    with engine.connect() as conn:
        results = {}
        for name, sql in checks.items():
            count = conn.execute(text(sql)).scalar()
            results[name] = count

    for table_name, count in results.items():
        print(f"{table_name}: {count:,} rows")

    if results["core.road_segment"] == 0:
        raise RuntimeError("Validation failed: core.road_segment is empty")

    if results["mart.lga_road_stats"] == 0:
        raise RuntimeError("Validation failed: mart.lga_road_stats is empty")


def run_quality_checks(engine, strict: bool = False) -> None:
    """
    Executes sql/checks/road_segment_quality.sql and enforces a small set of
    must-pass checks for a healthy pipeline run.
    """
    if not QUALITY_SQL.exists():
        print(f"\n>>> Skipping quality checks (missing {QUALITY_SQL})")
        return

    print(f"\n>>> Running quality checks: {QUALITY_SQL}")
    sql_text = QUALITY_SQL.read_text(encoding="utf-8")

    must_be_zero = {
        "invalid_geom_4283",
        "empty_geom_4283",
        "non_line_geometry",
        "missing_geom_4283",
        "non_positive_length_m",
    }

    with engine.connect() as conn:
        results = {}
        for stmt in [s.strip() for s in sql_text.split(";") if s.strip()]:
            if "check_name" not in stmt:
                continue
            row = conn.execute(text(stmt)).mappings().first()
            if not row:
                continue
            check_name = str(row.get("check_name", ""))
            result_count = int(row.get("result_count", 0) or 0)
            results[check_name] = result_count

    for name in sorted(results):
        print(f"{name}: {results[name]:,}")

    failures = []
    for name in sorted(must_be_zero):
        if results.get(name, 0) != 0:
            failures.append(f"{name}={results.get(name, 0)}")

    if strict:
        for name in (
            "missing_road_name",
            "missing_network_type",
            "missing_lg_name",
            "missing_ra_name",
        ):
            if results.get(name, 0) != 0:
                failures.append(f"{name}={results.get(name, 0)}")

    if failures:
        raise RuntimeError("Quality checks failed: " + ", ".join(failures))


def preview_results(engine) -> None:
    print("\n>>> Preview: top LGAs by total road length")
    sql = text(
        """
        SELECT lg_name, network_type, segment_count, total_length_km
        FROM mart.lga_road_stats
        ORDER BY total_length_km DESC
        LIMIT 10
        """
    )

    with engine.connect() as conn:
        rows = conn.execute(sql).fetchall()

    for row in rows:
        print(
            f"{row.lg_name} | {row.network_type} | "
            f"segments={row.segment_count} | km={row.total_length_km}"
        )


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the WA Roads pipeline")
    parser.add_argument(
        "--with-ddl",
        "--with-schema",
        dest="with_ddl",
        action="store_true",
        help="Run sql/ddl/*.sql before ingesting",
    )
    parser.add_argument(
        "--with-docker",
        action="store_true",
        help="Run `docker compose up -d` before connecting",
    )
    parser.add_argument(
        "--skip-ingest",
        action="store_true",
        help="Skip src/ingest/mainroads_api.py",
    )
    parser.add_argument(
        "--skip-load",
        action="store_true",
        help="Skip src/load/postgres.py",
    )
    parser.add_argument(
        "--skip-transform",
        action="store_true",
        help="Skip sql/transforms/lga_road_stats.sql",
    )
    parser.add_argument(
        "--skip-checks",
        action="store_true",
        help="Skip sql/checks/road_segment_quality.sql",
    )
    parser.add_argument(
        "--strict-checks",
        action="store_true",
        help="Fail on missing name fields in addition to geometry/length checks",
    )
    args = parser.parse_args()

    start = time.time()
    database_url = database_url_from_env()

    if args.with_docker:
        print("\n>>> Starting docker services")
        subprocess.run(["docker", "compose", "up", "-d"], cwd=ROOT, check=True)

    engine = create_engine(database_url)
    wait_for_db(engine, timeout_s=90)

    print("=== WA Roads Pipeline Start ===")

    if args.with_ddl:
        run_ddl(engine)

    if not args.skip_ingest:
        run_python_script(INGEST_SCRIPT)

    if not args.skip_load:
        run_python_script(LOAD_SCRIPT)

    if not args.skip_transform:
        run_sql_file(engine, TRANSFORM_SQL)

    validate_tables(engine)
    if not args.skip_checks:
        run_quality_checks(engine, strict=args.strict_checks)
    preview_results(engine)

    elapsed = time.time() - start
    print(f"\n=== Pipeline complete in {elapsed:.2f} seconds ===")


if __name__ == "__main__":
    main()
