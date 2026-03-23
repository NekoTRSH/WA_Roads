from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from pathlib import Path

import geopandas as gpd
import pandas as pd
import requests
from sqlalchemy import create_engine, text

LAYER_URL = os.getenv(
    "MAINROADS_ROAD_NETWORK_URL",
    "https://gisservices.mainroads.wa.gov.au/arcgis/rest/services/OpenData/RoadAssets_DataPortal/MapServer/17"
)

RAW_DIR = Path(os.getenv("RAW_ROAD_DIR", "data/raw/road_network"))
SILVER_DIR = Path(os.getenv("SILVER_DIR", "data/silver"))
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "2000"))
HTTP_TIMEOUT = int(os.getenv("HTTP_TIMEOUT", "60"))
DATABASE_URL = os.getenv("DATABASE_URL")

session = requests.Session()

########################################################

# Return UTC time in the format of YYYYMMDDHHMMSSZ
# For timestamps in filenames, logging etc
def utc_now_str() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")

# Create raw and silver data directories if it doesn't exist
# To prevent file-write failures later in the pipeline and nested paths are created properly
def ensure_dirs() -> None:
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    SILVER_DIR.mkdir(parents=True, exist_ok=True)

# Send HTTP GET to ArcGIS layer endpoint and retorn parsed JSON
# All queries share one path for timeout/error handling and session reuse
def query_json(params: dict) -> dict:
    response = session.get(f"{LAYER_URL}/query", params=params, timeout=HTTP_TIMEOUT)
    response.raise_for_status()
    return response.json()

# Calls query_json() with ArcGIS parameters to return total feature count and convert to int
# For pagination
def get_total_count() -> int:
    payload = query_json(
        {
            "where": "1=1",
            "returnCountOnly": "true",
            "f": "json"
        }
    )
    return int(payload["count"])

# Calls Main Roads ArcGIS "query" endpoint returns one page of road network features
# Also for pagination, able to pull whole layers in chunks wihtout timing out or loading all at once
def fetch_batch(offset: int, batch_size: int) -> dict:
    response = session.get(
        f"{LAYER_URL}/query",
        params={
            "where": "1=1",
            "outFields": "*",
            "returnGeometry": "true",
            "orderByFields": "OBJECTID ASC",
            "resultOffset": offset,
            "resultRecordCount": batch_size,
            "outSR": "4283",
            "f": "geojson",
        },
        timeout=HTTP_TIMEOUT,
    )
    response.raise_for_status()
    return response.json()

# Write raw JSON response for a batch to disk 
# For provenance (can recheck excatly what the API returned per batch) and to download from transform
def save_raw_payload(payload: dict, run_id: str, batch_number: int) -> Path:
    run_dir = RAW_DIR / run_id
    run_dir.mkdir(parents=True, exist_ok=True)
    output_path = run_dir / f"road_network_batch_{batch_number:04d}.json"
    with output_path.open("w", encoding="utf-8") as f:
        json.dump(payload, f)
    return output_path

# Converts GeoJSON features aarray into GeoDataFrame in EPSG:4283, normalises columns and perserve ids
# For turning API JSON into something that can be analysed
def geojson_to_gdf(payload: dict) -> gpd.GeoDataFrame:
    features = payload.get("features", [])
    if not features:
        return gpd.GeoDataFrame(geometry=[], crs="EPSG:4283")

    gdf = gpd.GeoDataFrame.from_features(features, crs="EPSG:4283")
    gdf.columns = [c.lower() for c in gdf.columns]

    rename_map = {
        "globalid": "source_globalid",
        "objectid": "source_objectid",
    }
    gdf = gdf.rename(columns=rename_map)

    return gdf

# Coordinates the whole paginated pull, get total feature count, loops offsets in BATCH_SIZE,
# fetches each batch, saves raw JSON, convert batch to GeoDataFrame. 
# This is the ingest + assemble step and returns both the combined data and the list of raw files
def build_run_dataframe(run_id: str) -> tuple[gpd.GeoDataFrame, list[Path]]:
    total_count = get_total_count()
    frames: list[gpd.GeoDataFrame] = []
    raw_files: list[Path] = []

    batch_number = 1
    for offset in range(0, total_count, BATCH_SIZE):
        payload = fetch_batch(offset=offset, batch_size=BATCH_SIZE)
        raw_path = save_raw_payload(payload, run_id=run_id, batch_number=batch_number)
        raw_files.append(raw_path)

        batch_gdf = geojson_to_gdf(payload)
        if not batch_gdf.empty:
            batch_gdf["source_run_id"] = run_id
            batch_gdf["ingested_at"] = pd.Timestamp.now(tz="UTC")
            frames.append(batch_gdf)

        batch_number += 1

    if not frames:
        return gpd.GeoDataFrame(geometry=[], crs="EPSG:4283"), raw_files

    combined = pd.concat(frames, ignore_index=True)
    gdf = gpd.GeoDataFrame(combined, geometry="geometry", crs="EPSG:4283")
    return gdf, raw_files

# Adds fields that are computed to EPSG:3857 and calculates length_m from the geometry
# For adding convienent attribute. Note that EPS:3857 is an approximation
def add_derived_columns(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    if gdf.empty:
        return gdf

    gdf = gdf.copy()
    gdf["geom_3857"] = gdf.geometry.to_crs(epsg=3857)
    gdf["length_m"] = gdf["geom_3857"].length
    return gdf

# Saves the (silver) file dataset to the SILVER_DIR
# For producing compact, columnar file which are fast to read downstream
def save_parquet(gdf: gpd.GeoDataFrame, run_id: str) -> Path:
    output_path = SILVER_DIR / f"road_network_{run_id}.parquet"
    gdf.to_parquet(output_path, index=False)
    return output_path

# Inserts a run record into audit.pipeline_runs (Postgres) with statys counts etc... 
# For operational observability, can track successes and failures and what was produced 
def write_audit_row(engine, run_id: str, status: str, rows_fetched: int, files_written: int, error_message: str | None = None) -> None:
    sql = text(
        """
        INSERT INTO audit.pipeline_runs (
            run_id, pipeline_name, source_name, layer_id,
            started_at, ended_at, status, rows_fetched, files_written, error_message, metadata
        )
        VALUES (
            CAST(:run_id AS uuid),
            :pipeline_name,
            :source_name,
            :layer_id,
            now(),
            now(),
            :status,
            :rows_fetched,
            :files_written,
            :error_message,
            CAST(:metadata AS jsonb)
        )
        """
    )

    with engine.begin() as conn:
        conn.execute(
            sql,
            {
                "run_id": run_id,
                "pipeline_name": "mainroads_road_network_ingest",
                "source_name": "mainroads",
                "layer_id": 17,
                "status": status,
                "rows_fetched": rows_fetched,
                "files_written": files_written,
                "error_message": error_message,
                "metadata": json.dumps({"layer_url": LAYER_URL}),
            },
        )


def main() -> None:
    ensure_dirs()
    run_id = os.getenv("RUN_ID", None) or str(pd.util.hash_pandas_object(pd.Series([utc_now_str()])).astype(str).iloc[0])

    if len(run_id) < 32:
        run_id = "00000000-0000-0000-0000-" + utc_now_str().replace("T", "").replace("Z", "")[-12:]

    run_id = run_id[:36]

    try:
        gdf, raw_files = build_run_dataframe(run_id=run_id)
        gdf = add_derived_columns(gdf)
        parquet_path = save_parquet(gdf, run_id=run_id)

        if DATABASE_URL:
            engine = create_engine(DATABASE_URL)
            write_audit_row(
                engine=engine,
                run_id=run_id,
                status="success",
                rows_fetched=len(gdf),
                files_written=len(raw_files) + 1,
            )

        print(f"Saved {len(gdf)} rows")
        print(f"Parquet: {parquet_path}")
        print(f"Raw files: {len(raw_files)}")

    except Exception as exc:
        if DATABASE_URL:
            engine = create_engine(DATABASE_URL)
            write_audit_row(
                engine=engine,
                run_id=run_id,
                status="failed",
                rows_fetched=0,
                files_written=0,
                error_message=str(exc),
            )
        raise


if __name__ == "__main__":
    main()