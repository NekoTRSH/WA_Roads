from __future__ import annotations

import os
from pathlib import Path

import geopandas as gpd
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text


load_dotenv()

SILVER_DIR = Path(os.getenv("SILVER_DIR", "data/silver"))


def default_database_url() -> str:
    user = os.getenv("POSTGRES_USER", "wa_admin")
    password = os.getenv("POSTGRES_PASSWORD", "change_me")
    db = os.getenv("POSTGRES_DB", "wa_roads")
    host = os.getenv("POSTGRES_HOST", "localhost")
    port = os.getenv("POSTGRES_PORT", "5432")
    return f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db}"


DATABASE_URL = os.getenv("DATABASE_URL", default_database_url())
TARGET_TABLE = "core.road_segment"

########################################################

# Finds most recent road_network.paraquet file and return the path
# For automatically loading the latest silver output
def latest_parquet_file(directory: Path) -> Path:
    files = sorted(directory.glob("road_network_*.parquet"))
    if not files:
        raise FileNotFoundError(f"No parquet files found in {directory}")
    return files[-1]

# Reads the paraquet into a GeoDataFrame, ensuring CRS is set to EPSG:4283, if missing derive geom_3875 and length_m
# For consistent, load-ready roads dataset regardless of what CRS metadata says in the parquet
def read_roads(parquet_path: Path) -> gpd.GeoDataFrame:
    gdf = gpd.read_parquet(parquet_path)

    if gdf.crs is None:
        gdf = gdf.set_crs(epsg=4283)

    if gdf.crs.to_epsg() != 4283:
        gdf = gdf.to_crs(epsg=4283)

    gdf = gdf.copy()
    gdf["geom_3857"] = gdf.geometry.to_crs(epsg=3857)
    gdf["length_m"] = gdf["geom_3857"].length

    return gdf

# Lowercase all column names and has an "expected" schema, missing columns will be "none"
# For making the load step easy even when fields change or are absent
def normalize_columns(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    gdf = gdf.copy()
    gdf.columns = [c.lower() for c in gdf.columns]

    expected = [
        "source_run_id",
        "source_objectid",
        "source_globalid",
        "road",
        "road_name",
        "common_usage_name",
        "start_slk",
        "end_slk",
        "cwy",
        "start_true_dist",
        "end_true_dist",
        "network_type",
        "ra_no",
        "ra_name",
        "lg_no",
        "lg_name",
        "start_node_no",
        "start_node_name",
        "end_node_no",
        "end_node_name",
        "datum_ne_id",
        "nm_begin_mp",
        "nm_end_mp",
        "network_element",
        "route_ne_id",
        "length_m",
        "geometry",
        "ingested_at",
    ]

    for col in expected:
        if col not in gdf.columns:
            gdf[col] = None

    gdf = gdf[expected]
    return gdf

# Executes TRUNCATE TABLE inside transaction for full refresh load
def truncate_target(engine) -> None:
    sql = text("TRUNCATE TABLE core.road_segment;")
    with engine.begin() as conn:
        conn.execute(sql)

# Loads the GeoDataFrame into core.road_segment row-by-row, builds geom_4283 from geometry
# computes a 3857 version, converts both geometries to WKT, and inserts
# Retuns the number of rows
def load_to_postgis(gdf: gpd.GeoDataFrame, engine) -> int:
    if gdf.empty:
        return 0

    df = gdf.copy()

    df["geom_4283"] = df["geometry"]
    df_3857 = gpd.GeoDataFrame(df.drop(columns=["geometry"]), geometry="geom_4283", crs="EPSG:4283")
    df["geom_3857"] = df_3857["geom_4283"].to_crs(epsg=3857)

    load_cols = [
        "source_run_id",
        "source_objectid",
        "source_globalid",
        "road",
        "road_name",
        "common_usage_name",
        "start_slk",
        "end_slk",
        "cwy",
        "start_true_dist",
        "end_true_dist",
        "network_type",
        "ra_no",
        "ra_name",
        "lg_no",
        "lg_name",
        "start_node_no",
        "start_node_name",
        "end_node_no",
        "end_node_name",
        "datum_ne_id",
        "nm_begin_mp",
        "nm_end_mp",
        "network_element",
        "route_ne_id",
        "length_m",
        "ingested_at",
    ]

    records = df[load_cols].to_dict(orient="records")
    geom_4283_wkt = df["geom_4283"].to_wkt().tolist()
    geom_3857_wkt = df["geom_3857"].to_wkt().tolist()

    insert_sql = text(
        """
        INSERT INTO core.road_segment (
            source_run_id,
            source_objectid,
            source_globalid,
            road,
            road_name,
            common_usage_name,
            start_slk,
            end_slk,
            cwy,
            start_true_dist,
            end_true_dist,
            network_type,
            ra_no,
            ra_name,
            lg_no,
            lg_name,
            start_node_no,
            start_node_name,
            end_node_no,
            end_node_name,
            datum_ne_id,
            nm_begin_mp,
            nm_end_mp,
            network_element,
            route_ne_id,
            geom_4283,
            geom_3857,
            length_m,
            ingested_at
        )
        VALUES (
            CAST(:source_run_id AS uuid),
            :source_objectid,
            :source_globalid,
            :road,
            :road_name,
            :common_usage_name,
            :start_slk,
            :end_slk,
            :cwy,
            :start_true_dist,
            :end_true_dist,
            :network_type,
            :ra_no,
            :ra_name,
            :lg_no,
            :lg_name,
            :start_node_no,
            :start_node_name,
            :end_node_no,
            :end_node_name,
            :datum_ne_id,
            :nm_begin_mp,
            :nm_end_mp,
            :network_element,
            :route_ne_id,
            ST_GeomFromText(:geom_4283_wkt, 4283),
            ST_GeomFromText(:geom_3857_wkt, 3857),
            :length_m,
            COALESCE(:ingested_at, now())
        )
        """
    )

    with engine.begin() as conn:
        for rec, wkt_4283, wkt_3857 in zip(records, geom_4283_wkt, geom_3857_wkt):
            rec["geom_4283_wkt"] = wkt_4283
            rec["geom_3857_wkt"] = wkt_3857
            conn.execute(insert_sql, rec)

    return len(df)

def main() -> None:
    parquet_path = latest_parquet_file(SILVER_DIR)
    gdf = read_roads(parquet_path)
    gdf = normalize_columns(gdf)

    engine = create_engine(DATABASE_URL)

    truncate_target(engine)
    row_count = load_to_postgis(gdf, engine)

    print(f"Loaded {row_count} rows from {parquet_path} into {TARGET_TABLE}")


if __name__ == "__main__":
    main()
