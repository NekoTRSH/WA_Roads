from __future__ import annotations

from pathlib import Path

import geopandas as gpd
import pandas as pd
from shapely.geometry import LineString

from src.load.postgres import latest_parquet_file, normalize_columns


def test_latest_parquet_file_selects_latest(tmp_path: Path) -> None:
    (tmp_path / "road_network_0001.parquet").write_bytes(b"")
    latest = tmp_path / "road_network_0002.parquet"
    latest.write_bytes(b"")
    assert latest_parquet_file(tmp_path) == latest


def test_normalize_columns_adds_expected_fields() -> None:
    df = pd.DataFrame(
        {
            "source_run_id": ["00000000-0000-0000-0000-000000000000"],
            "road_name": ["Test Rd"],
        }
    )
    gdf = gpd.GeoDataFrame(df, geometry=[LineString([(0, 0), (1, 1)])], crs="EPSG:4283")
    normalized = normalize_columns(gdf)
    assert "network_type" in normalized.columns
    assert "geometry" in normalized.columns
    assert normalized.shape[0] == 1
