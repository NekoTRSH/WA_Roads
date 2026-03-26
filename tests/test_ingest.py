from __future__ import annotations

import geopandas as gpd
from shapely.geometry import LineString, mapping

from src.ingest.mainroads_api import add_derived_columns, geojson_to_gdf


def test_geojson_to_gdf_empty() -> None:
    gdf = geojson_to_gdf({"type": "FeatureCollection", "features": []})
    assert isinstance(gdf, gpd.GeoDataFrame)
    assert gdf.empty
    assert gdf.crs is not None


def test_geojson_to_gdf_normalizes_columns() -> None:
    payload = {
        "type": "FeatureCollection",
        "features": [
            {
                "type": "Feature",
                "properties": {"OBJECTID": 1, "GLOBALID": "abc"},
                "geometry": mapping(LineString([(115.85, -31.95), (115.851, -31.951)])),
            }
        ],
    }
    gdf = geojson_to_gdf(payload)
    assert "source_objectid" in gdf.columns
    assert "source_globalid" in gdf.columns
    assert "geometry" in gdf.columns
    assert int(gdf.loc[0, "source_objectid"]) == 1
    assert str(gdf.loc[0, "source_globalid"]) == "abc"


def test_add_derived_columns_adds_length() -> None:
    payload = {
        "type": "FeatureCollection",
        "features": [
            {
                "type": "Feature",
                "properties": {"OBJECTID": 1},
                "geometry": mapping(LineString([(115.85, -31.95), (115.86, -31.96)])),
            }
        ],
    }
    gdf = geojson_to_gdf(payload)
    derived = add_derived_columns(gdf)
    assert "geom_3857" in derived.columns
    assert "length_m" in derived.columns
    assert float(derived.loc[0, "length_m"]) > 0
