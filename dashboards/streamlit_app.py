from __future__ import annotations

import json
import os
import re
from pathlib import Path

import branca.colormap as cm
import folium
import pandas as pd
import plotly.express as px
import requests
import streamlit as st
from dotenv import load_dotenv
from shapely.geometry import mapping, shape
from shapely.ops import unary_union
from sqlalchemy import create_engine, text
from streamlit_folium import st_folium

load_dotenv()

st.set_page_config(
    page_title="WA Roads Dashboard",
    page_icon="🛣️",
    layout="wide",
)


def default_database_url() -> str:
    user = os.getenv("POSTGRES_USER", "wa_admin")
    password = os.getenv("POSTGRES_PASSWORD", "change_me")
    db = os.getenv("POSTGRES_DB", "wa_roads")
    host = os.getenv("POSTGRES_HOST", "localhost")
    port = os.getenv("POSTGRES_PORT", "5432")
    return f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db}"


DATABASE_URL = os.getenv("DATABASE_URL", default_database_url())
WA_LGA_BOUNDARIES_URL = os.getenv(
    "WA_LGA_BOUNDARIES_URL",
    "https://geo.abs.gov.au/arcgis/rest/services/ASGS2024/LGA/FeatureServer/0",
)
WA_LGA_BOUNDARIES_GEOJSON_PATH = os.getenv("WA_LGA_BOUNDARIES_GEOJSON_PATH", "")


@st.cache_resource
def get_engine():
    return create_engine(DATABASE_URL)


@st.cache_data(ttl=300)
def get_available_dates() -> list[str]:
    sql = """
        SELECT DISTINCT stat_date::text AS stat_date
        FROM mart.lga_road_stats
        ORDER BY stat_date DESC
    """
    with get_engine().connect() as conn:
        df = pd.read_sql(sql, conn)
    return df["stat_date"].tolist()


@st.cache_data(ttl=300)
def load_lga_stats(stat_date: str) -> pd.DataFrame:
    sql = text(
        """
        SELECT
            stat_date,
            lg_no,
            lg_name,
            network_type,
            segment_count,
            total_length_km
        FROM mart.lga_road_stats
        WHERE stat_date = :stat_date
        ORDER BY total_length_km DESC
        """
    )
    with get_engine().connect() as conn:
        df = pd.read_sql(sql, conn, params={"stat_date": stat_date})
    return df


@st.cache_data(ttl=300)
def load_network_summary(stat_date: str) -> pd.DataFrame:
    sql = text(
        """
        SELECT
            network_type,
            SUM(segment_count)::int AS segment_count,
            ROUND(SUM(total_length_km)::numeric, 3) AS total_length_km
        FROM mart.lga_road_stats
        WHERE stat_date = :stat_date
        GROUP BY network_type
        ORDER BY total_length_km DESC
        """
    )
    with get_engine().connect() as conn:
        df = pd.read_sql(sql, conn, params={"stat_date": stat_date})
    return df


@st.cache_data(ttl=300)
def load_top_roads(lg_name: str | None, network_type: str | None, limit: int = 20) -> pd.DataFrame:
    filters = []
    params = {"limit": limit}

    if lg_name and lg_name != "All":
        filters.append("lg_name = :lg_name")
        params["lg_name"] = lg_name

    if network_type and network_type != "All":
        filters.append("network_type = :network_type")
        params["network_type"] = network_type

    where_clause = " AND ".join(filters)
    if where_clause:
        where_clause = "WHERE " + where_clause

    sql = text(
        f"""
        SELECT
            COALESCE(NULLIF(TRIM(road_name), ''), 'Unknown') AS road_name,
            COUNT(*)::int AS segment_count,
            ROUND((SUM(COALESCE(length_m, 0)) / 1000.0)::numeric, 3) AS total_length_km
        FROM core.road_segment
        {where_clause}
        GROUP BY COALESCE(NULLIF(TRIM(road_name), ''), 'Unknown')
        ORDER BY total_length_km DESC
        LIMIT :limit
        """
    )

    with get_engine().connect() as conn:
        df = pd.read_sql(sql, conn, params=params)
    return df


@st.cache_data(ttl=300)
def load_filtered_lga_stats(stat_date: str, selected_networks: list[str]) -> pd.DataFrame:
    if not selected_networks:
        return pd.DataFrame(columns=["lg_no", "lg_name", "segment_count", "total_length_km"])

    sql = text(
        """
        SELECT
            lg_no,
            lg_name,
            SUM(segment_count)::int AS segment_count,
            ROUND(SUM(total_length_km)::numeric, 3) AS total_length_km
        FROM mart.lga_road_stats
        WHERE stat_date = :stat_date
          AND network_type = ANY(:selected_networks)
        GROUP BY lg_no, lg_name
        ORDER BY total_length_km DESC
        """
    )

    with get_engine().connect() as conn:
        df = pd.read_sql(
            sql,
            conn,
            params={"stat_date": stat_date, "selected_networks": selected_networks},
        )
    return df


@st.cache_data(ttl=300)
def load_wa_lga_boundaries() -> dict:
    candidate_paths = []
    if WA_LGA_BOUNDARIES_GEOJSON_PATH:
        candidate_paths.append(Path(WA_LGA_BOUNDARIES_GEOJSON_PATH))
    candidate_paths.append(Path(__file__).resolve().parent / "assets" / "wa_lga_boundaries.geojson")

    for path in candidate_paths:
        if path.exists():
            payload = json.loads(path.read_text(encoding="utf-8"))
            if payload.get("type") != "FeatureCollection":
                raise ValueError(f"Boundary file is not GeoJSON FeatureCollection: {path}")
            return payload

    response = requests.get(
        f"{WA_LGA_BOUNDARIES_URL}/query",
        params={
            "where": "STATE_CODE_2021='5'",
            "outFields": "LGA_CODE_2024,LGA_NAME_2024",
            "returnGeometry": "true",
            "outSR": "4326",
            "f": "geojson",
        },
        timeout=60,
    )
    response.raise_for_status()
    payload = response.json()

    if payload.get("type") != "FeatureCollection":
        raise ValueError("Boundary API did not return GeoJSON FeatureCollection.")

    return payload


def normalize_lga_code(value: object) -> str:
    if value is None:
        return ""
    digits = re.sub(r"\D", "", str(value))
    return digits.lstrip("0") or "0" if digits else ""


def normalize_lga_name(value: object) -> str:
    if value is None:
        return ""
    return re.sub(r"[^A-Z0-9]", "", str(value).upper())


def build_lga_choropleth_feature_collection(
    boundaries_geojson: dict,
    lga_stats: pd.DataFrame,
) -> dict:
    stats_by_code: dict[str, dict] = {}
    stats_by_name: dict[str, dict] = {}

    for _, row in lga_stats.iterrows():
        metrics = {
            "lg_no": str(row.get("lg_no", "")),
            "lg_name": str(row.get("lg_name", "Unknown")),
            "segment_count": int(row.get("segment_count", 0) or 0),
            "total_length_km": float(row.get("total_length_km", 0) or 0),
        }

        code_key = normalize_lga_code(row.get("lg_no"))
        name_key = normalize_lga_name(row.get("lg_name"))
        if code_key:
            stats_by_code[code_key] = metrics
        if name_key:
            stats_by_name[name_key] = metrics

    features = []
    for feature in boundaries_geojson.get("features", []):
        props = feature.get("properties", {})
        geometry = feature.get("geometry")
        if not geometry:
            continue

        boundary_code = normalize_lga_code(props.get("LGA_CODE_2024"))
        boundary_name = normalize_lga_name(props.get("LGA_NAME_2024"))

        matched = stats_by_code.get(boundary_code) or stats_by_name.get(boundary_name)
        if matched is None:
            matched = {
                "lg_no": props.get("LGA_CODE_2024", ""),
                "lg_name": props.get("LGA_NAME_2024", "Unknown"),
                "segment_count": 0,
                "total_length_km": 0.0,
            }

        features.append(
            {
                "type": "Feature",
                "geometry": geometry,
                "properties": {
                    "lg_no": matched["lg_no"],
                    "lg_name": matched["lg_name"],
                    "segment_count": matched["segment_count"],
                    "total_length_km": matched["total_length_km"],
                },
            }
        )

    return {"type": "FeatureCollection", "features": features}


def compute_map_center(feature_collection: dict) -> tuple[float, float]:
    lats = []
    lons = []

    def walk_coordinates(node):
        if isinstance(node, list):
            if node and isinstance(node[0], int | float) and len(node) >= 2:
                lon, lat = node[0], node[1]
                lons.append(lon)
                lats.append(lat)
                return
            for child in node:
                walk_coordinates(child)

    for feature in feature_collection.get("features", []):
        geom = feature.get("geometry") or {}
        if not isinstance(geom, dict):
            continue
        coords = geom.get("coordinates") or []
        walk_coordinates(coords)

    if not lats or not lons:
        return -25.2744, 122.0

    return sum(lats) / len(lats), sum(lons) / len(lons)


def make_map(feature_collection: dict) -> folium.Map:
    center_lat, center_lon = compute_map_center(feature_collection)
    m = folium.Map(location=[center_lat, center_lon], zoom_start=6, tiles="CartoDB positron")

    lengths = [
        float(f.get("properties", {}).get("total_length_km", 0) or 0)
        for f in feature_collection.get("features", [])
    ]
    max_length = max(lengths) if lengths else 0.0
    scale_max = max(max_length, 1.0)
    colormap = cm.linear.YlGnBu_09.scale(0, scale_max)
    colormap.caption = "Total road length (km)"

    def style_fn(feature):
        value = float(feature["properties"].get("total_length_km", 0) or 0)
        if value <= 0:
            return {
                "fillColor": "#f2f2f2",
                "color": "#8a8a8a",
                "weight": 0.8,
                "fillOpacity": 0.45,
            }
        return {
            "fillColor": colormap(value),
            "color": "#4f4f4f",
            "weight": 0.8,
            "fillOpacity": 0.72,
        }

    folium.GeoJson(
        feature_collection,
        style_function=style_fn,
        highlight_function=lambda _: {"weight": 2, "color": "#111111", "fillOpacity": 0.9},
        tooltip=folium.GeoJsonTooltip(
            fields=["lg_name", "lg_no", "total_length_km", "segment_count"],
            aliases=["LGA", "LGA code", "Road length (km)", "Segment count"],
            localize=True,
            sticky=False,
        ),
        name="LGA choropleth",
    ).add_to(m)

    # Dissolve LGAs to show the WA state outline as a distinct boundary layer.
    shapes = [
        shape(f["geometry"]) for f in feature_collection.get("features", []) if f.get("geometry")
    ]
    if shapes:
        wa_outline = mapping(unary_union(shapes))
        folium.GeoJson(
            {
                "type": "Feature",
                "geometry": wa_outline,
                "properties": {"name": "Western Australia"},
            },
            style_function=lambda _: {
                "fillOpacity": 0,
                "color": "#000000",
                "weight": 2.5,
                "opacity": 1,
            },
            name="WA boundary",
        ).add_to(m)

    colormap.add_to(m)

    folium.LayerControl().add_to(m)
    return m


def main():
    st.title("Western Australia Roads Dashboard")
    st.caption("Road network analytics from your PostGIS pipeline")

    try:
        available_dates = get_available_dates()
    except Exception as exc:
        st.error(f"Could not read mart.lga_road_stats: {exc}")
        st.stop()

    if not available_dates:
        st.warning("No mart data found. Run your ingest, load, and transform steps first.")
        st.stop()

    selected_date = st.sidebar.selectbox("Snapshot date", available_dates, index=0)

    base_stats = load_lga_stats(selected_date)
    network_summary = load_network_summary(selected_date)

    network_options = sorted(base_stats["network_type"].dropna().unique().tolist())
    selected_networks = st.sidebar.multiselect(
        "Network types",
        options=network_options,
        default=network_options,
    )

    lga_summary = load_filtered_lga_stats(selected_date, selected_networks)

    lga_options = ["All"] + sorted(lga_summary["lg_name"].dropna().unique().tolist())
    selected_lga = st.sidebar.selectbox("LGA filter", lga_options, index=0)

    map_network_options = ["All"] + network_options
    selected_map_network = st.sidebar.selectbox("Map network type", map_network_options, index=0)

    if selected_lga != "All":
        lga_summary_view = lga_summary[lga_summary["lg_name"] == selected_lga].copy()
    else:
        lga_summary_view = lga_summary.copy()

    total_length = (
        float(lga_summary_view["total_length_km"].sum()) if not lga_summary_view.empty else 0.0
    )
    total_segments = (
        int(lga_summary_view["segment_count"].sum()) if not lga_summary_view.empty else 0
    )
    total_lgas = int(lga_summary_view["lg_name"].nunique()) if not lga_summary_view.empty else 0

    c1, c2, c3 = st.columns(3)
    c1.metric("Total road length (km)", f"{total_length:,.2f}")
    c2.metric("Segment count", f"{total_segments:,}")
    c3.metric("LGAs in view", f"{total_lgas:,}")

    left, right = st.columns([1.2, 1])

    with left:
        st.subheader("Top LGAs by road length")
        top_lgas = lga_summary_view.head(20).copy()

        if top_lgas.empty:
            st.info("No data for the selected filters.")
        else:
            fig_lga = px.bar(
                top_lgas.sort_values("total_length_km", ascending=True),
                x="total_length_km",
                y="lg_name",
                orientation="h",
                color="total_length_km",
                color_continuous_scale="Blues",
                labels={"total_length_km": "Road length (km)", "lg_name": "LGA"},
            )
            fig_lga.update_layout(height=650, coloraxis_showscale=False)
            st.plotly_chart(fig_lga, use_container_width=True)

    with right:
        st.subheader("Network type breakdown")
        if network_summary.empty:
            st.info("No network summary available.")
        else:
            network_view = network_summary[
                network_summary["network_type"].isin(selected_networks)
            ].copy()

            fig_network = px.bar(
                network_view,
                x="network_type",
                y="total_length_km",
                color="network_type",
                labels={
                    "network_type": "Network type",
                    "total_length_km": "Road length (km)",
                },
            )
            fig_network.update_layout(height=320, showlegend=False)
            st.plotly_chart(fig_network, use_container_width=True)

            fig_segments = px.pie(
                network_view,
                names="network_type",
                values="segment_count",
                hole=0.45,
            )
            fig_segments.update_layout(height=320)
            st.plotly_chart(fig_segments, use_container_width=True)

    st.subheader("Top roads")
    top_roads = load_top_roads(
        lg_name=None if selected_lga == "All" else selected_lga,
        network_type=None if selected_map_network == "All" else selected_map_network,
        limit=20,
    )
    st.dataframe(top_roads, use_container_width=True, hide_index=True)

    st.subheader("LGA road-length choropleth")
    try:
        wa_boundaries = load_wa_lga_boundaries()
    except Exception as exc:
        st.error("Could not load WA LGA boundaries.")
        st.caption(
            "Fix: set `WA_LGA_BOUNDARIES_GEOJSON_PATH` to a local GeoJSON file, or override "
            "`WA_LGA_BOUNDARIES_URL` to a working ArcGIS FeatureServer layer."
        )
        st.exception(exc)
    else:
        boundary_features = wa_boundaries.get("features") or []
        boundary_count = len(boundary_features) if isinstance(boundary_features, list) else 0

        feature_collection = build_lga_choropleth_feature_collection(
            wa_boundaries, lga_summary_view
        )
        choropleth_features = feature_collection.get("features") or []
        choropleth_count = len(choropleth_features) if isinstance(choropleth_features, list) else 0

        if choropleth_count == 0:
            st.warning(
                "No usable LGA geometries were found for the choropleth "
                f"(loaded {boundary_count} boundary features, rendered 0)."
            )
            st.caption(
                "This usually means the boundary response has missing/empty geometries or is not GeoJSON. "
                "Try setting `WA_LGA_BOUNDARIES_GEOJSON_PATH` to a local GeoJSON FeatureCollection, or "
                "update the `outFields`/property keys used in the join."
            )
            with st.expander("Choropleth debug"):
                sample_props = {}
                if isinstance(boundary_features, list) and boundary_features:
                    props = boundary_features[0].get("properties")
                    if isinstance(props, dict):
                        sample_props = {k: props.get(k) for k in list(props)[:15]}
                st.write(
                    {
                        "boundary_feature_count": boundary_count,
                        "choropleth_feature_count": choropleth_count,
                        "sample_boundary_properties": sample_props,
                    }
                )

            road_map = folium.Map(
                location=[-25.2744, 122.0],
                zoom_start=6,
                tiles="CartoDB positron",
            )
            st_folium(road_map, width=None, height=650)
        else:
            road_map = make_map(feature_collection)
            st_folium(road_map, width=None, height=650)

    st.subheader("LGA statistics table")
    st.dataframe(
        lga_summary_view.sort_values("total_length_km", ascending=False),
        use_container_width=True,
        hide_index=True,
    )


if __name__ == "__main__":
    main()
