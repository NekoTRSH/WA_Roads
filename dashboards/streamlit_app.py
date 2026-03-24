from __future__ import annotations

import json
import os

import folium
import pandas as pd
import plotly.express as px
import streamlit as st
from dotenv import load_dotenv
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
            ROUND(SUM(COALESCE(length_m, 0)) / 1000.0, 3) AS total_length_km
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
        return pd.DataFrame(
            columns=["lg_no", "lg_name", "segment_count", "total_length_km"]
        )

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
def load_map_segments(
    lg_name: str | None,
    network_type: str | None,
    limit: int = 500
) -> pd.DataFrame:
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
            source_objectid,
            COALESCE(road_name, 'Unknown') AS road_name,
            COALESCE(network_type, 'Unknown') AS network_type,
            COALESCE(lg_name, 'Unknown') AS lg_name,
            ST_AsGeoJSON(geom_4283) AS geom_geojson
        FROM core.road_segment
        {where_clause}
        ORDER BY source_objectid
        LIMIT :limit
        """
    )

    with get_engine().connect() as conn:
        df = pd.read_sql(sql, conn, params=params)
    return df


def build_feature_collection(df: pd.DataFrame) -> dict:
    features = []
    for _, row in df.iterrows():
        if not row["geom_geojson"]:
            continue
        geometry = json.loads(row["geom_geojson"])
        features.append(
            {
                "type": "Feature",
                "geometry": geometry,
                "properties": {
                    "road_name": row["road_name"],
                    "network_type": row["network_type"],
                    "lg_name": row["lg_name"],
                    "source_objectid": row["source_objectid"],
                },
            }
        )
    return {"type": "FeatureCollection", "features": features}


def compute_map_center(feature_collection: dict) -> tuple[float, float]:
    lats = []
    lons = []

    for feature in feature_collection.get("features", []):
        geom = feature.get("geometry", {})
        coords = geom.get("coordinates", [])
        geom_type = geom.get("type")

        if geom_type == "LineString":
            for lon, lat, *_ in coords:
                lons.append(lon)
                lats.append(lat)
        elif geom_type == "MultiLineString":
            for line in coords:
                for lon, lat, *_ in line:
                    lons.append(lon)
                    lats.append(lat)

    if not lats or not lons:
        return -25.2744, 122.0

    return sum(lats) / len(lats), sum(lons) / len(lons)


def network_color(value: str) -> str:
    palette = {
        "State Road": "#00C5FF",
        "Local Road": "#CCCCCC",
        "Main Roads Controlled Path": "#C500FF",
        "Miscellaneous Road": "#FFAA00",
        "Crossover": "#000000",
        "Unknown": "#666666",
    }
    return palette.get(value, "#666666")


def make_map(feature_collection: dict) -> folium.Map:
    center_lat, center_lon = compute_map_center(feature_collection)
    m = folium.Map(location=[center_lat, center_lon], zoom_start=6, tiles="CartoDB positron")

    def style_fn(feature):
        net = feature["properties"].get("network_type", "Unknown")
        return {
            "color": network_color(net),
            "weight": 2,
            "opacity": 0.9,
        }

    folium.GeoJson(
        feature_collection,
        style_function=style_fn,
        tooltip=folium.GeoJsonTooltip(
            fields=["road_name", "network_type", "lg_name", "source_objectid"],
            aliases=["Road", "Network type", "LGA", "ObjectID"],
            localize=True,
            sticky=False,
        ),
        name="Road segments",
    ).add_to(m)

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

    st.sidebar.markdown("---")
    map_limit = st.sidebar.slider("Map segment sample", min_value=50, max_value=1500, value=500, step=50)

    if selected_lga != "All":
        lga_summary_view = lga_summary[lga_summary["lg_name"] == selected_lga].copy()
    else:
        lga_summary_view = lga_summary.copy()

    total_length = float(lga_summary_view["total_length_km"].sum()) if not lga_summary_view.empty else 0.0
    total_segments = int(lga_summary_view["segment_count"].sum()) if not lga_summary_view.empty else 0
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

    st.subheader("Sample road map")
    map_df = load_map_segments(
        lg_name=None if selected_lga == "All" else selected_lga,
        network_type=None if selected_map_network == "All" else selected_map_network,
        limit=map_limit,
    )

    if map_df.empty:
        st.info("No road segments found for the selected map filters.")
    else:
        feature_collection = build_feature_collection(map_df)
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
