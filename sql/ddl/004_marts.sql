-- Mart tables used by the Streamlit dashboard.

CREATE TABLE IF NOT EXISTS mart.lga_road_stats (
    stat_date date NOT NULL,
    lg_no text NOT NULL,
    lg_name text NOT NULL,
    network_type text NOT NULL,
    segment_count integer NOT NULL,
    total_length_km numeric(18, 3) NOT NULL,
    PRIMARY KEY (stat_date, lg_no, network_type)
);
