-- Core modeled tables used by load + dashboard.

CREATE TABLE IF NOT EXISTS core.road_segment (
    source_run_id uuid NOT NULL,
    source_objectid bigint,
    source_globalid text,

    road text,
    road_name text,
    common_usage_name text,
    start_slk double precision,
    end_slk double precision,
    cwy text,
    start_true_dist double precision,
    end_true_dist double precision,
    network_type text,
    ra_no text,
    ra_name text,
    lg_no text,
    lg_name text,
    start_node_no text,
    start_node_name text,
    end_node_no text,
    end_node_name text,
    datum_ne_id text,
    nm_begin_mp double precision,
    nm_end_mp double precision,
    network_element text,
    route_ne_id text,

    geom_4283 geometry(Geometry, 4283),
    geom_3857 geometry(Geometry, 3857),
    length_m double precision,
    ingested_at timestamptz NOT NULL DEFAULT now(),

    PRIMARY KEY (source_run_id, source_objectid)
);

CREATE INDEX IF NOT EXISTS idx_road_segment_geom_4283
    ON core.road_segment
    USING gist (geom_4283);

CREATE INDEX IF NOT EXISTS idx_road_segment_geom_3857
    ON core.road_segment
    USING gist (geom_3857);

CREATE INDEX IF NOT EXISTS idx_road_segment_lg_name
    ON core.road_segment (lg_name);

CREATE INDEX IF NOT EXISTS idx_road_segment_network_type
    ON core.road_segment (network_type);
