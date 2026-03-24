TRUNCATE TABLE mart.lga_road_stats;

INSERT INTO mart.lga_road_stats (
    stat_date,
    lg_no,
    lg_name,
    network_type,
    segment_count,
    total_length_km
)
SELECT
    current_date AS stat_date,
    COALESCE(NULLIF(TRIM(lg_no), ''), 'UNK') AS lg_no,
    COALESCE(NULLIF(TRIM(lg_name), ''), 'Unknown') AS lg_name,
    COALESCE(NULLIF(TRIM(network_type), ''), 'Unknown') AS network_type,
    COUNT(*)::integer AS segment_count,
    ROUND((SUM(COALESCE(length_m, 0)) / 1000.0)::numeric, 3) AS total_length_km
FROM core.road_segment
GROUP BY
    COALESCE(NULLIF(TRIM(lg_no), ''), 'UNK'),
    COALESCE(NULLIF(TRIM(lg_name), ''), 'Unknown'),
    COALESCE(NULLIF(TRIM(network_type), ''), 'Unknown')
ORDER BY
    lg_name,
    network_type;
