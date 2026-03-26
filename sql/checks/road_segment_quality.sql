-- Basic row count
SELECT
    'row_count' AS check_name,
    COUNT(*)::bigint AS result_count
FROM core.road_segment;

-- Duplicate source object IDs
SELECT
    'duplicate_source_objectid' AS check_name,
    COUNT(*)::bigint AS result_count
FROM (
    SELECT source_objectid
    FROM core.road_segment
    GROUP BY source_objectid
    HAVING COUNT(*) > 1
) d;

-- Null or empty key business fields
SELECT
    'missing_road_name' AS check_name,
    COUNT(*)::bigint AS result_count
FROM core.road_segment
WHERE road_name IS NULL OR TRIM(road_name) = '';

SELECT
    'missing_network_type' AS check_name,
    COUNT(*)::bigint AS result_count
FROM core.road_segment
WHERE network_type IS NULL OR TRIM(network_type) = '';

SELECT
    'missing_lg_name' AS check_name,
    COUNT(*)::bigint AS result_count
FROM core.road_segment
WHERE lg_name IS NULL OR TRIM(lg_name) = '';

SELECT
    'missing_ra_name' AS check_name,
    COUNT(*)::bigint AS result_count
FROM core.road_segment
WHERE ra_name IS NULL OR TRIM(ra_name) = '';

-- Geometry presence and validity
SELECT
    'missing_geom_4283' AS check_name,
    COUNT(*)::bigint AS result_count
FROM core.road_segment
WHERE geom_4283 IS NULL;

SELECT
    'invalid_geom_4283' AS check_name,
    COUNT(*)::bigint AS result_count
FROM core.road_segment
WHERE geom_4283 IS NOT NULL
  AND NOT ST_IsValid(geom_4283);

SELECT
    'empty_geom_4283' AS check_name,
    COUNT(*)::bigint AS result_count
FROM core.road_segment
WHERE geom_4283 IS NOT NULL
  AND ST_IsEmpty(geom_4283);

-- Geometry type checks
SELECT
    'non_line_geometry' AS check_name,
    COUNT(*)::bigint AS result_count
FROM core.road_segment
WHERE geom_4283 IS NOT NULL
  AND GeometryType(geom_4283) NOT IN ('LINESTRING', 'MULTILINESTRING');

-- Length checks
SELECT
    'missing_length_m' AS check_name,
    COUNT(*)::bigint AS result_count
FROM core.road_segment
WHERE length_m IS NULL;

SELECT
    'non_positive_length_m' AS check_name,
    COUNT(*)::bigint AS result_count
FROM core.road_segment
WHERE COALESCE(length_m, 0) <= 0;

SELECT
    'very_long_segments_over_100km' AS check_name,
    COUNT(*)::bigint AS result_count
FROM core.road_segment
WHERE length_m > 100000;

-- SLK logic checks
SELECT
    'missing_start_slk' AS check_name,
    COUNT(*)::bigint AS result_count
FROM core.road_segment
WHERE start_slk IS NULL;

SELECT
    'missing_end_slk' AS check_name,
    COUNT(*)::bigint AS result_count
FROM core.road_segment
WHERE end_slk IS NULL;

SELECT
    'end_slk_less_than_start_slk' AS check_name,
    COUNT(*)::bigint AS result_count
FROM core.road_segment
WHERE start_slk IS NOT NULL
  AND end_slk IS NOT NULL
  AND end_slk < start_slk;

-- Allowed network types based on source renderer categories
SELECT
    'unexpected_network_type' AS check_name,
    COUNT(*)::bigint AS result_count
FROM core.road_segment
WHERE COALESCE(TRIM(network_type), '') <> ''
  AND network_type NOT IN (
      'State Road',
      'Local Road',
      'Main Roads Controlled Path',
      'Miscellaneous Road',
      'Crossover'
  );

-- Detailed sample rows for investigation
SELECT
    source_objectid,
    road_name,
    network_type,
    lg_name,
    ra_name,
    start_slk,
    end_slk,
    length_m
FROM core.road_segment
WHERE road_name IS NULL
   OR TRIM(COALESCE(road_name, '')) = ''
   OR network_type IS NULL
   OR TRIM(COALESCE(network_type, '')) = ''
   OR geom_4283 IS NULL
   OR NOT ST_IsValid(geom_4283)
   OR COALESCE(length_m, 0) <= 0
   OR (start_slk IS NOT NULL AND end_slk IS NOT NULL AND end_slk < start_slk)
LIMIT 100;
