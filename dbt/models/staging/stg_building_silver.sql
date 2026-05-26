-- Staging passthrough over the silver-zone building parquet.
--
-- DuckDB target: reads the GeoParquet directory the Sedona job wrote.
--   DBT_SILVER_GLOB env var points at a path glob, e.g.
--     export DBT_SILVER_GLOB='/tmp/silver/buildings/release=*/aoi=*/*.parquet'
--   The recursive ** matches Sedona's partitioned write layout.
--
-- BigQuery target: dbt resolves {{ source('silver', 'building_silver') }}
--   to the external table Terraform creates over the GCS silver zone,
--   so the same model file works for both backends.
--
-- Staging deliberately keeps every column — it's a passthrough rename
-- layer per dbt convention, with the geometry dropped because neither
-- DuckDB's spatial extension nor BigQuery GEOGRAPHY accept WKB-as-bytes
-- through a view. Marts that need geometry can re-read the source
-- directly via `ST_GeomFromWKB`.

{% if target.type == 'duckdb' %}
{% set silver_glob = env_var('DBT_SILVER_GLOB', '../target/silver/buildings/release=*/aoi=*/*.parquet') %}
with src as (
    select * from read_parquet('{{ silver_glob }}', union_by_name=true)
)
{% else %}
with src as (
    select * from {{ source('silver', 'building_silver') }}
)
{% endif %}

select
    id,
    centroid_lon,
    centroid_lat,
    building_class,
    building_subtype,
    num_floors,
    height_m,
    footprint_area_m2,
    h3_res6,
    h3_res7,
    h3_res8,
    h3_res9,
    pm25_annual_mean,
    pm25_p98_day,
    pm25_peak_week,
    nearest_monitor_id,
    monitor_distance_km,
    monitor_neighbor_count,
    population,
    daly_ihd,
    daly_stroke,
    daly_copd,
    daly_lung_cancer,
    daly_lri,
    daly_total
from src
