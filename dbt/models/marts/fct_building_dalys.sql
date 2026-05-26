-- Per-building expected annual DALYs from chronic PM2.5 exposure.
--
-- One row per Overture building × release × AOI. The headline column
-- is `daly_total`; per-cause breakdown is retained so the dashboard's
-- "top drivers" panel can show which disease dominates each tract.
--
-- The mart is deliberately thin — the Sedona silver job did the
-- expensive geospatial work, the dbt staging layer did the rename, so
-- this model is mostly a documented final shape with a few derived
-- audit columns:
--   * `monitor_distance_bucket` — bucketed distance for the dashboard
--     "monitor sparsity" map (CLAUDE.md §Pitfalls).
--   * `daly_per_capita` — DALY rate per person, equity-comparable
--     across tracts of different population density.

with stg as (
    select * from {{ ref('stg_building_silver') }}
)

select
    id                                                     as building_id,
    h3_res6,
    h3_res7,
    h3_res8,
    h3_res9,
    centroid_lon,
    centroid_lat,
    building_class,
    building_subtype,
    num_floors,
    footprint_area_m2,
    population,
    pm25_annual_mean,
    pm25_p98_day,
    pm25_peak_week,
    nearest_monitor_id,
    monitor_distance_km,
    monitor_neighbor_count,
    daly_ihd,
    daly_stroke,
    daly_copd,
    daly_lung_cancer,
    daly_lri,
    daly_total,
    case
        when daly_total is null or population is null or population <= 0 then null
        else daly_total / population
    end                                                    as daly_per_capita,
    case
        when monitor_distance_km is null then 'unknown'
        when monitor_distance_km <  10 then '0-10km'
        when monitor_distance_km <  50 then '10-50km'
        when monitor_distance_km < 100 then '50-100km'
        else '100km+'
    end                                                    as monitor_distance_bucket
from stg
