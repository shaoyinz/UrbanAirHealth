-- Fail if any building's total DALY is negative. CR coefficients are
-- positive log-linear slopes, occupancy and baseline mortality are
-- positive, so daly_total < 0 implies a code regression — typically a
-- flipped sign in attach_dalys() or a baseline_mortality_per_100k that
-- got loaded as negative from the YAML.
--
-- Returns the offending rows; dbt fails the test if any are returned.
select
    building_id,
    daly_total
from {{ ref('fct_building_dalys') }}
where daly_total < 0
