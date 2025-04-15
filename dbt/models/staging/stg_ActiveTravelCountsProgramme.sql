{{
    config(
        materialized='view'
    )
}}

with source as (

    select * from {{ source('staging', 'ActiveTravelCountsProgramme') }}

) 
select
    wave,
    site_id,
    date,
    weather,
    time,
    day,
    round,
    direction,
    path,
    mode,
    count
from source
where extract(year from date) <= 2025 and extract(year from date) >= 2014 


-- {% if var('is_test_run', default=true) %}

--     limit 100

-- {% endif %}