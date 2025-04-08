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


