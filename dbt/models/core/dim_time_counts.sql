{{ config(
    materialized='table',
    partition_by={
        "field": "year",
        "data_type": "int",
        "range": {
            "start": 2010,
            "end": 2030,
            "interval": 1
        }
    },
    cluster_by=["site_id", "month_of_date", "mode", "direction"]
) }}

SELECT 
    f.site_id,
    l.borough,
    extract(month from f.date) as month_of_date,
    extract(year from f.date) as year,
    f.day as day_type,
    f.mode,
    f.direction,
    f.count,
    f.date
FROM {{ ref('fact_ActiveTravelCountsProgramme') }} f
    join {{ ref('dim_locations') }} l
        on f.site_id = l.site_id