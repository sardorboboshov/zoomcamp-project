{{
    config(
        materialized='view'
    )
}}

with source as (

    select *,
        row_number() over(partition by rental_id) as rn 
    from {{ source('staging', 'usage-stats') }}
    where rental_id is not null
)

select
        rental_id,
        duration,
        bike_id,
        end_date,
        endstation_id,
        endstation_name,
        start_date,
        startstation_id,
        startstation_name

from source
where rn = 1
