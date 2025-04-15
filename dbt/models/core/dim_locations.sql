{{
    config(
        materialized='table'
    )
}}

select
    *
from {{ ref('locations_lookup') }}