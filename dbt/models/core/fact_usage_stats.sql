{{
    config(
        materialized='table'
    )
}}

select 
    *
from {{ ref('stg_usage-stats') }}