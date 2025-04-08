with 

source as (

    select * from {{ source('staging', 'usage-stats') }}

),

renamed as (

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

)

select * from renamed
