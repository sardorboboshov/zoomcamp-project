with 

source as (

    select * from {{ source('staging', 'ActiveTravelCountsProgramme') }}

),

renamed as (

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

)

select * from renamed
