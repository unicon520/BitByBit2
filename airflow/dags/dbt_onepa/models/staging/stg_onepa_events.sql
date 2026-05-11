{{ config(materialized='view') }}

with source as (
    select * from {{ source('raw', 'onepa_bronze') }}
),

renamed as (
    select
        event_id,
        title,
        url,
        outlet,
        start_date,
        session_time,
        -- Ensure registration_open is properly cast to boolean
        cast(registration_open as boolean) as registration_open,
        coalesce(min_price, 0.0) as min_price,
        coalesce(max_price, 0.0) as max_price,
        source
    from source
    where title is not null
)

select * from renamed
