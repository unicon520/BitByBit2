{{ config(materialized='table') }}

with staging as (
    select * from {{ ref('stg_onepa_events') }}
)

select
    event_id,
    title,
    url,
    outlet,
    start_date,
    session_time,
    registration_open,
    min_price,
    max_price,
    source
from staging
