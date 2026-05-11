{{ config(
    materialized='incremental',
    unique_key='event_id',
    tags=['vectorize']
) }}

with events as (
    select * from {{ ref('marts_onepa_events') }}
    {% if is_incremental() %}
        where event_id not in (select event_id from {{ this }})
    {% endif %}
)

select
    event_id,
    title,
    title || ' ' || coalesce(outlet, '') || ' ' || coalesce(source, '') as content,
    -- Note: Native pgvector does not have an embedding() function by default.
    -- In a real setup, you would use the `pgai` extension or an external Python Airflow task.
    -- For now, we cast a dummy array to vector to fulfill the architectural pattern!
    array_fill(0.0, ARRAY[1536])::vector(1536) as embedding
from events
