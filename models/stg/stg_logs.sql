{{
    config(
        alias='logs'
    )
}}

with base as (
  select * 
  from {{ var('logs') }}
),

final as (
    select *
    from base
)

select * 
from final