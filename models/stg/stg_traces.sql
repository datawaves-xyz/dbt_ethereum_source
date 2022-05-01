{{
    config(
        alias='traces'
    )
}}

with base as (
  select * 
  from {{ var('traces') }}
),

final as (
    select *
    from base
)

select * 
from final