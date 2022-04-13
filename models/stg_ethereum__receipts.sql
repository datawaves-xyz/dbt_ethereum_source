with base as (
  select * 
  from {{ var('receipts') }}
),

final as (
    select *
    from base
)

select * 
from final