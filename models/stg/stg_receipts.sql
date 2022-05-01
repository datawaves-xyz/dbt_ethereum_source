{{
    config(
        alias='receipts'
    )
}}

with base as (
  select * 
  from ethereum.receipts
),

final as (
    select *
    from base
)

select * 
from final