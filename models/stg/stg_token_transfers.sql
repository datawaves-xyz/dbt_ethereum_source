{{
    config(
        alias='token_transfers'
    )
}}

with base as (
  select * 
  from ethereum.token_transfers
),

final as (
    select *
    from base
)

select * 
from final