with base as (
  select * 
  from {{ var('tokens') }}
),

final as (
    tokens.address as contract_address,
    tokens.symbol,
    tokens.name,
    cast(tokens.decimals as {{ dbt_utils.type_int() }}) as decimals,
    cast(tokens.total_supply as {{ dbt_utils.type_int() }}) as total_supply
)

select * 
from final