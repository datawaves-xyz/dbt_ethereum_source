select 
  *
from {{ ref('common_ERC20_evt_Approval') }}
where owner is not null
and spender is not null
and value is not null
