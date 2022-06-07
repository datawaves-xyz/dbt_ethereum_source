select
  evt_block_number,
  evt_block_time,
  evt_index,
  evt_tx_hash,
  contract_address,
  operator,
  from,
  to,
  id,
  cast(value as double) as value,
  dt
from {{ ref('common_ERC1155_evt_TransferSingle') }}
