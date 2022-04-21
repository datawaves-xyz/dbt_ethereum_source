select /* REPARTITION(dt) */
    status==1 as call_success,
    block_number as call_block_number,
    block_timestamp as call_block_time,
    trace_address as call_trace_address,
    transaction_hash as call_tx_hash,
    to_address as contract_address,
    dt
from {{ ref('stg_ethereum__traces') }}
where to_address = lower("0x9c4e9cce4780062942a7fe34fa2fa7316c872956")
and address_hash = abs(hash(lower("0x9c4e9cce4780062942a7fe34fa2fa7316c872956"))) % 10
and selector = "0x30783731353031386136"
and selector_hash = abs(hash("0x30783731353031386136")) % 10

{% if is_incremental() %}
  and dt = var('dt')
{% endif %}