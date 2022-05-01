{{
    config(
        materialized='table',
        file_format='parquet',
        alias='linkusdswap_call_unkill_me'
    )
}}

select /* REPARTITION(dt) */
    status==1 as call_success,
    block_number as call_block_number,
    block_timestamp as call_block_time,
    trace_address as call_trace_address,
    transaction_hash as call_tx_hash,
    to_address as contract_address,
    dt
from {{ ref('stg_ethereum__traces') }}
where to_address = lower("0xe7a24ef0c5e95ffb0f6684b813a78f2a3ad7d171")
and address_hash = abs(hash(lower("0xe7a24ef0c5e95ffb0f6684b813a78f2a3ad7d171"))) % 10
and selector = "0x30783330343666393732"
and selector_hash = abs(hash("0x30783330343666393732")) % 10

{% if is_incremental() %}
  and dt = '{{ var("dt") }}'
{% endif %}
