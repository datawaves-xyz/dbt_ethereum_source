{{
    config(
        materialized='table',
        file_format='parquet',
        alias='husdswap_call_revert_transfer_ownership'
    )
}}

select /*+ REPARTITION(1) */
    status==1 as call_success,
    block_number as call_block_number,
    block_timestamp as call_block_time,
    trace_address as call_trace_address,
    transaction_hash as call_tx_hash,
    to_address as contract_address,
    dt
from {{ ref('stg_traces') }}
where to_address = lower("0x3eF6A01A0f81D6046290f3e2A8c5b843e738E604")
and address_hash = abs(hash(lower("0x3eF6A01A0f81D6046290f3e2A8c5b843e738E604"))) % 10
and selector = "0x86fbf193"
and selector_hash = abs(hash("0x86fbf193")) % 10

{% if is_incremental() %}
  and dt = '{{ var("dt") }}'
{% endif %}
