{{
    config(
        materialized='table',
        file_format='parquet',
        alias='cryptopunksmarket_call_withdraw'
    )
}}

select /*+ REPARTITION(50) */
    status==1 as call_success,
    block_number as call_block_number,
    block_timestamp as call_block_time,
    trace_address as call_trace_address,
    transaction_hash as call_tx_hash,
    to_address as contract_address,
    dt
from {{ ref('stg_traces') }}
where to_address = lower("0xb47e3cd837dDF8e4c57F05d70Ab865de6e193BBB") and address_hash = abs(hash(lower("0xb47e3cd837dDF8e4c57F05d70Ab865de6e193BBB"))) % 10 and selector = "0x3ccfd60b" and selector_hash = abs(hash("0x3ccfd60b")) % 10

{% if is_incremental() %}
  and dt = '{{ var("dt") }}'
{% endif %}
