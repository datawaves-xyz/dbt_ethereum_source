{{
    config(
        materialized='table',
        file_format='parquet',
        alias='shortnameclaims_call_ratifyclaims'
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
where to_address = lower("0xf7c83bd0c50e7a72b55a39fe0dabf5e3a330d749") and address_hash = abs(hash(lower("0xf7c83bd0c50e7a72b55a39fe0dabf5e3a330d749"))) % 10 and selector = "0x84fd49c9" and selector_hash = abs(hash("0x84fd49c9")) % 10

{% if is_incremental() %}
  and dt = '{{ var("dt") }}'
{% endif %}
