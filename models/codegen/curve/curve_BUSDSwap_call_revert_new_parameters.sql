{{
    config(
        materialized='table',
        file_format='parquet',
        alias='busdswap_call_revert_new_parameters'
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
where to_address = lower("0x79a8C46DeA5aDa233ABaFFD40F3A0A2B1e5A4F27")
and address_hash = abs(hash(lower("0x79a8C46DeA5aDa233ABaFFD40F3A0A2B1e5A4F27"))) % 10
and selector = "0x226840fb"
and selector_hash = abs(hash("0x226840fb")) % 10

{% if is_incremental() %}
  and dt = '{{ var("dt") }}'
{% endif %}
