{{
    config(
        materialized='table',
        file_format='parquet',
        alias='baseregistrarimplementation_call_renounceownership'
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
where to_address = lower("0x57f1887a8BF19b14fC0dF6Fd9B2acc9Af147eA85") and address_hash = abs(hash(lower("0x57f1887a8BF19b14fC0dF6Fd9B2acc9Af147eA85"))) % 10 and selector = "0x715018a6" and selector_hash = abs(hash("0x715018a6")) % 10

{% if is_incremental() %}
  and dt = '{{ var("dt") }}'
{% endif %}
