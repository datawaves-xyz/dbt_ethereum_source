{{
    config(
        materialized='table',
        file_format='parquet',
        alias='sethswap_call_withdraw_admin_fees'
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
where to_address = lower("0xc5424b857f758e906013f3555dad202e4bdb4567") and address_hash = abs(hash(lower("0xc5424b857f758e906013f3555dad202e4bdb4567"))) % 10 and selector = "0x30c54085" and selector_hash = abs(hash("0x30c54085")) % 10

{% if is_incremental() %}
  and dt = '{{ var("dt") }}'
{% endif %}
