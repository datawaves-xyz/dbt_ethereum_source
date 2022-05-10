{{
    config(
        materialized='table',
        file_format='parquet',
        alias='ethregistrarcontroller3_call_withdraw'
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
where to_address = lower("0x283Af0B28c62C092C9727F1Ee09c02CA627EB7F5") and address_hash = abs(hash(lower("0x283Af0B28c62C092C9727F1Ee09c02CA627EB7F5"))) % 10 and selector = "0x3ccfd60b" and selector_hash = abs(hash("0x3ccfd60b")) % 10

{% if is_incremental() %}
  and dt = '{{ var("dt") }}'
{% endif %}
