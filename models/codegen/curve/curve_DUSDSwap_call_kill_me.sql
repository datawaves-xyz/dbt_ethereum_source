{{
    config(
        materialized='table',
        file_format='parquet',
        alias='dusdswap_call_kill_me'
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
where to_address = lower("0x8038C01A0390a8c547446a0b2c18fc9aEFEcc10c") and address_hash = abs(hash(lower("0x8038C01A0390a8c547446a0b2c18fc9aEFEcc10c"))) % 10 and selector = "0xe3698853" and selector_hash = abs(hash("0xe3698853")) % 10

{% if is_incremental() %}
  and dt = '{{ var("dt") }}'
{% endif %}
