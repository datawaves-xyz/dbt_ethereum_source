{{
    config(
        materialized='table',
        file_format='parquet',
        alias='yearngovernance_call_updatefees'
    )
}}

select /* REPARTITION(1) */
    status==1 as call_success,
    block_number as call_block_number,
    block_timestamp as call_block_time,
    trace_address as call_trace_address,
    transaction_hash as call_tx_hash,
    to_address as contract_address,
    dt
from {{ ref('stg_traces') }}
where to_address = lower("0x3A22dF48d84957F907e67F4313E3D43179040d6E")
and address_hash = abs(hash(lower("0x3A22dF48d84957F907e67F4313E3D43179040d6E"))) % 10
and selector = "0xfc9fc6c7"
and selector_hash = abs(hash("0xfc9fc6c7")) % 10

{% if is_incremental() %}
  and dt = '{{ var("dt") }}'
{% endif %}
