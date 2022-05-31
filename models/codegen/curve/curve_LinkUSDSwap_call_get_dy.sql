{{
    config(
        materialized='table',
        file_format='parquet',
        alias='linkusdswap_call_get_dy',
        pre_hook={
            'sql': 'create or replace function curve_linkusdswap_get_dy_calldecodeudf as "io.iftech.sparkudf.hive.Curve_LinkUSDSwap_get_dy_CallDecodeUDF" using jar "s3a://blockchain-dbt/dist/jars/blockchain-dbt-udf-0.1.15.jar";'
        }
    )
}}

with base as (
    select
        status==1 as call_success,
        block_number as call_block_number,
        block_timestamp as call_block_time,
        trace_address as call_trace_address,
        transaction_hash as call_tx_hash,
        to_address as contract_address,
        dt,
        curve_linkusdswap_get_dy_calldecodeudf(unhex_input, unhex_output, '{"type": "function", "name": "get_dy", "stateMutability": "view", "inputs": [{"name": "i", "type": "int128"}, {"name": "j", "type": "int128"}, {"name": "dx", "type": "uint256"}], "outputs": [{"name": "", "type": "uint256"}]}', 'get_dy') as data
    from {{ ref('stg_traces') }}
    where to_address = lower("0xe7a24ef0c5e95ffb0f6684b813a78f2a3ad7d171") and address_hash = abs(hash(lower("0xe7a24ef0c5e95ffb0f6684b813a78f2a3ad7d171"))) % 10 and selector = "0x5e0d443f" and selector_hash = abs(hash("0x5e0d443f")) % 10

    {% if is_incremental() %}
      and dt = '{{ var("dt") }}'
    {% endif %}
),

final as (
    select
        call_success,
        call_block_number,
        call_block_time,
        call_trace_address,
        call_tx_hash,
        contract_address,
        dt,
        data.input.i as i, data.input.j as j, data.input.dx as dx, data.output.output_0 as output_0
    from base
)

select /*+ REPARTITION(50) */ *
from final
