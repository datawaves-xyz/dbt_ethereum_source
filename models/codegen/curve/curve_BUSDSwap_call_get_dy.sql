{{
    config(
        materialized='table',
        file_format='parquet',
        alias='busdswap_call_get_dy',
        pre_hook={
            'sql': 'create or replace function curve_busdswap_get_dy_calldecodeudf as "io.iftech.sparkudf.hive.Curve_BUSDSwap_get_dy_CallDecodeUDF" using jar "s3a://blockchain-dbt/dist/jars/blockchain-dbt-udf-0.1.16.jar";'
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
        curve_busdswap_get_dy_calldecodeudf(unhex_input, unhex_output, '{"type": "function", "name": "get_dy", "constant": true, "payable": false, "inputs": [{"name": "i", "type": "int128"}, {"name": "j", "type": "int128"}, {"name": "dx", "type": "uint256"}], "outputs": [{"name": "out", "type": "uint256"}]}', 'get_dy') as data
    from {{ ref('stg_traces') }}
    where to_address = lower("0x79a8C46DeA5aDa233ABaFFD40F3A0A2B1e5A4F27") and address_hash = abs(hash(lower("0x79a8C46DeA5aDa233ABaFFD40F3A0A2B1e5A4F27"))) % 10 and selector = "0x5e0d443f" and selector_hash = abs(hash("0x5e0d443f")) % 10

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
        data.input.i as i, data.input.j as j, data.input.dx as dx, data.output.output_out as output_out
    from base
)

select /*+ REPARTITION(50) */ *
from final
