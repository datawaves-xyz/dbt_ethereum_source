{{
    config(
        materialized='table',
        file_format='parquet',
        alias='dusdswap_call_get_dy_underlying',
        pre_hook={
            'sql': 'create or replace function curve_dusdswap_get_dy_underlying_calldecodeudf as "io.iftech.sparkudf.hive.Curve_DUSDSwap_get_dy_underlying_CallDecodeUDF" using jar "s3a://blockchain-dbt/dist/jars/blockchain-dbt-udf-0.1.13.jar";'
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
        curve_dusdswap_get_dy_underlying_calldecodeudf(unhex_input, unhex_output, '{"type": "function", "name": "get_dy_underlying", "stateMutability": "view", "inputs": [{"name": "i", "type": "int128"}, {"name": "j", "type": "int128"}, {"name": "dx", "type": "uint256"}], "outputs": [{"name": "", "type": "uint256"}]}', 'get_dy_underlying') as data
    from {{ ref('stg_traces') }}
    where to_address = lower("0x8038C01A0390a8c547446a0b2c18fc9aEFEcc10c") and address_hash = abs(hash(lower("0x8038C01A0390a8c547446a0b2c18fc9aEFEcc10c"))) % 10 and selector = "0x07211ef7" and selector_hash = abs(hash("0x07211ef7")) % 10

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
        data.input.*,
        data.output.*
    from base
)

select /*+ REPARTITION(50) */ *
from final
