{{
    config(
        materialized='table',
        file_format='parquet',
        alias='gusdswap_call_ramp_a',
        pre_hook={
            'sql': 'create or replace function curve_gusdswap_ramp_a_calldecodeudf as "io.iftech.sparkudf.hive.Curve_gUSDSwap_ramp_A_CallDecodeUDF" using jar "s3a://blockchain-dbt/dist/jars/blockchain-dbt-udf-0.1.16.jar";'
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
        curve_gusdswap_ramp_a_calldecodeudf(unhex_input, unhex_output, '{"type": "function", "name": "ramp_A", "stateMutability": "nonpayable", "inputs": [{"name": "_future_A", "type": "uint256"}, {"name": "_future_time", "type": "uint256"}], "outputs": []}', 'ramp_A') as data
    from {{ ref('stg_traces') }}
    where to_address = lower("0x4f062658EaAF2C1ccf8C8e36D6824CDf41167956") and address_hash = abs(hash(lower("0x4f062658EaAF2C1ccf8C8e36D6824CDf41167956"))) % 10 and selector = "0x3c157e64" and selector_hash = abs(hash("0x3c157e64")) % 10

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
        data.input._future_a as _future_A, data.input._future_time as _future_time
    from base
)

select /*+ REPARTITION(50) */ *
from final
