{{
    config(
        materialized='table',
        file_format='parquet',
        alias='linkusdswap_call_ramp_a',
        pre_hook={
            'sql': 'create or replace function curve_linkusdswap_ramp_a_calldecodeudf as "io.iftech.sparkudf.hive.Curve_LinkUSDSwap_ramp_A_CallDecodeUDF" using jar "s3a://blockchain-dbt/dist/jars/blockchain-dbt-udf-0.1.15.jar";'
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
        curve_linkusdswap_ramp_a_calldecodeudf(unhex_input, unhex_output, '{"type": "function", "name": "ramp_A", "stateMutability": "nonpayable", "inputs": [{"name": "_future_A", "type": "uint256"}, {"name": "_future_time", "type": "uint256"}], "outputs": []}', 'ramp_A') as data
    from {{ ref('stg_traces') }}
    where to_address = lower("0xe7a24ef0c5e95ffb0f6684b813a78f2a3ad7d171") and address_hash = abs(hash(lower("0xe7a24ef0c5e95ffb0f6684b813a78f2a3ad7d171"))) % 10 and selector = "0x3c157e64" and selector_hash = abs(hash("0x3c157e64")) % 10

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
