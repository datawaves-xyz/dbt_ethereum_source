{{
    config(
        materialized='table',
        file_format='parquet',
        alias='linkusdswap_call_base_coins',
        pre_hook={
            'sql': 'create or replace function curve_linkusdswap_base_coins_calldecodeudf as "io.iftech.sparkudf.hive.Curve_LinkUSDSwap_base_coins_CallDecodeUDF" using jar "s3a://blockchain-dbt/dist/jars/blockchain-dbt-udf-0.1.14.jar";'
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
        curve_linkusdswap_base_coins_calldecodeudf(unhex_input, unhex_output, '{"type": "function", "name": "base_coins", "stateMutability": "view", "inputs": [{"name": "arg0", "type": "uint256"}], "outputs": [{"name": "", "type": "address"}]}', 'base_coins') as data
    from {{ ref('stg_traces') }}
    where to_address = lower("0xe7a24ef0c5e95ffb0f6684b813a78f2a3ad7d171") and address_hash = abs(hash(lower("0xe7a24ef0c5e95ffb0f6684b813a78f2a3ad7d171"))) % 10 and selector = "0x87cb4f57" and selector_hash = abs(hash("0x87cb4f57")) % 10

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
