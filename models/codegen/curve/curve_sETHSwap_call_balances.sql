{{
    config(
        materialized='table',
        file_format='parquet',
        alias='sethswap_call_balances',
        pre_hook={
            'sql': 'create or replace function curve_sethswap_balances_calldecodeudf as "io.iftech.sparkudf.hive.Curve_sETHSwap_balances_CallDecodeUDF" using jar "s3a://blockchain-dbt/dist/jars/blockchain-dbt-udf-0.1.13.jar";'
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
        curve_sethswap_balances_calldecodeudf(unhex_input, unhex_output, '{"type": "function", "name": "balances", "stateMutability": "view", "inputs": [{"name": "arg0", "type": "uint256"}], "outputs": [{"name": "", "type": "uint256"}]}', 'balances') as data
    from {{ ref('stg_traces') }}
    where to_address = lower("0xc5424b857f758e906013f3555dad202e4bdb4567") and address_hash = abs(hash(lower("0xc5424b857f758e906013f3555dad202e4bdb4567"))) % 10 and selector = "0x4903b0d1" and selector_hash = abs(hash("0x4903b0d1")) % 10

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
