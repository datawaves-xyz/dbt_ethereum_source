{{
    config(
        materialized='table',
        file_format='parquet',
        alias='gusdswap_call_add_liquidity',
        pre_hook={
            'sql': 'create or replace function curve_gusdswap_add_liquidity_calldecodeudf as "io.iftech.sparkudf.hive.Curve_gUSDSwap_add_liquidity_CallDecodeUDF" using jar "s3a://blockchain-dbt/dist/jars/blockchain-dbt-udf-0.1.14.jar";'
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
        curve_gusdswap_add_liquidity_calldecodeudf(unhex_input, unhex_output, '{"type": "function", "name": "add_liquidity", "stateMutability": "nonpayable", "inputs": [{"name": "amounts", "type": "uint256[2]"}, {"name": "min_mint_amount", "type": "uint256"}], "outputs": [{"name": "", "type": "uint256"}]}', 'add_liquidity') as data
    from {{ ref('stg_traces') }}
    where to_address = lower("0x4f062658EaAF2C1ccf8C8e36D6824CDf41167956") and address_hash = abs(hash(lower("0x4f062658EaAF2C1ccf8C8e36D6824CDf41167956"))) % 10 and selector = "0x0b4c7e4d" and selector_hash = abs(hash("0x0b4c7e4d")) % 10

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
        data.input.amounts as amounts, data.input.min_mint_amount as min_mint_amount, data.output.output_0 as output_0
    from base
)

select /*+ REPARTITION(50) */ *
from final
