{{
    config(
        materialized='table',
        file_format='parquet',
        alias='busdswap_call_remove_liquidity_imbalance',
        pre_hook={
            'sql': 'create or replace function curve_busdswap_remove_liquidity_imbalance_calldecodeudf as "io.iftech.sparkudf.hive.Curve_BUSDSwap_remove_liquidity_imbalance_CallDecodeUDF" using jar "s3a://blockchain-dbt/dist/jars/blockchain-dbt-udf-0.1.16.jar";'
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
        curve_busdswap_remove_liquidity_imbalance_calldecodeudf(unhex_input, unhex_output, '{"type": "function", "name": "remove_liquidity_imbalance", "constant": false, "payable": false, "inputs": [{"name": "amounts", "type": "uint256[4]"}, {"name": "max_burn_amount", "type": "uint256"}], "outputs": []}', 'remove_liquidity_imbalance') as data
    from {{ ref('stg_traces') }}
    where to_address = lower("0x79a8C46DeA5aDa233ABaFFD40F3A0A2B1e5A4F27") and address_hash = abs(hash(lower("0x79a8C46DeA5aDa233ABaFFD40F3A0A2B1e5A4F27"))) % 10 and selector = "0x18a7bd76" and selector_hash = abs(hash("0x18a7bd76")) % 10

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
        data.input.amounts as amounts, data.input.max_burn_amount as max_burn_amount
    from base
)

select /*+ REPARTITION(50) */ *
from final
