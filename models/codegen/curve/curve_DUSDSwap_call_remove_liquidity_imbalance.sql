{{
    config(
        materialized='table',
        file_format='parquet',
        alias='dusdswap_call_remove_liquidity_imbalance',
        pre_hook={
            'sql': 'create or replace function curve_dusdswap_remove_liquidity_imbalance_calldecodeudf as "io.iftech.sparkudf.hive.Curve_DUSDSwap_remove_liquidity_imbalance_CallDecodeUDF" using jar "s3a://blockchain-dbt/dist/jars/blockchain-dbt-udf-0.1.10.jar";'
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
        curve_dusdswap_remove_liquidity_imbalance_calldecodeudf(unhex_input, unhex_output, '{"name": "remove_liquidity_imbalance", "outputs": [{"type": "uint256", "name": ""}], "inputs": [{"type": "uint256[2]", "name": "amounts"}, {"type": "uint256", "name": "max_burn_amount"}], "stateMutability": "nonpayable", "type": "function", "gas": 6136334}', 'remove_liquidity_imbalance') as data
    from {{ ref('stg_traces') }}
    where to_address = lower("0x8038C01A0390a8c547446a0b2c18fc9aEFEcc10c") and address_hash = abs(hash(lower("0x8038C01A0390a8c547446a0b2c18fc9aEFEcc10c"))) % 10 and selector = "0xe3103273" and selector_hash = abs(hash("0xe3103273")) % 10

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
