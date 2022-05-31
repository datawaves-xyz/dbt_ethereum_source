{{
    config(
        materialized='table',
        file_format='parquet',
        alias='metamaskswap_call_swap',
        pre_hook={
            'sql': 'create or replace function metamask_metamaskswap_swap_calldecodeudf as "io.iftech.sparkudf.hive.Metamask_MetamaskSwap_swap_CallDecodeUDF" using jar "s3a://blockchain-dbt/dist/jars/blockchain-dbt-udf-0.1.15.jar";'
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
        metamask_metamaskswap_swap_calldecodeudf(unhex_input, unhex_output, '{"type": "function", "name": "swap", "constant": false, "payable": false, "stateMutability": "nonpayable", "inputs": [{"name": "router", "type": "string"}, {"name": "token", "type": "address"}, {"name": "amount", "type": "uint256"}, {"name": "calldata", "type": "bytes"}], "outputs": []}', 'swap') as data
    from {{ ref('stg_traces') }}
    where to_address = lower("0x881D40237659C251811CEC9c364ef91dC08D300C") and address_hash = abs(hash(lower("0x881D40237659C251811CEC9c364ef91dC08D300C"))) % 10 and selector = "0x5f575529" and selector_hash = abs(hash("0x5f575529")) % 10

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
        data.input.router as router, data.input.token as token, data.input.amount as amount, data.input.calldata as calldata
    from base
)

select /*+ REPARTITION(50) */ *
from final
